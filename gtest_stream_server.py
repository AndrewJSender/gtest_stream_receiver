#!/usr/bin/env python3
"""Simple receiver for GoogleTest streaming output.

GoogleTest can stream test progress/results to a web server using:
    GTEST_OUTPUT="stream_result_to=HOST:PORT"

This server listens for those HTTP requests, records each event, and writes
an append-only JSONL log plus a rolling summary.
"""

from __future__ import annotations

import argparse
import json
import re
import socketserver
import threading
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Dict, List
from urllib.parse import parse_qs


@dataclass
class StreamStats:
    session_id: int = 0
    client: str = ""
    connection_started_at: str = ""
    connection_ended_at: str | None = None
    total_events: int = 0
    test_program_start: int = 0
    test_program_end: int = 0
    test_start: int = 0
    test_end: int = 0
    test_pass: int = 0
    test_fail: int = 0
    test_unfinished: int = 0
    unknown_events: int = 0
    last_event_time: str | None = None
    passed_tests: List[Dict[str, Any]] = field(default_factory=list)
    failed_tests: List[Dict[str, Any]] = field(default_factory=list)
    unfinished_tests: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SessionTestState:
    name: str
    started_mono: float | None = None
    ended_mono: float | None = None
    duration_seconds: float | None = None
    status: str | None = None


class ResultStore:
    def __init__(self, out_dir: Path, session_id: int, client: str, jsonl_name: str = "events.jsonl") -> None:
        self._lock = threading.Lock()
        safe_client = re.sub(r"[^0-9A-Za-z_.-]", "_", client)
        session_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.out_dir = out_dir / f"session_{session_id:06d}_{session_time}_{safe_client}"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.events_file = self.out_dir / jsonl_name
        self.summary_file = self.out_dir / "summary.json"

        self._stats = StreamStats(
            session_id=session_id,
            client=client,
            connection_started_at=_utc_now(),
        )
        self._tests: List[SessionTestState] = []
        self._started_tests: List[SessionTestState] = []
        self._finished_tests: List[SessionTestState] = []
        self._current_case_name: str | None = None

    def append_event(self, event: Dict[str, Any]) -> None:
        with self._lock:
            with self.events_file.open("a", encoding="utf-8") as file_handle:
                file_handle.write(json.dumps(event, ensure_ascii=False) + "\n")

            self._update_stats(event)

    def _update_stats(self, event: Dict[str, Any]) -> None:
        self._stats.total_events += 1
        self._stats.last_event_time = event["received_at"]

        params = event.get("params", {})
        event_name = str(params.get("event", "")).lower()
        status = self._extract_status(params)
        test_name = self._extract_test_name(params)
        duration_from_payload = self._extract_duration_seconds(params)
        now_mono = event.get("monotonic", monotonic())

        if event_name in {"testprogramstart", "test_program_start"}:
            self._stats.test_program_start += 1
        elif event_name in {"testprogramend", "test_program_end"}:
            self._stats.test_program_end += 1
        elif event_name in {"testcasestart", "test_case_start"}:
            self._current_case_name = self._extract_case_name(params)
        elif event_name in {"testcaseend", "test_case_end"}:
            self._current_case_name = None
        elif event_name in {"teststart", "test_start"}:
            self._stats.test_start += 1
            test_name = self._normalize_test_name(test_name)
            if test_name:
                state = SessionTestState(name=test_name)
                state.started_mono = now_mono
                self._tests.append(state)
                self._started_tests.append(state)
        elif event_name in {"testend", "test_end"}:
            self._stats.test_end += 1
            state = self._match_test_end_state(test_name)
            if state:
                if duration_from_payload is not None:
                    state.duration_seconds = duration_from_payload
                elif state.started_mono is not None:
                    state.duration_seconds = max(0.0, now_mono - state.started_mono)
                state.ended_mono = now_mono

                if status:
                    state.status = status
                self._finished_tests.append(state)
        else:
            self._stats.unknown_events += 1

        if event_name in {"testend", "test_end"}:
            if status in {"success", "passed", "pass", "ok"}:
                self._stats.test_pass += 1
            elif status in {"failure", "failed", "fail", "notrun", "timeout"}:
                self._stats.test_fail += 1

    def finalize_session(self) -> Dict[str, Any]:
        with self._lock:
            self._stats.connection_ended_at = _utc_now()
            self._stats.passed_tests = []
            self._stats.failed_tests = []
            self._stats.unfinished_tests = []

            for state in self._finished_tests:
                status = (state.status or "").lower()
                row = {
                    "name": state.name,
                    "time_used_seconds": round(state.duration_seconds, 6) if state.duration_seconds is not None else None,
                }
                if status in {"success", "passed", "pass", "ok"}:
                    self._stats.passed_tests.append(row)
                elif status in {"failure", "failed", "fail", "notrun", "timeout"}:
                    self._stats.failed_tests.append(row)

            for state in self._started_tests:
                self._stats.unfinished_tests.append(
                    {
                        "name": state.name,
                        "time_used_seconds": round(max(0.0, monotonic() - state.started_mono), 6)
                        if state.started_mono is not None
                        else None,
                    }
                )

            self._stats.test_unfinished = len(self._stats.unfinished_tests)

            self._write_summary()
            return asdict(self._stats)

    @staticmethod
    def _extract_test_name(params: Dict[str, Any]) -> str | None:
        direct_name = params.get("name") or params.get("test") or params.get("testname")
        if isinstance(direct_name, str) and direct_name.strip():
            return direct_name.strip()

        suite = params.get("testsuite") or params.get("testsuite") or params.get("testcase")
        case_name = params.get("test") or params.get("testname")
        if isinstance(suite, str) and suite and isinstance(case_name, str) and case_name:
            return f"{suite}.{case_name}"
        return None

    @staticmethod
    def _extract_case_name(params: Dict[str, Any]) -> str | None:
        name = params.get("name") or params.get("testsuite") or params.get("testcase")
        if isinstance(name, str) and name.strip():
            return name.strip()
        return None

    def _normalize_test_name(self, test_name: str | None) -> str | None:
        if not test_name:
            return None
        if "." in test_name:
            return test_name
        if self._current_case_name:
            return f"{self._current_case_name}.{test_name}"
        return test_name

    def _match_test_end_state(self, test_name: str | None) -> SessionTestState | None:
        normalized_name = self._normalize_test_name(test_name)

        if normalized_name:
            for index, state in enumerate(self._started_tests):
                if state.name == normalized_name:
                    return self._started_tests.pop(index)

        if self._started_tests:
            return self._started_tests.pop(0)
        return None

    @staticmethod
    def _extract_status(params: Dict[str, Any]) -> str:
        status = str(params.get("status", "")).strip().lower()
        if status:
            return status

        passed = str(params.get("passed", "")).strip().lower()
        if passed in {"1", "true", "yes"}:
            return "passed"
        if passed in {"0", "false", "no"}:
            return "failed"
        return ""

    @staticmethod
    def _extract_duration_seconds(params: Dict[str, Any]) -> float | None:
        keys = [
            ("elapsed_time_ms", 0.001),
            ("time_ms", 0.001),
            ("duration_ms", 0.001),
            ("elapsed_time", 1.0),
            ("time", 1.0),
            ("duration", 1.0),
        ]
        for key, scale in keys:
            if key not in params:
                continue
            raw = str(params[key]).strip().lower()
            if not raw:
                continue

            try:
                if raw.endswith("ms"):
                    return float(raw[:-2]) / 1000.0
                if raw.endswith("s"):
                    return float(raw[:-1])
                return float(raw) * scale
            except ValueError:
                continue
        return None

    def _write_summary(self) -> None:
        self.summary_file.write_text(
            json.dumps(asdict(self._stats), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _flatten_query_dict(query_dict: Dict[str, List[str]]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, values in query_dict.items():
        if not values:
            continue
        flat[key] = values[0] if len(values) == 1 else values
    return flat


class GTestTCPHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        client = f"{self.client_address[0]}:{self.client_address[1]}"
        store = self.server.create_session_store(client)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {client} - connected")

        while True:
            raw_line = self.rfile.readline()
            if not raw_line:
                break

            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue

            params = _flatten_query_dict(parse_qs(line, keep_blank_values=True))
            if not params and "=" in line:
                key, value = line.split("=", 1)
                params = {key: value}

            event = {
                "received_at": _utc_now(),
                "method": "TCP",
                "client": client,
                "params": params,
                "raw_line": line,
                "monotonic": monotonic(),
            }
            store.append_event(event)

        summary = store.finalize_session()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {client} - disconnected")
        print(
            f"[{timestamp}] {client} - summary: "
            f"passed={len(summary['passed_tests'])}, failed={len(summary['failed_tests'])}, "
            f"unfinished={len(summary['unfinished_tests'])}, events={summary['total_events']}"
        )


class GTestTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], out_dir: Path, jsonl_name: str):
        super().__init__(server_address, GTestTCPHandler)
        self.out_dir = out_dir
        self.jsonl_name = jsonl_name
        self._session_id_seq = 0
        self._session_id_lock = threading.Lock()

    def create_session_store(self, client: str) -> ResultStore:
        with self._session_id_lock:
            self._session_id_seq += 1
            session_id = self._session_id_seq
        return ResultStore(self.out_dir, session_id, client, jsonl_name=self.jsonl_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Receive and store GoogleTest streaming results")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    parser.add_argument(
        "--out-dir",
        default="./data",
        help="Directory to save output files (default: ./data)",
    )
    parser.add_argument(
        "--jsonl-name",
        default="events.jsonl",
        help="Per-session events JSONL file name (default: events.jsonl)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tcp_server = GTestTCPServer((args.host, args.port), out_dir, args.jsonl_name)
    print(f"Listening on tcp://{args.host}:{args.port}")
    print(f"Saving per-connection sessions under: {out_dir}")
    print("Each session writes: events.jsonl + summary.json")
    print("Protocol: raw TCP lines from GoogleTest stream_result_to")

    try:
        tcp_server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        tcp_server.server_close()


if __name__ == "__main__":
    main()
