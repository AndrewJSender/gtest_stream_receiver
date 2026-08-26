#!/usr/bin/env python3

# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.


from enum import StrEnum
from time import monotonic
from typing import Any, Dict, List
from zipfile import Path
import json

from result_store import ResultStore 


# Every "event=" in
# https://github.com/google/googletest/blob/3ff51c3e80f2c2eb105d43ecb9acab9a62e01600/googletest/src/gtest-internal-inl.h
class EventType(StrEnum):
        TEST_PROGRAM_START = "testprogramstart"
        TEST_PROGRAM_END = "testprogramend"
        TEST_CASE_START = "testcasestart"
        TEST_CASE_END = "testcaseend"
        PROTOCOL_VERSION = "gtest_streaming_protocol_version"
        TEST_START = "teststart"
        TEST_END = "testend"
        TEST_ITERATION_START = "testiterationstart"
        TEST_ITERATION_END = "testiterationend"
        TEST_PART_RESULT = "testpartresult"


def process_event(event: Dict[str, Any]) -> None:
    # self._stats.total_events += 1
    # self._stats.last_event_time = event["received_at"]

    params = event.get("params", {})
    event_name = str(params.get("event", "")).lower()
    status = self._extract_status(params)
    test_name = self._extract_test_name(params)
    duration_from_payload = self._extract_duration_seconds(params)
    now_mono = event.get("monotonic", monotonic())
    output_lines: List[str] = []

    match event_name:
        case EventType.TEST_PROGRAM_START:
            self._stats.test_program_start += 1
        case EventType.TEST_PROGRAM_END:
            self._stats.test_program_end += 1
        case EventType.TEST_CASE_START:
            self._current_case_name = self._extract_case_name(params)
            if self._current_case_name:
                expected_count = self._extract_test_count(params)
                self._suite_output_state[self._current_case_name] = SuiteOutputState(expected_count=expected_count)

                if self._has_output_content:
                    output_lines.append("")
                self._has_output_content = True

                header_count = str(expected_count) if expected_count is not None else "?"
                noun = "test" if expected_count == 1 else "tests"
                output_lines.append(f"[----------] {header_count} {noun} from {self._current_case_name}")
        case EventType.TEST_CASE_END:
            ended_case = self._extract_case_name(params) or self._current_case_name
            if ended_case:
                suite_state = self._suite_output_state.get(ended_case)
                if suite_state:
                    noun = "test" if suite_state.finished_count == 1 else "tests"
                    output_lines.append(
                        f"[----------] {suite_state.finished_count} {noun} from {ended_case} "
                        f"({suite_state.total_ms} ms total)"
                    )
                
            self._current_case_name = None
        case EventType.TEST_START:
            self._stats.test_start += 1
            self._current_test_name = test_name
            test_name = self._normalize_test_name(test_name)
            if test_name:
                suite_name, _ = self._split_test_name(test_name)
                if suite_name not in self._suite_output_state:
                    self._suite_output_state[suite_name] = SuiteOutputState(expected_count=None)
                    if self._has_output_content:
                        output_lines.append("")
                    self._has_output_content = True
                    output_lines.append(f"[----------] ? tests from {suite_name}")

                state = SessionTestState(name=test_name)
                state.started_mono = now_mono
                self._tests.append(state)
                self._started_tests.append(state)
                output_lines.append(f"[ RUN      ] {state.name}")
        case EventType.TEST_END:
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

                duration_ms = self._duration_to_ms(state.duration_seconds)
                if status in {"success", "passed", "pass", "ok"}:
                    output_lines.append(f"[       OK ] {state.name} ({duration_ms} ms)")
                else:
                    output_lines.append(f"[  FAILED  ] {state.name} ({duration_ms} ms)")

                suite_name, _ = self._split_test_name(state.name)
                suite_state = self._suite_output_state.setdefault(suite_name, SuiteOutputState())
                suite_state.finished_count += 1
                suite_state.total_ms += duration_ms
        case EventType.TEST_PART_RESULT:
            file_path = str(params.get("file", "")).strip()
            line_no = str(params.get("line", "")).strip()
            message = str(params.get("message", "")).rstrip()

            if file_path:
                if line_no:
                    output_lines.append(f"{file_path}:{line_no}: Failure")
                else:
                    output_lines.append(f"{file_path}: Failure")
            else:
                output_lines.append("Failure")
            if message:
                output_lines.extend(message.splitlines())
        case EventType.TEST_ITERATION_START:
            pass
        case EventType.TEST_ITERATION_END:
            pass
        case _:
            if "params" in event and EventType.PROTOCOL_VERSION.value in event["params"]:
                pass
            else:
                self._stats.unknown_events += 1

    if event_name in {"testend", "test_end"}:
        if status in {"success", "passed", "pass", "ok"}:
            self._stats.test_pass += 1
        elif status in {"failure", "failed", "fail", "notrun", "timeout"}:
            self._stats.test_fail += 1

    if output_lines:
        self._append_output_lines(output_lines)

def finalize_session() -> Dict[str, Any]:
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

def _append_output_lines(self, lines: List[str]) -> None:
    if not self.output_file:
        return
    with self.output_file.open("a", encoding="utf-8") as file_handle:
        for line in lines:
            file_handle.write(line + "\n")

def _write_gtest_output(self) -> None:
    suites: Dict[str, List[SessionTestState]] = {}

    for state in self._finished_tests:
        suite_name, _ = self._split_test_name(state.name)
        suites.setdefault(suite_name, []).append(state)

    lines: List[str] = []
    first_suite = True
    for suite_name, tests in suites.items():
        if not first_suite:
            lines.append("")
        first_suite = False

        lines.append(f"[----------] {len(tests)} test{'s' if len(tests) != 1 else ''} from {suite_name}")

        suite_total_ms = 0
        for state in tests:
            status = (state.status or "").lower()
            duration_ms = self._duration_to_ms(state.duration_seconds)
            suite_total_ms += duration_ms

            lines.append(f"[ RUN      ] {state.name}")
            if status in {"success", "passed", "pass", "ok"}:
                lines.append(f"[       OK ] {state.name} ({duration_ms} ms)")
            else:
                lines.append(f"[  FAILED  ] {state.name} ({duration_ms} ms)")

        lines.append(
            f"[----------] {len(tests)} test{'s' if len(tests) != 1 else ''} "
            f"from {suite_name} ({suite_total_ms} ms total)"
        )

    output_text = "\n".join(lines)
    if output_text:
        output_text += "\n"
    self.output_file.write_text(output_text, encoding="utf-8")

@staticmethod
def _split_test_name(test_name: str) -> tuple[str, str]:
    if "." in test_name:
        suite_name, case_name = test_name.split(".", 1)
        return suite_name, case_name
    return "UnknownTestSuite", test_name

@staticmethod
def _duration_to_ms(duration_seconds: float | None) -> int:
    if duration_seconds is None:
        return 0
    return int(round(duration_seconds * 1000.0))

@staticmethod
def _extract_test_count(params: Dict[str, Any]) -> int | None:
    for key in ("tests", "test_count", "total_test_count"):
        if key not in params:
            continue
        try:
            value = int(str(params[key]).strip())
            if value >= 0:
                return value
        except ValueError:
            continue
    return None

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
    if self.sqlite_conn:
        sql.append_sql_summary(self.sqlite_conn, asdict(self._stats))
    if not self.summary_file:
        return
    self.summary_file.write_text(
        json.dumps(asdict(self._stats), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

def parse_args() -> Any:
    import argparse

    parser = argparse.ArgumentParser(description="Process GoogleTest streaming events.")
    parser.add_argument("--jsonl-path", help="Path to the JSONL file containing events.")
    parser.add_argument("--db-path", help="Path to the SQLite database file (optional).")
    parser.add_argument("--summary-json-path", help="Path to the summary json file.")
    return parser.parse_args()

def main() -> None:
    import json

    args = parse_args()

    with open(args.jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            event = json.loads(line)
            process_event(event)

    summary = finalize_session()
    with open(args.summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()