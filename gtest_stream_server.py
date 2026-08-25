#!/usr/bin/env python3

# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.

"""Receiver for GoogleTest streaming output.

GoogleTest can stream test progress/results to a web server using:
    GTEST_OUTPUT="stream_result_to=HOST:PORT"

This server listens for those HTTP requests, records each event, and writes
an append-only JSONL log plus a rolling summary.
"""

from __future__ import annotations

import argparse
import logging
import socketserver
import threading
from datetime import datetime
from pathlib import Path
from time import monotonic
from urllib.parse import parse_qs

from result_store import ResultStore, _utc_now, _flatten_query_dict


class GTestTCPHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        client = f"{self.client_address[0]}:{self.client_address[1]}"
        store = self.server.create_session_store(client)
        logging.info(f"{client} - connected")

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
        logging.info(f"{client} - disconnected")
        summary_msg = f"{client} - summary: passed={len(summary['passed_tests'])}, failed={len(summary['failed_tests'])}, events={summary['total_events']}"

        if "unfinished_tests" in summary:
            summary_msg += f", unfinished={len(summary['unfinished_tests'])}"
        logging.info(summary_msg)



class GTestTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], out_dir: Path, jsonl_name: str, sqlite_db: bool = False) -> None:
        super().__init__(server_address, GTestTCPHandler)
        self.out_dir = out_dir
        self.jsonl_name = jsonl_name
        self.sqlite_db = sqlite_db
        self._session_id_seq = 0
        self._session_id_lock = threading.Lock()

    def create_session_store(self, client: str) -> ResultStore:
        with self._session_id_lock:
            self._session_id_seq += 1
            session_id = self._session_id_seq
        return ResultStore(self.out_dir, session_id, client, jsonl_name=self.jsonl_name, sqlite_db=self.sqlite_db)


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
        help="Per-session events JSONL file name",
    )
    parser.add_argument(
        "--sqlite-db",
        action='store_true',
        help="Store events in a SQLite database file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tcp_server = GTestTCPServer((args.host, args.port), out_dir, args.jsonl_name, args.sqlite_db)
    logging.info(f"Listening on tcp://{args.host}:{args.port}")
    logging.info(f"Saving per-connection sessions under: {out_dir}")
    logging.info("Each session writes: events.jsonl + output.txt + summary.json")
    logging.info("Protocol: raw TCP lines from GoogleTest stream_result_to")

    try:
        tcp_server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        tcp_server.server_close()


if __name__ == "__main__":
    main()
