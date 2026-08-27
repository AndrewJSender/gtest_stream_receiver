#!/usr/bin/env python3

# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.

"""Receiver for GoogleTest streaming output.

GoogleTest can stream test progress/results to a web server using:
    GTEST_OUTPUT="stream_result_to=HOST:PORT"

This server listens for those TCP requests, records each event to jsonl or sqlite db
"""

from __future__ import annotations

import argparse
from ast import Dict, List
from datetime import datetime, timezone
import logging
import socketserver
import threading
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import parse_qs

from result_store import ResultStore
import my_logging


class GTestTCPHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        client = f"{self.client_address[0]}:{self.client_address[1]}"
        store = self.server.create_session_store()
        logging.info(f"{client} - session {store.session_id} - connected")

        while True:
            raw_line = self.rfile.readline()
            if not raw_line:
                break

            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                logging.warning(f"{client} - received empty line, skipping")
                continue

            params = _flatten_query_dict(parse_qs(line, keep_blank_values=True))
            if not params and "=" in line:
                key, value = line.split("=", 1)
                params = {key: value}

            event = {
                "utc": _utc_now(),
                "monotonic": monotonic(),
                "session_id": store.session_id,
                "client": client,
                "params": params,
            }
            store.append_event(event)

        logging.info(f"{client} - session {store.session_id} - disconnected")

def _flatten_query_dict(query_dict: Dict[str, List[str]]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, values in query_dict.items():
        if not values:
            continue
        flat[key] = values[0] if len(values) == 1 else values
    return flat

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

class GTestTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    def __init__(self, server_address: tuple[str, int], out_dir: Path, jsonl: str, sqlite_db: bool = False) -> None:
        super().__init__(server_address, GTestTCPHandler)
        self.out_dir = out_dir
        self.jsonl = jsonl
        self.sqlite_db = sqlite_db
        self._session_id_seq = 0
        self._session_id_lock = threading.Lock()

    def create_session_store(self) -> ResultStore:
        with self._session_id_lock:
            self._session_id_seq += 1
            session_id = self._session_id_seq
        return ResultStore(self.out_dir, session_id, jsonl=self.jsonl, sqlite_db=self.sqlite_db)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Receive and store GoogleTest streaming results", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    my_logging.add_argument(parser)
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    parser.add_argument(
        "--out-dir",
        default="./data",
        help="Directory to save output files (default: ./data)",
    )
    parser.add_argument(
        "--jsonl",
        action="store_true",
        help="Store events in a JSONL file (one JSON object per line)",
    )
    parser.add_argument(
        "--sqlite-db",
        action='store_true',
        help="Store events in a SQLite database file",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    my_logging.configure(args)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tcp_server = GTestTCPServer((args.host, args.port), out_dir, args.jsonl, args.sqlite_db)
    logging.info(f"Listening on tcp://{args.host}:{args.port}")
    logging.info(f"Saving per-connection sessions under: {out_dir}")
    files = []
    if args.jsonl:
        files.append("events.jsonl")
    if args.sqlite_db:
        files.append("events.db")
    if not files:
        logging.error("No output files specified. Use --jsonl and/or --sqlite-db to store events.")
        return
    logging.info(f"Each session will write: {', '.join(files)}")
    logging.info("Protocol: raw TCP lines from GoogleTest stream_result_to")

    try:
        tcp_server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        tcp_server.server_close()


if __name__ == "__main__":
    main()
