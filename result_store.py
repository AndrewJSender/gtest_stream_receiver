#!/usr/bin/env python3

# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict

class ResultStore:
    def __init__(self, out_dir: Path, session_id: int, jsonl: str, sqlite_db: bool = False) -> None:
        self.session_id = session_id
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)

        if sqlite_db:
            self.sqlite_conn = sqlite3.connect(self.out_dir / "events.db")
            self.create_table()
        else:
            self.sqlite_conn = None

        if jsonl:
            self.events_file = self.out_dir / "events.jsonl"
        else:
            self.events_file = None

    def append_event(self, event: Dict[str, Any]) -> None:
        if self.events_file:
            with self.events_file.open("a", encoding="utf-8") as file_handle:
                file_handle.write(json.dumps(event, ensure_ascii=False) + "\n")

        if self.sqlite_conn:
            self.append_sql_event(event)


    def create_table(self) -> None:
        if not self.sqlite_conn:
            return
        events_table_sql = """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utc TEXT NOT NULL,
                monotonic REAL,
                session_id TEXT NOT NULL,
                client TEXT NOT NULL,
                params TEXT
            )
        """

        cursor = self.sqlite_conn.cursor()
        cursor.execute(events_table_sql)
        self.sqlite_conn.commit()

    def append_sql_event(self, event) -> None:
        cursor = self.sqlite_conn.cursor()
        cursor.execute(
            "INSERT INTO events (utc, monotonic, session_id, client, params) VALUES (?, ?, ?, ?, ?)",
            (
                event["utc"],
                event.get("monotonic"),
                event["session_id"],
                event["client"],
                json.dumps(event.get("params", {}), ensure_ascii=False),
            ),
        )
        self.sqlite_conn.commit()
