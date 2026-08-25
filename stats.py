# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.

"""Simple receiver for GoogleTest streaming output.

GoogleTest can stream test progress/results to a web server using:
    GTEST_OUTPUT="stream_result_to=HOST:PORT"

This server listens for those HTTP requests, records each event, and writes
an append-only JSONL log plus a rolling summary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Stats:
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
