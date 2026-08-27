#!/usr/bin/env python3

# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.


"""Process Google Test Stream Events
"""

from __future__ import annotations

import json
import sqlite3
from xml.etree import ElementTree as ET
from enum import StrEnum
from typing import Any, Dict
from pathlib import Path
import logging

import my_logging

class EventType(StrEnum):
    """
    Enumerable for Event types
    See each "event=" in
    https://github.com/google/googletest/blob/3ff51c3e80f2c2eb105d43ecb9acab9a62e01600/googletest/src/gtest-internal-inl.h

    Args:
        StrEnum (_type_): _description_
    """
    PROGRAM_START = "testprogramstart"
    PROGRAM_END = "testprogramend"
    CASE_START = "testcasestart"
    CASE_END = "testcaseend"
    PROTOCOL_VERSION = "gtest_streaming_protocol_version"
    TEST_START = "teststart"
    TEST_END = "testend"
    ITERATION_START = "testiterationstart"
    ITERATION_END = "testiterationend"
    PART_RESULT = "testpartresult"


class Processor:
    # Globals
    protocol_version = "1.0"
    protocol_version_key = "gtest_streaming_protocol_version"
    name_key = "name"
    passed_key = "passed"
    elapsed_time_key = "elapsed_time"
    started_key = "started"
    ended_key = "ended"
    iterations_key = "iterations"
    suites_key = "suites"
    tests_key = "tests"

    def __init__(self):
        self.sessions: Dict = {}

    def started(self, name: str, mono: float, utc: str):
        return {
            "name": name,
            "started": {
                "mono": mono,
                "utc": utc,
            }
        }

    def ended(self, passed: bool, elapsed_time: str, mono: float, utc: str):
        return {
            self.passed_key: passed,
            self.elapsed_time_key: elapsed_time,
            self.ended_key: {
                "mono": mono,
                "utc": utc,
            }
        }

    def output_json(self, session: Dict, path: Path):
        session_path = path.parent / f"{path.stem}.session_{session['name']}{path.suffix}"
        with open(session_path, "w") as file:
            json.dump(session, file)

    def dict_to_xml(self, tag, d):
        elem = ET.Element(tag)
        for key, val in d.items():
            if isinstance(val, dict):
                # Recursively handle nested dictionaries
                elem.append(self.dict_to_xml(key, val))
            elif isinstance(val, list):
                single_key = key[:-1]
                elem.extend(self.dict_to_xml(single_key, list_val) for list_val in val)
            else:
                # flat -> attribute
                elem.attrib[key] = str(val)
        return elem


    def output_xml(self, session: Dict, path: Path):
        session_path = path.parent / f"{path.stem}.session_{session['name']}{path.suffix}"
        root = self.dict_to_xml("session", session)
        tree = ET.ElementTree(root)
        with open(session_path, "wb") as file:
            tree.write(file, encoding="utf-8", xml_declaration=True)

    def process_event(self, event: Dict[str, Any], output_json, output_xml) -> None:
        session_id = event["session_id"]
        mono = event["monotonic"]
        utc = event["utc"]
        params = event.get("params", {})
        if self.protocol_version_key in params:
            self.protocol_version = params[self.protocol_version_key]
            return

        if "event" not in params:
            raise "not event name provided"
        event_name = str(params.get("event", "")).lower()
        match event_name:
            case EventType.PROGRAM_START:
                new_session = self.started(session_id, mono, utc)
                new_session.update({self.iterations_key: []})
                self.sessions[session_id] = new_session
            case EventType.PROGRAM_END:
                current_session = self.sessions.pop(session_id)
                passed = bool(params[self.passed_key])
                elapsed_time = None
                current_session.update(self.ended(passed, elapsed_time, mono, utc))
                if output_json:
                    self.output_json(current_session, output_json)
                if output_xml:
                    self.output_xml(current_session, output_xml)
            case EventType.ITERATION_START:
                iteration_idx = params["iteration"]
                new_iteration = self.started(iteration_idx, mono, utc)
                new_iteration[self.suites_key] = []
                current_session = self.sessions[session_id]
                current_session[self.iterations_key].append(new_iteration)
            case EventType.ITERATION_END:
                current_session = self.sessions[session_id]
                last_iteration = current_session[self.iterations_key][-1]
                passed = bool(params[self.passed_key])
                elapsed_time = params[self.elapsed_time_key]
                last_iteration.update(self.ended(passed, elapsed_time, mono, utc))
            case EventType.CASE_START:
                current_session = self.sessions[session_id]
                last_iteration = current_session[self.iterations_key][-1]
                new_suite = self.started(params["name"], mono, utc)
                new_suite[self.tests_key] = []
                last_iteration[self.suites_key].append(new_suite)
            case EventType.CASE_END:
                current_session = self.sessions[session_id]
                last_iteration = current_session[self.iterations_key][-1]
                last_suite = last_iteration[self.suites_key][-1]
                passed = bool(params[self.passed_key])
                elapsed_time = params[self.elapsed_time_key]
                last_suite.update(self.ended(passed, elapsed_time, mono, utc))
            case EventType.TEST_START:
                current_session = self.sessions[session_id]
                last_iteration = current_session[self.iterations_key][-1]
                last_suite = last_iteration[self.suites_key][-1]
                new_test = self.started(params["name"], mono, utc)
                last_suite[self.tests_key].append(new_test)
            case EventType.TEST_END:
                current_session = self.sessions[session_id]
                last_iteration = current_session[self.iterations_key][-1]
                last_suite = last_iteration[self.suites_key][-1]
                last_test = last_suite[self.tests_key][-1]
                passed = bool(params[self.passed_key])
                elapsed_time = params[self.elapsed_time_key]
                last_test.update(self.ended(passed, elapsed_time, mono, utc))
            case _:
                raise f"Unknown Event Type: {event_name}"

def parse_args() -> Any:
    import argparse

    parser = argparse.ArgumentParser(description="Process GoogleTest streaming events.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    my_logging.add_argument(parser)
    parser.add_argument("--jsonl-path", help="Path to the JSONL file containing events.")
    parser.add_argument("--db-path", help="Path to the SQLite database file (optional).")
    parser.add_argument("--output-json-path", type=Path, help="Path to the output json file.")
    parser.add_argument("--output-xml-path", type=Path, help="Path to the output json file.")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    my_logging.configure(args)
    processor = Processor()
    if args.jsonl_path:
        with open(args.jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                event = json.loads(line)
                processor.process_event(event, args.output_json_path, args.output_xml_path)

    if args.db_path:
        sqlite_conn = sqlite3.connect(args.db_path)
        sql_cmd = "SELECT * FROM events"
        cursor = sqlite_conn.cursor()
        cursor.execute(sql_cmd)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        sqlite_conn.close()
        results = [dict(zip(columns, row)) for row in rows]
        for event in results:
            event['params'] = json.loads(event['params'])
            processor.process_event(event, args.output_json_path, args.output_xml_path)

    if len(processor.sessions):
        logging.warning("test sessions not fully processed")
    else:
        logging.info("all test sessions fully processed")

if __name__ == "__main__":
    main()