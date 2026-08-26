#!/usr/bin/env python3

# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.


"""Process Google Test Stream Events

Raises:
    f: _description_

Returns:
    _type_: _description_
"""

from __future__ import annotations

import json
from xml.etree import ElementTree as ET
from enum import StrEnum
from typing import Any, Dict
from pathlib import Path



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

    def dict_to_xml(tag, d):
        """Converts a python dictionary to an XML Element."""
        elem = ET.Element(tag)
        for key, val in d.items():
            child = ET.Element(key)
            if isinstance(val, dict):
                # Recursively handle nested dictionaries
                child.append(self.dict_to_xml(key, val))
            else:
                child.text = str(val)
            elem.append(child)
        return elem

    def output_json(self, session: Dict, path: Path):
        with open(path, "w") as file:
            json.dump(session, file)

    def process_event(self, event: Dict[str, Any]) -> None:
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
                new_session.update({"iterations": []})
                self.sessions[session_id] = new_session
            case EventType.PROGRAM_END:
                current_session = self.sessions.pop(session_id)
                passed = bool(params[self.passed_key])
                elapsed_time = None
                current_session.update(self.ended(passed, elapsed_time, mono, utc))
                self.output_json(current_session, "session_output.json")
            case EventType.ITERATION_START:
                iteration_idx = params["iteration"]
                new_iteration = self.started(iteration_idx, mono, utc)
                current_session = self.sessions[session_id]
                current_session["iterations"].append(new_iteration)
            case EventType.ITERATION_END:
                current_session = self.sessions[session_id]
                last_iteration = current_session["iterations"][-1]
                passed = bool(params[self.passed_key])
                elapsed_time = params[self.elapsed_time_key]
                last_iteration.update(self.ended(passed, elapsed_time, mono, utc))
            # case EventType.CASE_START:
            #     current_session = self.sessions[session_id]
            #     last_iteration = current_session.iterations[-1]
            #     suite = Suite(params["name"], mono, utc)
            #     last_iteration.add_suite(suite)
            # case EventType.CASE_END:
            #     current_session = self.sessions[session_id]
            #     last_iteration = current_session.iterations[-1]
            #     last_suite = last_iteration.suites[-1]
            #     passed = bool(params[self.passed_key])
            #     elapsed_time = params[self.elapsed_time_key]
            #     last_suite.ended(passed, elapsed_time, mono, utc)
            # case EventType.TEST_START:
            #     current_session = self.sessions[session_id]
            #     last_iteration = current_session.iterations[-1]
            #     last_suite = last_iteration.suites[-1]
            #     test = Test(params["name"], mono, utc)
            #     last_suite.add_test(test)
            # case EventType.TEST_END:
            #     current_session = self.sessions[session_id]
            #     last_iteration = current_session.iterations[-1]
            #     last_suite = last_iteration.suites[-1]
            #     last_test = last_suite.tests[-1]
            #     passed = bool(params[self.passed_key])
            #     elapsed_time = params[self.elapsed_time_key]
            #     last_test.ended(passed, elapsed_time, mono, utc)
            case _:
                pass
                # raise f"Unknown Event Type: {event_name}"

def parse_args() -> Any:
    import argparse

    parser = argparse.ArgumentParser(description="Process GoogleTest streaming events.")
    parser.add_argument("--jsonl-path", help="Path to the JSONL file containing events.")
    parser.add_argument("--db-path", help="Path to the SQLite database file (optional).")
    parser.add_argument("--output-json-path", help="Path to the output json file.")
    parser.add_argument("--output-xml-path", help="Path to the output json file.")
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    processor = Processor()
    with open(args.jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            event = json.loads(line)
            processor.process_event(event)

    print(processor.sessions)

if __name__ == "__main__":
    main()