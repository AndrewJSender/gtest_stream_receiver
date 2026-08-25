#!/usr/bin/env python3

# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.

from __future__ import annotations

from dataclasses import dataclass

@dataclass
class SessionTestState:
    name: str
    started_mono: float | None = None
    ended_mono: float | None = None
    duration_seconds: float | None = None
    status: str | None = None
