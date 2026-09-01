#!/usr/bin/env python3

# Copyright (c) 2026 Andrew J. Sender <AndrewJ.Sender@gmail.com>
# All rights reserved.
#
# This source code is licensed under the BSD 3-Clause License found in
# the LICENSE file in the root directory of this source tree.

import argparse
import logging

CHOICES = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

def add_argument(parser: argparse.ArgumentParser):
    parser.add_argument("--log_level", nargs="?", type=str, default="ERROR", choices=CHOICES, help="Logging level")

def configure(args):
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s [%(levelname)s] %(message)s")

