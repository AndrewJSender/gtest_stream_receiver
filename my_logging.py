
import logging

CHOICES = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

def add_argument(parser: ar):
    parser.add_argument("--log_level", nargs="?", type=str, default="ERROR", choices=CHOICES, help="Logging level")

def configure(args):
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s [%(levelname)s] %(message)s")

