from __future__ import annotations

import argparse

from .cli.metro2hdf import cli as metro2hdf
from .cli.sort_events import cli as sort_events


def main():
    parser = argparse.ArgumentParser(prog="metro-eval")
    subparsers = parser.add_subparsers(dest="command", required=True)

    metro2hdf.parser(subparsers)
    sort_events.parser(subparsers)

    args = parser.parse_args()
    args.func(args)
