
import argparse
import sys

from kivedevel import build_vm


def entry() -> None:
    parser = argparse.ArgumentParser(
        prog="kivedevel",
        description="Development utilities for Kive.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    build_vm.register_subcommand(subparsers)

    args = parser.parse_args()
    args.func(args)
