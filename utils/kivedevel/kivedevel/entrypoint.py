
import argparse
import sys
from typing import Sequence

from . import build_vm


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="kivedevel",
        description="Development utilities for Kive.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    build_vm.register_subcommand(subparsers)

    args = parser.parse_args(argv)
    args.func(args)
    return 0


def entry() -> None:
    sys.exit(main(sys.argv[1:]))
