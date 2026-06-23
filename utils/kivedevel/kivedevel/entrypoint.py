
import argparse
import sys
from typing import Sequence

from . import build_vm
from . import checks
from . import local_install
from .backends import incus_host, incus_network


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="kivedevel",
        description="Development utilities for Kive.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    build_vm.register_subcommand(subparsers)
    checks.register_subcommands(subparsers)
    incus_host.register_subcommand(subparsers)
    incus_network.register_subcommand(subparsers)
    local_install.register_subcommand(subparsers)

    args = parser.parse_args(argv)
    args.func(args)
    return 0


def entry() -> None:
    sys.exit(main(sys.argv[1:]))
