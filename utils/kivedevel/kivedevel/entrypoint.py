
import argparse
import sys
from typing import Sequence

from . import build_vm
from . import checks
from . import enter_vm
from . import local_install
from . import reload as reload_mod
from .backends import incus_host, incus_network
from .shared import default_root


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="kivedevel",
        description="Development utilities for Kive.",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        default=False,
        help="Purge all Kive development resources (shortcut for 'purge' subcommand)",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    build_vm.register_subcommand(subparsers)
    checks.register_subcommands(subparsers)
    enter_vm.register_subcommand(subparsers)
    incus_host.register_subcommand(subparsers)
    incus_network.register_subcommand(subparsers)
    local_install.register_subcommand(subparsers)
    reload_mod.register_subcommand(subparsers)

    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)

    if args.purge:
        from .build_vm.purge import run_purge as purge_func
        purge_args = argparse.Namespace(
            root=default_root(),
            instances=[],
            workdirs=[],
            quiet=False,
            verbose=False,
            debug=True,
            log_file=None,
        )
        purge_func(purge_args)
        return 0

    args.func(args)
    return 0


def entry() -> None:
    sys.exit(main(sys.argv[1:]))
