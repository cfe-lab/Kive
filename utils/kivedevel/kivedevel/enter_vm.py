from __future__ import annotations

import argparse
import logging
import subprocess
import sys

from .kv_commands import Cmds
from .shared import configure_console_logging, instance_exists, instance_is_running


logger = logging.getLogger("kivedevel")


def _add_log_flags(parser: argparse.ArgumentParser) -> None:
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument("--quiet", action="store_true", help="Only show errors")
    log_group.add_argument("--verbose", action="store_true", help="Show informational progress messages")
    log_group.add_argument("--debug", action="store_true", help="Show debug logging, including full command lines")


def register_subcommand(subparsers) -> None:
    parser = subparsers.add_parser(
        "enter-vm",
        help="Open an interactive shell inside an Incus instance",
    )
    parser.add_argument("instance", nargs="?", default="kive-minimal", help="Incus instance name (default: kive-minimal)")
    parser.add_argument("--user", default="ubuntu", help="Username to log in as (default: ubuntu)")
    parser.add_argument("--shell", default="/bin/bash", help="Shell to launch (default: /bin/bash)")
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Optional command to run inside the instance (prefix with --)",
    )
    _add_log_flags(parser)
    parser.set_defaults(func=run_enter_vm)


def run_enter_vm(args: argparse.Namespace) -> None:
    configure_console_logging(args)
    cmds = Cmds.create()
    cmds.incus.require()

    instance = args.instance

    if not instance_exists(cmds, instance):
        logger.error(
            "Instance %s does not exist. Run ./utils/dev build-vm %s first.",
            instance,
            instance,
        )
        sys.exit(1)

    if not instance_is_running(cmds, instance):
        logger.info("Starting instance %s...", instance)
        cmds.incus.run(["start", instance])

    exec_args = ["incus", "exec", instance, "--", "sudo", "-iu", args.user]

    if args.command:
        stripped = [c for c in args.command if c != "--"]
        if stripped:
            exec_args.extend(["--"] + stripped)

    if args.command:
        subprocess.run(exec_args)
    else:
        raise SystemExit(subprocess.call(exec_args))
