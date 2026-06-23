"""Commands for local install smoke testing and cleanup."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from .kv_commands import Cmds
from .shared import configure_logging, default_root, instance_exists


logger = logging.getLogger("kivedevel.local_install")


def run_smoke_local_install(args: argparse.Namespace) -> None:
    workdir: Path = args.workdir.resolve()
    configure_logging(args, workdir)

    instance: str = args.instance
    instance_type: str = args.instance_type
    debug: bool = getattr(args, "debug", False)

    workdir.mkdir(parents=True, exist_ok=True)

    commands: list[list[str]] = []

    for subcmd in ("build-vm", "validate-vm", "test-api"):
        cmd = [
            "sudo", "--preserve-env=PATH,BUILD_VM_WORKDIR",
            "utils/dev", subcmd, instance,
            "--workdir", str(workdir),
        ]
        if subcmd != "test-api":
            cmd.extend(["--instance-type", instance_type])
        if subcmd == "build-vm":
            cmd.append("--provision")
        if debug:
            cmd.append("--debug")
        commands.append(cmd)

    for cmd in commands:
        logger.info("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            logger.error("Command failed with exit code %s: %s", result.returncode, " ".join(cmd))
            sys.exit(result.returncode)

    logger.info("Smoke test passed.")


def run_cleanup_local_install(args: argparse.Namespace) -> None:
    workdir: Path | None = getattr(args, "workdir", None)
    if workdir is not None:
        workdir = workdir.resolve()

    instance: str = args.instance

    cmds = Cmds.create()

    if instance_exists(cmds, instance):
        logger.info("Deleting instance %s...", instance)
        cmds.incus.run(["delete", "-f", instance], check=False)
    else:
        logger.info("Instance %s does not exist, skipping.", instance)

    if workdir is not None and workdir.exists():
        logger.info("Removing workdir %s...", workdir)
        subprocess.run(["sudo", "rm", "-rf", str(workdir)], check=False)

    logger.info("Cleanup complete.")


def _add_log_flags(parser: argparse.ArgumentParser) -> None:
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument("--quiet", action="store_true", help="Only show errors")
    log_group.add_argument(
        "--verbose", action="store_true", help="Show informational progress messages"
    )
    log_group.add_argument(
        "--debug",
        action="store_true",
        help="Show debug logging, including full command lines",
    )


def register_subcommand(subparsers) -> None:  # type: ignore[type-arg]
    smoke = subparsers.add_parser(
        "smoke-local-install",
        help="Run the end-to-end local install smoke test (build-vm + validate-vm + test-api)",
    )
    smoke.add_argument(
        "--instance",
        default="ci-smoke",
        help="Instance name for the smoke test (default: ci-smoke)",
    )
    smoke.add_argument(
        "--instance-type",
        choices=("vm", "container"),
        default="container",
        help="Instance type (default: container)",
    )
    smoke.add_argument(
        "--workdir",
        type=Path,
        default=default_root() / "tmp~" / "build",
        help="Working directory (default: <root>/tmp~/build)",
    )
    _add_log_flags(smoke)
    smoke.set_defaults(func=run_smoke_local_install)

    cleanup = subparsers.add_parser(
        "cleanup-local-install",
        help="Clean up a local install smoke run (delete instance, remove workdir)",
    )
    cleanup.add_argument(
        "--instance",
        default="ci-smoke",
        help="Instance name to delete (default: ci-smoke)",
    )
    cleanup.add_argument(
        "--workdir",
        type=Path,
        default=default_root() / "tmp~" / "build",
        help="Working directory to remove (default: <root>/tmp~/build)",
    )
    cleanup.set_defaults(func=run_cleanup_local_install)
