"""Commands for local install smoke testing and cleanup."""

from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

from . import checks
from .build_vm.runner import run_build_vm
from .kv_commands import Cmds
from .shared import configure_logging, default_root, instance_exists


logger = logging.getLogger("kivedevel.local_install")


def _build_vm_args(
    instance: str,
    instance_type: str,
    workdir: Path,
    debug: bool,
) -> argparse.Namespace:
    root = default_root()
    return argparse.Namespace(
        instance=instance,
        instance_type=instance_type,
        workdir=workdir,
        root=root,
        image_name="kive-code.qcow2",
        pool="default",
        profile="default",
        root_size="10GiB",
        memory="8GiB",
        cpu="4",
        host_interface="",
        provision=True,
        web_port=8000,
        no_web_proxy=False,
        quiet=False,
        verbose=False,
        debug=debug,
        log_file=None,
        vm_network="kivebr0",
        vm_cidr="10.247.172.1/24",
        vm_ip="10.247.172.80",
    )


def _validate_vm_args(
    instance: str,
    instance_type: str,
    workdir: Path,
    debug: bool,
) -> argparse.Namespace:
    return argparse.Namespace(
        instance=instance,
        instance_type=instance_type,
        workdir=workdir,
        quiet=False,
        verbose=False,
        debug=debug,
    )


def _test_api_args(
    instance: str,
    workdir: Path,
    debug: bool,
) -> argparse.Namespace:
    return argparse.Namespace(
        instance=instance,
        workdir=workdir,
        port=8000,
        base_url=None,
        username="kive",
        password="kive",
        quiet=False,
        verbose=False,
        debug=debug,
    )


def run_smoke_local_install(args: argparse.Namespace) -> None:
    workdir: Path = args.workdir.resolve()
    configure_logging(args, workdir)

    instance: str = args.instance
    instance_type: str = args.instance_type
    debug: bool = getattr(args, "debug", False)

    workdir.mkdir(parents=True, exist_ok=True)

    build_args = _build_vm_args(instance, instance_type, workdir, debug)
    logger.info("Running: build-vm %s --instance-type %s --provision --workdir %s", instance, instance_type, workdir)
    run_build_vm(build_args)

    validate_args = _validate_vm_args(instance, instance_type, workdir, debug)
    logger.info("Running: validate-vm %s --instance-type %s --workdir %s", instance, instance_type, workdir)
    checks.run_validate_vm(validate_args)

    api_args = _test_api_args(instance, workdir, debug)
    logger.info("Running: test-api %s --workdir %s", instance, workdir)
    checks.run_test_api(api_args)

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
        subprocess.run(["rm", "-rf", str(workdir)], check=False)

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
