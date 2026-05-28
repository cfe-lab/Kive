"""Validation and smoke-check subcommands for Incus development instances."""

from __future__ import annotations

import argparse
import compileall
import logging
import re
import sys
from pathlib import Path

from . import build_vm


logger = logging.getLogger("kivedevel.checks")


def _is_instance_running(cmds: build_vm.Cmds, instance: str) -> bool:
    out = cmds.incus.output(["info", instance])
    return bool(re.search(r"^Status:\s+Running$", out, re.IGNORECASE | re.MULTILINE))


def _device_exists(cmds: build_vm.Cmds, instance: str, device: str) -> bool:
    out = cmds.incus.output(["config", "device", "show", instance])
    return bool(re.search(rf"^{re.escape(device)}:\s*$", out, re.MULTILINE))


def _required_device_value(
    cmds: build_vm.Cmds, instance: str, device: str, key: str
) -> str:
    value = cmds.incus.output(["config", "device", "get", instance, device, key]).strip()
    if not value:
        logger.error("Missing %s for device %s on %s.", key, device, instance)
        sys.exit(1)
    return value


def _run_validate_vm(args: argparse.Namespace) -> None:
    workdir: Path = args.workdir.resolve()
    build_vm.configure_logging(args, workdir)

    cmds = build_vm.Cmds.create()
    cmds.incus.require()

    instance = args.instance
    if not build_vm.instance_exists(cmds, instance):
        logger.error("Instance %s does not exist.", instance)
        sys.exit(1)

    if not _is_instance_running(cmds, instance):
        logger.error("Instance %s exists but is not running.", instance)
        sys.exit(1)

    if not _device_exists(cmds, instance, "kive-code"):
        logger.error("Instance %s is missing required device 'kive-code'.", instance)
        sys.exit(1)

    if args.instance_type == "container":
        # Container mode mounts repo source as a host directory at /mnt/kive-code.
        mount_path = _required_device_value(cmds, instance, "kive-code", "path")
        source_path = Path(_required_device_value(cmds, instance, "kive-code", "source"))
        if mount_path != "/mnt/kive-code":
            logger.error("Unexpected kive-code path %s (expected /mnt/kive-code).", mount_path)
            sys.exit(1)
        if not source_path.is_dir():
            logger.error("kive-code source directory does not exist: %s", source_path)
            sys.exit(1)

    logger.info("validate-vm checks passed for %s.", instance)


def _run_test_api(args: argparse.Namespace) -> None:
    workdir: Path = args.workdir.resolve()
    build_vm.configure_logging(args, workdir)

    cmds = build_vm.Cmds.create()
    cmds.incus.require()

    instance = args.instance
    if not build_vm.instance_exists(cmds, instance):
        logger.error("Instance %s does not exist.", instance)
        sys.exit(1)

    source_path = Path(_required_device_value(cmds, instance, "kive-code", "source"))
    api_package = source_path / "api" / "kiveapi"
    if not api_package.is_dir():
        fallback = build_vm.default_root() / "api" / "kiveapi"
        if fallback.is_dir():
            logger.warning(
                "Mounted source %s has no API tree; falling back to %s",
                source_path,
                fallback,
            )
            api_package = fallback
        else:
            logger.error("API package directory not found at %s", api_package)
            sys.exit(1)

    # Keep this lightweight for CI smoke use: compile API sources to catch syntax regressions.
    if not compileall.compile_dir(str(api_package), quiet=1):
        logger.error("API byte-compilation failed under %s", api_package)
        sys.exit(1)

    logger.info("test-api checks passed for %s.", instance)


def register_subcommands(subparsers) -> None:  # type: ignore[type-arg]
    log_opts = {
        "quiet": dict(action="store_true", help="Only show errors"),
        "verbose": dict(action="store_true", help="Show informational progress messages"),
        "debug": dict(action="store_true", help="Show debug logging, including full command lines"),
    }

    validate = subparsers.add_parser(
        "validate-vm",
        help="Validate that a build-vm instance is running and properly mounted",
    )
    validate.add_argument("instance", nargs="?", default="kive-minimal", help="Incus instance name")
    validate.add_argument(
        "--instance-type",
        choices=("vm", "container"),
        default="vm",
        help="Expected instance type for mount checks (default: vm)",
    )
    validate.add_argument(
        "--workdir",
        type=Path,
        default=build_vm.default_root() / "tmp~" / "build",
        help="Working directory used for logs",
    )
    for opt, kwargs in log_opts.items():
        validate.add_argument(f"--{opt}", **kwargs)
    validate.set_defaults(func=_run_validate_vm)

    test_api = subparsers.add_parser(
        "test-api",
        help="Run lightweight API source checks inside an instance",
    )
    test_api.add_argument("instance", nargs="?", default="kive-minimal", help="Incus instance name")
    test_api.add_argument(
        "--workdir",
        type=Path,
        default=build_vm.default_root() / "tmp~" / "build",
        help="Working directory used for logs",
    )
    for opt, kwargs in log_opts.items():
        test_api.add_argument(f"--{opt}", **kwargs)
    test_api.set_defaults(func=_run_test_api)
