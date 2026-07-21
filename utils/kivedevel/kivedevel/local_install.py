"""Commands for local install smoke testing and cleanup."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from . import checks
from . import reload as reload_mod
from .build_vm.runner import run_build_vm
from .kv_commands import Cmds
from .shared import configure_logging, default_root


logger = logging.getLogger("kivedevel.local_install")


def _build_vm_args(
    instance: str,
    instance_type: str,
    workdir: Path,
    debug: bool,
    vm_network: str | None = None,
) -> argparse.Namespace:
    root = default_root()
    return argparse.Namespace(
        instance=instance,
        instance_type=instance_type,
        workdir=workdir,
        root=root,
        image_name="kive-code.img",
        pool="default",
        profile="default",
        root_size="60GiB",
        memory="8GiB",
        cpu="4",
        host_interface=None,
        provision=True,
        web_port=8000,
        no_web_proxy=False,
        quiet=False,
        verbose=False,
        debug=debug,
        log_file=None,
        vm_network=vm_network,
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
        base_url="http://127.0.0.1:8000",
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
    vm_network: str | None = args.vm_network
    debug: bool = args.debug

    workdir.mkdir(parents=True, exist_ok=True)

    build_args = _build_vm_args(instance, instance_type, workdir, debug, vm_network)
    logger.info("Running: build-vm %s --instance-type %s --provision --workdir %s", instance, instance_type, workdir)
    run_build_vm(build_args)

    validate_args = _validate_vm_args(instance, instance_type, workdir, debug)
    logger.info("Running: validate-vm %s --instance-type %s --workdir %s", instance, instance_type, workdir)
    checks.run_validate_vm(validate_args)

    api_args = _test_api_args(instance, workdir, debug)
    logger.info("Running: test-api %s --workdir %s", instance, workdir)
    checks.run_test_api(api_args)

    # Reload smoke test: create a marker, reload, verify, delete, reload, verify gone.
    marker_name = ".kive-reload-smoke-marker"
    marker_path = default_root() / marker_name
    try:
        marker_path.write_text("smoke-test-marker\n")
        logger.info("Created host marker %s for reload test.", marker_path)

        reload_args = _build_vm_args(instance, instance_type, workdir, debug, vm_network)
        reload_args.root = default_root()
        reload_args.workdir = workdir
        reload_mod.run_reload(reload_args)

        cmds = Cmds.create()
        result = cmds.incus.run(
            ["exec", instance, "--", "test", "-f", f"/usr/local/share/Kive/{marker_name}"],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Marker file not found in guest after reload: {marker_name}")

        logger.info("Reload smoke test: marker propagated. Checking API health after reload...")
        checks.run_test_api(api_args)

        marker_path.unlink()
        logger.info("Deleted host marker. Reloading again to verify deletion...")
        reload_mod.run_reload(reload_args)

        result = cmds.incus.run(
            ["exec", instance, "--", "test", "-f", f"/usr/local/share/Kive/{marker_name}"],
            check=False,
        )
        if result.returncode == 0:
            raise RuntimeError(f"Deleted marker still present in guest after reload: {marker_name}")

        logger.info("Reload smoke test: deletion propagated.")
    finally:
        if marker_path.exists():
            marker_path.unlink()

    logger.info("Smoke test passed.")


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


def register_subcommand(subparsers) -> None:
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
        default="vm",
        help="Instance type (default: vm)",
    )
    smoke.add_argument(
        "--workdir",
        type=Path,
        default=default_root() / "tmp~" / "build",
        help="Working directory (default: <root>/tmp~/build)",
    )
    smoke.add_argument(
        "--vm-network",
        default=None,
        help="Managed Incus bridge for VM NIC (default: kive-lab-br).  Rejected in container mode.",
    )
    _add_log_flags(smoke)
    smoke.set_defaults(func=run_smoke_local_install)
