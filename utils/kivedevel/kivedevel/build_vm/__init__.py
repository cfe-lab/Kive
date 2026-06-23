from __future__ import annotations

import argparse
from pathlib import Path

from ..kv_commands import Cmds
from ..shared import configure_logging, default_root, instance_exists
from . import purge
from .runner import run_build_vm


def register_subcommand(subparsers) -> None:  # type: ignore[type-arg]
    root = default_root()
    parser = subparsers.add_parser(
        "build-vm",
        help="Build and configure an Incus VM with cloud-init",
    )
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument("--quiet", action="store_true", help="Only show errors")
    log_group.add_argument("--verbose", action="store_true", help="Show informational progress messages")
    log_group.add_argument("--debug", action="store_true", help="Show debug logging, including full command lines")
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="Write a persistent log file (default: <workdir>/build-vm.log)",
    )
    parser.add_argument("instance", nargs="?", default="kive-minimal", help="Incus instance name (default: kive-minimal)")
    parser.add_argument(
        "--instance-type",
        choices=("vm", "container"),
        default="vm",
        help="Incus instance type to create (default: vm)",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=root,
        metavar="DIR",
        help=f"Repository root to sync into the workspace disk (default: {root})",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Working directory for build artefacts (default: <root>/tmp~/build)",
    )
    parser.add_argument(
        "--image-name",
        default="kive-code.qcow2",
        metavar="NAME",
        help="Filename of the workspace qcow2 image (default: kive-code.qcow2)",
    )
    parser.add_argument("--pool", default="default", help="Incus storage pool (default: default)")
    parser.add_argument("--profile", default="default", help="Incus profile (default: default)")
    parser.add_argument("--root-size", default="10GiB", help="Root disk size (default: 10GiB)")
    parser.add_argument("--memory", default="1GB", help="VM memory limit (default: 1GB)")
    parser.add_argument("--cpu", default="1", help="VM CPU count (default: 1)")
    parser.add_argument(
        "--host-interface",
        default="",
        metavar="IFACE",
        help="Host network interface for the VM NIC (auto-detected by default)",
    )
    parser.add_argument(
        "--no-provision",
        action="store_false",
        dest="provision",
        help="Skip provisioning after build (default: enabled)",
    )
    parser.set_defaults(func=run_from_args, provision=True)

    purge.register_subcommand(subparsers)


def main(args: argparse.Namespace) -> None:
    run_build_vm(args)


def run_from_args(args: argparse.Namespace) -> None:
    if args.workdir is None:
        args.workdir = args.root / "tmp~" / "build"
    configure_logging(args, args.workdir)
    run_build_vm(args)


__all__ = [
    "Cmds",
    "default_root",
    "instance_exists",
    "configure_logging",
    "register_subcommand",
    "main",
    "run_from_args",
    "Path",
]
