from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

from ..kv_commands import Cmds
from ..shared import configure_logging, default_root

logger = logging.getLogger("kivedevel.build_vm.purge")


def _is_mountpoint(path: Path) -> bool:
    result = subprocess.run(
        ["mountpoint", "-q", str(path)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def _umount(path: Path) -> None:
    try:
        subprocess.run(
            ["sudo", "umount", "--", str(path)],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def _detach_stale_nbd() -> None:
    pid_file = Path("/sys/block/nbd0/pid")
    if not pid_file.exists():
        return

    pid = pid_file.read_text().strip()
    if not pid or pid == "0":
        return

    logger.info("Detaching stale /dev/nbd0...")
    cmds = Cmds.create()
    cmds.qemu_nbd.run(["-d", "/dev/nbd0"], sudo=True, check=False)


def _device_attached(cmds: Cmds, instance: str) -> bool:
    out = cmds.incus.output(["config", "show", instance])
    return any(line.strip() == "kive-code:" for line in out.splitlines())


def _remove_device(cmds: Cmds, instance: str) -> None:
    logger.info("Removing device 'kive-code' from instance '%s'...", instance)
    cmds.incus.run(["config", "device", "remove", instance, "kive-code"], check=False)


def _remove_image(image_path: Path) -> None:
    if image_path.exists():
        logger.info("Removing workspace image '%s'...", image_path)
        try:
            image_path.unlink()
        except OSError:
            subprocess.run(["sudo", "rm", "-f", "--", str(image_path)], check=False)


def _remove_workdir(workdir: Path) -> None:
    if workdir.exists():
        logger.info("Removing build directory '%s'...", workdir)
        subprocess.run(["sudo", "rm", "-rf", "--", str(workdir)], check=False)


def _delete_instance(cmds: Cmds, instance: str) -> None:
    cmds.incus.run(["delete", "-f", "--", instance], check=False)


def run_purge(args: argparse.Namespace) -> None:
    workdir: Path = args.workdir.resolve()
    configure_logging(args, workdir)

    cmds = Cmds.create()
    cmds.incus.require()

    mountpoint_path = workdir / "kive-code-mount"
    if mountpoint_path.exists() and _is_mountpoint(mountpoint_path):
        logger.info("Cleaning stale mountpoint '%s'...", mountpoint_path)
        _umount(mountpoint_path)

    _detach_stale_nbd()

    if _device_attached(cmds, args.instance):
        _remove_device(cmds, args.instance)
    else:
        logger.info("No kive-code device attached to '%s'.", args.instance)

    image_path = args.workdir / args.image_name if not args.image_name.is_absolute() else args.image_name
    _remove_image(image_path)
    _remove_workdir(workdir)
    _delete_instance(cmds, args.instance)

    print("Purge complete. Build assets removed.")


def register_subcommand(subparsers) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "purge",
        help="Remove build assets, the workspace image, and the Incus instance",
    )
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument("--quiet", action="store_true", help="Only show errors")
    log_group.add_argument("--verbose", action="store_true", help="Show informational progress messages")
    log_group.add_argument("--debug", action="store_true", help="Show debug logging, including full command lines")
    parser.add_argument(
        "instance",
        nargs="?",
        default="kive-minimal",
        help="Incus instance name (default: kive-minimal)",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=default_root() / "tmp~" / "build",
        help="Working directory for build artefacts (default: <root>/tmp~/build)",
    )
    parser.add_argument(
        "--image-name",
        type=Path,
        default=Path("kive-code.qcow2"),
        help="Filename of the workspace qcow2 image (default: kive-code.qcow2)",
    )
    parser.set_defaults(func=run_purge)
