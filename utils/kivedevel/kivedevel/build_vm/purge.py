from __future__ import annotations

import argparse
import json
import logging
import subprocess
from pathlib import Path

from ..kv_commands import Cmds
from ..shared import configure_logging, default_root

logger = logging.getLogger("kivedevel.build_vm.purge")

_RESOURCE_MARKER = ".kive-devel-resource.json"
_KNOWN_LEGACY_INSTANCES = ("kive-minimal", "ci-smoke", "network-smoke")


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


def _remove_device(cmds: Cmds, instance: str, dry_run: bool) -> None:
    if dry_run:
        logger.info("[dry-run] Would remove device 'kive-code' from instance '%s'.", instance)
        return
    logger.info("Removing device 'kive-code' from instance '%s'...", instance)
    cmds.incus.run(["config", "device", "remove", instance, "kive-code"], check=False)


def _remove_image(image_path: Path, dry_run: bool) -> None:
    if image_path.exists():
        if dry_run:
            logger.info("[dry-run] Would remove workspace image '%s'.", image_path)
            return
        logger.info("Removing workspace image '%s'...", image_path)
        try:
            image_path.unlink()
        except OSError:
            subprocess.run(["sudo", "rm", "-f", "--", str(image_path)], check=False)


def _remove_workdir(workdir: Path, dry_run: bool) -> None:
    if workdir.exists():
        if dry_run:
            logger.info("[dry-run] Would remove build directory '%s'.", workdir)
            return
        logger.info("Removing build directory '%s'...", workdir)
        subprocess.run(["sudo", "rm", "-rf", "--", str(workdir)], check=False)


def _delete_instance(cmds: Cmds, instance: str, dry_run: bool) -> None:
    if dry_run:
        logger.info("[dry-run] Would delete instance '%s'.", instance)
        return
    cmds.incus.run(["delete", "-f", "--", instance], check=False)


def _find_tagged_instances(cmds: Cmds) -> list[str]:
    out = cmds.incus.output(["list", "--format", "csv", "--columns", "n"])
    found = []
    for line in out.splitlines():
        name = line.strip()
        if not name:
            continue
        config = cmds.incus.output(["config", "show", name])
        if "user.kive.devel.created-by: utils/dev" in config:
            found.append(name)
    if found:
        logger.info("Found tagged instances: %s", ", ".join(found))
    return found


def _find_marked_workdirs(root: Path) -> list[Path]:
    candidates = []
    for marker in root.rglob(_RESOURCE_MARKER):
        try:
            data = json.loads(marker.read_text())
            if data.get("created-by") == "utils/dev":
                candidates.append(marker.parent.resolve())
        except (json.JSONDecodeError, OSError):
            continue
    if candidates:
        logger.info("Found marked workdirs: %s", ", ".join(str(p) for p in candidates))
    return candidates


def run_purge(args: argparse.Namespace) -> None:
    root = args.root.resolve()
    workdir_default = root / "tmp~" / "build"
    configure_logging(args, workdir_default)

    cmds = Cmds.create()
    cmds.incus.require()

    dry_run: bool = getattr(args, "dry_run", False)

    # Phase 1: Discover tagged instances.
    tagged = _find_tagged_instances(cmds)

    # Phase 2: Legacy fallback — check known instance names.
    legacy_instances = [i for i in _KNOWN_LEGACY_INSTANCES if i not in tagged]
    extra_instances = getattr(args, "instances", []) or []

    all_instances = list(tagged) + legacy_instances + extra_instances

    # Phase 3: Discover marked workdirs.
    marked = _find_marked_workdirs(root)

    # Phase 4: Legacy fallback — default workdir path.
    extra_workdirs = getattr(args, "workdirs", []) or []

    all_workdirs = list(marked)
    if workdir_default not in all_workdirs:
        all_workdirs.append(workdir_default)
    for w in extra_workdirs:
        w_resolved = w.resolve() if isinstance(w, Path) else Path(w).resolve()
        if w_resolved not in all_workdirs:
            all_workdirs.append(w_resolved)

    if not all_instances and not all_workdirs:
        logger.info("No Kive development resources found to purge.")
        return

    logger.info("Resources to purge: %d instance(s), %d workdir(s)", len(all_instances), len(all_workdirs))

    # Phase 5: Detach stale NBD.
    _detach_stale_nbd()

    # Phase 6: Cleanup per-instance resources.
    for instance in all_instances:
        if _device_attached(cmds, instance):
            _remove_device(cmds, instance, dry_run)
        elif not dry_run:
            logger.debug("No kive-code device attached to '%s'.", instance)

    # Phase 7: Cleanup per-workdir resources.
    for workdir in all_workdirs:
        mountpoint_path = workdir / "kive-code-mount"
        if mountpoint_path.exists() and _is_mountpoint(mountpoint_path):
            if dry_run:
                logger.info("[dry-run] Would unmount '%s'.", mountpoint_path)
            else:
                logger.info("Cleaning stale mountpoint '%s'...", mountpoint_path)
                _umount(mountpoint_path)

        image_path = workdir / "kive-code.qcow2"
        _remove_image(image_path, dry_run)
        _remove_workdir(workdir, dry_run)

    # Phase 8: Delete instances.
    for instance in all_instances:
        _delete_instance(cmds, instance, dry_run)

    logger.info("Purge complete. All Kive development resources removed.")


def register_subcommand(subparsers) -> None:
    root = default_root()
    parser = subparsers.add_parser(
        "purge",
        help="Remove all development resources (instances, workdirs, images) created by utils/dev",
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
    parser.add_argument(
        "--root",
        type=Path,
        default=root,
        metavar="DIR",
        help=f"Repository root used to discover marked workdirs (default: {root})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be removed without deleting anything",
    )
    parser.add_argument(
        "--instance",
        action="append",
        dest="instances",
        default=None,
        help="Additional instance name to purge (may be repeated, in addition to discovered instances)",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        action="append",
        dest="workdirs",
        default=None,
        help="Additional workdir path to purge (may be repeated, in addition to discovered workdirs)",
    )
    parser.set_defaults(func=run_purge)
