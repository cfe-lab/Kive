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


def _device_attached(cmds: Cmds, instance: str, device: str = "kive-code") -> bool:
    out = cmds.incus.output(["config", "show", instance])
    return any(line.strip() == f"{device}:" for line in out.splitlines())


def _remove_device(cmds: Cmds, instance: str, device: str = "kive-code") -> None:
    logger.info("Removing device '%s' from instance '%s'...", device, instance)
    cmds.incus.run(["config", "device", "remove", instance, device], check=False)


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
            entries = data if isinstance(data, list) else [data]
        except (json.JSONDecodeError, OSError):
            continue
        for entry in entries:
            created_by = entry.get("created_by") or entry.get("created-by")
            if created_by != "utils/dev":
                continue
            kind = entry.get("kind")
            if kind is None or kind == "build-workdir":
                candidates.append(marker.parent.resolve())
                break
    if candidates:
        logger.info("Found marked workdirs: %s", ", ".join(str(p) for p in candidates))
    return candidates


def _find_marked_networks(root: Path) -> list[str]:
    tagged = []
    for marker in root.rglob(_RESOURCE_MARKER):
        try:
            data = json.loads(marker.read_text())
            entries = data if isinstance(data, list) else [data]
        except (json.JSONDecodeError, OSError):
            continue
        for entry in entries:
            if entry.get("kind") == "network" and entry.get("created_by") == "utils/dev":
                name = entry.get("name")
                if name:
                    tagged.append(name)
    if tagged:
        logger.info("Found tagged networks: %s", ", ".join(tagged))
    return tagged


def run_purge(args: argparse.Namespace) -> None:
    root = args.root.resolve()
    workdir_default = root / "tmp~" / "build"
    configure_logging(args, workdir_default)

    cmds = Cmds.create()
    cmds.incus.require()

    # Phase 1: Discover tagged instances.
    tagged = _find_tagged_instances(cmds)
    tagged_set = set(tagged)

    # Phase 2: Explicit --instance overrides.
    extra_instances = getattr(args, "instances", []) or []

    all_instances = list(tagged) + extra_instances

    # Log any legacy untagged instances that were discovered but skipped.
    _check_legacy_skipped(cmds, tagged_set)

    # Phase 3: Discover tagged networks.
    tagged_networks = _find_marked_networks(root)

    # Phase 4: Discover marked workdirs.
    marked = _find_marked_workdirs(root)

    # Phase 4: Explicit --workdir overrides.
    extra_workdirs = getattr(args, "workdirs", []) or []

    all_workdirs = list(marked)
    for w in extra_workdirs:
        w_resolved = w.resolve() if isinstance(w, Path) else Path(w).resolve()
        if w_resolved not in all_workdirs:
            all_workdirs.append(w_resolved)

    if not all_instances and not all_workdirs and not tagged_networks:
        logger.info("No Kive development resources found to purge.")
        return

    logger.info("Resources to purge: %d instance(s), %d workdir(s)", len(all_instances), len(all_workdirs))

    _detach_stale_nbd()

    for instance in all_instances:
        if _device_attached(cmds, instance, "kive-code"):
            _remove_device(cmds, instance, "kive-code")
        else:
            logger.debug("No kive-code device attached to '%s'.", instance)
        if _device_attached(cmds, instance, "kive-web"):
            _remove_device(cmds, instance, "kive-web")
        else:
            logger.debug("No kive-web device attached to '%s'.", instance)

    for workdir in all_workdirs:
        mountpoint_path = workdir / "kive-code-mount"
        if mountpoint_path.exists() and _is_mountpoint(mountpoint_path):
            logger.info("Cleaning stale mountpoint '%s'...", mountpoint_path)
            _umount(mountpoint_path)

        image_path = workdir / "kive-code.qcow2"
        _remove_image(image_path)
        _remove_workdir(workdir)

    for instance in all_instances:
        _delete_instance(cmds, instance)

    if tagged_networks:
        remaining = set()
        for name in tagged_networks:
            out = cmds.incus.run(
                ["list", "--format", "csv", "--columns", "n"],
                check=False,
                capture_output=True,
            )
            if out.returncode == 0:
                remaining = set(line.strip() for line in out.stdout.splitlines() if line.strip())
                break
        if not remaining:
            for net in tagged_networks:
                logger.info("Removing managed network '%s'...", net)
                cmds.incus.run(["network", "delete", net], check=False)
        else:
            logger.info(
                "Skipping network removal: %d instance(s) still exist.",
                len(remaining),
            )

    logger.info("Purge complete. All Kive development resources removed.")


def _check_legacy_skipped(cmds: Cmds, tagged_set: set[str]) -> None:
    """Log a warning for any untagged legacy-name instances found running."""
    known_legacy = ("kive-minimal", "ci-smoke", "network-smoke")
    for name in known_legacy:
        if name not in tagged_set:
            out = cmds.incus.run(
                ["list", name, "--format", "csv", "--columns", "n"],
                check=False,
                capture_output=True,
            )
            if out.returncode == 0 and out.stdout.strip():
                logger.warning(
                    "Skipping untagged legacy instance '%s'; "
                    "pass --instance %s to remove it explicitly.",
                    name, name,
                )


def register_subcommand(subparsers) -> None:
    root = default_root()
    parser = subparsers.add_parser(
        "purge",
        help="Remove all development resources created by utils/dev. "
        "This deletes tagged Incus instances and marked local build directories.",
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
        "--instance",
        action="append",
        dest="instances",
        default=None,
        help="Explicit instance name to purge (may be repeated, in addition to discovered tagged instances)",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        action="append",
        dest="workdirs",
        default=None,
        help="Explicit workdir path to purge (may be repeated, in addition to discovered marked workdirs)",
    )
    parser.set_defaults(func=run_purge)
