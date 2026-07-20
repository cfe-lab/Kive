from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
from pathlib import Path

from ..kv_commands import Cmds
from ..shared import configure_logging, default_root

logger = logging.getLogger("kivedevel.build_vm.purge")

_RESOURCE_MARKER = ".kive-devel-resource.json"
_REGISTRY_NAME = ".kive-devel-resources.json"


def _is_mountpoint(path: Path) -> bool:
    result = subprocess.run(
        ["mountpoint", "-q", str(path)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def _umount(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["sudo", "umount", "--", str(path)],
        capture_output=True, text=True,
    )


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


def _remove_workdir_safely(workdir: Path) -> bool:
    mountpoint_path = workdir / "kive-code-mount"
    if mountpoint_path.exists() and _is_mountpoint(mountpoint_path):
        result = _umount(mountpoint_path)
        if result.returncode != 0 or _is_mountpoint(mountpoint_path):
            logger.warning(
                "Cannot remove workdir '%s': failed to unmount '%s' (rc=%s).",
                workdir, mountpoint_path, result.returncode,
            )
            return False

    image_path = workdir / "kive-code.img"
    _remove_image(image_path)
    _remove_workdir(workdir)
    if workdir.exists():
        return False
    return True


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
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(data, dict):
            continue
        if data.get("created_by") != "utils/dev":
            continue
        if data.get("kind") != "build-workdir":
            continue
        candidates.append(marker.parent.resolve())
    if candidates:
        logger.info("Found marked workdirs: %s", ", ".join(str(p) for p in candidates))
    return candidates


def _find_tagged_networks(cmds: Cmds) -> list[str]:
    """Discover networks tagged with ``user.kive.devel.created-by=utils/dev``.

    Raises ``RuntimeError`` if the Incus query fails or returns unparseable
    output, so the caller can distinguish 'none found' from 'could not check'.
    """
    import json as _json
    result = cmds.incus.run(
        ["network", "list", "--format", "json"],
        check=False, capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"incus network list failed (rc={result.returncode}): "
            f"{result.stderr}"
        )
    out = result.stdout or ""
    if not out:
        return []
    try:
        networks = _json.loads(out)
    except _json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse incus network list output: {exc}") from exc
    tagged = []
    for net in networks:
        if not isinstance(net, dict):
            continue
        config = net.get("config", {})
        if config.get("user.kive.devel.created-by") == "utils/dev":
            name = net.get("name")
            if name:
                tagged.append(name)
    return tagged


def run_purge(args: argparse.Namespace) -> None:
    cmds = Cmds.create()
    cmds.incus.require()
    _run_purge(args, cmds)


def _run_purge(args: argparse.Namespace, cmds: Cmds) -> None:
    root = args.root.resolve()
    workdir_default = root / "tmp~" / "build"
    configure_logging(args, workdir_default)

    # Phase 1: Discover tagged instances.
    tagged = _find_tagged_instances(cmds)
    tagged_set = set(tagged)

    # Phase 2: Explicit --instance overrides, deduplicated with tagged.
    extra_instances = getattr(args, "instances", []) or []
    all_instances = list(tagged_set)
    for inst in extra_instances:
        if inst not in tagged_set:
            all_instances.append(inst)

    # Phase 3: Discover tagged networks.
    tagged_networks = _find_tagged_networks(cmds)

    # Phase 4: Discover marked workdirs.
    marked = _find_marked_workdirs(root)

    # Phase 4: Explicit --workdir overrides.
    extra_workdirs = getattr(args, "workdirs", []) or []

    all_workdirs = list(marked)
    for w in extra_workdirs:
        w_resolved = w.resolve() if isinstance(w, Path) else Path(w).resolve()
        if w_resolved not in all_workdirs:
            all_workdirs.append(w_resolved)

    registry_entries = _read_registry(root)
    has_registry = bool(registry_entries)

    if not all_instances and not all_workdirs and not tagged_networks and not has_registry:
        logger.info("No Kive development resources found to purge.")
        return

    logger.info("Resources to purge: %d instance(s), %d workdir(s), %d network(s)",
                 len(all_instances), len(all_workdirs), len(tagged_networks))

    deleted_instances = 0
    failed_instances = 0
    for instance in all_instances:
        if _device_attached(cmds, instance, "kive-code"):
            _remove_device(cmds, instance, "kive-code")
        if _device_attached(cmds, instance, "kive-web"):
            _remove_device(cmds, instance, "kive-web")
        del_result = cmds.incus.run(
            ["delete", "-f", "--", instance],
            check=False, capture_output=True,
        )
        if del_result.returncode == 0:
            deleted_instances += 1
        else:
            logger.warning("Failed to delete instance '%s': %s", instance, (del_result.stderr or "").strip())
            failed_instances += 1

    deleted_workdirs = 0
    failed_workdirs = 0
    for workdir in all_workdirs:
        if _remove_workdir_safely(workdir):
            deleted_workdirs += 1
        else:
            failed_workdirs += 1

    deleted_networks = 0
    retained_networks = 0
    failed_networks = 0
    if tagged_networks:
        import json as _json
        all_nets = ""
        nets_result = cmds.incus.run(
            ["network", "list", "--format", "json"],
            check=False, capture_output=True,
        )
        if nets_result.returncode == 0:
            all_nets = nets_result.stdout or ""
        nets_by_name = {}
        if all_nets:
            try:
                parsed = _json.loads(all_nets)
                for entry in parsed if isinstance(parsed, list) else []:
                    if isinstance(entry, dict):
                        name = entry.get("name", "")
                        nets_by_name[name] = entry
            except _json.JSONDecodeError:
                pass
        for net in tagged_networks:
            info = nets_by_name.get(net, {})
            used_by = info.get("used_by", []) if isinstance(info, dict) else []
            if used_by:
                logger.info(
                    "Skipping network '%s': still in use by %d resource(s).",
                    net, len(used_by),
                )
                retained_networks += 1
                continue
            logger.info("Removing managed network '%s'...", net)
            del_result = cmds.incus.run(
                ["network", "delete", net],
                check=False, capture_output=True,
            )
            if del_result.returncode != 0:
                logger.warning("Failed to delete network '%s': %s", net, (del_result.stderr or "").strip())
                failed_networks += 1
            else:
                deleted_networks += 1

    # Phase 5: Remove port forwards and registry (only when nothing remains).
    removed_port_forwards = 0
    failed_port_forwards = 0
    for entry in _read_registry(root):
        if entry.get("kind") == "host-forward":
            pid = entry.get("pid")
            if pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                    removed_port_forwards += 1
                except ProcessLookupError:
                    removed_port_forwards += 1
                except OSError:
                    failed_port_forwards += 1

    all_succeeded = (
        failed_instances == 0 and failed_workdirs == 0
        and failed_networks == 0 and retained_networks == 0
        and failed_port_forwards == 0
    )
    if all_succeeded:
        _remove_registry(root)
        logger.info(
            "Purge complete. All Kive development resources removed "
            "(%d instances, %d workdirs, %d networks, %d port forwards).",
            deleted_instances, deleted_workdirs, deleted_networks, removed_port_forwards,
        )
    else:
        logger.info(
            "Purge finished with %d owned resource(s) remaining. "
            "Deleted: %d instances, %d workdirs, %d networks, %d port forwards. "
            "Skipped: %d networks (in use). "
            "Failed: %d instances, %d workdirs, %d networks.",
            failed_instances + failed_workdirs + retained_networks + failed_networks,
            deleted_instances, deleted_workdirs, deleted_networks, removed_port_forwards,
            retained_networks,
            failed_instances, failed_workdirs, failed_networks,
        )


def _read_registry(root: Path) -> list[dict]:
    path = root / "tmp~" / _REGISTRY_NAME
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, list) else [data]
    except (json.JSONDecodeError, OSError):
        return []


def _remove_registry(root: Path) -> None:
    path = root / "tmp~" / _REGISTRY_NAME
    if path.exists():
        logger.info("Removing resource registry '%s'...", path)
        path.unlink()


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
