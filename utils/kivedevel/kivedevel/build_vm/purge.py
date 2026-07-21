from __future__ import annotations

import argparse
import dataclasses
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


@dataclasses.dataclass(frozen=True)
class NetworkInfo:
    name: str
    used_by: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class HostForwardEntry:
    pid: int
    port: int
    vm_ip: str
    kind: str = "host-forward"
    created_by: str = "utils/dev"

    @classmethod
    def from_dict(cls, d: dict) -> HostForwardEntry:
        kind = d.get("kind")
        if kind != "host-forward":
            raise RuntimeError(f"Expected kind 'host-forward', got {kind!r}")
        if d.get("created_by") != "utils/dev":
            raise RuntimeError(
                f"Expected created_by 'utils/dev', got {d.get('created_by')!r}")
        pid = d.get("pid")
        if not isinstance(pid, int) or pid <= 0:
            raise RuntimeError(f"Invalid or missing PID: {pid!r}")
        port = d.get("port")
        if not isinstance(port, int) or port <= 0 or port > 65535:
            raise RuntimeError(f"Invalid or missing port: {port!r}")
        vm_ip = d.get("vm_ip")
        if not isinstance(vm_ip, str) or not vm_ip:
            raise RuntimeError(f"Invalid or missing vm_ip: {vm_ip!r}")
        return cls(kind=kind, created_by="utils/dev", pid=pid, port=port, vm_ip=vm_ip)


@dataclasses.dataclass(frozen=True)
class PurgeInventory:
    instances: tuple[str, ...]
    networks: tuple[NetworkInfo, ...]
    workdirs: tuple[Path, ...]
    registry_entries: tuple[HostForwardEntry, ...]

    @property
    def total_count(self) -> int:
        return (
            len(self.instances) + len(self.workdirs)
            + len(self.networks) + len(self.registry_entries)
        )


@dataclasses.dataclass
class PurgeOutcome:
    deleted_instances: int = 0
    failed_instances: int = 0
    deleted_workdirs: int = 0
    failed_workdirs: int = 0
    deleted_networks: int = 0
    retained_networks: int = 0
    failed_networks: int = 0
    removed_port_forwards: int = 0
    already_absent_port_forwards: int = 0
    failed_port_forwards: int = 0
    failed_forward_entries: list[HostForwardEntry] = dataclasses.field(default_factory=list)

    @property
    def all_succeeded(self) -> bool:
        return (
            self.failed_instances == 0
            and self.failed_workdirs == 0
            and self.failed_networks == 0
            and self.retained_networks == 0
            and self.failed_port_forwards == 0
        )

    @property
    def remaining_count(self) -> int:
        return (
            self.failed_instances
            + self.failed_workdirs
            + self.retained_networks
            + self.failed_networks
            + self.failed_port_forwards
        )


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
    result = cmds.incus.run(
        ["list", "--format", "json"],
        check=False, capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"incus list failed (rc={result.returncode}): {result.stderr}")
    out = result.stdout or ""
    if not out:
        raise RuntimeError("incus list returned empty output")
    try:
        instances = json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Failed to parse incus list output: {exc}") from exc
    if not isinstance(instances, list):
        raise RuntimeError(
            f"Expected incus list to return a JSON list, got {type(instances).__name__}")
    found = []
    for entry in instances:
        if not isinstance(entry, dict):
            raise RuntimeError(
                f"Expected JSON object entry in incus list, got {type(entry).__name__}")
        name = entry.get("name")
        if not isinstance(name, str):
            raise RuntimeError(
                f"Instance entry missing or non-string name: {entry}")
        config = entry.get("config")
        if not isinstance(config, dict):
            raise RuntimeError(
                f"Instance {name} has missing or non-dict config: {config}")
        if config.get("user.kive.devel.created-by") == "utils/dev":
            found.append(name)
    if found:
        logger.info("Found tagged instances: %s", ", ".join(found))
    return found


def _find_marked_workdirs(root: Path) -> list[Path]:
    candidates = []
    for marker in root.rglob(_RESOURCE_MARKER):
        try:
            data = json.loads(marker.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            raise RuntimeError(
                f"Malformed resource marker at {marker}: {exc}") from exc
        if not isinstance(data, dict):
            raise RuntimeError(
                f"Resource marker at {marker} is not a JSON object: {data!r}")
        if data.get("created_by") != "utils/dev":
            continue
        if data.get("kind") != "build-workdir":
            continue
        candidates.append(marker.parent.resolve())
    if candidates:
        logger.info("Found marked workdirs: %s", ", ".join(str(p) for p in candidates))
    return candidates


def _find_tagged_networks(cmds: Cmds) -> list[NetworkInfo]:
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
        raise RuntimeError("incus network list returned empty output")
    try:
        networks = json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse incus network list output: {exc}") from exc
    if not isinstance(networks, list):
        raise RuntimeError(
            f"Expected incus network list to return a JSON list, "
            f"got {type(networks).__name__}")
    tagged = []
    for net in networks:
        if not isinstance(net, dict):
            raise RuntimeError(
                f"Expected JSON object in network list, got {type(net).__name__}")
        name = net.get("name")
        if not isinstance(name, str):
            raise RuntimeError(f"Network entry missing string 'name': {net}")
        config = net.get("config", {})
        if not isinstance(config, dict):
            raise RuntimeError(f"Network {name} has non-dict config: {config}")
        if config.get("user.kive.devel.created-by") == "utils/dev":
            used_by_raw = net.get("used_by", [])
            if not isinstance(used_by_raw, list):
                raise RuntimeError(
                    f"Network {name} has non-list used_by: {used_by_raw}")
            for u in used_by_raw:
                if not isinstance(u, str):
                    raise RuntimeError(
                        f"Network {name} has non-string used_by member: {u!r}")
            used_by = tuple(used_by_raw)
            tagged.append(NetworkInfo(name=name, used_by=used_by))
    return tagged


def build_purge_inventory(args: argparse.Namespace, cmds: Cmds) -> PurgeInventory:
    root = args.root.resolve()

    tagged = _find_tagged_instances(cmds)
    tagged_set = set(tagged)

    extra_instances = args.instances or []
    all_instances = list(tagged_set)
    for inst in extra_instances:
        if inst not in tagged_set:
            all_instances.append(inst)

    networks = _find_tagged_networks(cmds)

    marked = _find_marked_workdirs(root)
    extra_workdirs = args.workdirs or []
    all_workdirs = list(marked)
    for w in extra_workdirs:
        w_resolved = w.resolve() if isinstance(w, Path) else Path(w).resolve()
        if w_resolved not in all_workdirs:
            all_workdirs.append(w_resolved)

    registry_entries = _read_registry(root)

    return PurgeInventory(
        instances=tuple(all_instances),
        networks=tuple(networks),
        workdirs=tuple(all_workdirs),
        registry_entries=tuple(registry_entries),
    )


def execute_purge(inventory: PurgeInventory, cmds: Cmds, root: Path) -> PurgeOutcome:
    outcome = PurgeOutcome()

    for instance in inventory.instances:
        if _device_attached(cmds, instance, "kive-code"):
            _remove_device(cmds, instance, "kive-code")
        if _device_attached(cmds, instance, "kive-web"):
            _remove_device(cmds, instance, "kive-web")
        del_result = cmds.incus.run(
            ["delete", "-f", "--", instance],
            check=False, capture_output=True,
        )
        if del_result.returncode == 0:
            outcome.deleted_instances += 1
        else:
            logger.warning("Failed to delete instance '%s': %s", instance, (del_result.stderr or "").strip())
            outcome.failed_instances += 1

    for workdir in inventory.workdirs:
        if _remove_workdir_safely(workdir):
            outcome.deleted_workdirs += 1
        else:
            outcome.failed_workdirs += 1

    for net in inventory.networks:
        if net.used_by:
            logger.info(
                "Skipping network '%s': still in use by %d resource(s).",
                net.name, len(net.used_by),
            )
            outcome.retained_networks += 1
            continue
        logger.info("Removing managed network '%s'...", net.name)
        del_result = cmds.incus.run(
            ["network", "delete", net.name],
            check=False, capture_output=True,
        )
        if del_result.returncode != 0:
            logger.warning("Failed to delete network '%s': %s", net.name, (del_result.stderr or "").strip())
            outcome.failed_networks += 1
        else:
            outcome.deleted_networks += 1

    for entry in inventory.registry_entries:
        pid = entry.pid
        try:
            os.kill(pid, signal.SIGTERM)
            outcome.removed_port_forwards += 1
        except ProcessLookupError:
            outcome.already_absent_port_forwards += 1
        except OSError:
            outcome.failed_port_forwards += 1
            outcome.failed_forward_entries.append(entry)

    _rewrite_registry(root, outcome)

    return outcome


def _rewrite_registry(root: Path, outcome: PurgeOutcome) -> None:
    path = root / "tmp~" / _REGISTRY_NAME
    if outcome.all_succeeded and outcome.remaining_count == 0:
        if path.exists():
            logger.info("Removing resource registry '%s'...", path)
            path.unlink()
        return

    remaining_entries = [dataclasses.asdict(e) for e in outcome.failed_forward_entries]
    if remaining_entries:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(remaining_entries, indent=2) + "\n")
        logger.info("Registry updated with %d remaining entry(ies).", len(remaining_entries))
    else:
        if path.exists():
            logger.info("Removing resource registry '%s'...", path)
            path.unlink()


def _read_registry(root: Path) -> list[HostForwardEntry]:
    path = root / "tmp~" / _REGISTRY_NAME
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"Malformed registry file {path}: {exc}") from exc
    if not isinstance(data, list):
        raise RuntimeError(
            f"Registry at {path} is not a JSON list; got {type(data).__name__}")
    entries: list[HostForwardEntry] = []
    for entry in data:
        if not isinstance(entry, dict):
            raise RuntimeError(
                f"Registry entry is not a JSON object: {entry!r}")
        kind = entry.get("kind")
        if kind == "host-forward":
            entries.append(HostForwardEntry.from_dict(entry))
        else:
            raise RuntimeError(
                f"Unknown registry entry kind {kind!r}: {entry!r}")
    return entries


def run_purge(args: argparse.Namespace) -> None:
    cmds = Cmds.create()
    cmds.incus.require()
    _run_purge(args, cmds)


def _run_purge(args: argparse.Namespace, cmds: Cmds) -> None:
    root = args.root.resolve()
    workdir_default = root / "tmp~" / "build"
    configure_logging(args, workdir_default)

    inventory = build_purge_inventory(args, cmds)

    if inventory.total_count == 0:
        logger.info("No Kive development resources found to purge.")
        return

    logger.info(
        "Resources to purge: %d instance(s), %d workdir(s), %d network(s), %d registry entry(ies)",
        len(inventory.instances), len(inventory.workdirs),
        len(inventory.networks), len(inventory.registry_entries),
    )

    outcome = execute_purge(inventory, cmds, root)

    if outcome.all_succeeded:
        logger.info(
            "Purge complete. All Kive development resources removed "
            "(%d instances, %d workdirs, %d networks, %d port forwards).",
            outcome.deleted_instances, outcome.deleted_workdirs,
            outcome.deleted_networks, outcome.removed_port_forwards,
        )
    else:
        logger.info(
            "Purge finished with %d owned resource(s) remaining. "
            "Deleted: %d instances, %d workdirs, %d networks, %d port forwards. "
            "Skipped: %d networks (in use). "
            "Failed: %d instances, %d workdirs, %d networks, %d port forwards.",
            outcome.remaining_count,
            outcome.deleted_instances, outcome.deleted_workdirs,
            outcome.deleted_networks, outcome.removed_port_forwards,
            outcome.retained_networks,
            outcome.failed_instances, outcome.failed_workdirs,
            outcome.failed_networks, outcome.failed_port_forwards,
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
