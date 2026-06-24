from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")

_RESOURCE_MARKER = ".kive-devel-resource.json"


def _network_marker_path(workdir: Path) -> Path:
    return workdir / _RESOURCE_MARKER


def _read_marker_entries(marker_path: Path) -> list[dict]:
    if not marker_path.exists():
        return []
    try:
        data = json.loads(marker_path.read_text())
        return data if isinstance(data, list) else [data]
    except (json.JSONDecodeError, OSError):
        return []


def _write_marker_entry(marker_path: Path, entry: dict) -> None:
    entries = _read_marker_entries(marker_path)
    entries.append(entry)
    marker_path.write_text(json.dumps(entries, indent=2) + "\n")


def _network_owned_by_marker(workdir: Path, network: str) -> bool:
    for entry in _read_marker_entries(_network_marker_path(workdir)):
        if entry.get("kind") == "network" and entry.get("name") == network:
            return True
    return False


def get_default_host_interface(cmds: Cmds) -> str:
    if cmds.ip.ok(["link", "show", "docker0"]):
        return "docker0"
    out = cmds.ip.output(["route", "get", "8.8.8.8"])
    m = re.search(r"dev\s+(\S+)", out)
    if m:
        return m.group(1)
    out = cmds.ip.output(["route"])
    for line in out.splitlines():
        m = re.search(r"default.*dev\s+(\S+)", line)
        if m:
            return m.group(1)
    return ""


def get_bridge_cidr(cmds: Cmds, iface: str) -> str:
    out = cmds.ip.output(["-o", "-f", "inet", "addr", "show", iface])
    parts = out.split()
    return parts[3] if len(parts) >= 4 else ""


def ensure_network_device(cmds: Cmds, instance: str, host_interface: str) -> bool:
    out = cmds.incus.output(["config", "device", "list", instance])
    if re.search(r"^eth0\s*$", out, re.MULTILINE):
        return False
    expanded = cmds.incus.output(["config", "show", "--expanded", instance])
    if re.search(r"^  eth0:\s*$", expanded, re.MULTILINE):
        logger.info("Network device eth0 already configured via profile on %s.", instance)
        return False
    if not host_interface:
        host_interface = get_default_host_interface(cmds)
        if not host_interface:
            logger.error("Unable to determine host network interface for VM network device.")
            sys.exit(1)
    nictype = "bridged" if (host_interface == "docker0" or host_interface.startswith("br-")) else "macvlan"
    logger.info("Adding network device eth0 on %s (nictype=%s)...", host_interface, nictype)
    cmds.incus.run(
        [
            "config",
            "device",
            "add",
            instance,
            "eth0",
            "nic",
            f"nictype={nictype}",
            f"parent={host_interface}",
        ]
    )
    return True


def get_existing_network_parent(cmds: Cmds, instance: str) -> str:
    """Return the configured parent interface for eth0, if present."""
    out = cmds.incus.output(["config", "device", "show", instance])
    if not out:
        return ""

    in_eth0 = False
    for raw in out.splitlines():
        line = raw.rstrip()
        if re.match(r"^\S", line):
            in_eth0 = bool(re.match(r"^eth0:\s*$", line))
            continue
        if not in_eth0:
            continue
        m = re.match(r"^\s+parent:\s*(\S+)\s*$", line)
        if m:
            return m.group(1)
    return ""


def ensure_vm_network(cmds: Cmds, network: str, cidr: str, vm_ip: str, workdir: Path) -> tuple[str, str]:
    if _network_owned_by_marker(workdir, network):
        logger.debug("Owned network %s already exists.", network)
        return cidr, vm_ip

    out = cmds.incus.output(["network", "list", "--format", "csv", "--columns", "n"])
    networks = [line.strip() for line in out.splitlines() if line.strip()]

    if network in networks:
        logger.error(
            "Network '%s' already exists but is not recorded as owned by utils/dev.\n"
            "Choose a different --vm-network name or remove the conflicting resource manually.",
            network,
        )
        sys.exit(1)

    logger.info("Creating managed network %s (%s)...", network, cidr)
    try:
        cmds.incus.run(
            [
                "network", "create", "--type=bridge", network,
                f"ipv4.address={cidr}",
                "ipv4.nat=true",
                "ipv6.address=none",
            ]
        )
    except Exception:
        logger.error(
            "Failed to create bridge network '%s'.\n"
            "  Verify Incus is properly initialized.",
            network,
        )
        sys.exit(1)

    marker_path = _network_marker_path(workdir)
    _write_marker_entry(marker_path, {
        "type": "incus-network",
        "name": network,
        "created_by": "utils/dev",
        "project": "Kive",
        "kind": "network",
    })
    logger.info("Recorded managed network %s in %s", network, marker_path)

    # Best-effort: tag the network in Incus for in-app visibility.
    # Failure must not block build-vm.
    try:
        cmds.incus.run(
            [
                "network", "set", network,
                "user.kive.devel.created-by=utils/dev",
                "user.kive.devel.project=Kive",
                "user.kive.devel.kind=network",
            ],
            check=False,
        )
    except Exception:
        logger.debug("Best-effort network metadata set failed (non-fatal).")

    return cidr, vm_ip


def _eth0_exists(cmds: Cmds, instance: str) -> bool:
    out = cmds.incus.output(["config", "device", "list", instance])
    return bool(re.search(r"^eth0\s*$", out, re.MULTILINE))


def ensure_vm_nic(cmds: Cmds, instance: str, network: str, static_ip: str) -> bool:
    if _eth0_exists(cmds, instance):
        logger.debug("NIC eth0 already exists on %s.", instance)
        return False
    logger.info("Adding NIC eth0 to %s (network=%s, ipv4.address=%s)...", instance, network, static_ip)
    cmds.incus.run(
        [
            "config", "device", "add",
            instance, "eth0", "nic",
            f"network={network}",
            f"ipv4.address={static_ip}",
        ]
    )
    return True


def find_tagged_networks(root: Path) -> list[str]:
    tagged = []
    for marker in root.rglob(_RESOURCE_MARKER):
        for entry in _read_marker_entries(marker):
            if entry.get("kind") == "network" and entry.get("created_by") == "utils/dev":
                name = entry.get("name")
                if name:
                    tagged.append(name)
    if tagged:
        logger.info("Found tagged networks: %s", ", ".join(tagged))
    return tagged
