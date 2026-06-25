from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")

_RESOURCE_MARKER = ".kive-devel-resource.json"


def _registry_path(root: Path) -> Path:
    return root / "tmp~" / ".kive-devel-resources.json"


def _read_registry(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, list) else [data]
    except (json.JSONDecodeError, OSError):
        return []


def _register_resource(path: Path, entry: dict) -> None:
    entries = _read_registry(path)
    entries.append(entry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, indent=2) + "\n")


def _registry_has(path: Path, kind: str, name: str) -> bool:
    for entry in _read_registry(path):
        if entry.get("kind") == kind and entry.get("name") == name:
            return True
    return False


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


def _owns_bridge(root: Path, bridge: str) -> bool:
    return _registry_has(_registry_path(root), "linux-bridge", bridge)


def _bridge_exists(cmds: Cmds, bridge: str) -> bool:
    return cmds.ip.ok(["link", "show", bridge])


def _create_owned_bridge(cmds: Cmds, root: Path, bridge: str, cidr: str) -> None:
    logger.info("Creating owned bridge %s (%s)...", bridge, cidr)
    try:
        cmds.ip.run(["link", "add", bridge, "type", "bridge"], sudo=True)
    except Exception:
        logger.error("Failed to create bridge '%s'.", bridge)
        sys.exit(1)

    try:
        cmds.ip.run(["addr", "add", cidr, "dev", bridge], sudo=True)
    except Exception:
        logger.error("Failed to add address %s to bridge '%s'.", cidr, bridge)
        _delete_bridge(cmds, bridge)
        sys.exit(1)

    try:
        cmds.ip.run(["link", "set", bridge, "up"], sudo=True)
    except Exception:
        logger.error("Failed to bring bridge '%s' up.", bridge)
        _delete_bridge(cmds, bridge)
        sys.exit(1)

    reg = _registry_path(root)
    _register_resource(reg, {
        "type": "linux-bridge", "name": bridge,
        "created_by": "utils/dev", "project": "Kive", "kind": "linux-bridge",
    })
    _register_resource(reg, {
        "type": "bridge-ip", "name": cidr,
        "created_by": "utils/dev", "project": "Kive", "kind": "bridge-ip",
    })
    logger.info("Registered owned bridge %s.", bridge)


def _ensure_ip_forward() -> None:
    """Check ``net.ipv4.ip_forward`` is enabled and fail early if not."""
    import subprocess as _sp
    result = _sp.run(["sysctl", "-n", "net.ipv4.ip_forward"], capture_output=True, text=True, check=False)
    val = result.stdout.strip()
    if val != "1":
        logger.error(
            "IP forwarding is disabled (net.ipv4.ip_forward=%s). "
            "VM guests will not be able to reach the internet through the NAT.\n"
            "Enable it with:  sudo sysctl -w net.ipv4.ip_forward=1",
            val,
        )
        sys.exit(1)


def _setup_host_nat(cmds: Cmds, root: Path, bridge_name: str, cidr: str) -> None:
    logger.info("Setting up NAT for bridge %s (%s)...", bridge_name, cidr)
    _ensure_ip_forward()
    try:
        cmds.nft.run(["add", "table", "inet", "kive_devel"], sudo=True)
    except Exception:
        logger.warning("nftables table 'kive_devel' may already exist (non-fatal).")

    try:
        cmds.nft.run([
            "add", "chain", "inet", "kive_devel", "postrouting",
            "{ type nat hook postrouting priority srcnat ; }",
        ], sudo=True)
    except Exception:
        logger.warning("nftables chain 'kive_devel.postrouting' may already exist (non-fatal).")

    try:
        prefix_len = cidr.split("/")[1]
        network_base = cidr.rsplit(".", 1)[0]
        network = f"{network_base}.0/{prefix_len}"
        cmds.nft.run([
            "add", "rule", "inet", "kive_devel", "postrouting",
            f"ip saddr {network} masquerade",
        ], sudo=True)
    except Exception:
        logger.error("Failed to add NAT masquerade rule for %s.", network)
        sys.exit(1)

    reg = _registry_path(root)
    _register_resource(reg, {
        "type": "nft-table", "name": "inet kive_devel",
        "created_by": "utils/dev", "project": "Kive", "kind": "nft-table",
    })


def _delete_bridge(cmds: Cmds, bridge: str) -> None:
    logger.info("Deleting owned bridge %s...", bridge)
    cmds.ip.run(["link", "delete", bridge], sudo=True, check=False)


def ensure_owned_bridge(cmds: Cmds, root: Path, bridge_name: str, cidr: str, vm_ip: str, workdir: Path) -> tuple[str, str]:
    if _owns_bridge(root, bridge_name):
        logger.debug("Owned bridge %s already registered.", bridge_name)
        return cidr, vm_ip

    if _bridge_exists(cmds, bridge_name):
        logger.error(
            "Bridge '%s' already exists but is not recorded as owned by utils/dev.\n"
            "Choose a different --vm-network name or remove the conflicting resource manually.",
            bridge_name,
        )
        sys.exit(1)

    _create_owned_bridge(cmds, root, bridge_name, cidr)
    _setup_host_nat(cmds, root, bridge_name, cidr)

    return cidr, vm_ip


def _eth0_exists(cmds: Cmds, instance: str) -> bool:
    out = cmds.incus.output(["config", "device", "list", instance])
    return bool(re.search(r"^eth0\s*$", out, re.MULTILINE))


def ensure_vm_nic(cmds: Cmds, instance: str, bridge_name: str) -> bool:
    if _eth0_exists(cmds, instance):
        logger.debug("NIC eth0 already exists on %s.", instance)
        return False
    logger.info("Adding NIC eth0 to %s (bridge=%s)...", instance, bridge_name)
    cmds.incus.run(
        [
            "config", "device", "add",
            instance, "eth0", "nic",
            "nictype=bridged",
            f"parent={bridge_name}",
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
