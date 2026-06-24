from __future__ import annotations

import json
import logging
import re
import subprocess
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
    # Check per-instance devices first, then profile-inherited devices via expanded config.
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


def _cidr_in_use(cmds: Cmds, cidr: str) -> bool:
    out = cmds.ip.output(["route", "show", cidr])
    return bool(out.strip())


def _derive_vm_ip_from_cidr(cidr: str) -> str:
    base = cidr.rsplit(".", 1)[0]
    return f"{base}.80"


def _get_network_info(cmds: Cmds, name: str) -> dict | None:
    """Inspect an Incus network via ``incus network show``.

    Returns ``None`` if the network does not exist in Incus.
    Otherwise returns a dict with keys ``name``, ``managed`` (bool),
    and ``ipv4_address`` (str, possibly empty).
    """
    try:
        out = cmds.incus.output(["network", "show", name])
    except Exception:
        return None
    if not out:
        return None
    import yaml
    try:
        data = yaml.safe_load(out)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    config = data.get("config") or {}
    if not isinstance(config, dict):
        config = {}
    return {
        "name": data.get("name", name),
        "managed": bool(data.get("managed", False)),
        "ipv4_address": config.get("ipv4.address", ""),
    }


def _repair_bridge_nat(cmds: Cmds, bridge: str) -> None:
    for key in ("ipv4.nat", "ipv4.routing", "ipv4.firewall"):
        cmds.incus.run(["network", "set", bridge, f"{key}=true"])
    subprocess.run(
        ["sysctl", "-w", "net.ipv4.ip_forward=1"],
        check=True, capture_output=True, text=True,
    )


def ensure_vm_network(cmds: Cmds, network: str, cidr: str, vm_ip: str, workdir: Path) -> tuple[str, str]:
    info = _get_network_info(cmds, network)

    if info is not None:
        if not info["managed"]:
            logger.error(
                "Network '%s' exists but is not an Incus-managed bridge.\n"
                "It may be an OS-level bridge that Incus cannot configure.\n"
                "Use a different managed network name, for example:\n"
                "  utils/dev prepare-host --bridge kivebr0 --debug\n"
                "  utils/dev build-vm --vm-network kivebr0",
                network,
            )
            sys.exit(1)

        ipv4_address = info["ipv4_address"]
        if not ipv4_address:
            logger.error(
                "Network '%s' is managed but has no ipv4.address configured.\n"
                "Run: utils/dev prepare-host --bridge %s --debug",
                network, network,
            )
            sys.exit(1)

        if network == "incusbr0":
            logger.debug("Using existing managed bridge %s.", network)
            _repair_bridge_nat(cmds, network)
            actual_vm_ip = _derive_vm_ip_from_cidr(ipv4_address)
            return ipv4_address, actual_vm_ip

        if _network_owned_by_marker(workdir, network):
            logger.debug("Managed network %s already exists and is owned by utils/dev.", network)
            return cidr, vm_ip

        logger.error(
            "Network '%s' already exists but is not owned by utils/dev.\n"
            "Use --vm-network to specify a different network name.",
            network,
        )
        sys.exit(1)

    if network == "incusbr0":
        logger.error(
            "Default bridge '%s' is not configured in Incus.\n"
            "Run: utils/dev prepare-host --bridge incusbr0 --debug",
            network,
        )
        sys.exit(1)

    if _cidr_in_use(cmds, cidr):
        logger.error(
            "CIDR %s is already in use on this host.\n"
            "Use --vm-cidr to specify a different network CIDR.",
            cidr,
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
            "Failed to create managed bridge network '%s'.\n"
            "  The Incus bridge creation command failed. This may be a known bug\n"
            "  in this version of Incus ('Can't parse a version: UNKNOWN').\n"
            "  Workarounds:\n"
            "    - Run: utils/dev prepare-host --bridge incusbr0 --debug\n"
            "    - Or use an existing bridge: --vm-network incusbr0\n"
            "  Failing command: incus network create --type=bridge %s ipv4.address=%s ipv4.nat=true ipv6.address=none",
            network, network, cidr,
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
