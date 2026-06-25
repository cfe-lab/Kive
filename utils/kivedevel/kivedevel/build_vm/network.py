from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")


def get_managed_networks(cmds: Cmds) -> list[dict]:
    """Return existing Incus managed bridge networks (JSON list from ``incus network list``)."""
    out = cmds.incus.output(["network", "list", "--format", "json"])
    if not out:
        return []
    try:
        networks = json.loads(out)
        return networks if isinstance(networks, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _network_names(networks: list[dict]) -> list[str]:
    return [n.get("name", "") for n in networks if n.get("type") == "bridge"]


def choose_existing_vm_network(cmds: Cmds, requested: str | None = None) -> str:
    """Choose an existing Incus managed network for VM NIC attachment.

    If *requested* is given and exists return it.
    Otherwise prefer ``incusbr0``, or the only existing managed bridge.
    Fail clearly if no usable network is found.
    """
    networks = get_managed_networks(cmds)
    names = _network_names(networks)

    if requested:
        if requested in names:
            return requested
        available = ", ".join(names) if names else "(none)"
        logger.error(
            "Requested VM network '%s' does not exist or is not a managed bridge.\n"
            "Available managed bridges: %s\n"
            "This command does not create or repair host networking.",
            requested, available,
        )
        sys.exit(1)

    if "incusbr0" in names:
        return "incusbr0"

    if len(names) == 1:
        return names[0]

    if len(names) > 1:
        logger.error(
            "Multiple existing Incus managed bridges found (%s).\n\n"
            "Local VM smoke install requires an existing Incus network with outbound "
            "internet access.\n"
            "Configure Incus networking outside Kive, or rerun with:\n\n"
            "  utils/dev smoke-local-install --vm-network NAME --debug\n\n"
            "This command does not create or repair host networking.",
            ", ".join(names),
        )
        sys.exit(1)

    logger.error(
        "No usable existing Incus managed network found.\n\n"
        "Local VM smoke install requires an existing Incus network with outbound "
        "internet access.\n"
        "Configure Incus networking outside Kive, or rerun with:\n\n"
        "  utils/dev smoke-local-install --vm-network NAME --debug\n\n"
        "This command does not create or repair host networking.",
    )
    sys.exit(1)


def _parse_device_block(text: str, device: str) -> dict[str, str]:
    """Parse a YAML-like device config block from ``incus config device show`` output.

    Returns a dict of key → value for the named device, or empty dict if not found.
    """
    in_block = False
    result: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.rstrip()
        if not in_block:
            if re.match(rf"^{re.escape(device)}:\s*$", line):
                in_block = True
            continue
        if re.match(r"^\S", line):
            break
        m = re.match(r"^\s+(\S+):\s*(.*?)\s*$", line)
        if m:
            result[m.group(1)] = m.group(2)
    return result


def _eth0_config(cmds: Cmds, instance: str) -> dict[str, str]:
    """Return eth0 device configuration from the instance's own devices.

    Returns empty dict if eth0 is not configured directly on the instance.
    """
    out = cmds.incus.output(["config", "device", "show", instance])
    return _parse_device_block(out, "eth0")


def _eth0_config_expanded(cmds: Cmds, instance: str) -> dict[str, str]:
    """Return eth0 device configuration from the expanded (profile-merged) view.

    Returns empty dict if eth0 is not present even through profiles.
    """
    out = cmds.incus.output(["config", "show", "--expanded", instance])
    return _parse_device_block(out, "eth0")


def ensure_vm_nic(cmds: Cmds, instance: str, network_name: str) -> bool:
    """Ensure the VM instance has an eth0 NIC attached to *network_name*.

    Returns True if a change was made (instance restart required).
    Cases handled:

    1. No eth0 device at all — add via ``incus config device add ... network=NAME``.
    2. eth0 exists through an expanded profile and provides usable networking — no-op.
    3. eth0 already has ``network=NETWORK_NAME`` — no-op.
    4. eth0 has a stale ``parent=kive-devel-br`` (old Kive-owned bridge) — replace.
    5. eth0 has an unrelated user-controlled parent/network — fail clearly.
    """
    device = _eth0_config(cmds, instance)

    if not device:
        # eth0 not on the instance directly; check expanded (profile) view
        expanded = _eth0_config_expanded(cmds, instance)
        if expanded:
            expanded_network = expanded.get("network", "")
            expanded_parent = expanded.get("parent", "")
            if expanded_network or expanded_parent:
                logger.info(
                    "eth0 provided by profile (network=%s, parent=%s); no instance-level change needed.",
                    expanded_network, expanded_parent,
                )
                return False
        logger.info("Adding NIC eth0 to %s (network=%s)...", instance, network_name)
        cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={network_name}"])
        return True

    existing_network = device.get("network", "")
    existing_parent = device.get("parent", "")
    existing_nictype = device.get("nictype", "")

    if existing_network == network_name:
        logger.debug("eth0 already has network=%s on %s.", network_name, instance)
        return False

    if existing_network and existing_network != network_name:
        logger.error(
            "eth0 on %s is already configured with network=%s (requested %s).\n"
            "Use --vm-network %s to match the existing network, or purge and retry.",
            instance, existing_network, network_name, existing_network,
        )
        sys.exit(1)

    if existing_parent:
        if existing_parent == "kive-devel-br":
            logger.info("Replacing old Kive NIC on %s (parent=%s) with network=%s...",
                        instance, existing_parent, network_name)
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={network_name}"])
            return True

        logger.error(
            "eth0 on %s has an unexpected parent=%s (nictype=%s).\n"
            "Remove or reconfigure it manually, then retry.",
            instance, existing_parent, existing_nictype,
        )
        sys.exit(1)

    if existing_nictype:
        logger.info("Replacing unknown eth0 config on %s (nictype=%s) with network=%s...",
                    instance, existing_nictype, network_name)
        cmds.incus.run(["config", "device", "remove", instance, "eth0"])
        cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={network_name}"])
        return True

    logger.info("Adding NIC eth0 to %s (network=%s)...", instance, network_name)
    cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={network_name}"])
    return True


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


def find_tagged_networks(root: Path) -> list[str]:
    return []
