from __future__ import annotations

import dataclasses
import json
import logging
import re
import sys
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")


@dataclasses.dataclass(frozen=True)
class VmNicTarget:
    """Describes how to attach a VM NIC to an existing bridge.

    ``managed=True`` means the bridge is an Incus-managed network and should
    be attached via ``network=NAME``.

    ``managed=False`` means the bridge is an unmanaged host bridge and should
    be attached via ``nictype=bridged parent=NAME``.
    """
    name: str
    managed: bool


def get_existing_bridges(cmds: Cmds) -> list[dict]:
    """Return existing Incus-visible bridge networks (JSON list from ``incus network list``).

    Each entry has at least ``name``, ``type``, and ``managed`` fields.
    """
    out = cmds.incus.output(["network", "list", "--format", "json"])
    if not out:
        return []
    try:
        networks = json.loads(out)
        return networks if isinstance(networks, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _managed_bridge_names(bridges: list[dict]) -> list[str]:
    """Return names of Incus-managed bridges (``managed == True``)."""
    return [
        b.get("name", "") for b in bridges
        if b.get("type") == "bridge" and b.get("managed") is True
    ]


def choose_existing_vm_network(cmds: Cmds, requested: str | None = None) -> VmNicTarget:
    """Choose an existing bridge for VM NIC attachment.

    Auto-detection only considers **managed** Incus networks (where Incus
    provides DHCP + NAT).  Unmanaged host bridges are never auto-selected
    because they may lack DHCP/NAT/outbound internet.

    If a bridge is explicitly requested via ``--vm-network NAME`` the
    request is accepted regardless of the managed flag, but a warning is
    logged for unmanaged bridges.
    """
    bridges = get_existing_bridges(cmds)
    bridge_map: dict[str, dict] = {}
    for b in bridges:
        name = b.get("name", "")
        if b.get("type") == "bridge" and name:
            bridge_map[name] = b

    if requested:
        info = bridge_map.get(requested)
        if info is not None:
            managed = bool(info.get("managed", False))
            if not managed:
                logger.warning(
                    "Bridge %s is not an Incus-managed network. "
                    "Kive cannot guarantee DHCP, NAT, or outbound internet on "
                    "unmanaged host bridges. The guest preflight will verify "
                    "connectivity before provisioning.",
                    requested,
                )
            return VmNicTarget(name=requested, managed=managed)
        available = ", ".join(bridge_map) if bridge_map else "(none)"
        logger.error(
            "Requested VM network '%s' does not exist or is not a bridge.\n"
            "Available bridges: %s\n"
            "This command does not create or repair host networking.",
            requested, available,
        )
        sys.exit(1)

    managed_names = _managed_bridge_names(bridges)

    if "incusbr0" in managed_names:
        return VmNicTarget(name="incusbr0", managed=True)

    if len(managed_names) == 1:
        return VmNicTarget(name=managed_names[0], managed=True)

    if len(managed_names) > 1:
        logger.error(
            "Multiple Incus-managed networks found (%s).\n\n"
            "Local VM smoke install requires an existing Incus-managed network "
            "with outbound internet access.\n"
            "Configure Incus networking outside Kive, or rerun with:\n\n"
            "  utils/dev smoke-local-install --vm-network NAME --debug\n\n"
            "This command does not create or repair host networking.",
            ", ".join(managed_names),
        )
        sys.exit(1)

    if bridge_map:
        logger.error(
            "No Incus-managed network found. Only unmanaged host bridges are "
            "available (%s).\n\n"
            "Local VM smoke install requires an Incus-managed network with "
            "outbound internet access.\n"
            "If you have a host bridge that provides DHCP/NAT, rerun with:\n\n"
            "  utils/dev smoke-local-install --vm-network NAME --debug\n\n"
            "This command does not create or repair host networking.",
            ", ".join(bridge_map),
        )
        sys.exit(1)

    logger.error(
        "No usable existing bridge found.\n\n"
        "Local VM smoke install requires an existing Incus-managed network with "
        "outbound internet access.\n"
        "Configure Incus networking outside Kive, or rerun with:\n\n"
        "  utils/dev smoke-local-install --vm-network NAME --debug\n\n"
        "This command does not create or repair host networking.",
    )
    sys.exit(1)


def _bridge_has_ipv4(bridge_name: str) -> bool:
    """Check whether a host bridge has an IPv4 address.

    Uses ``ip -4 addr show dev <name>``.  Returns ``True`` if at least one
    ``inet`` address (not link-local) is present.
    """
    import subprocess as _sp
    try:
        r = _sp.run(["ip", "-4", "addr", "show", "dev", bridge_name],
                    capture_output=True, text=True, timeout=10, check=False)
        if r.returncode != 0:
            return False
        for line in r.stdout.splitlines():
            if "inet " in line and "scope global" in line:
                return True
        return False
    except Exception:
        return False


def check_vm_network_target_usable(cmds: Cmds, target: VmNicTarget) -> bool:
    """Verify the selected NIC target is likely to provide IPv4 connectivity.

    For **managed** Incus networks: checks that ``ipv4.address`` is
    configured (Incus provides DHCP + NAT automatically).

    For **unmanaged** host bridges: host-side checks are unreliable — the
    guest preflight is authoritative.  Returns ``True`` with a warning so
    the guest can validate at runtime.
    """
    if target.managed:
        ipv4 = cmds.incus.output(["network", "get", target.name, "ipv4.address"])
        if ipv4 and ipv4 != "none":
            return True
        logger.warning(
            "Managed network %s has no IPv4 address configured (ipv4.address=%s).",
            target.name, ipv4 or "(empty)",
        )
        return False

    logger.info(
        "Unmanaged host bridge %s — Kive cannot verify DHCP/NAT from the host. "
        "The guest network preflight will validate connectivity.",
        target.name,
    )
    return True


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
    """Return eth0 device configuration from the instance's own devices."""
    out = cmds.incus.output(["config", "device", "show", instance])
    return _parse_device_block(out, "eth0")


def _eth0_config_expanded(cmds: Cmds, instance: str) -> dict[str, str]:
    """Return eth0 device configuration from the expanded (profile-merged) view."""
    out = cmds.incus.output(["config", "show", "--expanded", instance])
    return _parse_device_block(out, "eth0")


def _nic_args(target: VmNicTarget) -> list[str]:
    """Return the ``incus config device add`` arguments for the given target."""
    if target.managed:
        return ["nic", f"network={target.name}"]
    return ["nic", "nictype=bridged", f"parent={target.name}"]


def ensure_vm_nic(cmds: Cmds, instance: str, target: VmNicTarget) -> bool:
    """Ensure the VM instance has an eth0 NIC matching *target*.

    Returns True if a change was made (instance restart required).

    * No eth0 → add using the target's mode.
    * Profile-provided eth0 with matching network/parent → no-op.
    * Existing eth0 with ``network=NAME`` and target is managed same NAME → no-op.
    * Existing eth0 with ``parent=NAME`` and target is unmanaged same NAME → no-op.
    * Existing stale ``parent=kive-devel-br`` → replace (for stopped instances).
    * Existing unrelated network/parent → fail clearly.
    """
    device = _eth0_config(cmds, instance)

    if not device:
        expanded = _eth0_config_expanded(cmds, instance)
        if expanded:
            en = expanded.get("network", "")
            ep = expanded.get("parent", "")
            if en or ep:
                logger.info(
                    "eth0 provided by profile (network=%s, parent=%s); no instance-level change needed.",
                    en, ep,
                )
                return False
        logger.info("Adding NIC eth0 to %s (%s)...", instance, " ".join(_nic_args(target)))
        cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", *_nic_args(target)[1:]])
        return True

    existing_network = device.get("network", "")
    existing_parent = device.get("parent", "")
    existing_nictype = device.get("nictype", "")

    if target.managed:
        if existing_network == target.name:
            logger.debug("eth0 already has network=%s on %s.", target.name, instance)
            return False
        if existing_network and existing_network != target.name:
            logger.error(
                "eth0 on %s is already configured with network=%s (requested %s).\n"
                "Use --vm-network %s to match the existing network, or purge and retry.",
                instance, existing_network, target.name, existing_network,
            )
            sys.exit(1)
    else:
        if existing_parent == target.name:
            logger.debug("eth0 already has parent=%s on %s.", target.name, instance)
            return False
        if existing_parent and existing_parent != target.name and existing_parent != "kive-devel-br":
            logger.error(
                "eth0 on %s is already configured with parent=%s (requested %s).\n"
                "Use --vm-network %s to match, or purge and retry.",
                instance, existing_parent, target.name, existing_parent,
            )
            sys.exit(1)

    if existing_parent == "kive-devel-br":
        logger.info("Replacing old Kive NIC on %s (parent=%s) with %s...",
                    instance, existing_parent, " ".join(_nic_args(target)))
        cmds.incus.run(["config", "device", "remove", instance, "eth0"])
        cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", *_nic_args(target)[1:]])
        return True

    if existing_nictype and not existing_parent and not existing_network:
        logger.info("Replacing unknown eth0 config on %s (nictype=%s) with %s...",
                    instance, existing_nictype, " ".join(_nic_args(target)))
        cmds.incus.run(["config", "device", "remove", instance, "eth0"])
        cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", *_nic_args(target)[1:]])
        return True

    logger.info("Adding NIC eth0 to %s (%s)...", instance, " ".join(_nic_args(target)))
    cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", *_nic_args(target)[1:]])
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
