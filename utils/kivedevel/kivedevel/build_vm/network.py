from __future__ import annotations

import logging
import re
import sys

import yaml

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")


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


_KIVE_NET_OWNER = "user.kive.devel.created-by"


def _network_config(cmds: Cmds, network: str) -> dict:
    out = cmds.incus.output(["network", "show", network])
    try:
        data = yaml.safe_load(out)
    except yaml.YAMLError as exc:
        logger.error("Failed to parse network config for %s: %s", network, exc)
        return {}
    if not isinstance(data, dict):
        logger.error("Expected a mapping from network show %s, got: %s", network, type(data).__name__)
        return {}
    return data.get("config", {})


def _network_is_tagged(cmds: Cmds, network: str) -> bool:
    config = _network_config(cmds, network)
    return isinstance(config, dict) and config.get(_KIVE_NET_OWNER) == "utils/dev"


def _cidr_in_use(cmds: Cmds, cidr: str) -> bool:
    out = cmds.ip.output(["route", "show", cidr])
    return bool(out.strip())


def ensure_vm_network(cmds: Cmds, network: str, cidr: str) -> None:
    out = cmds.incus.output(["network", "list", "--format", "csv", "--columns", "n"])
    networks = [line.strip() for line in out.splitlines() if line.strip()]

    if network in networks:
        if _network_is_tagged(cmds, network):
            logger.debug("Managed network %s already exists and is owned by utils/dev.", network)
            return
        logger.error(
            "Network %s already exists but is not owned by utils/dev.\n"
            "Use --vm-network to specify a different network name.",
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
    cmds.incus.run(
        [
            "network", "create", network,
            f"ipv4.address={cidr}",
            "ipv4.nat=true",
            "ipv6.address=none",
            f"{_KIVE_NET_OWNER}=utils/dev",
            "user.kive.devel.project=Kive",
            "user.kive.devel.kind=network",
        ]
    )


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


def find_tagged_networks(cmds: Cmds) -> list[str]:
    out = cmds.incus.output(["network", "list", "--format", "csv", "--columns", "n"])
    tagged = []
    for line in out.splitlines():
        name = line.strip()
        if name and _network_is_tagged(cmds, name):
            tagged.append(name)
    if tagged:
        logger.info("Found tagged networks: %s", ", ".join(tagged))
    return tagged
