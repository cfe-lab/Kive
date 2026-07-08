from __future__ import annotations

import dataclasses
import json
import logging
import re
import subprocess
import sys
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")

DEFAULT_VM_BRIDGE = "kive-lab-br"


@dataclasses.dataclass(frozen=True)
class VmNicTarget:
    """Describes how to attach a VM NIC to a managed Incus bridge."""
    name: str
    managed: bool = True


def get_existing_bridges(cmds: Cmds) -> list[dict]:
    """Return existing Incus-visible bridge networks (JSON list from ``incus network list``)."""
    out = cmds.incus.output(["network", "list", "--format", "json"])
    if not out:
        return []
    try:
        networks = json.loads(out)
        return networks if isinstance(networks, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _managed_bridge_info(cmds: Cmds, name: str) -> dict | None:
    """Return the network info dict for a managed bridge, or None."""
    for b in get_existing_bridges(cmds):
        if b.get("name") == name and b.get("type") == "bridge" and b.get("managed") is True:
            return b
    return None


def _ensure_incus_network_create(cmds: Cmds, name: str) -> None:
    """Create a managed Incus bridge with DHCP/NAT.  On ``Can't parse a
    version: UNKNOWN`` (commonly from a broken ``dnsmasq``), print a
    targeted diagnostic."""
    logger.info("Creating managed Incus bridge %s...", name)
    try:
        cmds.incus.run([
            "network", "create", name,
            "--type=bridge",
            "ipv4.address=auto",
            "ipv4.nat=true",
            "ipv6.address=none",
        ])
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or "") + (exc.output or "")
        logger.error("Failed to create Incus network %s:\n%s", name, err.strip())
        if "Can't parse a version" in err:
            logger.error(
                "Incus failed to detect a helper version, commonly dnsmasq.\n"
                "Check that dnsmasq reports a usable version:\n\n"
                "  dnsmasq --version\n\n"
                "If dnsmasq prints 'Dnsmasq version UNKNOWN', try installing or\n"
                "replacing the dnsmasq package so dnsmasq --version returns a\n"
                "numeric version.  Then retry this command."
            )
        sys.exit(1)


def ensure_managed_vm_network(cmds: Cmds, requested: str | None) -> VmNicTarget:
    """Ensure a managed Incus bridge exists for VM networking.

    Returns a ``VmNicTarget(name=..., managed=True)``.

    Behavior:
    * If *requested* is given, ensure exactly that managed network exists.
    * Otherwise, prefer an existing ``kive-lab-br`` or ``incusbr0``.
    * Otherwise create the default ``kive-lab-br``.
    * On creation or mismatch, log diagnostics.
    """
    if requested:
        name = requested
    else:
        bridges = get_existing_bridges(cmds)
        names = [b.get("name", "") for b in bridges
                 if b.get("type") == "bridge" and b.get("managed") is True]
        if DEFAULT_VM_BRIDGE in names:
            name = DEFAULT_VM_BRIDGE
        elif "incusbr0" in names:
            name = "incusbr0"
        else:
            name = DEFAULT_VM_BRIDGE

    info = _managed_bridge_info(cmds, name)
    if info is not None:
        _repair_managed_bridge(cmds, name)
        logger.info("Using existing managed Incus bridge %s.", name)
        return VmNicTarget(name=name)

    # Bridge does not exist — check for name collision
    for b in get_existing_bridges(cmds):
        if b.get("name") == name:
            logger.error(
                "Requested network '%s' exists but is not an Incus-managed bridge.\n"
                "Kive requires a managed Incus bridge for VM networking.\n"
                "Remove or rename the conflicting resource, then retry.",
                name,
            )
            sys.exit(1)

    _ensure_incus_network_create(cmds, name)
    _repair_managed_bridge(cmds, name)
    return VmNicTarget(name=name)


def _ensure_host_ip_forward() -> None:
    import subprocess as _sp
    try:
        r = _sp.run(["sysctl", "-n", "net.ipv4.ip_forward"], capture_output=True, text=True, timeout=10)
    except (_sp.TimeoutExpired, OSError) as exc:
        logger.warning("Could not check ip_forward: %s", exc)
        return
    val = r.stdout.strip()
    if val == "1":
        return
    logger.info("Enabling net.ipv4.ip_forward (was %s) — required for VM egress.", val)
    try:
        _sp.run(["sudo", "--", "sysctl", "-w", "net.ipv4.ip_forward=1"],
                check=True, capture_output=True, text=True, timeout=15)
    except (_sp.TimeoutExpired, OSError) as exc:
        logger.warning("Could not enable ip_forward (sudo may require password): %s", exc)


def _get_bridge_cidr(cmds: Cmds, name: str) -> str | None:
    """Return the CIDR (e.g. ``10.166.248.1/24``) of an Incus managed bridge."""
    raw = cmds.incus.output(["network", "get", name, "ipv4.address"])
    if not raw or raw == "none":
        return None
    raw = raw.strip()
    # incus may return "10.166.248.1/24" or "10.166.248.1/24 dhcp"
    raw = raw.split()[0]
    return raw if "/" in raw else None


def _ensure_host_egress_iptables(cmds: Cmds, bridge_name: str, cidr: str) -> None:
    """Add iptables rules for forwarding and NAT for the bridge CIDR.

    Uses idempotent ``-C`` checks before ``-I`` insertion.  Handles:
    * DOCKER-USER ingress/return forwarding (if Docker chain exists).
    * FORWARD ingress/return forwarding.
    * POSTROUTING masquerade for bridge CIDR to non-bridge destinations.
    """
    import subprocess as _sp

    def _ipt(cmd: list[str]) -> int:
        return _sp.run(["iptables"] + cmd, capture_output=True, text=True, timeout=15, check=False).returncode

    # DOCKER-USER
    if _ipt(["-nL", "DOCKER-USER"]) == 0:
        if _ipt(["-C", "DOCKER-USER", "-i", bridge_name, "-j", "ACCEPT"]) != 0:
            _ipt(["-I", "DOCKER-USER", "1", "-i", bridge_name, "-j", "ACCEPT"])
        if _ipt(["-C", "DOCKER-USER", "-o", bridge_name, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"]) != 0:
            _ipt(["-I", "DOCKER-USER", "1", "-o", bridge_name, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"])

    # FORWARD ingress/return
    if _ipt(["-C", "FORWARD", "-i", bridge_name, "-j", "ACCEPT"]) != 0:
        _ipt(["-I", "FORWARD", "1", "-i", bridge_name, "-j", "ACCEPT"])
    if _ipt(["-C", "FORWARD", "-o", bridge_name, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"]) != 0:
        _ipt(["-I", "FORWARD", "1", "-o", bridge_name, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"])

    # POSTROUTING masquerade
    _add_masquerade_rule(bridge_name, cidr)


def _add_masquerade_rule(bridge_name: str, cidr: str) -> None:
    """Add iptables masquerade for *cidr* to non-*cidr* destinations, if not present."""
    import subprocess as _sp
    import ipaddress
    try:
        net = ipaddress.ip_network(cidr, strict=False)
        netcidr = net.with_prefixlen
    except ValueError:
        logger.warning("Cannot parse CIDR %s for masquerade rule.", cidr)
        return
    check = _sp.run(
        ["iptables", "-t", "nat", "-C", "POSTROUTING",
         "-s", netcidr, "!", "-d", netcidr, "-j", "MASQUERADE"],
        capture_output=True, text=True, timeout=15, check=False,
    )
    if check.returncode != 0:
        _sp.run(
            ["iptables", "-t", "nat", "-I", "POSTROUTING", "1",
             "-s", netcidr, "!", "-d", netcidr, "-j", "MASQUERADE"],
            capture_output=True, text=True, timeout=15, check=False,
        )


def _verify_host_egress(cmds: Cmds, bridge_name: str) -> None:
    """Log host-side egress diagnostics in debug mode."""
    import subprocess as _sp
    logger.info("=== Host egress verification ===")
    try:
        r = _sp.run(["sysctl", "-n", "net.ipv4.ip_forward"], capture_output=True, text=True, timeout=10)
        logger.info("ip_forward: %s", r.stdout.strip())
    except Exception:
        pass
    logger.info("incus network show %s:\n%s", bridge_name,
                cmds.incus.output(["network", "show", bridge_name]) or "(empty)")
    for table in ("filter", "nat"):
        out = _sp.run(["iptables", "-t", table, "-S"], capture_output=True, text=True, timeout=10, check=False)
        if out.stdout:
            logger.info("iptables -t %s -S:\n%s", table, out.stdout.strip())


def _repair_managed_bridge(cmds: Cmds, name: str) -> None:
    """Ensure a managed bridge has IPv4, NAT, routing, firewall, and host egress enabled."""
    ipv4 = cmds.incus.output(["network", "get", name, "ipv4.address"])
    if not ipv4 or ipv4 == "none":
        logger.info("Setting ipv4.address=auto on %s...", name)
        cmds.incus.run(["network", "set", name, "ipv4.address", "auto"])

    for key, val in [("ipv4.nat", "true"), ("ipv4.routing", "true"), ("ipv4.firewall", "true")]:
        current = cmds.incus.output(["network", "get", name, key])
        if current != val:
            logger.info("Setting %s=%s on %s...", key, val, name)
            cmds.incus.run(["network", "set", name, key, val])

    ipv6 = cmds.incus.output(["network", "get", name, "ipv6.address"])
    if ipv6 and ipv6 != "none":
        logger.info("Setting ipv6.address=none on %s...", name)
        cmds.incus.run(["network", "set", name, "ipv6.address", "none"])

    _ensure_host_ip_forward()
    cidr = _get_bridge_cidr(cmds, name)
    if cidr:
        _ensure_host_egress_iptables(cmds, name, cidr)
        logger.info("Host egress forwarding rules added for %s (%s).", name, cidr)
        _verify_host_egress(cmds, name)
    else:
        logger.warning("Could not determine bridge CIDR for %s; host egress rules not added.", name)


def _parse_device_block(text: str, device: str) -> dict[str, str]:
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
    out = cmds.incus.output(["config", "device", "show", instance])
    return _parse_device_block(out, "eth0")


def _eth0_config_expanded(cmds: Cmds, instance: str) -> dict[str, str]:
    out = cmds.incus.output(["config", "show", "--expanded", instance])
    return _parse_device_block(out, "eth0")


def _device_config_str(device: dict[str, str]) -> str:
    """Human-readable representation of a device config dict."""
    parts = [f"{k}={v}" for k, v in sorted(device.items())]
    return " ".join(parts) if parts else "(empty)"


def ensure_vm_nic(cmds: Cmds, instance: str, target: VmNicTarget) -> bool:
    """Ensure the VM instance has an eth0 NIC attached to *target*.

    The VM must end up with an instance-level device ``eth0`` having
    ``type=nic`` and ``network=TARGET_NAME``.

    Returns True if a change was made (instance restart required).
    """
    target_network = target.name
    device = _eth0_config(cmds, instance)

    if device:
        existing_network = device.get("network", "")
        existing_nictype = device.get("nictype", "")
        existing_parent = device.get("parent", "")

        if existing_network == target_network:
            logger.debug("eth0 already has network=%s on %s.", target_network, instance)
            return False

        if not existing_network and not existing_parent and not existing_nictype:
            logger.warning(
                "eth0 on %s has empty config; replacing with network=%s.",
                instance, target_network,
            )
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
            return True

        if existing_nictype == "macvlan":
            logger.info(
                "eth0 on %s uses macvlan (parent=%s); replacing with network=%s.",
                instance, existing_parent, target_network,
            )
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
            return True

        if existing_network and existing_network != target_network:
            logger.info(
                "eth0 on %s is on network=%s; replacing with network=%s.",
                instance, existing_network, target_network,
            )
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
            return True

        if existing_parent:
            logger.info(
                "eth0 on %s has parent=%s; replacing with network=%s.",
                instance, existing_parent, target_network,
            )
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
            return True

        logger.info(
            "eth0 on %s has unexpected config (%s); replacing with network=%s.",
            instance, _device_config_str(device), target_network,
        )
        cmds.incus.run(["config", "device", "remove", instance, "eth0"])
        cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
        return True

    # No instance-level eth0 — check profile
    expanded = _eth0_config_expanded(cmds, instance)
    if expanded:
        expanded_network = expanded.get("network", "")
        expanded_nictype = expanded.get("nictype", "")
        expanded_parent = expanded.get("parent", "")

        if expanded_network == target_network:
            logger.info(
                "eth0 provided by profile (network=%s) matches target; no change needed.",
                target_network,
            )
            return False

        if expanded_parent or expanded_nictype:
            logger.info(
                "Profile eth0 uses %s; overriding with network=%s.",
                _device_config_str(expanded), target_network,
            )
        else:
            logger.info(
                "Profile eth0 has network=%s; overriding with network=%s.",
                expanded_network, target_network,
            )

        cmds.incus.run(["config", "device", "override", instance, "eth0"])
        cmds.incus.run(["config", "device", "set", instance, "eth0", "network", target_network])
        if expanded_parent:
            cmds.incus.run(["config", "device", "unset", instance, "eth0", "parent"], check=False)
        if expanded_nictype:
            cmds.incus.run(["config", "device", "unset", instance, "eth0", "nictype"], check=False)
        return True

    logger.info("Adding NIC eth0 to %s (network=%s)...", instance, target_network)
    cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
    return True


def _vm_mac_address(cmds: Cmds, instance: str) -> str | None:
    """Return the MAC address of the VM's eth0, if available from expanded config."""
    expanded = _eth0_config_expanded(cmds, instance)
    hwaddr = expanded.get("hwaddr", "")
    return hwaddr if hwaddr else None


def wait_vm_dhcp_lease(cmds: Cmds, instance: str, bridge_name: str, timeout: float = 120) -> str | None:
    """Wait for a DHCP lease on *bridge_name* associated with *instance*.

    Matches by MAC address (preferred) or hostname.  Returns the IPv4
    address or None.
    """
    import time as _time
    deadline = _time.monotonic() + timeout
    last_error = ""
    mac = _vm_mac_address(cmds, instance)
    if mac:
        logger.debug("VM %s has MAC %s; matching leases by MAC.", instance, mac)
    while _time.monotonic() < deadline:
        try:
            out = cmds.incus.output(["network", "list-leases", bridge_name, "--format", "json"])
            if out:
                leases = json.loads(out)
                if isinstance(leases, list):
                    for lease in leases:
                        if mac and lease.get("hwaddr", "").lower() == mac.lower():
                            ip = lease.get("address", "")
                            if ip:
                                logger.info("VM %s got DHCP lease %s on %s (by MAC).", instance, ip, bridge_name)
                                return ip
                        if not mac and lease.get("hostname", "").startswith(instance):
                            ip = lease.get("address", "")
                            if ip:
                                logger.info("VM %s got DHCP lease %s on %s (by hostname).", instance, ip, bridge_name)
                                return ip
        except (json.JSONDecodeError, OSError) as exc:
            last_error = str(exc)
        _time.sleep(2)

    logger.warning(
        "VM %s did not receive a DHCP lease on %s within %.0fs.%s",
        instance, bridge_name, timeout,
        f"  Last error: {last_error}" if last_error else "",
    )
    return None


def _incus_list_output(cmds: Cmds, instance: str) -> str:
    return cmds.incus.output(["list", instance, "--format", "yaml"]) or "(empty)"


def print_network_diagnostics(cmds: Cmds, instance: str, bridge_name: str) -> None:
    """Print host-side networking diagnostics for debugging."""
    import subprocess as _sp
    logger.error("=== Network diagnostics ===")
    logger.error("incus network show %s:\n%s", bridge_name,
                 cmds.incus.output(["network", "show", bridge_name]) or "(empty)")
    logger.error("incus config show --expanded %s:\n%s", instance,
                 cmds.incus.output(["config", "show", "--expanded", instance]) or "(empty)")
    logger.error("incus list %s:\n%s", instance, _incus_list_output(cmds, instance))
    logger.error("incus network list-leases %s:\n%s", bridge_name,
                 cmds.incus.output(["network", "list-leases", bridge_name, "--format", "json"]) or "(empty)")
    try:
        r = _sp.run(["ip", "-4", "addr", "show", "dev", bridge_name], capture_output=True, text=True, timeout=10)
        logger.error("ip -4 addr show dev %s:\n%s", bridge_name, (r.stdout or r.stderr or "(empty)").strip())
    except Exception:
        pass


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
    cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"nictype={nictype}", f"parent={host_interface}"])
    return True


def get_existing_network_parent(cmds: Cmds, instance: str) -> str:
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
