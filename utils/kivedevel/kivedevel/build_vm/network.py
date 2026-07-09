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
DEFAULT_VM_BRIDGE_CIDR = "10.77.77.1/24"
DEFAULT_VM_BRIDGE_DNS = "1.1.1.1,8.8.8.8"

# Mutable bridge keys that are safe to repair without breaking existing VMs.
_MUTABLE_KEYS = {
    "ipv4.nat": "true",
    "ipv4.routing": "true",
    "ipv4.firewall": "true",
    "ipv4.dhcp": "true",
    "ipv6.address": "none",
    "raw.dnsmasq": f"dhcp-option=option:dns-server,{DEFAULT_VM_BRIDGE_DNS}",
}


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
    for b in get_existing_bridges(cmds):
        if b.get("name") == name and b.get("type") == "bridge" and b.get("managed") is True:
            return b
    return None


def _incus_network_get(cmds: Cmds, name: str, key: str) -> str:
    """Return a config value from an Incus managed network, or empty string."""
    out = cmds.incus.output(["network", "get", name, key])
    return out.strip() if out else ""


def _parse_cidr(raw: str) -> str | None:
    """Extract a CIDR like ``10.77.77.1/24`` from an incus config value."""
    raw = raw.strip().split()[0]
    return raw if "/" in raw else None


# ── Validation helpers ──────────────────────────────────────────────

def validate_incus_bridge_config(cmds: Cmds, name: str) -> None:
    """Validate an existing managed bridge matches Kive's expected config.

    Fails if ``ipv4.address`` differs (never changes it).  Safe mutable
    keys are repaired silently.  Exits on address mismatch.
    """
    expected_cidr = DEFAULT_VM_BRIDGE_CIDR

    actual_cidr_raw = _incus_network_get(cmds, name, "ipv4.address")
    actual_cidr = _parse_cidr(actual_cidr_raw) if actual_cidr_raw else ""

    if actual_cidr and actual_cidr != expected_cidr:
        logger.error(
            "Existing Incus network %s has ipv4.address=%s, but Kive expects %s.\n\n"
            "Kive will not change the address of an existing Incus bridge because\n"
            "existing VMs may retain stale DHCP leases/routes.\n\n"
            "Delete stale VMs and recreate the bridge manually, or set\n"
            "KIVE_VM_BRIDGE_CIDR to the existing subnet.\n\n"
            "Commands to clean up stale state:\n"
            "  incus list -c n --format csv | xargs -r incus delete -f\n"
            "  incus network delete %s\n"
            "  sudo ip link delete %s\n",
            name, actual_cidr, expected_cidr, name, name,
        )
        sys.exit(1)

    if not actual_cidr:
        logger.error(
            "Incus network %s has no ipv4.address configured.\n"
            "Recreate it or set the address manually:\n"
            "  incus network set %s ipv4.address %s\n",
            name, name, expected_cidr,
        )
        sys.exit(1)

    for key, val in _MUTABLE_KEYS.items():
        current = _incus_network_get(cmds, name, key)
        if current != val:
            logger.info("Setting %s=%s on %s...", key, val, name)
            cmds.incus.run(["network", "set", name, key, val])


def validate_live_bridge_address(bridge_name: str, expected_cidr: str) -> None:
    """Compare the live kernel bridge address against *expected_cidr*.

    Fails if they disagree (stale host/Incus state).
    """
    import subprocess as _sp
    try:
        r = _sp.run(["ip", "-4", "-o", "addr", "show", "dev", bridge_name],
                    capture_output=True, text=True, timeout=10, check=False)
    except OSError:
        return
    if r.returncode != 0:
        return
    # Parse: "2: kive-lab-br    inet 10.77.77.1/24 scope global ..."
    live = ""
    for line in r.stdout.splitlines():
        parts = line.split()
        for i, p in enumerate(parts):
            if p == "inet" and i + 1 < len(parts):
                cidr = parts[i + 1]
                if "/" in cidr:
                    live = cidr
                    break
        if live:
            break

    if live and live != expected_cidr:
        logger.error(
            "Live kernel bridge %s has address %s, but Incus config says %s.\n\n"
            "This is stale host/Incus state.  Clean up manually:\n"
            "  incus list -c n --format csv | xargs -r incus delete -f\n"
            "  incus network delete %s\n"
            "  sudo pkill -f 'dnsmasq.*%s'\n"
            "  sudo ip link delete %s\n"
            "  sudo rm -f /tmp/kive-lab-dnsmasq.pid\n",
            bridge_name, live, expected_cidr,
            bridge_name, bridge_name, bridge_name,
        )
        sys.exit(1)


# ── Docker detection ────────────────────────────────────────────────

def detect_docker_forward_drop() -> str | None:
    """Check if Docker's global FORWARD policy is DROP.

    Returns a diagnostic message if the bad state is detected, or None.
    """
    import subprocess as _sp
    # Check if Docker is running
    try:
        r = _sp.run(["iptables", "-S", "FORWARD"], capture_output=True, text=True, timeout=10, check=False)
    except FileNotFoundError:
        return None
    if r.returncode != 0:
        return None
    if "-P FORWARD DROP" in r.stdout:
        return (
            "Docker appears to have configured the host FORWARD policy to DROP.\n\n"
            "Incus bridge VMs require the host to forward packets for NAT egress.\n"
            "This can make Kive VMs get DHCP and DNS but fail raw internet access.\n\n"
            "Recommended Docker daemon config:\n"
            "  {\n"
            '    "ip-forward-no-drop": true\n'
            "  }\n\n"
            "Then restart Docker:\n"
            "  sudo systemctl restart docker\n\n"
            "If the live policy remains DROP after restart, set it explicitly:\n"
            "  sudo iptables -P FORWARD ACCEPT\n"
        )
    return None


# ── dnsmasq bind failure diagnostic ────────────────────────────────

def diagnose_dnsmasq_bind_failure(stderr: str, bridge_name: str) -> None:
    """Print targeted diagnostics when ``incus network create`` fails due
    to a stale dnsmasq process bound to the bridge address."""
    if "failed to create listening socket" in stderr.lower() and bridge_name in stderr:
        logger.error(
            "Incus could not start dnsmasq for %s because something is already\n"
            "bound to the bridge DNS address. This is often a stale dnsmasq\n"
            "process from a previous failed/manual bridge setup.\n\n"
            "Check for stale processes:\n"
            "  sudo ss -lntup | grep -E '10\\.77\\.77\\.1:53|dnsmasq'\n"
            "  ps -ef | grep '[d]nsmasq'\n\n"
            "Cleanup commands (diagnostic only, not executed):\n"
            "  sudo pkill -f 'dnsmasq.*%s'\n"
            "  sudo rm -f /tmp/kive-lab-dnsmasq.pid\n"
            "  sudo ip link delete %s\n",
            bridge_name, bridge_name, bridge_name,
        )


# ── CIDR conflict check ────────────────────────────────────────────

def check_cidr_conflict(cidr: str) -> None:
    """Check whether *cidr* conflicts with existing host routes/interfaces.

    Fails if the CIDR's network is already in use outside the intended bridge.
    """
    import subprocess as _sp
    import ipaddress
    try:
        net = ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return

    try:
        r = _sp.run(["ip", "-4", "route", "show"], capture_output=True, text=True, timeout=10, check=False)
    except OSError:
        return

    if r.returncode != 0:
        return

    for line in r.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 1:
            route_net_str = parts[0]
            if "/" in route_net_str:
                try:
                    route_net = ipaddress.ip_network(route_net_str, strict=False)
                    if net.overlaps(route_net):
                        dev = ""
                        for i, p in enumerate(parts):
                            if p == "dev" and i + 1 < len(parts):
                                dev = parts[i + 1]
                                break
                        if dev != DEFAULT_VM_BRIDGE:
                            logger.error(
                                "CIDR %s overlaps with existing route %s (dev %s).\n"
                                "This will cause IP conflicts.  Set KIVE_VM_BRIDGE_CIDR\n"
                                "to a different subnet or remove the conflicting route.",
                                cidr, route_net_str, dev,
                            )
                            sys.exit(1)
                except ValueError:
                    pass


# ── Bridge creation ────────────────────────────────────────────────

def _ensure_incus_network_create(cmds: Cmds, name: str) -> None:
    """Create a managed Incus bridge with deterministic CIDR, NAT, and DNS."""
    cidr = DEFAULT_VM_BRIDGE_CIDR
    dns_opt = f"dhcp-option=option:dns-server,{DEFAULT_VM_BRIDGE_DNS}"

    check_cidr_conflict(cidr)
    logger.info("Creating managed Incus bridge %s (%s)...", name, cidr)
    try:
        cmds.incus.run([
            "network", "create", name,
            "--type=bridge",
            f"ipv4.address={cidr}",
            "ipv4.nat=true",
            "ipv4.routing=true",
            "ipv4.firewall=true",
            "ipv4.dhcp=true",
            f"raw.dnsmasq={dns_opt}",
            "ipv6.address=none",
        ])
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or "") + (exc.output or "")
        logger.error("Failed to create Incus network %s:\n%s", name, err.strip())
        diagnose_dnsmasq_bind_failure(err, name)
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


# ── Main entry point ───────────────────────────────────────────────

def ensure_managed_vm_network(cmds: Cmds, requested: str | None) -> VmNicTarget:
    """Ensure a managed Incus bridge exists for VM networking.

    Returns a ``VmNicTarget(name=..., managed=True)``.

    * If *requested* is given, ensure exactly that managed network exists.
    * Otherwise, prefer an existing ``kive-lab-br`` or ``incusbr0``.
    * Otherwise create the default ``kive-lab-br``.
    * Validates existing bridge config; fails on address mismatch.
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
        validate_incus_bridge_config(cmds, name)
        validate_live_bridge_address(name, DEFAULT_VM_BRIDGE_CIDR)
        logger.info("Using existing managed Incus bridge %s.", name)
        return VmNicTarget(name=name)

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
    validate_live_bridge_address(name, DEFAULT_VM_BRIDGE_CIDR)
    return VmNicTarget(name=name)


# ── NIC attachment ─────────────────────────────────────────────────

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
    parts = [f"{k}={v}" for k, v in sorted(device.items())]
    return " ".join(parts) if parts else "(empty)"


def ensure_vm_nic(cmds: Cmds, instance: str, target: VmNicTarget) -> bool:
    """Ensure the VM instance has an eth0 NIC attached to *target*.

    The VM must end up with an instance-level device ``eth0`` having
    ``type=nic`` and ``network=TARGET_NAME``.

    Returns True if a change was made.
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
            logger.warning("eth0 on %s has empty config; replacing with network=%s.", instance, target_network)
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
            return True

        if existing_nictype == "macvlan":
            logger.info("eth0 on %s uses macvlan (parent=%s); replacing with network=%s.", instance, existing_parent, target_network)
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
            return True

        if existing_network and existing_network != target_network:
            logger.info("eth0 on %s is on network=%s; replacing with network=%s.", instance, existing_network, target_network)
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
            return True

        if existing_parent:
            logger.info("eth0 on %s has parent=%s; replacing with network=%s.", instance, existing_parent, target_network)
            cmds.incus.run(["config", "device", "remove", instance, "eth0"])
            cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
            return True

        logger.info("eth0 on %s has unexpected config (%s); replacing with network=%s.", instance, _device_config_str(device), target_network)
        cmds.incus.run(["config", "device", "remove", instance, "eth0"])
        cmds.incus.run(["config", "device", "add", instance, "eth0", "nic", f"network={target_network}"])
        return True

    expanded = _eth0_config_expanded(cmds, instance)
    if expanded:
        expanded_network = expanded.get("network", "")
        expanded_nictype = expanded.get("nictype", "")
        expanded_parent = expanded.get("parent", "")

        if expanded_network == target_network:
            logger.info("eth0 provided by profile (network=%s) matches target; no change needed.", target_network)
            return False

        if expanded_parent or expanded_nictype:
            logger.info("Profile eth0 uses %s; overriding with network=%s.", _device_config_str(expanded), target_network)
        else:
            logger.info("Profile eth0 has network=%s; overriding with network=%s.", expanded_network, target_network)

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
    expanded = _eth0_config_expanded(cmds, instance)
    hwaddr = expanded.get("hwaddr", "")
    return hwaddr if hwaddr else None


def wait_vm_dhcp_lease(cmds: Cmds, instance: str, bridge_name: str, timeout: float = 120) -> str | None:
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

    logger.warning("VM %s did not receive a DHCP lease on %s within %.0fs.%s", instance, bridge_name, timeout,
                   f"  Last error: {last_error}" if last_error else "")
    return None


def _incus_list_output(cmds: Cmds, instance: str) -> str:
    return cmds.incus.output(["list", instance, "--format", "yaml"]) or "(empty)"


def print_network_diagnostics(cmds: Cmds, instance: str, bridge_name: str) -> None:
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
