from __future__ import annotations

import logging
import re
import sys

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
    out = cmds.ip.output(["-o", "-f", "inet", "addr", "show", "--", iface])
    parts = out.split()
    return parts[3] if len(parts) >= 4 else ""


def ensure_network_device(cmds: Cmds, instance: str, host_interface: str) -> bool:
    out = cmds.incus.output(["config", "device", "list", instance])
    if re.search(r"^eth0\s*$", out, re.MULTILINE):
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
