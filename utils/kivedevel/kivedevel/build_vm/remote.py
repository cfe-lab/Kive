from __future__ import annotations

import json
import logging
import subprocess
import time

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")


def _instance_ipv4_candidates(cmds: Cmds, instance: str) -> list[str]:
    out = cmds.incus.output(["list", instance, "--format", "json"])
    if not out:
        return []

    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        return []

    if not isinstance(payload, list) or not payload:
        return []

    candidates: list[str] = []
    state = payload[0].get("state", {})
    network = state.get("network", {}) if isinstance(state, dict) else {}
    for iface_data in network.values():
        addresses = iface_data.get("addresses", []) if isinstance(iface_data, dict) else []
        for addr in addresses:
            family = addr.get("family")
            scope = addr.get("scope")
            address = addr.get("address")
            if family == "inet" and scope == "global" and isinstance(address, str) and address:
                candidates.append(address)
    return candidates


def resolve_instance_ip(cmds: Cmds, instance: str, *, timeout_seconds: int = 180) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        candidates = _instance_ipv4_candidates(cmds, instance)
        if candidates:
            return candidates[0]
        time.sleep(2)
    raise RuntimeError(f"Could not determine IPv4 address for {instance}")


def wait_for_ssh(ip: str, *, user: str = "ubuntu", timeout_seconds: int = 240) -> None:
    deadline = time.time() + timeout_seconds
    argv = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ConnectTimeout=8",
        f"{user}@{ip}",
        "true",
    ]
    while time.time() < deadline:
        result = subprocess.run(argv, check=False, capture_output=True, text=True)
        if result.returncode == 0:
            return
        time.sleep(2)

    raise RuntimeError(f"SSH did not become ready on {user}@{ip}")


def run_ssh_script(ip: str, script: str, *, user: str = "ubuntu") -> None:
    argv = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ConnectTimeout=12",
        f"{user}@{ip}",
        "bash",
        "-se",
    ]
    result = subprocess.run(argv, input=script, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        details = stderr or stdout or "unknown SSH provisioning failure"
        raise RuntimeError(details)
