"""Host preparation for Incus backend: initialize daemon, configure bridge, repair egress."""

from __future__ import annotations

import argparse
import logging
import subprocess

import yaml

from ..build_vm.incus import ensure_profile_with_root_disk
from ..build_vm.network import (
    DEFAULT_VM_BRIDGE,
    ensure_managed_vm_network,
)
from ..kv_commands import Cmds
from ..shared import configure_console_logging


logger = logging.getLogger("kivedevel.backends.incus_host")


def _run(cmd: list[str], *, check: bool = True, sudo: bool = False, **kwargs) -> subprocess.CompletedProcess[str]:
    if sudo:
        cmd = ["sudo", "--"] + cmd
    logger.debug("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=check, capture_output=True, text=True, **kwargs)
    if logger.isEnabledFor(logging.DEBUG):
        if result.stdout:
            logger.debug("stdout:\n%s", result.stdout.rstrip())
        if result.stderr:
            logger.debug("stderr:\n%s", result.stderr.rstrip())
    return result


def _print_diagnostics(cmds: Cmds, bridge: str) -> None:
    logger.info("=== host networking diagnostics ===")
    cmds.incus.run(["network", "show", bridge], check=False)
    sysctl_val = _run(["sysctl", "net.ipv4.ip_forward"], sudo=True, check=False)
    logger.info("ip_forward: %s", sysctl_val.stdout.strip() if sysctl_val.returncode == 0 else "unknown")
    cmds.ip.run(["addr"], check=False)
    cmds.ip.run(["route"], check=False)
    _run(["iptables", "-S"], sudo=True, check=False)
    _run(["iptables", "-t", "nat", "-S"], sudo=True, check=False)
    _run(["nft", "list", "ruleset"], sudo=True, check=False)


def _enable_ipv4_forwarding() -> None:
    _run(["sysctl", "-w", "net.ipv4.ip_forward=1"], sudo=True)
    _run(["sysctl", "-w", "net.ipv4.conf.all.forwarding=1"], sudo=True, check=False)
    _run(["sysctl", "-w", "net.ipv4.conf.default.forwarding=1"], sudo=True, check=False)


def _set_bridge_options(cmds: Cmds, bridge: str) -> None:
    cmds.incus.run(["network", "set", bridge, "ipv4.nat", "true"])
    cmds.incus.run(["network", "set", bridge, "ipv4.routing", "true"])
    cmds.incus.run(["network", "set", bridge, "ipv4.firewall", "true"])


def _remove_stale_profile_nic(cmds: Cmds, bridge: str) -> None:
    raw = cmds.incus.output(["profile", "device", "show", "default"])
    if not raw:
        return
    try:
        devices = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        logger.error("Failed to parse default profile device list: %s", exc)
        return
    if not isinstance(devices, dict):
        logger.error("Expected device mapping in default profile, got %s", type(devices).__name__)
        return

    eth0 = devices.get("eth0")
    if eth0 is None:
        return
    if not isinstance(eth0, dict):
        logger.warning("eth0 in default profile is not a device mapping; skipping cleanup.")
        return

    nic_network = eth0.get("network")
    nic_parent = eth0.get("parent")
    nic_nictype = eth0.get("nictype", "")

    if nic_network == bridge or (nic_parent == bridge and nic_nictype == "bridged"):
        logger.info("Removing stale Kive NIC (eth0) from default profile (bridge=%s)...", bridge)
        result = cmds.incus.run(
            ["profile", "device", "remove", "default", "eth0"],
            check=False, capture_output=True,
        )
        if result.returncode != 0:
            logger.error(
                "Failed to remove eth0 from default profile:\n"
                "Command: incus profile device remove default eth0\n"
                "Return code: %s\nstderr: %s",
                result.returncode, (result.stderr or "").strip(),
            )
            return
        after = cmds.incus.output(["profile", "device", "show", "default"])
        if after and bridge in after:
            logger.error("Stale Kive NIC still present in default profile after removal attempt.")
    elif nic_network or nic_parent:
        logger.warning(
            "Default profile has eth0 targeting %s; not removing automatically.",
            nic_network or nic_parent,
        )


def _add_bridge_forwarding_rules(bridge: str) -> None:
    docker_chain = _run(["iptables", "-nL", "DOCKER-USER"], sudo=True, check=False)
    if docker_chain.returncode == 0:
        accept_in = _run(
            ["iptables", "-C", "DOCKER-USER", "-i", bridge, "-j", "ACCEPT"],
            sudo=True, check=False,
        )
        if accept_in.returncode != 0:
            _run(["iptables", "-I", "DOCKER-USER", "1", "-i", bridge, "-j", "ACCEPT"],
                 sudo=True)

        accept_out = _run(
            ["iptables", "-C", "DOCKER-USER", "-o", bridge,
             "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"],
            sudo=True, check=False,
        )
        if accept_out.returncode != 0:
            _run(
                ["iptables", "-I", "DOCKER-USER", "1", "-o", bridge,
                 "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"],
                sudo=True,
            )

    fwd_in = _run(
        ["iptables", "-C", "FORWARD", "-i", bridge, "-j", "ACCEPT"],
        sudo=True, check=False,
    )
    if fwd_in.returncode != 0:
        _run(["iptables", "-I", "FORWARD", "1", "-i", bridge, "-j", "ACCEPT"],
             sudo=True)

    fwd_out = _run(
        ["iptables", "-C", "FORWARD", "-o", bridge,
         "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"],
        sudo=True, check=False,
    )
    if fwd_out.returncode != 0:
        _run(
            ["iptables", "-I", "FORWARD", "1", "-o", bridge,
             "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"],
            sudo=True,
        )


def _add_masquerade_fallback(cmds: Cmds, bridge: str) -> None:
    cidr4 = cmds.incus.output(["network", "get", bridge, "ipv4.address"])
    if not cidr4:
        return
    result = _run(
        ["python3", "-c",
         "import ipaddress,sys; net=ipaddress.ip_network(sys.argv[1], strict=False); print(net.with_prefixlen)",
         cidr4],
        check=False,
    )
    if result.returncode != 0:
        return
    netcidr = result.stdout.strip()
    check = _run(
        ["iptables", "-t", "nat", "-C", "POSTROUTING",
         "-s", netcidr, "!", "-d", netcidr, "-j", "MASQUERADE"],
        sudo=True, check=False,
    )
    if check.returncode != 0:
        _run(
            ["iptables", "-t", "nat", "-I", "POSTROUTING", "1",
             "-s", netcidr, "!", "-d", netcidr, "-j", "MASQUERADE"],
            sudo=True,
        )


def run_prepare_host(args: argparse.Namespace) -> None:
    configure_console_logging(args)

    # CLI is backend-generic; only incus is implemented today.
    assert args.backend == "incus", f"Unsupported backend: {args.backend}"

    bridge: str = args.bridge
    debug: bool = getattr(args, "debug", False)

    cmds = Cmds.create()

    if debug:
        logger.info("Preparing host for Incus backend (bridge=%s)...", bridge)

    # Start and enable Incus service/socket (forgiving).
    for unit in ("incus.service", "incus.socket"):
        _run(["systemctl", "enable", "--now", unit], sudo=True, check=False)
        _run(["systemctl", "start", unit], sudo=True, check=False)

    # Initialize Incus with dir-backed storage pool only.
    # The bridge and profile are set up procedurally below.
    preseed = """config: {}
storage_pools:
- name: default
  driver: dir
"""
    cmds.incus.run(["admin", "init", "--preseed"], input=preseed, check=False)
    cmds.incus.run(["info"], check=False)

    # Create or validate the Kive managed bridge.
    ensure_managed_vm_network(cmds, bridge)

    # Ensure the default profile has a root disk (no NIC — VM mode uses per-instance --network).
    ensure_profile_with_root_disk(cmds, "default", "default", "60GiB")
    # Remove any stale Kive-created NIC from the default profile.
    _remove_stale_profile_nic(cmds, bridge)

    if debug:
        _print_diagnostics(cmds, bridge)

    _enable_ipv4_forwarding()
    _add_bridge_forwarding_rules(bridge)
    _add_masquerade_fallback(cmds, bridge)

    if debug:
        logger.info("=== host networking after repair ===")
        _run(["sysctl", "net.ipv4.ip_forward"], sudo=True, check=False)
        _run(["sysctl", "net.ipv4.conf.all.forwarding"], sudo=True, check=False)
        _run(["sysctl", "net.ipv4.conf.default.forwarding"], sudo=True, check=False)
        _run(["iptables", "-S", "FORWARD"], sudo=True, check=False)
        _run(["iptables", "-S", "DOCKER-USER"], sudo=True, check=False)
        _run(["iptables", "-t", "nat", "-S", "POSTROUTING"], sudo=True, check=False)
        _run(["nft", "list", "ruleset"], sudo=True, check=False)

    logger.info("Host preparation complete.")


def _add_log_flags(parser: argparse.ArgumentParser) -> None:
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument("--quiet", action="store_true", help="Only show errors")
    log_group.add_argument("--verbose", action="store_true", help="Show informational progress messages")
    log_group.add_argument("--debug", action="store_true", help="Show debug logging, including full command lines")


def register_subcommand(subparsers) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "prepare-host",
        help="Prepare the local machine so build-vm or smoke-local-install can run",
    )
    parser.add_argument(
        "--backend",
        default="incus",
        choices=("incus",),
        help="Backend to prepare for (default: incus)",
    )
    parser.add_argument(
        "--bridge",
        default=DEFAULT_VM_BRIDGE,
        help="Backend bridge network (default: kive-lab-br)",
    )
    _add_log_flags(parser)
    parser.set_defaults(func=run_prepare_host)
