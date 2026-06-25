from __future__ import annotations

import json
import logging
import re
import subprocess as _sp
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
    """Enable ``net.ipv4.ip_forward`` if disabled and log the change."""
    import subprocess as _sp
    result = _sp.run(["sysctl", "-n", "net.ipv4.ip_forward"], capture_output=True, text=True, check=False)
    val = result.stdout.strip()
    if val != "1":
        logger.info(
            "Enabling net.ipv4.ip_forward (was %s) — required for the "
            "kive-devel-br NAT to forward VM traffic to the internet.",
            val,
        )
        _sp.run(["sudo", "--", "sysctl", "-w", "net.ipv4.ip_forward=1"], check=True,
                capture_output=True, text=True)


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

    try:
        cmds.nft.run([
            "add", "chain", "inet", "kive_devel", "forward",
            "{ type filter hook forward priority filter ; policy accept ; }",
        ], sudo=True)
    except Exception:
        logger.warning("nftables chain 'kive_devel.forward' may already exist (non-fatal).")

    try:
        cmds.nft.run([
            "add", "rule", "inet", "kive_devel", "forward",
            "iifname", bridge_name, "accept",
        ], sudo=True)
    except Exception as exc:
        logger.error("Failed to add forward accept rule for %s: %s", bridge_name, exc)
        sys.exit(1)

    try:
        cmds.nft.run([
            "add", "rule", "inet", "kive_devel", "forward",
            "oifname", bridge_name, "ct state established,related accept",
        ], sudo=True)
    except Exception as exc:
        logger.warning("Failed to add return forward rule for %s: %s", bridge_name, exc)

    reg = _registry_path(root)
    _register_resource(reg, {
        "type": "nft-table", "name": "inet kive_devel",
        "created_by": "utils/dev", "project": "Kive", "kind": "nft-table",
    })


def _get_nft_json_list(cmds: Cmds) -> list[dict]:
    """Return the parsed JSON output of ``nft --json list ruleset``."""
    result = cmds.nft.run(["--json", "list", "ruleset"], sudo=True, capture_output=True, check=False)
    if result.returncode != 0:
        return []
    try:
        data = json.loads(result.stdout)
        return data.get("nftables", [])
    except (json.JSONDecodeError, AttributeError, OSError):
        return []


def _find_forward_chains_with_drop_policy(nft_json: list[dict]) -> list[dict]:
    """Find forward base chains (type=filter, hook=forward) with ``policy drop``
    in tables other than our owned ``inet kive_devel``."""
    found: list[dict] = []
    for item in nft_json:
        table = item.get("table")
        if not table:
            continue
        family = table.get("family", "")
        table_name = table.get("name", "")
        if family == "inet" and table_name == "kive_devel":
            continue
        for chain in table.get("chain", []):
            if not isinstance(chain, dict):
                continue
            if chain.get("type") == "filter" and chain.get("hook") == "forward":
                if chain.get("policy") == "drop":
                    found.append({
                        "family": family,
                        "table": table_name,
                        "chain": chain["name"],
                    })
    return found


def _ensure_existing_forward_accept(cmds: Cmds, root: Path, bridge_name: str) -> None:
    """Add accept rules to existing forward chains that would drop our traffic.

    In nftables an ``accept`` verdict in one table's forward chain does not
    prevent another table's forward chain from dropping forwarded packets
    (`priority` only affects ordering, not override).  This function:

    * adds an ``iifname <bridge_name> accept`` rule to each native nftables
      forward base chain that has ``policy drop``
    * adds an iptables FORWARD accept rule for the bridge (covers iptables-compat)
    * adds a DOCKER-USER accept rule if the Docker user chain exists

    Every rule is registered in the Kive resource registry for clean removal
    during ``utils/dev purge``.
    """
    reg = _registry_path(root)

    # --- native nftables: forward chains with policy drop ---
    nft_json = _get_nft_json_list(cmds)
    drop_chains = _find_forward_chains_with_drop_policy(nft_json)

    if drop_chains:
        logger.warning(
            "Found %d forward chain(s) with policy drop that would block "
            "forwarding from %s. Adding owned accept rules.",
            len(drop_chains), bridge_name,
        )

    for info in drop_chains:
        family = info["family"]
        tbl = info["table"]
        chn = info["chain"]
        rule_name = f"{family}/{tbl}/{chn}"
        if _registry_has(reg, "forward-rule", rule_name):
            continue
        try:
            cmds.nft.run([
                "add", "rule", family, tbl, chn,
                "iifname", bridge_name, "accept",
            ], sudo=True)
            _register_resource(reg, {
                "kind": "forward-rule",
                "name": rule_name,
                "family": family,
                "table": tbl,
                "chain": chn,
                "bridge": bridge_name,
            })
            logger.info("Added accept rule to %s %s %s (policy drop)", family, tbl, chn)
        except Exception as exc:
            logger.warning("Failed to add accept rule to %s %s %s: %s", family, tbl, chn, exc)

    def _try_iptables_cmd(args: list[str], check_: bool = False) -> _sp.CompletedProcess[str] | None:
        """Run an iptables command safely — ``iptables`` may not be
        available or installed on the host (e.g. inside a container)."""
        try:
            return _sp.run(["iptables"] + args, capture_output=True, timeout=10, check=check_)
        except FileNotFoundError:
            return None
        except Exception:
            return None

    # --- iptables FORWARD chain ---
    ipt_rule_name = "iptables/FORWARD"
    if not _registry_has(reg, "forward-rule", ipt_rule_name):
        ipt_check = _try_iptables_cmd(["-C", "FORWARD", "-i", bridge_name, "-j", "ACCEPT"])
        if ipt_check is not None and ipt_check.returncode != 0:
            added = _try_iptables_cmd(["-I", "FORWARD", "1", "-i", bridge_name, "-j", "ACCEPT"], check_=True)
            if added is not None:
                _register_resource(reg, {
                    "kind": "forward-rule",
                    "name": ipt_rule_name,
                    "family": "",
                    "table": "iptables",
                    "chain": "FORWARD",
                    "bridge": bridge_name,
                })
                logger.info("Added accept rule to iptables FORWARD for %s", bridge_name)
            else:
                logger.warning("Failed to add iptables FORWARD accept rule for %s", bridge_name)
        elif ipt_check is not None:
            logger.debug("iptables FORWARD already has accept rule for %s", bridge_name)
        else:
            logger.debug("iptables not available on this host; skipping FORWARD rule")
    else:
        logger.debug("iptables FORWARD accept rule already registered for %s", bridge_name)

    # --- Docker DOCKER-USER chain ---
    docker_rule_name = "iptables/DOCKER-USER"
    if not _registry_has(reg, "forward-rule", docker_rule_name):
        docker_exists = _try_iptables_cmd(["-nL", "DOCKER-USER"])
        if docker_exists is not None and docker_exists.returncode == 0:
            docker_check = _try_iptables_cmd(["-C", "DOCKER-USER", "-i", bridge_name, "-j", "ACCEPT"])
            if docker_check is not None and docker_check.returncode != 0:
                added = _try_iptables_cmd(["-I", "DOCKER-USER", "1", "-i", bridge_name, "-j", "ACCEPT"], check_=True)
                if added is not None:
                    _register_resource(reg, {
                        "kind": "forward-rule",
                        "name": docker_rule_name,
                        "family": "",
                        "table": "iptables",
                        "chain": "DOCKER-USER",
                        "bridge": bridge_name,
                    })
                    logger.info("Added accept rule to DOCKER-USER for %s", bridge_name)
            elif docker_check is not None:
                logger.debug("DOCKER-USER already has accept rule for %s; registering", bridge_name)
                _register_resource(reg, {
                    "kind": "forward-rule",
                    "name": docker_rule_name,
                    "family": "",
                    "table": "iptables",
                    "chain": "DOCKER-USER",
                    "bridge": bridge_name,
                })
        elif docker_exists is not None:
            logger.debug("DOCKER-USER chain does not exist; skipping")
        else:
            logger.debug("iptables not available on this host; skipping DOCKER-USER rule")


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
    _ensure_existing_forward_accept(cmds, root, bridge_name)

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
