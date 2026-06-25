from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
from pathlib import Path

from ..kv_commands import Cmds
from .cloud_init import enable_network_config, ensure_user_data
from .incus import ensure_incus_daemon, ensure_profile_with_root_disk, ensure_storage_pool
from .instance import ensure_instance, maybe_restart_after_config
from .models import BuildVmConfig
from .network import (
    _read_registry,
    _register_resource,
    _registry_path,
    ensure_network_device,
    ensure_owned_bridge,
    ensure_vm_nic,
    get_default_host_interface,
    get_existing_network_parent,
)
from .provision import maybe_provision_instance
from .workspace import handle_workspace_attachment


logger = logging.getLogger("kivedevel")

_MAX_ETAG_RETRIES = 5
_ETAG_BACKOFF = 1.0


def _retry_on_etag(fn, max_retries=_MAX_ETAG_RETRIES, initial_delay=_ETAG_BACKOFF):
    """Call *fn* and retry up to *max_retries* times if the error
    contains ``ETag doesn't match``."""
    import time as _time
    for attempt in range(max_retries):
        try:
            return fn()
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "") + (getattr(exc, "output", None) or "")
            if "ETag doesn't match" in stderr and attempt < max_retries - 1:
                delay = initial_delay * (attempt + 1)
                logger.info("ETag mismatch on attempt %d/%d, retrying in %.1fs...", attempt + 1, max_retries, delay)
                _time.sleep(delay)
                continue
            raise


PROXY_DEVICE = "kive-web"
GUEST_WEB_PORT = 8000
VM_NETWORK = "kive-devel-br"
VM_CIDR = ""
VM_IP = ""


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _find_owned_forward(reg: Path, port: int, vm_ip: str) -> int | None:
    for entry in _read_registry(reg):
        if (
            entry.get("kind") == "host-forward"
            and entry.get("port") == port
            and entry.get("vm_ip") == vm_ip
        ):
            pid = entry.get("pid")
            if pid and _pid_alive(pid):
                return pid
    return None


def _find_owned_forward_by_port(reg: Path, port: int) -> int | None:
    for entry in _read_registry(reg):
        if entry.get("kind") == "host-forward" and entry.get("port") == port:
            pid = entry.get("pid")
            if pid:
                return pid
    return None


def _kill_forward(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
    except (OSError, ProcessLookupError):
        pass


def _remove_registry_entry(reg: Path, kind: str, port: int) -> None:
    entries = _read_registry(reg)
    entries = [e for e in entries if not (e.get("kind") == kind and e.get("port") == port)]
    reg.write_text(json.dumps(entries, indent=2) + "\n")


def _wait_for_listening_pid(port: int, timeout: float = 30.0) -> int:
    """Wait for a process to start listening on *port* and return its PID.

    Parses ``ss -tlnp`` output so it always returns the PID of the process
    that actually holds the socket open (not a guix wrapper parent).
    """
    import time as _time
    deadline = _time.monotonic() + timeout
    pid_re = re.compile(r"pid=(\d+)")
    while _time.monotonic() < deadline:
        result = subprocess.run(
            ["sudo", "ss", "-tlnp"],
            capture_output=True, text=True, timeout=10,
        )
        for line in result.stdout.splitlines():
            if f"127.0.0.1:{port}" not in line:
                continue
            m = pid_re.search(line)
            if m:
                return int(m.group(1))
        _time.sleep(0.5)
    raise TimeoutError(f"socat did not start listening on port {port} within {timeout}s")


def _start_socat(port: int, vm_ip: str, cmds: Cmds) -> int:
    logger.info("Starting host port forward via socat (127.0.0.1:%d -> %s:%d)...", port, vm_ip, port)
    socat_args = [
        f"TCP-LISTEN:{port},bind=127.0.0.1,reuseaddr,fork",
        f"TCP:{vm_ip}:{port}",
    ]
    if shutil.which("socat") is not None:
        subprocess.Popen(
            ["socat"] + socat_args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        socat_argv = cmds.socat._argv(socat_args, sudo=False)
        subprocess.Popen(
            socat_argv,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    pid = _wait_for_listening_pid(port)
    return pid


def _ensure_web_port_forward(cfg: BuildVmConfig, vm_ip: str, root: Path, cmds: Cmds) -> None:
    if cfg.no_web_proxy:
        logger.debug("Web port forward disabled by --no-web-proxy.")
        return

    reg = _registry_path(root)
    port = cfg.web_port

    existing_pid = _find_owned_forward(reg, port, vm_ip)
    if existing_pid is not None:
        logger.debug("Host port forward for %d already running (pid %d).", port, existing_pid)
        return

    stale_pid = _find_owned_forward_by_port(reg, port)
    if stale_pid is not None:
        logger.info("Removing stale host forward (pid %d)...", stale_pid)
        _kill_forward(stale_pid)
        _remove_registry_entry(reg, "host-forward", port)

    if _port_in_use(port):
        logger.error(
            "Port %d is already in use by a process not owned by utils/dev.\n"
            "Choose a different port with --web-port or stop the other process.",
            port,
        )
        sys.exit(1)

    pid = _start_socat(port, vm_ip, cmds)
    _register_resource(reg, {
        "kind": "host-forward",
        "pid": pid,
        "port": port,
        "vm_ip": vm_ip,
        "created_by": "utils/dev",
        "project": "Kive",
    })


def _proxy_config(cfg: BuildVmConfig) -> dict[str, str]:
    config = {
        "listen": f"tcp:127.0.0.1:{cfg.web_port}",
        "type": "proxy",
        "connect": f"tcp:127.0.0.1:{GUEST_WEB_PORT}",
    }
    return config


def _current_proxy_config(cmds: Cmds, instance: str) -> dict[str, str] | None:
    out = cmds.incus.output(["config", "show", instance])
    block_pattern = re.compile(
        rf"^{PROXY_DEVICE}:\s*\n((?:\s+\w+: .+\n?)*)",
        re.MULTILINE,
    )
    m = block_pattern.search(out)
    if not m:
        return None
    config: dict[str, str] = {}
    for line in m.group(1).splitlines():
        line = line.strip()
        if ":" in line:
            key, _, val = line.partition(":")
            config[key.strip()] = val.strip()
    return config


def _proxy_args(desired: dict[str, str]) -> list[str]:
    args_list = [
        desired["type"],
        f"listen={desired['listen']}",
        f"connect={desired['connect']}",
    ]
    if "nat" in desired:
        args_list.append(f"nat={desired['nat']}")
    return args_list


def _ensure_web_proxy_device(cmds: Cmds, cfg: BuildVmConfig) -> None:
    if cfg.no_web_proxy:
        logger.debug("Web proxy device creation disabled by --no-web-proxy.")
        return

    desired = _proxy_config(cfg)
    current = _current_proxy_config(cmds, cfg.instance)

    if current is None:
        logger.info(
            "Creating proxy device '%s' (%s -> %s)...",
            PROXY_DEVICE, desired["listen"], desired["connect"],
        )
        cmds.incus.run([
            "config", "device", "add", cfg.instance, PROXY_DEVICE,
            *_proxy_args(desired),
        ])
        return

    if current.get("listen") == desired["listen"] and current.get("connect") == desired["connect"]:
        logger.debug(
            "Proxy device '%s' already present with matching config (%s -> %s).",
            PROXY_DEVICE, desired["listen"], desired["connect"],
        )
        return

    logger.debug(
        "Updating proxy device '%s': was (%s -> %s), now (%s -> %s).",
        PROXY_DEVICE,
        current.get("listen", "?"), current.get("connect", "?"),
        desired["listen"], desired["connect"],
    )
    cmds.incus.run(["config", "device", "remove", cfg.instance, PROXY_DEVICE])
    cmds.incus.run([
        "config", "device", "add", cfg.instance, PROXY_DEVICE,
        *_proxy_args(desired),
    ])


def _run_build_vm_container(cfg: BuildVmConfig, cmds: Cmds) -> str:
    """Run build-vm for container mode (CI/lightweight smoke testing)."""
    created_new_instance, actual_instance_type = ensure_instance(
        cmds,
        cfg.instance,
        cfg.instance_type,
        cfg.profile,
        cfg.cpu,
        cfg.memory,
    )

    restart_required = False
    host_interface = cfg.host_interface
    added_network = ensure_network_device(cmds, cfg.instance, host_interface)
    if added_network:
        if not host_interface:
            host_interface = get_default_host_interface(cmds)
        restart_required = True
    elif not host_interface:
        host_interface = get_existing_network_parent(cmds, cfg.instance) or get_default_host_interface(cmds)

    if ensure_user_data(cmds, cfg.instance, provision=cfg.provision):
        restart_required = True

    if enable_network_config(cmds, cfg.instance, host_interface, actual_instance_type):
        restart_required = True

    if not created_new_instance:
        logger.info(
            "Note: cloud-init usually runs only on first boot. "
            "Existing instances may require recreation to apply updated login/agent settings."
        )

    maybe_restart_after_config(cmds, cfg.instance, restart_required)

    out = cmds.incus.output(["config", "show", cfg.instance])
    if out and "kive-code:" not in out:
        handle_workspace_attachment(cmds, cfg.instance, cfg.image_path, cfg.root, cfg.workdir, actual_instance_type)
    elif out and "kive-code:" in out:
        logger.info("Device 'kive-code' is already attached to %s.", cfg.instance)

    _ensure_web_proxy_device(cmds, cfg)

    maybe_provision_instance(cmds, cfg.instance, actual_instance_type, provision=cfg.provision)

    if cfg.provision and not cfg.no_web_proxy:
        print(f"Kive is available at: http://127.0.0.1:{cfg.web_port}/login/")

    return actual_instance_type


def _run_build_vm_vm(cfg: BuildVmConfig, cmds: Cmds) -> str:
    """Run build-vm for VM mode (default, recommended local dev)."""
    actual_cidr, actual_vm_ip = ensure_owned_bridge(
        cmds, cfg.root, cfg.vm_network, cfg.vm_cidr, cfg.vm_ip, cfg.workdir,
    )
    gateway = actual_cidr.rsplit(".", 1)[0] + ".1"
    logger.debug("Using bridge %s (cidr=%s, vm_ip=%s, gateway=%s)", cfg.vm_network, actual_cidr, actual_vm_ip, gateway)

    created_new_instance, actual_instance_type = ensure_instance(
        cmds,
        cfg.instance,
        cfg.instance_type,
        cfg.profile,
        cfg.cpu,
        cfg.memory,
    )

    restart_required = False
    added_nic = ensure_vm_nic(cmds, cfg.instance, cfg.vm_network)
    if added_nic:
        restart_required = True

    if ensure_user_data(cmds, cfg.instance, provision=cfg.provision):
        restart_required = True

    if enable_network_config(cmds, cfg.instance, "", actual_instance_type,
                             static_ip=actual_vm_ip, cidr=actual_cidr, gateway=gateway):
        restart_required = True

    if not created_new_instance:
        logger.info(
            "Note: cloud-init usually runs only on first boot. "
            "Existing VMs may require recreation to apply updated login/agent settings."
        )

    out = cmds.incus.output(["config", "show", cfg.instance])
    if out and "kive-code:" not in out:
        _retry_on_etag(lambda: handle_workspace_attachment(
            cmds, cfg.instance, cfg.image_path, cfg.root, cfg.workdir, actual_instance_type,
        ))
    elif out and "kive-code:" in out:
        logger.info("Device 'kive-code' is already attached to %s.", cfg.instance)

    maybe_restart_after_config(cmds, cfg.instance, restart_required)

    _ensure_web_port_forward(cfg, actual_vm_ip, cfg.root, cmds)

    maybe_provision_instance(cmds, cfg.instance, actual_instance_type, provision=cfg.provision, bridge_name=cfg.vm_network)

    if cfg.provision and not cfg.no_web_proxy:
        print(f"Kive is available at: http://127.0.0.1:{cfg.web_port}/login/")

    return actual_instance_type


def run_build_vm(args: argparse.Namespace) -> None:
    cfg = BuildVmConfig.from_args(args)
    cmds = Cmds.create()

    cfg.workdir.mkdir(parents=True, exist_ok=True)

    cmds.require_all()
    ensure_incus_daemon(cmds, cfg.root, cfg.workdir)
    ensure_storage_pool(cmds, cfg.pool)
    ensure_profile_with_root_disk(cmds, cfg.profile, cfg.pool, cfg.root_size)

    if cfg.instance_type == "vm":
        _run_build_vm_vm(cfg, cmds)
    else:
        _run_build_vm_container(cfg, cmds)

    logger.info(
        "Build step complete. Use ./utils/dev enter-vm %s to connect.",
        cfg.instance,
    )
