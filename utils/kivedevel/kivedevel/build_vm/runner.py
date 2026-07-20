from __future__ import annotations

import argparse
import dataclasses
import logging
import platform
import re
import shutil
import subprocess
import sys

from ..kv_commands import Cmds
from .cloud_init import enable_network_config, ensure_user_data
from .incus import ensure_incus_daemon, ensure_profile_with_root_disk, ensure_storage_pool
from .instance import ensure_instance, maybe_restart_after_config
from .models import BuildVmConfig
from .network import (
    ensure_managed_vm_network,
    ensure_network_device,
    ensure_vm_nic,
    get_default_host_interface,
    get_existing_network_parent,
    print_network_diagnostics,
    wait_vm_dhcp_lease,
)
from .provision import maybe_provision_instance
from .workspace import handle_workspace_attachment


logger = logging.getLogger("kivedevel")

_MAX_ETAG_RETRIES = 5
_ETAG_BACKOFF = 1.0

PROXY_DEVICE = "kive-web"
GUEST_WEB_PORT = 8000


def _retry_on_etag(fn, max_retries=_MAX_ETAG_RETRIES, initial_delay=_ETAG_BACKOFF):
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
    created_new_instance, actual_instance_type = ensure_instance(
        cmds,
        cfg.instance,
        cfg.instance_type,
        cfg.profile,
        cfg.cpu,
        cfg.memory,
    )

    restart_required = False
    host_interface: str | None = cfg.host_interface
    added_network = ensure_network_device(cmds, cfg.instance, host_interface)
    if added_network:
        if host_interface is None:
            host_interface = get_default_host_interface(cmds)
        restart_required = True
    elif host_interface is None:
        existing = get_existing_network_parent(cmds, cfg.instance)
        host_interface = existing if existing is not None else get_default_host_interface(cmds)

    if ensure_user_data(cmds, cfg.instance, provision=cfg.provision):
        restart_required = True

    if enable_network_config(cmds, cfg.instance, actual_instance_type):
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


def qemu_system_command_for_host() -> str | None:
    """Return the QEMU system emulator binary for the host architecture, or None."""
    machine = platform.machine().lower()
    candidates_by_arch = {
        "x86_64": ["qemu-system-x86_64"],
        "amd64": ["qemu-system-x86_64"],
        "aarch64": ["qemu-system-aarch64"],
        "arm64": ["qemu-system-aarch64"],
    }
    for candidate in candidates_by_arch.get(machine, []):
        if shutil.which(candidate):
            return candidate
    return None


def _check_vm_capability() -> None:
    """Fail early if the host cannot run Incus VM instances."""
    if qemu_system_command_for_host() is None:
        logger.error(
            "Incus VM mode requires a QEMU system emulator for %s, "
            "but none was found.\n\n"
            "On Ubuntu, install the package:\n"
            "  sudo apt-get install qemu-system-x86\n\n"
            "For CI or lightweight testing without VMs, use:\n"
            "  utils/dev smoke-local-install --instance-type container\n",
            platform.machine(),
        )
        sys.exit(1)


def _run_build_vm_vm(cfg: BuildVmConfig, cmds: Cmds) -> str:
    """Run build-vm for VM mode (default, recommended local dev)."""
    _check_vm_capability()
    nic_target = ensure_managed_vm_network(cmds, cfg.vm_network)
    bridge_name = nic_target.name
    logger.info("Using managed Incus bridge %s for %s.", bridge_name, cfg.instance)

    created_new_instance, actual_instance_type = ensure_instance(
        cmds,
        cfg.instance,
        cfg.instance_type,
        cfg.profile,
        cfg.cpu,
        cfg.memory,
    )

    restart_required = False
    added_nic = ensure_vm_nic(cmds, cfg.instance, nic_target)
    if added_nic:
        restart_required = True

    if ensure_user_data(cmds, cfg.instance, provision=cfg.provision):
        restart_required = True

    if enable_network_config(cmds, cfg.instance, actual_instance_type):
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

    if cfg.provision:
        logger.info("Waiting for DHCP lease on %s for %s...", bridge_name, cfg.instance)
        ip = wait_vm_dhcp_lease(cmds, cfg.instance, bridge_name)
        if ip is None:
            print_network_diagnostics(cmds, cfg.instance, bridge_name)
            logger.error(
                "VM %s did not receive a DHCP lease on %s. "
                "Check the bridge configuration and Incus network health.",
                cfg.instance, bridge_name,
            )
            sys.exit(1)
        logger.info("VM %s has IP %s.", cfg.instance, ip)

        logger.info("Checking VM network egress via incus exec...")
        _check_vm_egress(cmds, cfg.instance)

    maybe_provision_instance(cmds, cfg.instance, actual_instance_type, provision=cfg.provision)

    if cfg.provision:
        print(f"VM {cfg.instance} provisioned. Use incus exec {cfg.instance} -- bash to connect.")

    return actual_instance_type


_VM_EGRESS_CHECK_SCRIPT = """
ip addr
ip route
cat /etc/resolv.conf
getent ahostsv4 archive.ubuntu.com
python3 -c 'import socket; sock=socket.create_connection(("1.1.1.1",443), timeout=10); sock.close()' && echo "raw IPv4 egress OK"
python3 -c 'import socket; addr=socket.getaddrinfo("archive.ubuntu.com",80,socket.AF_INET,socket.SOCK_STREAM)[0][4]; sock=socket.create_connection(addr, timeout=10); sock.close()' && echo "archive.ubuntu.com:80 OK"
"""


_KNOWN_TRANSPORT_ERRORS = [
    "VM agent isn't currently running",
    "websocket: bad handshake",
    "connection refused",
    "not connected",
]


def _is_transport_error(stderr: str) -> bool:
    """Check if *stderr* matches a known Incus transport/agent error."""
    lower = stderr.strip().lower()
    for err in _KNOWN_TRANSPORT_ERRORS:
        if err.lower() in lower:
            return True
    return False


def _check_vm_egress(cmds: Cmds, instance: str) -> None:
    """Run a bounded egress check inside the VM via ``incus exec``.

    Server-side (build-vm host) runs this after DHCP lease is confirmed
    but before full provisioning begins.  It distinguishes:
    * DHCP lease present but guest TCP egress failed,
    * raw IPv4 TCP fails,
    * DNS works but TCP fails,
    * ``incus exec``/agent unavailable.

    If ``incus exec`` is unavailable, this is reported without blocking
    provisioning (the provision script inside the guest will do its own
    checks).
    """
    try:
        result = cmds.incus.run(
            ["exec", instance, "--", "sh", "-c", _VM_EGRESS_CHECK_SCRIPT],
            check=False, capture_output=True, timeout=60,
        )
    except subprocess.TimeoutExpired:
        logger.warning("VM egress check timed out (incus exec may be slow on first boot).")
        return
    except Exception as exc:
        logger.warning("VM egress check skipped (incus exec unavailable): %s", exc)
        return

    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        if _is_transport_error(stderr):
            logger.info(
                "VM egress check skipped: incus exec transport unavailable (%s). "
                "The guest provision script will verify connectivity.",
                stderr,
            )
            return
        output = (result.stdout or "") + stderr
        logger.warning(
            "VM egress command exited %s but no transport error detected. "
            "Output:\n%s",
            result.returncode, output,
        )
        return

    output = (result.stdout or "") + stderr
    if "raw IPv4 egress OK" not in output:
        logger.error(
            "VM %s has DHCP and a default route, but raw IPv4 egress failed.\n"
            "The Incus managed bridge is not providing outbound connectivity.\n"
            "Check the incusd daemon environment and Incus networking backend.\n\n"
            "Diagnostics:\n"
            "  incus network show kive-lab-br\n"
            "  incus network list-leases kive-lab-br\n"
            "  incus config show --expanded %s\n"
            "  incus info\n"
            "  dnsmasq --version\n"
            "  nft --version\n"
            "  iptables --version\n",
            instance, instance,
        )
        sys.exit(1)

    if "archive.ubuntu.com:80 OK" not in output:
        logger.warning(
            "VM %s has raw IP egress but cannot reach archive.ubuntu.com:80. "
            "This may be a transient DNS or routing issue.\n%s",
            instance, output,
        )
        return

    logger.info("VM egress check passed.")


def run_build_vm(args: argparse.Namespace) -> None:
    cfg = BuildVmConfig.from_args(args)
    cmds = Cmds.create()

    cfg.workdir.mkdir(parents=True, exist_ok=True)

    # Validate mode-specific flags.
    if cfg.instance_type == "vm" and cfg.host_interface is not None:
        logger.error(
            "--host-interface is not supported in VM mode. "
            "VM networking uses the managed Incus bridge.",
        )
        sys.exit(1)
    if cfg.instance_type == "container" and cfg.vm_network is not None:
        logger.error(
            "--vm-network is not supported in container mode. "
            "Container networking uses the host interface.",
        )
        sys.exit(1)

    # Resolve defaults per mode.
    if cfg.instance_type == "vm" and cfg.vm_network is None:
        cfg = dataclasses.replace(cfg, vm_network="kive-lab-br")
    if cfg.instance_type == "container" and cfg.host_interface is None:
        cfg = dataclasses.replace(cfg, host_interface=None)

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
