from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
from pathlib import Path

from ..kv_commands import Cmds
from .cloud_init import enable_network_config, ensure_user_data
from .incus import ensure_incus_daemon, ensure_profile_with_root_disk, ensure_storage_pool
from .instance import ensure_instance, maybe_restart_after_config
from .models import BuildVmConfig
from .network import (
    choose_existing_vm_network,
    ensure_network_device,
    ensure_vm_nic,
    get_default_host_interface,
    get_existing_network_parent,
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
    network_name = choose_existing_vm_network(cmds, cfg.vm_network or None)
    logger.info("Using existing Incus network %s for %s.", network_name, cfg.instance)

    created_new_instance, actual_instance_type = ensure_instance(
        cmds,
        cfg.instance,
        cfg.instance_type,
        cfg.profile,
        cfg.cpu,
        cfg.memory,
    )

    restart_required = False
    added_nic = ensure_vm_nic(cmds, cfg.instance, network_name)
    if added_nic:
        restart_required = True

    if ensure_user_data(cmds, cfg.instance, provision=cfg.provision):
        restart_required = True

    if enable_network_config(cmds, cfg.instance, "", actual_instance_type):
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

    maybe_provision_instance(cmds, cfg.instance, actual_instance_type, provision=cfg.provision)

    if cfg.provision:
        print(f"VM {cfg.instance} provisioned. Use incus exec {cfg.instance} -- bash to connect.")

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
