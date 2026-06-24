from __future__ import annotations

import argparse
import logging
import re

from ..kv_commands import Cmds
from .cloud_init import enable_network_config, ensure_user_data
from .incus import ensure_incus_daemon, ensure_profile_with_root_disk, ensure_storage_pool
from .instance import ensure_instance, maybe_restart_after_config
from .models import BuildVmConfig
from .network import ensure_network_device, get_default_host_interface, get_existing_network_parent
from .provision import maybe_provision_instance
from .workspace import handle_workspace_attachment


logger = logging.getLogger("kivedevel")


PROXY_DEVICE = "kive-web"
GUEST_WEB_PORT = 8000


def _proxy_config(host_port: int) -> dict[str, str]:
    return {
        "listen": f"tcp:127.0.0.1:{host_port}",
        "connect": f"tcp:127.0.0.1:{GUEST_WEB_PORT}",
        "type": "proxy",
    }


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


def _ensure_web_proxy_device(cmds: Cmds, cfg: BuildVmConfig) -> None:
    if cfg.no_web_proxy:
        logger.debug("Web proxy device creation disabled by --no-web-proxy.")
        return

    desired = _proxy_config(cfg.web_port)
    current = _current_proxy_config(cmds, cfg.instance)

    if current is None:
        logger.info(
            "Creating proxy device '%s' (%s -> %s)...",
            PROXY_DEVICE, desired["listen"], desired["connect"],
        )
        cmds.incus.run([
            "config", "device", "add", cfg.instance, PROXY_DEVICE,
            desired["type"],
            f"listen={desired['listen']}",
            f"connect={desired['connect']}",
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
        desired["type"],
        f"listen={desired['listen']}",
        f"connect={desired['connect']}",
    ])


def run_build_vm(args: argparse.Namespace) -> None:
    cfg = BuildVmConfig.from_args(args)
    cmds = Cmds.create()

    cfg.workdir.mkdir(parents=True, exist_ok=True)

    cmds.require_all()
    ensure_incus_daemon(cmds, cfg.root, cfg.workdir)
    ensure_storage_pool(cmds, cfg.pool)
    ensure_profile_with_root_disk(cmds, cfg.profile, cfg.pool, cfg.root_size)

    created_new_instance, actual_instance_type = ensure_instance(
        cmds,
        cfg.instance,
        cfg.instance_type,
        cfg.profile,
        cfg.cpu,
        cfg.memory,
    )

    restart_required = False
    instance_type = actual_instance_type
    host_interface = cfg.host_interface
    added_network = ensure_network_device(cmds, cfg.instance, host_interface)
    if added_network:
        if not host_interface:
            host_interface = get_default_host_interface(cmds)
        restart_required = True
    elif not host_interface:
        # For existing instances, infer the configured parent NIC so cloud-init
        # network config can match the actual bridge setup.
        host_interface = get_existing_network_parent(cmds, cfg.instance) or get_default_host_interface(cmds)

    if ensure_user_data(cmds, cfg.instance, provision=cfg.provision):
        restart_required = True

    if enable_network_config(cmds, cfg.instance, host_interface, instance_type):
        restart_required = True

    if not created_new_instance:
        logger.info(
            "Note: cloud-init usually runs only on first boot. Existing VMs may require recreation to apply updated login/agent settings."
        )

    maybe_restart_after_config(cmds, cfg.instance, restart_required)

    out = cmds.incus.output(["config", "show", cfg.instance])
    if out and "kive-code:" not in out:
        handle_workspace_attachment(
            cmds,
            cfg.instance,
            cfg.image_path,
            cfg.root,
            cfg.workdir,
            instance_type,
        )
    elif out and "kive-code:" in out:
        logger.info("Device 'kive-code' is already attached to %s.", cfg.instance)

    _ensure_web_proxy_device(cmds, cfg)

    maybe_provision_instance(
        cmds,
        cfg.instance,
        instance_type,
        provision=cfg.provision,
    )

    logger.info(
        "Build step complete. Use ./utils/dev enter-vm %s to connect.",
        cfg.instance,
    )

    if cfg.provision and not cfg.no_web_proxy:
        print(f"Kive is available at: http://127.0.0.1:{cfg.web_port}/login/")
