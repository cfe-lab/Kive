from __future__ import annotations

import argparse
import logging

from ..kv_commands import Cmds
from .cloud_init import enable_network_config, ensure_user_data
from .incus import ensure_incus_daemon, ensure_profile_with_root_disk, ensure_storage_pool
from .instance import ensure_instance, maybe_restart_after_config
from .models import BuildVmConfig
from .network import ensure_network_device, get_default_host_interface, get_existing_network_parent
from .workspace import handle_workspace_attachment


logger = logging.getLogger("kivedevel")


def run_build_vm(args: argparse.Namespace) -> None:
    cfg = BuildVmConfig.from_args(args)
    cmds = Cmds.create()

    cfg.workdir.mkdir(parents=True, exist_ok=True)

    cmds.require_all()
    ensure_incus_daemon(cmds, cfg.root, cfg.workdir)
    ensure_storage_pool(cmds, cfg.pool)
    ensure_profile_with_root_disk(cmds, cfg.profile, cfg.pool, cfg.root_size)

    created_new_instance = ensure_instance(
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
        # For existing instances, infer the configured parent NIC so cloud-init
        # network config can match the actual bridge setup.
        host_interface = get_existing_network_parent(cmds, cfg.instance) or get_default_host_interface(cmds)

    if ensure_user_data(cmds, cfg.instance):
        restart_required = True

    if enable_network_config(cmds, cfg.instance, host_interface, cfg.instance_type):
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
            cfg.instance_type,
        )
    elif out and "kive-code:" in out:
        logger.info("Device 'kive-code' is already attached to %s.", cfg.instance)

    logger.info(
        "Build step complete. Use ws-enter-vm (or ./utils/dev enter-vm) to connect to %s.",
        cfg.instance,
    )
