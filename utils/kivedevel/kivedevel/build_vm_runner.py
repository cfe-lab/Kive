from __future__ import annotations

import argparse
import logging

from .build_vm_cloud_init import enable_network_config, ensure_user_data
from .build_vm_incus import ensure_incus_daemon, ensure_profile_with_root_disk, ensure_storage_pool
from .build_vm_instance import ensure_instance, maybe_restart_after_config
from .build_vm_network import ensure_network_device, get_default_host_interface
from .build_vm_workspace import handle_workspace_attachment
from .kv_commands import Cmds


logger = logging.getLogger("kivedevel")


def run_build_vm(args: argparse.Namespace) -> None:
    cmds = Cmds.create()

    root = args.root.resolve()
    workdir = args.workdir.resolve()
    instance = args.instance
    instance_type = args.instance_type
    image_path = workdir / args.image_name
    pool = args.pool
    profile = args.profile
    root_size = args.root_size
    memory = args.memory
    cpu = args.cpu
    host_interface = args.host_interface

    workdir.mkdir(parents=True, exist_ok=True)

    cmds.require_all()
    ensure_incus_daemon(cmds, root, workdir)
    ensure_storage_pool(cmds, pool)
    ensure_profile_with_root_disk(cmds, profile, pool, root_size)

    created_new_instance = ensure_instance(cmds, instance, instance_type, profile, cpu, memory)

    restart_required = False
    if ensure_network_device(cmds, instance, host_interface):
        if not host_interface:
            host_interface = get_default_host_interface(cmds)
        restart_required = True

    if ensure_user_data(cmds, instance):
        restart_required = True

    if enable_network_config(cmds, instance, host_interface):
        restart_required = True

    if not created_new_instance:
        logger.info(
            "Note: cloud-init usually runs only on first boot. Existing VMs may require recreation to apply updated login/agent settings."
        )

    maybe_restart_after_config(cmds, instance, restart_required)

    out = cmds.incus.output(["config", "show", instance])
    if out and "kive-code:" not in out:
        handle_workspace_attachment(cmds, instance, image_path, root, workdir, instance_type)
    elif out and "kive-code:" in out:
        logger.info("Device 'kive-code' is already attached to %s.", instance)

    logger.info(
        "Build step complete. Use ws-enter-vm (or ./utils/dev enter-vm) to connect to %s.",
        instance,
    )
