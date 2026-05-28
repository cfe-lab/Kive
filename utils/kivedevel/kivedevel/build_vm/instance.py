from __future__ import annotations

import logging
import re
import sys

from ..kv_commands import Cmds
from ..shared import instance_exists
from .helpers import instance_is_cloud_variant


logger = logging.getLogger("kivedevel")


def ensure_instance(cmds: Cmds, instance: str, instance_type: str, profile: str, cpu: str, memory: str) -> bool:
    if not instance_exists(cmds, instance):
        logger.info("Creating %s instance %s...", instance_type, instance)
        create_args = [
            "create",
            "images:ubuntu/noble/cloud",
            instance,
            "--config",
            f"limits.cpu={cpu}",
            "--config",
            f"limits.memory={memory}",
            "--profile",
            profile,
        ]
        if instance_type == "vm":
            create_args.insert(3, "--vm")
        cmds.incus.run(create_args)
        return True

    if not instance_is_cloud_variant(cmds, instance):
        logger.error(
            "Existing VM '%s' was not created from a cloud image and won't reliably apply login/agent config.",
            instance,
        )
        logger.error("Please run: incus delete -f -- %s && ./utils/dev build-vm %s", instance, instance)
        sys.exit(2)

    logger.info("Instance %s already exists. Skipping creation.", instance)
    out = cmds.incus.output(["info", instance])
    if out and not re.search(r"^Status:\s+Running$", out, re.IGNORECASE | re.MULTILINE):
        logger.info("Starting instance %s...", instance)
        cmds.incus.run(["start", instance])
    return False


def maybe_restart_after_config(cmds: Cmds, instance: str, restart_required: bool) -> None:
    if not restart_required:
        return

    try:
        out = cmds.incus.output(["info", instance])
        if out and re.search(r"^Status:\s+Running$", out, re.IGNORECASE | re.MULTILINE):
            logger.info("Restarting %s to apply configuration changes...", instance)
            cmds.incus.run(["restart", instance])
        elif out:
            logger.info("Starting %s after configuration changes...", instance)
            cmds.incus.run(["start", instance])
    except Exception as exc:
        logger.warning("Could not restart instance: %s", exc)
