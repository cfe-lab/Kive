from __future__ import annotations

import json
import logging
import re
import sys

from ..kv_commands import Cmds
from ..shared import instance_exists
from .helpers import instance_is_cloud_variant


logger = logging.getLogger("kivedevel")


def _parse_incus_instance_type(cmds: Cmds, instance: str) -> str:
    """Query Incus for the actual type of *instance* and normalize it.

    Returns ``"vm"`` for ``virtual-machine`` and ``"container"`` for
    ``container``.  Fails with ``SystemExit`` on empty, malformed, or
    unknown output.
    """
    out = cmds.incus.output(["list", instance, "--format", "json"])
    if not out:
        logger.error("Empty Incus output for %s.", instance)
        sys.exit(2)
    try:
        payload = json.loads(out)
    except json.JSONDecodeError as exc:
        logger.error("Invalid Incus JSON for %s: %s", instance, exc)
        sys.exit(2)
    if not isinstance(payload, list):
        logger.error("Expected a JSON list from incus list, got %s.", type(payload).__name__)
        sys.exit(2)
    if not payload:
        logger.error("Instance %s returned no data from Incus.", instance)
        sys.exit(2)
    entry = payload[0]
    if not isinstance(entry, dict):
        logger.error("Expected a JSON object for %s, got %s.", instance, type(entry).__name__)
        sys.exit(2)
    raw = entry.get("type", "")
    if raw == "virtual-machine":
        return "vm"
    if raw == "container":
        return "container"
    logger.error(
        "Instance %s has unknown type %r.  Expected 'container' or 'virtual-machine'.",
        instance, raw,
    )
    sys.exit(2)


def ensure_instance(cmds: Cmds, instance: str, instance_type: str, profile: str, cpu: str, memory: str, vm_network: str | None = None) -> tuple[bool, str]:
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
            "--config",
            "user.kive.devel.created-by=utils/dev",
            "--profile",
            profile,
        ]
        if instance_type == "vm":
            create_args.insert(3, "--vm")
            if vm_network:
                create_args += ["--network", vm_network]

        result = cmds.incus.run(create_args, check=False, capture_output=True)
        if result.returncode != 0:
            stderr_lower = (result.stderr or "").lower()
            cmd_str = " ".join(str(a) for a in create_args)
            stderr_text = (result.stderr or "").strip()
            stdout_text = (result.stdout or "").strip()
            if instance_type == "vm" and "instance type \"virtual-machine\" is not supported" in stderr_lower:
                logger.error(
                    "Incus VM instances are not supported on this host.\n"
                    "The default local Kive development environment requires an Incus VM "
                    "so Singularity jobs can run reliably.\n"
                    "Enable Incus VM support, or explicitly run with --instance-type container "
                    "for CI/lightweight smoke testing only."
                )
                raise RuntimeError("Incus VM instances are not supported on this host.")
            if instance_type == "container":
                if "no uid/gid allocation configured" in stderr_lower or "no map found for user" in stderr_lower:
                    logger.warning(
                        "Unprivileged container creation failed; retrying %s as privileged.", instance,
                    )
                    privileged_args = create_args + ["--config", "security.privileged=true"]
                    privileged_result = cmds.incus.run(privileged_args, check=False, capture_output=True)
                    if privileged_result.returncode == 0:
                        logger.info("Successfully created privileged instance %s.", instance)
                        return True, instance_type

                    pri_stderr = (privileged_result.stderr or "").strip()
                    pri_stdout = (privileged_result.stdout or "").strip()
                    logger.error(
                        "Automatic privileged retry also failed for %s.\n"
                        "stdout: %s\nstderr: %s",
                        instance,
                        pri_stdout or "(empty)",
                        pri_stderr or "(empty)",
                    )
                    raise RuntimeError(
                        f"Automatic privileged retry failed for {instance}. "
                        f"See the log output above for details."
                    )
            logger.error(
                "Failed to create instance.\n"
                "Command: %s\n"
                "Return code: %s\n"
                "stdout: %s\n"
                "stderr: %s",
                cmd_str, result.returncode,
                stdout_text or "(empty)",
                stderr_text or "(empty)",
            )
            raise RuntimeError(f"Failed to create instance {instance} (rc={result.returncode}).")
        return True, instance_type

    # Verify the existing instance type matches the requested type first.
    actual_type = _parse_incus_instance_type(cmds, instance)
    if actual_type != instance_type:
        incus_display = {"virtual-machine": "vm", "container": "container"}.get(actual_type, actual_type)
        logger.error(
            "Instance %s exists as %s, but --instance-type=%s was requested.\n"
            "Delete it first:  incus delete -f -- %s",
            instance, incus_display, instance_type, instance,
        )
        sys.exit(2)

    if not instance_is_cloud_variant(cmds, instance):
        type_label = {"vm": "VM", "container": "container"}.get(actual_type, actual_type)
        logger.error(
            "Existing %s '%s' was not created from a cloud image and won't reliably apply login/agent config.",
            type_label, instance,
        )
        logger.error("Please run: incus delete -f -- %s && ./utils/dev build-vm %s", instance, instance)
        sys.exit(2)

    logger.info("Instance %s already exists. Skipping creation.", instance)
    out = cmds.incus.output(["info", instance])
    if out and not re.search(r"^Status:\s+Running$", out, re.IGNORECASE | re.MULTILINE):
        logger.info("Starting instance %s...", instance)
        cmds.incus.run(["start", instance])
    return False, instance_type


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
