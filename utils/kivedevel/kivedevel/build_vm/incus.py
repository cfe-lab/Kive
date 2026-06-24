from __future__ import annotations

import json
import logging
import re
import subprocess
import sys
import time
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")


def ensure_incus_daemon(cmds: Cmds, root: Path, workdir: Path) -> None:
    if cmds.incus.ok(["info"]):
        return

    logger.info("Incus is not running. Attempting to start the daemon via systemctl...")
    subprocess.run(
        ["systemctl", "start", "incus.service", "incus.socket"],
        capture_output=True,
    )

    time.sleep(3)
    if cmds.incus.ok(["info"]):
        return

    logger.error(
        "Incus does not appear to be running or accessible.\n"
        "Please install and initialize Incus, ensure your user can access it,"
        " then rerun this command."
    )
    sys.exit(1)


def ensure_storage_pool(cmds: Cmds, pool: str) -> None:
    try:
        out = cmds.incus.output(["storage", "list", "--format=csv"])
        if not out or not re.search(f"^{re.escape(pool)},", out, re.MULTILINE):
            logger.info("Creating storage pool %s...", pool)
            cmds.incus.run(["storage", "create", pool, "dir"])
    except Exception as exc:
        logger.warning("Could not create storage pool: %s", exc)


def ensure_profile_with_root_disk(cmds: Cmds, profile: str, pool: str, root_size: str) -> None:
    if not cmds.incus.ok(["profile", "show", profile]):
        logger.info("Profile %s not found. Creating it...", profile)
        cmds.incus.run(["profile", "create", profile])

    out = cmds.incus.output(["profile", "show", profile, "--format", "json"])
    try:
        profile_data = json.loads(out) if out else {}
    except json.JSONDecodeError:
        profile_data = {}
    devices = profile_data.get("devices", {}) if isinstance(profile_data, dict) else {}

    if not devices:
        logger.info("Adding root disk to profile %s...", profile)
        cmds.incus.run(
            [
                "profile",
                "device",
                "add",
                profile,
                "root",
                "disk",
                f"pool={pool}",
                "path=/",
                f"size={root_size}",
            ]
        )
        return

    root_device = devices.get("root")
    if root_device is None:
        logger.info("Adding root disk to profile %s...", profile)
        cmds.incus.run(
            [
                "profile",
                "device",
                "add",
                profile,
                "root",
                "disk",
                f"pool={pool}",
                "path=/",
                f"size={root_size}",
            ]
        )
        return

    if not isinstance(root_device, dict) or root_device.get("type") != "disk" or root_device.get("path") != "/":
        logger.error(
            "Profile %s has a device named 'root' that is not a disk at /.\n"
            "Found: %s\n"
            "Fix the profile or use a different profile with --profile.",
            profile,
            root_device,
        )
        sys.exit(1)

    logger.debug("Profile %s already has a valid root disk device.", profile)
