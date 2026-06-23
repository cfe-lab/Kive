from __future__ import annotations

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

    if not cmds.incus.ok(["profile", "device", "show", profile]):
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
