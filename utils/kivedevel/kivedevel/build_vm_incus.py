from __future__ import annotations

import logging
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

from .kv_commands import Cmds


logger = logging.getLogger("kivedevel")


def ensure_incus_daemon(cmds: Cmds, root: Path, workdir: Path) -> None:
    if cmds.incus.ok(["info"]):
        return

    if not shutil.which("ws-start-incus-daemon"):
        logger.error("Incus daemon is not running and ws-start-incus-daemon is not available.")
        logger.error("Start the daemon manually or install the helper script.")
        sys.exit(1)

    logger.info("Starting Incus daemon using ws-start-incus-daemon...")
    subprocess.run(["sudo", "--", "pkill", "-f", "incusd --group incus-admin"], check=False)

    log_file = workdir / "incusd_restart.log"
    with open(log_file, "w") as handle:
        subprocess.Popen(["ws-start-incus-daemon", "--", str(root)], stdout=handle, stderr=handle)

    time.sleep(5)
    if not cmds.incus.ok(["info"]):
        logger.error("Failed to start Incus daemon. See %s.", log_file)
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
