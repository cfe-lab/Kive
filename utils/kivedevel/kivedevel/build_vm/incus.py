from __future__ import annotations

import logging
import re
import subprocess
import sys
import time
from pathlib import Path

import yaml

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


_SIZE_RE = re.compile(r"^(\d+)\s*(?:GiB|[gG][bB])?$")


def _parse_gib(text: str) -> int | None:
    """Parse a size string like ``10GiB``, ``60GiB``, ``10GB``, or ``10``
    and return the value in GiB.  Returns ``None`` if unparseable."""
    m = _SIZE_RE.match(text.strip())
    if m:
        return int(m.group(1))
    return None


def _incus_profile_device_set(cmds: Cmds, profile: str, device: str, key: str, value: str) -> None:
    """Set a profile device property via ``incus profile device set``."""
    cmds.incus.run(["profile", "device", "set", profile, device, key, value])


def _ensure_root_size(cmds: Cmds, profile: str, root_device: dict, requested_size: str) -> None:
    """Ensure the root disk has at least *requested_size*.

    * If the root disk has no size, set it to *requested_size*.
    * If it has a parsable size smaller than *requested_size*, update it.
    * If it has a parsable size >= *requested_size*, no-op.
    * If the existing size is unparseable, log a warning and skip.
    """
    existing_raw = root_device.get("size", "").strip()
    if not existing_raw:
        logger.info(
            "Root disk in profile %s has no explicit size; setting to %s.",
            profile, requested_size,
        )
        _incus_profile_device_set(cmds, profile, "root", "size", requested_size)
        return

    existing_gib = _parse_gib(existing_raw)
    requested_gib = _parse_gib(requested_size)

    if existing_gib is not None and requested_gib is not None:
        if existing_gib >= requested_gib:
            logger.debug(
                "Root disk in profile %s already has %s (>= requested %s).",
                profile, existing_raw, requested_size,
            )
            return
        logger.info(
            "Root disk in profile %s is %s — enlarging to %s.",
            profile, existing_raw, requested_size,
        )
        _incus_profile_device_set(cmds, profile, "root", "size", requested_size)
        return

    logger.warning(
        "Root disk in profile %s has unparseable size %r; "
        "keeping existing size. Requested was %s.",
        profile, existing_raw, requested_size,
    )


def ensure_profile_with_root_disk(cmds: Cmds, profile: str, pool: str, root_size: str) -> None:
    if not cmds.incus.ok(["profile", "show", profile]):
        logger.info("Profile %s not found. Creating it...", profile)
        cmds.incus.run(["profile", "create", profile])

    out = cmds.incus.output(["profile", "device", "show", profile])
    try:
        devices = yaml.safe_load(out)
    except yaml.YAMLError as exc:
        logger.error(
            "Failed to parse device list from profile %s: %s",
            profile,
            exc,
        )
        sys.exit(1)

    if not isinstance(devices, dict):
        logger.error(
            "Expected a device mapping from profile %s, got: %s",
            profile,
            type(devices).__name__,
        )
        sys.exit(1)

    root_device = devices.get("root")
    if root_device is not None:
        if isinstance(root_device, dict) and root_device.get("type") == "disk" and root_device.get("path") == "/":
            _ensure_root_size(cmds, profile, root_device, root_size)
            return
        logger.error(
            "Profile %s has a device named 'root' that is not a disk at /.\n"
            "Found: %s\n"
            "Fix the profile or use a different profile with --profile.",
            profile,
            root_device,
        )
        sys.exit(1)

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
