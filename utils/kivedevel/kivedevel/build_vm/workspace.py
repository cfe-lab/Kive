from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")


_RESOURCE_MARKER = ".kive-devel-resource.json"


def _write_marker(workdir: Path) -> None:
    marker = workdir / _RESOURCE_MARKER
    if not marker.exists():
        marker.write_text(json.dumps({"created-by": "utils/dev"}) + "\n")
        logger.debug("Created resource marker %s", marker)


def _sudo(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["sudo", "--"] + list(args),
        check=True,
        capture_output=True,
        text=True,
    )


def _cleanup_umount(path: Path) -> None:
    try:
        _sudo("umount", "--", str(path))
    except subprocess.CalledProcessError:
        # Cleanup path may exist without an active mount.
        pass


def _workspace_rsync_args(root: Path, workdir: Path, destination: Path) -> list[str]:
    rsync_args = ["-a", "--exclude=/tmp/"]
    try:
        rel_path = workdir.relative_to(root).as_posix()
    except ValueError:
        rel_path = None
    if rel_path is not None:
        rsync_args += [f"--exclude=/{rel_path}/"]
    rsync_args += ["--", str(root) + "/", str(destination) + "/"]
    return rsync_args


def _attach_container_workspace(cmds: Cmds, instance: str, root: Path, workdir: Path) -> None:
    host_mount_dir = workdir / "kive-code-host"
    logger.info("Building container workspace directory at %s...", host_mount_dir)
    host_mount_dir.mkdir(parents=True, exist_ok=True)
    cmds.rsync.run(_workspace_rsync_args(root, workdir, host_mount_dir))

    logger.info("Attaching workspace directory to %s...", instance)
    cmds.incus.run(
        [
            "config",
            "device",
            "add",
            instance,
            "kive-code",
            "disk",
            f"source={host_mount_dir}",
            "path=/mnt/kive-code",
        ]
    )


def _build_and_attach_vm_workspace(cmds: Cmds, instance: str, image_path: Path, root: Path, workdir: Path) -> None:
    logger.info("Building workspace disk image at %s...", image_path)

    if image_path.exists():
        logger.info("Removing existing workspace image %s to rebuild from source...", image_path)
        image_path.unlink()

    logger.info("Creating workspace disk image with loop-mounted ext4 filesystem...")

    mount_dir = workdir / "kive-code-mount"
    if mount_dir.exists():
        logger.info("Cleaning stale mountpoint %s...", mount_dir)
        _cleanup_umount(mount_dir)

    subprocess.run(["truncate", "-s", "2G", str(image_path)], check=True)

    mount_dir.mkdir(parents=True, exist_ok=True)
    try:
        # Label the filesystem so VM cloud-init can mount it by label.
        _sudo("mkfs.ext4", "-F", "-L", "KIVE_CODE", str(image_path))
        _sudo("mount", "-o", "loop", "--", str(image_path), str(mount_dir))

        cmds.rsync.run(_workspace_rsync_args(root, workdir, mount_dir), sudo=True)
        subprocess.run(["sync"], check=True)
    finally:
        _cleanup_umount(mount_dir)

    try:
        mount_dir.rmdir()
    except OSError:
        pass

    logger.info("Attaching workspace disk to %s...", instance)
    cmds.incus.run(
        [
            "config",
            "device",
            "add",
            instance,
            "kive-code",
            "disk",
            f"source={image_path}",
        ]
    )


def handle_workspace_attachment(cmds: Cmds, instance: str, image_path: Path, root: Path, workdir: Path, instance_type: str) -> None:
    _write_marker(workdir)

    if instance_type == "container":
        _attach_container_workspace(cmds, instance, root, workdir)
        return

    _build_and_attach_vm_workspace(cmds, instance, image_path, root, workdir)

