from __future__ import annotations

import logging
import subprocess
import time
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")


def _sudo(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["sudo", "--"] + list(args),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


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

    if not image_path.exists():
        logger.info("Creating workspace disk image with qemu-img and nbd...")

        _sudo("modprobe", "nbd", "max_part=8")

        mount_dir = workdir / "kive-code-mount"
        if mount_dir.exists():
            logger.info("Cleaning stale mountpoint %s...", mount_dir)
            _sudo("umount", "--", str(mount_dir))

        nbd_pid_file = Path("/sys/block/nbd0/pid")
        if nbd_pid_file.exists():
            try:
                pid = int(nbd_pid_file.read_text().strip())
                if pid != 0:
                    logger.info("Cleaning stale /dev/nbd0 attachment...")
                    cmds.qemu_nbd.run(["-d", "/dev/nbd0"], sudo=True)
                    _sudo("kill", "--", str(pid))
                    time.sleep(1)
            except (ValueError, OSError):
                pass

        cmds.qemu_img.run(["create", "-f", "qcow2", "--", str(image_path), "2G"])
        cmds.qemu_nbd.run(["-c", "/dev/nbd0", "--", str(image_path)], sudo=True)

        _sudo("mkfs.ext4", "-F", "/dev/nbd0")
        mount_dir.mkdir(parents=True, exist_ok=True)
        _sudo("mount", "--", "/dev/nbd0", str(mount_dir))

        cmds.rsync.run(_workspace_rsync_args(root, workdir, mount_dir), sudo=True)
        subprocess.run(["sync"], check=True)

        _sudo("umount", "--", str(mount_dir))
        cmds.qemu_nbd.run(["-d", "/dev/nbd0"], sudo=True)
        try:
            mount_dir.rmdir()
        except OSError:
            pass
    else:
        logger.info("Using existing image %s.", image_path)

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
    if instance_type == "container":
        _attach_container_workspace(cmds, instance, root, workdir)
        return

    _build_and_attach_vm_workspace(cmds, instance, image_path, root, workdir)
