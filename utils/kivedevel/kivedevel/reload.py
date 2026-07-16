"""utils/dev reload — copy the host working tree into a running development instance."""

from __future__ import annotations

import argparse
import fcntl
import logging
import subprocess
import sys
import time
import uuid
from pathlib import Path
from tempfile import mkdtemp

from .kv_commands import Cmds
from .shared import configure_logging, default_root, instance_exists, instance_is_running

logger = logging.getLogger("kivedevel")


_DEV_SERVICE = "kive-dev-web.service"
_APACHE_SERVICE = "apache2.service"
_HEALTH_URL = "http://127.0.0.1:8000/login/"
_HEALTH_TIMEOUT = 120


def _host_source_snapshot(cmds: Cmds, root: Path, workdir: Path) -> Path:
    """Create a complete snapshot of *root* under *workdir* using rsync --delete.

    Returns the path to the snapshot directory.
    """
    from .build_vm.workspace import _workspace_rsync_args

    snapshot = Path(mkdtemp(prefix="kive-reload-", dir=workdir))
    rsync_args = _workspace_rsync_args(root, workdir, snapshot)
    # Insert --delete after -a and before the trailing --
    separator = rsync_args.index("--")
    rsync_args.insert(separator, "--delete")

    logger.info("Creating host source snapshot at %s...", snapshot)
    cmds.rsync.run(rsync_args)
    return snapshot


def _transfer_snapshot(cmds: Cmds, instance: str, snapshot: Path) -> str:
    """Transfer *snapshot* into a unique guest staging directory via incus file push.

    Because the destination does not exist, Incus creates it and places the
    source directory's contents directly underneath.  After the push:

      /usr/local/share/.Kive.reload-<stamp>/kive/
      /usr.local/share/.Kive.reload-<stamp>/dev-env/
      ...

    Returns the guest-side staging path.
    """
    guest_staging = f"/usr/local/share/.Kive.reload-{uuid.uuid4().hex}"

    logger.info("Transferring snapshot to %s:%s...", instance, guest_staging)
    cmds.incus.run(
        ["file", "push", "-r", "--", str(snapshot), f"{instance}{guest_staging}"],
        check=True,
        capture_output=True,
        timeout=120,
    )

    return guest_staging


def _validate_guest_tree(cmds: Cmds, instance: str, guest_staging: str) -> None:
    """Verify that expected files exist in the transferred tree."""
    for path in ("kive/manage.py",):
        result = cmds.incus.run(
            ["exec", instance, "--", "test", "-f", f"{guest_staging}/{path}"],
            check=False, capture_output=True,
        )
        if result.returncode != 0:
            logger.error("Expected file %s not found in staged tree at %s.", path, guest_staging)
            sys.exit(1)


def _fix_ownership(cmds: Cmds, instance: str, guest_staging: str) -> None:
    """Ensure the transferred tree is owned by the kive user."""
    cmds.incus.run(
        ["exec", instance, "--", "chown", "-R", "kive:kive", guest_staging],
        check=True, capture_output=True, timeout=60,
    )


def _stop_services(cmds: Cmds, instance: str, stopped: list[str]) -> None:
    """Stop services that load Kive source code.

    Appends each successfully stopped service to *stopped* immediately,
    so the caller can see partial progress when a later stop fails.
    """
    for svc in (_DEV_SERVICE, _APACHE_SERVICE):
        result = cmds.incus.run(
            ["exec", instance, "--", "systemctl", "stop", svc],
            check=False, capture_output=True,
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            if "not-found" in stderr:
                logger.error(
                    "Required service %s does not exist in instance %s.\n"
                    "The instance must be rebuilt or reprovisioned before reload.",
                    svc, instance,
                )
                sys.exit(1)
            logger.error("Failed to stop %s on %s: %s", svc, instance, stderr)
            sys.exit(1)
        stopped.append(svc)


def _switch_tree(cmds: Cmds, instance: str, guest_staging: str, backup: str) -> bool:
    """Atomically replace /usr/local/share/Kive with the staged tree.

    Returns True if the original tree was successfully moved aside (backup
    exists).  Returns False if the first mv failed and the active tree is
    still in place — callers should NOT attempt to restore from a nonexistent
    backup.
    """
    kive_root = "/usr/local/share/Kive"
    result = cmds.incus.run(
        ["exec", instance, "--", "mv", kive_root, backup],
        check=False, capture_output=True,
    )
    if result.returncode != 0:
        logger.error("Failed to move existing tree aside: %s", (result.stderr or "").strip())
        return False

    cmds.incus.run(
        ["exec", instance, "--", "mv", guest_staging, kive_root],
        check=True, capture_output=True,
    )
    return True


def _restore_tree(cmds: Cmds, instance: str, backup: str) -> None:
    """Restore the original tree from *backup*.

    Only call this when *backup* is known to exist (the first mv succeeded).
    Removes the existing (failed) tree at ``/usr/local/share/Kive`` first so
    that ``mv`` does not nest the backup inside it.
    """
    kive_root = "/usr/local/share/Kive"
    cmds.incus.run(
        ["exec", instance, "--", "rm", "-rf", kive_root],
        check=True, capture_output=True,
    )
    cmds.incus.run(
        ["exec", instance, "--", "mv", backup, kive_root],
        check=True, capture_output=True,
    )


def _start_services(cmds: Cmds, instance: str) -> None:
    """Start the full set of services that load Kive source code."""
    _start_services_of([_APACHE_SERVICE, _DEV_SERVICE], cmds, instance)


def _start_services_of(services: list[str], cmds: Cmds, instance: str) -> None:
    """Start a specific list of services."""
    for svc in services:
        cmds.incus.run(
            ["exec", instance, "--", "systemctl", "start", svc],
            check=True, capture_output=True, timeout=30,
        )


def _health_check(cmds: Cmds, instance: str) -> None:
    """Poll until the development web server responds healthily."""
    deadline = time.monotonic() + _HEALTH_TIMEOUT
    while time.monotonic() < deadline:
        result = cmds.incus.run(
            ["exec", instance, "--", "curl", "-fsS", _HEALTH_URL],
            check=False, capture_output=True, timeout=15,
        )
        if result.returncode == 0:
            logger.info("Health check passed for %s.", instance)
            return
        time.sleep(2)

    logger.error(
        "Health check timed out after %ss for %s.\n\n"
        "The new source tree is installed but the web server did not become healthy.\n"
        "Previous tree backed up at the path reported above.\n\n"
        "Diagnostics:\n"
        "  incus exec %s -- systemctl status %s --no-pager\n"
        "  incus exec %s -- journalctl -u %s --no-pager --lines=50\n"
        "  incus exec %s -- systemctl status %s --no-pager\n",
        _HEALTH_TIMEOUT, instance,
        instance, _DEV_SERVICE,
        instance, _DEV_SERVICE,
        instance, _APACHE_SERVICE,
    )
    sys.exit(1)


def _cleanup_stale(cmds: Cmds, instance: str) -> None:
    """Remove reload staging and backup directories from previous runs."""
    result = cmds.incus.run(
        ["exec", instance, "--", "sh", "-c",
         "ls -d /usr/local/share/.Kive.reload-* /usr/local/share/.Kive.backup-* 2>/dev/null || true"],
        check=False, capture_output=True,
    )
    for entry in (result.stdout or "").split():
        entry = entry.strip()
        if not entry:
            continue
        cmds.incus.run(
            ["exec", instance, "--", "rm", "-rf", entry],
            check=False, capture_output=True,
        )


def run_reload(args: argparse.Namespace) -> None:
    configure_logging(args, args.workdir)

    cmds = Cmds.create()
    cmds.incus.require()

    instance: str = args.instance

    if not instance_exists(cmds, instance):
        logger.error(
            "Instance %s does not exist.  Create it first:\n"
            "  utils/dev build-vm %s",
            instance, instance,
        )
        sys.exit(1)

    if not instance_is_running(cmds, instance):
        logger.error("Instance %s exists but is not running.", instance)
        sys.exit(1)

    result = cmds.incus.run(
        ["exec", instance, "--", "test", "-d", "/usr/local/share/Kive"],
        check=False, capture_output=True,
    )
    if result.returncode != 0:
        logger.error(
            "Instance %s is not a provisioned Kive development environment.\n"
            "Rebuild it first:\n"
            "  utils/dev build-vm %s",
            instance, instance,
        )
        sys.exit(1)

    for svc in (_DEV_SERVICE, _APACHE_SERVICE):
        result = cmds.incus.run(
            ["exec", instance, "--", "systemctl", "cat", svc],
            check=False, capture_output=True,
        )
        if result.returncode != 0:
            logger.error(
                "Required service %s not found in instance %s.\n"
                "The instance must be rebuilt or reprovisioned before reload.",
                svc, instance,
            )
            sys.exit(1)

    snapshot = _host_source_snapshot(cmds, args.root, args.workdir)
    try:
        guest_staging = _transfer_snapshot(cmds, instance, snapshot)
        try:
            _validate_guest_tree(cmds, instance, guest_staging)
            _fix_ownership(cmds, instance, guest_staging)
            suffix = uuid.uuid4().hex
            backup = f"/usr/local/share/.Kive.backup-{suffix}"

            # Host-side per-instance lock — held for the full transaction.
            lock_path = args.workdir / f".kive-reload-{instance}.lock"
            lock_file = open(lock_path, "w")
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                logger.error("Another reload is already running for %s.", instance)
                sys.exit(1)

            stopped: list[str] = []
            _stop_services(cmds, instance, stopped)
            try:
                backup_created = _switch_tree(cmds, instance, guest_staging, backup)
            except Exception:
                try:
                    _restore_tree(cmds, instance, backup)
                except Exception as restore_err:
                    raise RuntimeError(
                        f"Switch failed and restoration also failed: {restore_err}"
                    ) from restore_err
                raise

            if not backup_created:
                logger.error("Reload aborted. Active tree unchanged.")
                sys.exit(1)

            try:
                _validate_guest_tree(cmds, instance, "/usr/local/share/Kive")
            except SystemExit:
                _restore_tree(cmds, instance, backup)
                raise

            _start_services(cmds, instance)
            stopped.clear()
            _health_check(cmds, instance)
            _cleanup_stale(cmds, instance)
            logger.info(
                "Reload complete for %s.  Source: %s  Backup: %s",
                instance, args.root, backup,
            )
        except BaseException:
            if stopped:
                _start_services_of(stopped, cmds, instance)
            raise
        finally:
            cmds.incus.run(
                ["exec", instance, "--", "rm", "-rf", guest_staging],
                check=False, capture_output=True,
            )
            try:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass
    finally:
        subprocess.run(["rm", "-rf", str(snapshot)], check=False)


def register_subcommand(subparsers) -> None:
    root = default_root()
    parser = subparsers.add_parser(
        "reload",
        help="Copy the host working tree into a running development instance "
        "and restart web services",
    )
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument("--quiet", action="store_true", help="Only show errors")
    log_group.add_argument("--verbose", action="store_true", help="Show informational progress messages")
    log_group.add_argument("--debug", action="store_true", help="Show debug logging, including full command lines")
    parser.add_argument("instance", nargs="?", default="kive-minimal", help="Incus instance name (default: kive-minimal)")
    parser.add_argument(
        "--root",
        type=Path,
        default=root,
        metavar="DIR",
        help=f"Repository root to snapshot (default: {root})",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=root / "tmp~" / "build",
        metavar="DIR",
        help="Working directory for temporary snapshot (default: <root>/tmp~/build)",
    )
    parser.set_defaults(func=run_reload)
