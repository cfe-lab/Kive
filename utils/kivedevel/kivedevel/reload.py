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

from .kv_commands import Cmds
from .shared import configure_logging, default_root, instance_exists, instance_is_running

logger = logging.getLogger("kivedevel")


class ReloadError(RuntimeError):
    """Raised by transactional helper functions to signal a recoverable failure.

    The caller catches this exception, restarts any stopped services,
    then calls sys.exit(1).
    """


_DEV_SERVICE = "kive-dev-web.service"
_APACHE_SERVICE = "apache2.service"
_HEALTH_URL = "http://127.0.0.1:8000/login/"
_HEALTH_TIMEOUT = 120


def _host_source_snapshot(cmds: Cmds, root: Path, workdir: Path, staging_name: str) -> Path:
    """Create a complete snapshot of *root* under *workdir* using rsync --delete.

    The snapshot directory is named *staging_name* so that its basename
    matches the guest staging path used by ``_transfer_snapshot``.

    Returns the path to the snapshot directory.
    """
    from .build_vm.workspace import _workspace_rsync_args

    snapshot = workdir / staging_name
    snapshot.mkdir(parents=True, exist_ok=True)
    rsync_args = _workspace_rsync_args(root, workdir, snapshot)
    separator = rsync_args.index("--")
    rsync_args.insert(separator, "--delete")
    # Exclude host-specific artefacts — patterns without leading / so they
    # match at every depth (e.g. utils/kivedevel/.venv/).
    rsync_args.insert(separator, "--exclude=.venv/")
    rsync_args.insert(separator, "--exclude=__pycache__/")
    rsync_args.insert(separator, "--exclude=*.pyc")
    rsync_args.insert(separator, "--exclude=.git/")

    logger.info("Creating host source snapshot at %s...", snapshot)
    cmds.rsync.run(rsync_args)
    return snapshot


def _transfer_snapshot(cmds: Cmds, instance: str, snapshot: Path) -> str:
    """Transfer *snapshot* into a unique guest staging directory via incus file push.

    ``--create-dirs`` creates the parent tree if needed.  The destination
    is ``/usr/local/share``, so Incus places the snapshot's basename
    (``.Kive.reload-<uuid>``) directly under that directory.  After the
    push the resulting tree is:

      /usr/local/share/.Kive.reload-<uuid>/kive/
      /usr/local/share/.Kive.reload-<uuid>/dev-env/
      ...

    Returns the guest-side staging path.
    """
    parent = "/usr/local/share"
    guest_staging = f"{parent}/{snapshot.name}"

    logger.info("Transferring snapshot to %s:%s...", instance, guest_staging)
    cmds.incus.run(
        ["file", "push", "-r", "--create-dirs", "--", str(snapshot), f"{instance}{parent}"],
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
            raise ReloadError(f"Expected file {path} not found in staged tree at {guest_staging}.")


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

    Raises ``ReloadError`` on failure so the caller can restart whatever
    was already stopped.
    """
    for svc in (_DEV_SERVICE, _APACHE_SERVICE):
        result = cmds.incus.run(
            ["exec", instance, "--", "systemctl", "stop", svc],
            check=False, capture_output=True,
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            if "not-found" in stderr:
                raise ReloadError(
                    f"Required service {svc} does not exist in instance {instance}. "
                    "The instance must be rebuilt or reprovisioned before reload."
                )
            raise ReloadError(f"Failed to stop {svc} on {instance}: {stderr}")
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


def _health_check(cmds: Cmds, instance: str, backup: str | None = None) -> None:
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

    backup_msg = f"\nPrevious tree backed up at: {backup}" if backup else ""
    raise ReloadError(
        f"Health check timed out after {_HEALTH_TIMEOUT}s for {instance}.\n"
        f"The new source tree is installed but the web server did not become healthy."
        f"{backup_msg}\n\n"
        f"Diagnostics:\n"
        f"  incus exec {instance} -- systemctl status {_DEV_SERVICE} --no-pager\n"
        f"  incus exec {instance} -- journalctl -u {_DEV_SERVICE} --no-pager --lines=50\n"
        f"  incus exec {instance} -- systemctl status {_APACHE_SERVICE} --no-pager"
    )


def _cleanup_stale(cmds: Cmds, instance: str) -> None:
    """Remove backup directories from previous reload runs.

    Each reload removes its own staging path in its own ``finally`` block,
    so this function only cleans up old ``.Kive.backup-*`` directories.
    It also preserves the current reload's backup so the success message
    can still reference it.
    """
    _cleanup_stale_except(cmds, instance, None)


def _cleanup_stale_except(cmds: Cmds, instance: str, preserve: str | None) -> None:
    """Remove old backup directories, preserving *preserve*."""
    result = cmds.incus.run(
        ["exec", instance, "--", "sh", "-c",
         "ls -d /usr/local/share/.Kive.backup-* 2>/dev/null || true"],
        check=False, capture_output=True,
    )
    for entry in (result.stdout or "").split():
        entry = entry.strip()
        if not entry:
            continue
        if preserve and entry == preserve:
            continue
        cmds.incus.run(
            ["exec", instance, "--", "rm", "-rf", entry],
            check=False, capture_output=True,
        )


def _reload_lock_path(workdir: Path, instance: str) -> Path:
    """Return the path to a persistent per-instance lock file.

    The lock file is never deleted — its presence in the workdir is
    normal and harmless.
    """
    return workdir / f".kive-reload-{instance}.lock"


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

    try:
        _run_reload(args, cmds, instance)
    except ReloadError as error:
        logger.error("%s", error)
        raise SystemExit(1)


def _run_reload(args: argparse.Namespace, cmds: Cmds, instance: str) -> None:
    # Shared staging name ensures the local snapshot basename matches the
    # guest staging path that _transfer_snapshot creates under /usr/local/share.
    staging_name = f".Kive.reload-{uuid.uuid4().hex}"

    # Phase 1: host snapshot (outside lock, does not touch the guest).
    snapshot = _host_source_snapshot(cmds, args.root, args.workdir, staging_name)
    try:
        # Phase 2: guest staging (outside lock, read-only guest operations).
        # Recovery state — must exist before any code in the inner try can raise.
        stopped: list[str] = []
        guest_staging = _transfer_snapshot(cmds, instance, snapshot)
        try:
            _validate_guest_tree(cmds, instance, guest_staging)
            _fix_ownership(cmds, instance, guest_staging)

            # Phase 3: destructive guest transaction (under lock).
            backup = f"/usr/local/share/.Kive.backup-{uuid.uuid4().hex}"
            current_backup = backup
            lock_path = _reload_lock_path(args.workdir, instance)

            with open(lock_path, "w") as lock_file:
                try:
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    logger.error("Another reload is already running for %s.", instance)
                    raise ReloadError("Another reload is already running.")

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
                    raise ReloadError("Reload aborted. Active tree unchanged.")

                try:
                    _validate_guest_tree(cmds, instance, "/usr/local/share/Kive")
                except ReloadError:
                    _restore_tree(cmds, instance, backup)
                    raise

                _start_services(cmds, instance)
                stopped.clear()
                _health_check(cmds, instance, backup)
                _cleanup_stale_except(cmds, instance, current_backup)
                # Lock released via `with` block.

            logger.info(
                "Reload complete for %s.  Previous tree backup: %s",
                instance, backup,
            )
        except BaseException as primary:
            if stopped:
                try:
                    _start_services_of(stopped, cmds, instance)
                except Exception as recovery_err:
                    logger.exception(
                        "Reload failed, and restarting stopped services also failed: %s",
                        recovery_err,
                    )
                    primary.add_note(
                        f"Restarting stopped services also failed: {recovery_err}"
                    )
            raise
        finally:
            cmds.incus.run(
                ["exec", instance, "--", "rm", "-rf", guest_staging],
                check=False, capture_output=True,
            )
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
