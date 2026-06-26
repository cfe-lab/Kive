from __future__ import annotations

import logging
import subprocess
import time

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")
DEFAULT_PROVISION_TIMEOUT = 900
PROVISION_STUCK_THRESHOLD = 840
PROVISION_POLL_INTERVAL = 2
PROVISION_POLL_INTERVAL_AFTER_STARTED = 10
PROBE_TIMEOUT = 30
DIAGNOSTICS_TIMEOUT = 60
MAX_CONSECUTIVE_TRANSPORT_FAILURES = 5

_KNOWN_TRANSPORT_ERRORS = [
    "VM agent isn't currently running",
    "websocket: bad handshake",
    "connection refused",
    "not connected",
]


def _pull_file(cmds: Cmds, instance: str, path: str) -> tuple[bool, str]:
    result = cmds.incus.run(["file", "pull", f"{instance}{path}", "-"], check=False, capture_output=True)
    if result.returncode == 0:
        logger.debug("incus file pull %s returned 0", path)
        return True, (result.stdout or "").strip()
    stderr_text = (result.stderr or "").strip()
    logger.debug(
        "incus file pull %s returned %s; stdout=%r; stderr=%r",
        path,
        result.returncode,
        result.stdout,
        stderr_text,
    )
    return False, stderr_text


def _is_transport_stderr(stderr: str) -> bool:
    """Check if *stderr* matches a known Incus transport/agent error."""
    lower = stderr.strip().lower()
    for err in _KNOWN_TRANSPORT_ERRORS:
        if err.lower() in lower:
            return True
    return False


def _is_transport_failure(result: subprocess.CompletedProcess[str]) -> bool:
    return result.returncode != 0 and _is_transport_stderr(result.stderr or "")


def _probe_markers_via_file_pull(cmds: Cmds, instance: str) -> tuple[str | None, str | None]:
    """Probe marker files via ``incus file pull``.

    Preferred for VM mode where ``incus exec`` may be unreliable
    during early agent startup.
    """
    transport_errors: list[str] = []
    for marker_name in ("done", "failed", "started"):
        path = f"/var/lib/kive-provision/{marker_name}"
        ok, body = _pull_file(cmds, instance, path)
        if ok:
            return marker_name, None
        if _is_transport_stderr(body):
            transport_errors.append(body)

    if transport_errors:
        return None, transport_errors[0]

    return "none", None


_BOUNDED_PROBE_SCRIPT = """
set -e
for f in /var/lib/kive-provision/done /var/lib/kive-provision/failed /var/lib/kive-provision/started; do
  if [ -f "$f" ]; then
    echo "${f##*/}"
    exit 0
  fi
done
echo "none"
"""

_DIAGNOSTICS_SCRIPT = """
echo '=== cloud-init status ==='
cloud-init status --long 2>&1 || true
echo '=== systemctl status ==='
systemctl status mount-kive-code.service cloud-init --no-pager 2>&1 || true
echo '=== mount status ==='
mount | grep /mnt/kive-code 2>&1 || true
echo '=== file list ==='
ls -la /mnt/kive-code /usr/local/bin/kive-provision.sh /usr/local/bin/mount-kive-code.sh 2>&1 || true
echo '=== log tails ==='
tail -n 40 /var/log/kive-provision.log /var/log/cloud-init-output.log 2>&1 || true
echo '=== ps ==='
ps -ef 2>&1 || true
echo '=== provision dir ==='
ls -la /var/lib/kive-provision 2>&1 || true
"""


def _probe_markers(cmds: Cmds, instance: str) -> tuple[str | None, str | None]:
    """Exec a single bounded probe to find which marker file exists.

    Returns (marker_name, error_detail) where marker_name is one of
    ``"done"``, ``"failed"``, ``"started"``, ``"none"``, or None on transport failure.
    """
    try:
        result = cmds.incus.run(
            ["exec", instance, "--", "sh", "-c", _BOUNDED_PROBE_SCRIPT],
            check=False,
            capture_output=True,
            timeout=PROBE_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        logger.error("Provision marker probe timed out after %ss on %s", PROBE_TIMEOUT, instance)
        return None, "probe timed out"
    except Exception as exc:
        logger.error("Provision marker probe failed on %s: %s", instance, exc)
        return None, str(exc)

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        logger.debug("Marker probe exited %s on %s: %s", result.returncode, instance, stderr)
        if _is_transport_failure(result):
            return None, stderr or "transport failure"
        return None, stderr or f"exit code {result.returncode}"

    marker = (result.stdout or "").strip()
    if marker in ("done", "failed", "started", "none"):
        return marker, None

    logger.debug("Unexpected marker probe output on %s: %s", instance, result.stdout)
    return None, f"unexpected output: {result.stdout}"


def _cloud_init_diagnostics(cmds: Cmds, instance: str) -> str:
    diagnostics: list[str] = []

    try:
        status_result = cmds.incus.run(
            ["exec", instance, "--", "sh", "-c", _DIAGNOSTICS_SCRIPT],
            check=False,
            capture_output=True,
            timeout=DIAGNOSTICS_TIMEOUT,
        )
        status_output = (status_result.stdout or status_result.stderr or "").strip()
        if status_output:
            diagnostics.append(f"instance probe:\n{status_output}")
    except subprocess.TimeoutExpired:
        diagnostics.append("instance probe: timed out")
    except Exception as exc:
        diagnostics.append(f"instance probe: {exc}")

    for path in ("/var/log/cloud-init.log", "/var/log/cloud-init-output.log", "/var/log/cloud-init-local.log", "/var/log/kive-provision.log"):
        try:
            result = cmds.incus.run(
                ["file", "pull", f"{instance}{path}", "-"],
                check=False,
                capture_output=True,
                timeout=PROBE_TIMEOUT,
            )
            ok = result.returncode == 0
            body = (result.stdout or "").strip() if ok else ""
            stderr_text = (result.stderr or "").strip() if not ok else ""
            if ok and body:
                diagnostics.append(f"{path}:\n{body}")
            elif stderr_text:
                diagnostics.append(f"{path}: could not pull file; stderr:\n{stderr_text}")
        except subprocess.TimeoutExpired:
            diagnostics.append(f"{path}: timed out pulling file")
        except Exception as exc:
            diagnostics.append(f"{path}: {exc}")

    return "\n\n".join(diagnostics) if diagnostics else "No cloud-init diagnostics available."


def maybe_provision_instance(
    cmds: Cmds,
    instance: str,
    instance_type: str,
    *,
    provision: bool,
    timeout: int = DEFAULT_PROVISION_TIMEOUT,
) -> None:
    if not provision:
        return

    logger.info("Waiting for %s instance %s to finish cloud-init provisioning...", instance_type, instance)
    start_time = time.monotonic()
    deadline = start_time + timeout
    attempt = 0
    consecutive_transport_failures = 0
    started_seen = False
    while time.monotonic() < deadline:
        attempt += 1

        if instance_type == "vm":
            marker, error = _probe_markers_via_file_pull(cmds, instance)
        else:
            marker, error = _probe_markers(cmds, instance)

        if error:
            if started_seen:
                logger.debug(
                    "Transient transport failure on %s (attempt %s): %s",
                    instance, attempt, error,
                )
            else:
                consecutive_transport_failures += 1
                logger.debug(
                    "Marker probe failed on %s (attempt %s): %s",
                    instance, attempt, error,
                )
                if instance_type != "vm" and consecutive_transport_failures >= MAX_CONSECUTIVE_TRANSPORT_FAILURES:
                    diagnostics = _cloud_init_diagnostics(cmds, instance)
                    raise RuntimeError(
                        f"Provisioning aborted after {consecutive_transport_failures} "
                        f"consecutive transport failures on {instance}: {error}"
                        f"\n\n{diagnostics}"
                    )
            time.sleep(PROVISION_POLL_INTERVAL_AFTER_STARTED if started_seen else PROVISION_POLL_INTERVAL)
            continue
        consecutive_transport_failures = 0

        if marker == "done":
            logger.info("Provisioning completed for %s.", instance)
            return

        if marker == "failed":
            log_result = cmds.incus.run(
                ["file", "pull", f"{instance}/var/log/kive-provision.log", "-"],
                check=False,
                capture_output=True,
            )
            log_body = (log_result.stdout or "").strip() if log_result.returncode == 0 else ""
            details = log_body or "Provisioning failed inside instance."
            diagnostics = _cloud_init_diagnostics(cmds, instance)
            raise RuntimeError(f"{details}\n\n{diagnostics}")

        if marker == "started":
            started_seen = True
            if time.monotonic() > start_time + PROVISION_STUCK_THRESHOLD:
                diagnostics = _cloud_init_diagnostics(cmds, instance)
                raise RuntimeError(
                    "Provisioning appears stuck: /var/lib/kive-provision/started exists, "
                    "but neither done nor failed appeared after 14 minutes."
                    f"\n\n{diagnostics}"
                )

        if attempt % 10 == 0:
            logger.debug("Provision polling attempt %s for %s (marker=%s): no done/failed yet", attempt, instance, marker)

        time.sleep(PROVISION_POLL_INTERVAL_AFTER_STARTED if started_seen else PROVISION_POLL_INTERVAL)

    log_result = cmds.incus.run(
        ["file", "pull", f"{instance}/var/log/kive-provision.log", "-"],
        check=False,
        capture_output=True,
    )
    log_body = (log_result.stdout or "").strip() if log_result.returncode == 0 else ""
    details = log_body or "Provisioning timed out waiting for /var/lib/kive-provision/done"
    diagnostics = _cloud_init_diagnostics(cmds, instance)
    raise RuntimeError(f"{details}\n\n{diagnostics}")
