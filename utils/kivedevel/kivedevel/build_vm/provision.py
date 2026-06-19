from __future__ import annotations

import logging
import time

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")
DEFAULT_PROVISION_TIMEOUT = 900
PROVISION_STUCK_THRESHOLD = 840
PROVISION_POLL_INTERVAL = 2

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


def _cloud_init_diagnostics(cmds: Cmds, instance: str) -> str:
    diagnostics: list[str] = []

    status_result = cmds.incus.run(
        ["exec", instance, "--", "sh", "-c", "echo '=== PATH ==='; command -v cloud-init || true; echo '=== cloud-init status ==='; cloud-init status --long 2>&1 || true; echo '=== systemctl status ==='; systemctl status mount-kive-code.service cloud-init --no-pager 2>&1 || true; echo '=== mount status ==='; mount | grep /mnt/kive-code 2>&1 || true; echo '=== file list ==='; ls -la /mnt/kive-code /usr/local/bin/kive-provision.sh /usr/local/bin/mount-kive-code.sh /etc/systemd/system/mount-kive-code.service 2>&1 || true; echo '=== provision dir ==='; ls -la /var/lib/kive-provision /var/log 2>&1 || true; echo '=== log tails ==='; tail -n 40 /var/log/kive-provision.log /var/log/cloud-init-output.log 2>&1 || true; echo '=== API log tail ==='; tail -n 40 /var/log/kive-api-smoke.log 2>&1 || true; echo '=== resolv.conf ==='; cat /etc/resolv.conf 2>&1 || true; echo '=== ip addr ==='; ip addr 2>&1 || true; echo '=== ip route ==='; ip route 2>&1 || true; echo '=== ps ==='; ps -ef 2>&1 || true; echo '=== pstree ==='; command -v pstree >/dev/null 2>&1 && pstree -ap || true; echo '=== cloud-init unit ==='; systemctl status cloud-init --no-pager 2>&1 || true"],
        check=False,
        capture_output=True,
    )
    status_output = (status_result.stdout or status_result.stderr or "").strip()
    if status_output:
        diagnostics.append(f"instance probe:\n{status_output}")

    for path in ("/var/log/cloud-init.log", "/var/log/cloud-init-output.log", "/var/log/cloud-init-local.log", "/var/log/kive-provision.log"):
        ok, body = _pull_file(cmds, instance, path)
        if ok and body:
            diagnostics.append(f"{path}:\n{body}")
        elif body:
            diagnostics.append(f"{path}: could not pull file; stderr:\n{body}")

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
    start_time = time.time()
    deadline = start_time + timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        done, _ = _pull_file(cmds, instance, "/var/lib/kive-provision/done")
        if done:
            logger.info("Provisioning completed for %s.", instance)
            return

        failed, _ = _pull_file(cmds, instance, "/var/lib/kive-provision/failed")
        if failed:
            _, log_body = _pull_file(cmds, instance, "/var/log/kive-provision.log")
            details = log_body or "Provisioning failed inside instance."
            diagnostics = _cloud_init_diagnostics(cmds, instance)
            raise RuntimeError(f"{details}\n\n{diagnostics}")

        status_result = cmds.incus.run(
            ["exec", instance, "--", "sh", "-c", "cloud-init status --long 2>&1 || true"],
            check=False,
            capture_output=True,
        )
        status_text = (status_result.stdout or status_result.stderr or "").strip()
        if status_text and "status: error" in status_text.lower():
            diagnostics = _cloud_init_diagnostics(cmds, instance)
            raise RuntimeError(f"cloud-init reported error status before provisioning succeeded:\n\n{status_text}\n\n{diagnostics}")

        started, _ = _pull_file(cmds, instance, "/var/lib/kive-provision/started")
        if started and time.time() > start_time + PROVISION_STUCK_THRESHOLD:
            diagnostics = _cloud_init_diagnostics(cmds, instance)
            raise RuntimeError(
                "Provisioning appears stuck: /var/lib/kive-provision/started exists, "
                "but neither done nor failed appeared after 14 minutes."
                f"\n\ncloud-init status:\n{status_text}\n\n{diagnostics}"
            )

        if attempt % 10 == 0 and logger.isEnabledFor(logging.DEBUG):
            logger.debug("Provision polling attempt %s for %s: no done/failed markers yet", attempt, instance)
            probe_result = cmds.incus.run(
                [
                    "exec",
                    instance,
                    "--",
                    "sh",
                    "-c",
                    "printf '%s\n' '--- file layout ---'; ls -la /var/lib/kive-provision /var/log 2>/dev/null || true; printf '%s\n' '--- provision tree ---'; find /var/lib/kive-provision -maxdepth 2 -type f 2>/dev/null | sort || true; printf '%s\n' '--- tail logs ---'; tail -n 20 /var/log/kive-provision.log /var/log/cloud-init-output.log /var/log/kive-api-smoke.log 2>/dev/null || true; printf '%s\n' '--- cloud-init status ---'; cloud-init status --long 2>/dev/null || true; printf '%s\n' '--- process list ---'; ps -ef | grep -E 'cloud-init|manage.py|ansible' | grep -v grep || true; printf '%s\n' '--- network ---'; ip addr 2>/dev/null || true; ip route 2>/dev/null || true; cat /etc/resolv.conf 2>/dev/null || true",
                ],
                check=False,
                capture_output=True,
            )
            if probe_result.stdout:
                logger.debug("Provision probe stdout:\n%s", probe_result.stdout.strip())
            if probe_result.stderr:
                logger.debug("Provision probe stderr:\n%s", probe_result.stderr.strip())

        time.sleep(PROVISION_POLL_INTERVAL)

    _, log_body = _pull_file(cmds, instance, "/var/log/kive-provision.log")
    details = log_body or "Provisioning timed out waiting for /var/lib/kive-provision/done"
    diagnostics = _cloud_init_diagnostics(cmds, instance)
    raise RuntimeError(f"{details}\n\n{diagnostics}")
