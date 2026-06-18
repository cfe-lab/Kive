from __future__ import annotations

import logging
import time

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")

def _pull_file(cmds: Cmds, instance: str, path: str) -> tuple[bool, str]:
    result = cmds.incus.run(["file", "pull", f"{instance}{path}", "-"], check=False, capture_output=True)
    if result.returncode == 0:
        return True, (result.stdout or "").strip()
    return False, ""


def _cloud_init_diagnostics(cmds: Cmds, instance: str) -> str:
    diagnostics: list[str] = []

    status_result = cmds.incus.run(
        ["exec", instance, "--", "cloud-init", "status", "--long"],
        check=False,
        capture_output=True,
    )
    status_output = (status_result.stdout or status_result.stderr or "").strip()
    if status_output:
        diagnostics.append(f"cloud-init status:\n{status_output}")

    for path in ("/var/log/cloud-init.log", "/var/log/cloud-init-output.log", "/var/log/cloud-init-local.log"):
        ok, body = _pull_file(cmds, instance, path)
        if ok and body:
            diagnostics.append(f"{path}:\n{body}")

    return "\n\n".join(diagnostics) if diagnostics else "No cloud-init diagnostics available."


def maybe_provision_instance(
    cmds: Cmds,
    instance: str,
    instance_type: str,
    *,
    provision: bool,
) -> None:
    if not provision:
        return

    logger.info("Waiting for %s instance %s to finish cloud-init provisioning...", instance_type, instance)
    deadline = time.time() + 1200
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

        if attempt % 10 == 0 and logger.isEnabledFor(logging.DEBUG):
            logger.debug("Provision polling attempt %s for %s: no done/failed markers yet", attempt, instance)
            probe_result = cmds.incus.run(
                ["exec", instance, "--", "sh", "-c", "ls -la /var/lib/kive-provision /var/log/kive-provision.log /var/log/cloud-init-output.log 2>/dev/null || true"],
                check=False,
                capture_output=True,
            )
            if probe_result.stdout:
                logger.debug("Provision probe stdout:\n%s", probe_result.stdout.strip())
            if probe_result.stderr:
                logger.debug("Provision probe stderr:\n%s", probe_result.stderr.strip())

        time.sleep(2)

    _, log_body = _pull_file(cmds, instance, "/var/log/kive-provision.log")
    details = log_body or "Provisioning timed out waiting for /var/lib/kive-provision/done"
    diagnostics = _cloud_init_diagnostics(cmds, instance)
    raise RuntimeError(f"{details}\n\n{diagnostics}")
