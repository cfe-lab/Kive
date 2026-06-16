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
    while time.time() < deadline:
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

        time.sleep(2)

    _, log_body = _pull_file(cmds, instance, "/var/log/kive-provision.log")
    details = log_body or "Provisioning timed out waiting for /var/lib/kive-provision/done"
    diagnostics = _cloud_init_diagnostics(cmds, instance)
    raise RuntimeError(f"{details}\n\n{diagnostics}")
