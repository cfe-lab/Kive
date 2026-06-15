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
    deadline = time.time() + 60 * 60 * 3 # 3 hours
    while time.time() < deadline:
        done, _ = _pull_file(cmds, instance, "/run/kive-provision.done")
        if done:
            logger.info("Provisioning completed for %s.", instance)
            return

        failed, _ = _pull_file(cmds, instance, "/run/kive-provision.failed")
        if failed:
            _, log_body = _pull_file(cmds, instance, "/var/log/kive-provision.log")
            details = log_body or "Provisioning failed inside instance."
            raise RuntimeError(details)

        time.sleep(2)

    _, log_body = _pull_file(cmds, instance, "/var/log/kive-provision.log")
    details = log_body or "Provisioning timed out waiting for /run/kive-provision.done"
    raise RuntimeError(details)
