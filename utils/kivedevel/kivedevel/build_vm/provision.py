from __future__ import annotations

import json
import logging
import subprocess
import time
from dataclasses import dataclass

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")

DEFAULT_PROVISION_TIMEOUT = 3000
PROVISIONING_DEADLINE = 3000
PROVISION_POLL_INTERVAL = 10
PROBE_TIMEOUT = 30
DIAGNOSTICS_TIMEOUT = 60


@dataclass
class ProvisionStatus:
    schema_version: int = 0
    provision_id: str = ""
    state: str = ""
    phase: str = ""
    pid: int = 0
    service_result: str | None = None
    exit_code: str | None = None
    message: str | None = None

    @classmethod
    def from_json(cls, raw: str, expected_id: str) -> ProvisionStatus:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Malformed status JSON: {exc}")

        if not isinstance(data, dict):
            raise RuntimeError(f"Status JSON is not a dict: {data!r}")

        sv = data.get("schema_version")
        if sv != 1:
            raise RuntimeError(f"Unsupported schema version: {sv}")

        pid = data.get("provision_id")
        if pid != expected_id:
            raise RuntimeError(
                f"Stale provision_id: expected {expected_id!r}, got {pid!r}"
            )

        return cls(
            schema_version=sv,
            provision_id=pid,
            state=data.get("state", ""),
            phase=data.get("phase", ""),
            pid=data.get("pid", 0),
            service_result=data.get("service_result"),
            exit_code=data.get("exit_code"),
            message=data.get("message"),
        )


def _pull_status(cmds: Cmds, instance: str, expected_id: str) -> ProvisionStatus | None | str:
    result = cmds.incus.run(
        ["file", "pull", f"{instance}/var/lib/kive-provision/status.json", "-"],
        check=False, capture_output=True,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if "not found" in stderr.lower() or "not found" in (result.stdout or "").lower():
            return None
        return stderr
    body = (result.stdout or "").strip()
    if not body:
        return None
    try:
        return ProvisionStatus.from_json(body, expected_id)
    except RuntimeError as exc:
        return str(exc)


def _cloud_init_diagnostics(cmds: Cmds, instance: str) -> str:
    diagnostics: list[str] = []

    try:
        result = cmds.incus.run(
            ["exec", instance, "--", "sh", "-c", """
systemctl status kive-provision.service --no-pager 2>&1 || true
systemctl show kive-provision.service --property=ActiveState,SubState,Result,ExecMainCode,ExecMainStatus 2>&1 || true
journalctl -u kive-provision.service --no-pager --lines=200 2>&1 || true
tail -200 /var/log/kive-provision.log 2>&1 || true
systemd-cgls --unit kive-provision.service 2>&1 || true
"""],
            check=False, capture_output=True, timeout=DIAGNOSTICS_TIMEOUT,
        )
        output = (result.stdout or "").strip()
        if output:
            diagnostics.append(output)
    except subprocess.TimeoutExpired:
        diagnostics.append("diagnostics timed out")
    except Exception as exc:
        diagnostics.append(f"diagnostics failed: {exc}")

    for path in ("/var/log/kive-provision.log",):
        try:
            result = cmds.incus.run(
                ["file", "pull", f"{instance}{path}", "-"],
                check=False, capture_output=True, timeout=PROBE_TIMEOUT,
            )
            if result.returncode == 0 and (result.stdout or "").strip():
                diagnostics.append(f"{path}:\n{(result.stdout or '').strip()}")
        except subprocess.TimeoutExpired:
            diagnostics.append(f"{path}: timed out")

    return "\n\n".join(diagnostics) if diagnostics else "No diagnostics available."


def maybe_provision_instance(
    cmds: Cmds,
    instance: str,
    instance_type: str,
    *,
    provision: bool,
    provision_id: str = "",
    timeout: int = DEFAULT_PROVISION_TIMEOUT,
) -> None:
    if not provision:
        return

    logger.info(
        "Waiting for %s instance %s to finish provisioning (id=%s)...",
        instance_type, instance, provision_id,
    )
    deadline = time.monotonic() + timeout
    last_phase = ""
    while time.monotonic() < deadline:
        status = _pull_status(cmds, instance, provision_id)

        if status is None:
            logger.debug("Status file not yet available on %s.", instance)
            time.sleep(PROVISION_POLL_INTERVAL)
            continue

        if isinstance(status, str):
            logger.debug("Status probe issue on %s: %s", instance, status)
            time.sleep(PROVISION_POLL_INTERVAL)
            continue

        if status.phase != last_phase:
            logger.info(
                "Provisioning phase changed: %s/%s on %s",
                status.state, status.phase, instance,
            )
            last_phase = status.phase

        if status.state == "succeeded":
            logger.info("Provisioning completed for %s.", instance)
            return

        if status.state == "failed":
            log_body = _pull_file_text(cmds, instance, "/var/log/kive-provision.log")
            details = log_body or status.message or "Provisioning failed."
            diagnostics = _cloud_init_diagnostics(cmds, instance)
            raise RuntimeError(f"{details}\n\n{diagnostics}")

        time.sleep(PROVISION_POLL_INTERVAL)

    diagnostics = _cloud_init_diagnostics(cmds, instance)
    _stop_provisioning_service(cmds, instance)
    raise RuntimeError(
        f"Provisioning timed out after {timeout}s on {instance}.\n\n{diagnostics}"
    )


def _pull_file_text(cmds: Cmds, instance: str, path: str) -> str:
    result = cmds.incus.run(
        ["file", "pull", f"{instance}{path}", "-"],
        check=False, capture_output=True,
    )
    if result.returncode == 0:
        return (result.stdout or "").strip()
    return ""


def _stop_provisioning_service(cmds: Cmds, instance: str) -> None:
    cmds.incus.run(
        ["exec", instance, "--", "systemctl", "stop", "kive-provision.service"],
        check=False, timeout=60,
    )
    cmds.incus.run(
        ["exec", instance, "--",
         "systemctl", "kill", "--kill-who=all", "--signal=KILL",
         "kive-provision.service"],
        check=False, timeout=30,
    )
