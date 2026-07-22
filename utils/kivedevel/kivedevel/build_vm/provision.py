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

ALLOWED_STATES = frozenset({"starting", "running", "succeeded", "failed"})

_REQUIRED_PHASES = [
    "starting",
    "network-preflight",
    "apt-update",
    "apt-install",
    "workspace-wait",
    "workspace-copy",
    "ansible",
    "web-config",
    "web-readiness",
    "slurm-readiness",
    "slurm-job",
    "complete",
]


class ProvisionStatusUnavailable(Exception):
    pass


class ProvisionTransportError(Exception):
    pass


class ProvisionProtocolError(Exception):
    pass


@dataclass
class ProvisionStatus:
    schema_version: int
    provision_id: str
    state: str
    phase: str
    pid: int = 0
    service_result: str | None = None
    exit_code: str | None = None
    message: str | None = None

    @classmethod
    def from_json(cls, raw: str, expected_id: str) -> ProvisionStatus:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProvisionProtocolError(f"Malformed status JSON: {exc}")

        if not isinstance(data, dict):
            raise ProvisionProtocolError(f"Status JSON is not a dict: {data!r}")

        sv = data.get("schema_version")
        if not isinstance(sv, int):
            raise ProvisionProtocolError(
                f"Missing or non-integer schema_version: {sv!r}")
        if sv != 1:
            raise ProvisionProtocolError(f"Unsupported schema version: {sv}")

        pid = data.get("provision_id")
        if not isinstance(pid, str) or not pid:
            raise ProvisionProtocolError(
                f"Missing or empty provision_id: {pid!r}")
        if pid != expected_id:
            raise ProvisionProtocolError(
                f"Stale provision_id: expected {expected_id!r}, got {pid!r}")

        state = data.get("state", "")
        if not isinstance(state, str) or state not in ALLOWED_STATES:
            raise ProvisionProtocolError(f"Invalid or missing state: {state!r}")

        phase = data.get("phase", "")
        if not isinstance(phase, str) or not phase:
            raise ProvisionProtocolError(f"Missing phase: {phase!r}")

        pid_value = data.get("pid", 0)
        if not isinstance(pid_value, int):
            raise ProvisionProtocolError(f"Invalid PID: {pid_value!r}")

        exit_code = data.get("exit_code")
        if exit_code is not None and not isinstance(exit_code, int):
            raise ProvisionProtocolError(f"Invalid exit_code type: {exit_code!r}")

        if state == "succeeded" and exit_code is not None and exit_code != 0:
            raise ProvisionProtocolError(
                f"Succeeded state with nonzero exit_code: {exit_code}")

        return cls(
            schema_version=sv,
            provision_id=pid,
            state=state,
            phase=phase,
            pid=pid_value,
            service_result=data.get("service_result"),
            exit_code=exit_code,
            message=data.get("message"),
        )


def _pull_status(
    cmds: Cmds, instance: str, expected_id: str,
) -> ProvisionStatus:
    try:
        result = cmds.incus.run(
            ["file", "pull", f"{instance}/var/lib/kive-provision/status.json", "-"],
            check=False, capture_output=True, timeout=PROBE_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        raise ProvisionTransportError(
            f"Status pull timed out on {instance}: {exc}") from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if "not found" in stderr.lower() or "not found" in (result.stdout or "").lower():
            raise ProvisionStatusUnavailable(
                f"Status file not found on {instance}")
        if _is_transport_stderr(stderr):
            raise ProvisionTransportError(
                f"Transport error pulling status on {instance}: {stderr}")
        raise ProvisionTransportError(
            f"Status pull failed on {instance} (rc={result.returncode}): {stderr}")

    body = (result.stdout or "").strip()
    if not body:
        raise ProvisionStatusUnavailable(f"Empty status file on {instance}")
    return ProvisionStatus.from_json(body, expected_id)


def _is_transport_stderr(stderr: str) -> bool:
    known = [
        "VM agent isn't currently running",
        "websocket: bad handshake",
        "connection refused",
        "not connected",
    ]
    lower = stderr.strip().lower()
    for err in known:
        if err.lower() in lower:
            return True
    return False


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

    try:
        result = cmds.incus.run(
            ["file", "pull", f"{instance}/var/log/kive-provision.log", "-"],
            check=False, capture_output=True, timeout=PROBE_TIMEOUT,
        )
        if result.returncode == 0 and (result.stdout or "").strip():
            diagnostics.append(
                f"/var/log/kive-provision.log:\n{(result.stdout or '').strip()}")
    except subprocess.TimeoutExpired:
        diagnostics.append("log pull timed out")

    return "\n\n".join(diagnostics) if diagnostics else "No diagnostics available."


def _stop_service(cmds: Cmds, instance: str) -> None:
    try:
        cmds.incus.run(
            ["exec", instance, "--", "systemctl", "stop", "kive-provision.service"],
            check=False, timeout=60,
        )
    except subprocess.TimeoutExpired:
        pass
    try:
        cmds.incus.run(
            ["exec", instance, "--",
             "systemctl", "kill", "--kill-who=all", "--signal=KILL",
             "kive-provision.service"],
            check=False, timeout=30,
        )
    except subprocess.TimeoutExpired:
        pass


def maybe_provision_instance(
    cmds: Cmds,
    instance: str,
    instance_type: str,
    *,
    provision: bool,
    provision_id: str,
    timeout: int = DEFAULT_PROVISION_TIMEOUT,
) -> None:
    if not provision:
        return
    if not provision_id:
        raise ValueError("provision_id is required when provisioning is enabled")

    logger.info(
        "Waiting for %s instance %s to finish provisioning (id=%s)...",
        instance_type, instance, provision_id,
    )
    deadline = time.monotonic() + timeout
    last_phase = ""
    consecutive_transport_failures = 0
    startup_window = 30.0
    startup_end = time.monotonic() + startup_window

    while time.monotonic() < deadline:
        try:
            status = _pull_status(cmds, instance, provision_id)
        except ProvisionStatusUnavailable:
            if time.monotonic() < startup_end:
                time.sleep(PROVISION_POLL_INTERVAL)
                continue
            logger.debug("Status not yet available on %s.", instance)
            time.sleep(PROVISION_POLL_INTERVAL)
            continue
        except ProvisionTransportError as exc:
            consecutive_transport_failures += 1
            if consecutive_transport_failures >= 5:
                diagnostics = _cloud_init_diagnostics(cmds, instance)
                raise RuntimeError(
                    f"Provisioning aborted after {consecutive_transport_failures} "
                    f"transport failures on {instance}: {exc}\n\n{diagnostics}")
            time.sleep(PROVISION_POLL_INTERVAL)
            continue
        except ProvisionProtocolError:
            _stop_service(cmds, instance)
            diagnostics = _cloud_init_diagnostics(cmds, instance)
            raise RuntimeError(
                f"Provisioning protocol error on {instance}.\n\n{diagnostics}")

        consecutive_transport_failures = 0

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
            log_text = ""
            try:
                result = cmds.incus.run(
                    ["file", "pull", f"{instance}/var/log/kive-provision.log", "-"],
                    check=False, capture_output=True, timeout=PROBE_TIMEOUT,
                )
                if result.returncode == 0:
                    log_text = (result.stdout or "").strip()
            except subprocess.TimeoutExpired:
                pass
            details = log_text or status.message or "Provisioning failed."
            diagnostics = _cloud_init_diagnostics(cmds, instance)
            raise RuntimeError(f"{details}\n\n{diagnostics}")

        time.sleep(PROVISION_POLL_INTERVAL)

    _stop_service(cmds, instance)
    diagnostics = _cloud_init_diagnostics(cmds, instance)
    raise RuntimeError(
        f"Provisioning timed out after {timeout}s on {instance}.\n\n{diagnostics}")
