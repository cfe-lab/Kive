"""Host-facing Kive web endpoint verification with hop classification."""

from __future__ import annotations

import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

from .kv_commands import Cmds


logger = logging.getLogger("kivedevel.web_endpoint")

HTTP_REQUEST_TIMEOUT_SECONDS = 5
HOST_ENDPOINT_DEADLINE_SECONDS = 60
HOST_ENDPOINT_POLL_INTERVAL_SECONDS = 2


@dataclass(frozen=True)
class WebEndpointResult:
    guest_loopback_status: int | None
    guest_address_status: int | None
    host_guest_address_status: int | None
    host_localhost_status: int | None
    vm_ip: str | None
    failure_stage: str | None
    last_error: str | None


def _build_local_opener():
    handlers = [urllib.request.ProxyHandler({})]
    return urllib.request.build_opener(*handlers)


def _http_status(url: str) -> int | None:
    try:
        opener = _build_local_opener()
        with opener.open(url, timeout=HTTP_REQUEST_TIMEOUT_SECONDS) as r:
            return r.status
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.debug("Request to %s failed: %r", url, exc)
        return None


def _get_vm_ip(cmds: Cmds, instance: str) -> str | None:
    import json
    result = cmds.incus.run(
        ["list", instance, "--format", "json"],
        check=False, capture_output=True, timeout=10,
    )
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout or "[]")
        if isinstance(data, list) and data:
            state = data[0].get("state", {})
            network = state.get("network", {}) if isinstance(state, dict) else {}
            for iface_data in network.values():
                if isinstance(iface_data, dict):
                    for addr in iface_data.get("addresses", []):
                        if isinstance(addr, dict) and addr.get("family") == "inet" and addr.get("scope") == "global":
                            return addr["address"]
    except (json.JSONDecodeError, IndexError, KeyError):
        pass
    return None


def _curl_cmd(url: str, timeout: int) -> list[str]:
    return ["curl", "--noproxy", "*", "--connect-timeout", "3", "--max-time", str(timeout), url]


def check_host_endpoint(cmds: Cmds, instance: str, port: int = 8000) -> WebEndpointResult:
    vm_ip = _get_vm_ip(cmds, instance)
    deadline = time.monotonic() + HOST_ENDPOINT_DEADLINE_SECONDS
    last_error: str | None = None
    attempt = 0

    while time.monotonic() < deadline:
        attempt += 1
        guest_loop = _http_status(f"http://127.0.0.1:{port}/login/")
        guest_addr = _http_status(f"http://{vm_ip}:{port}/login/") if vm_ip else None
        host_guest = _http_status(f"http://{vm_ip}:{port}/login/") if vm_ip else None
        host_local = _http_status(f"http://127.0.0.1:{port}/login/")

        if attempt == 1 or attempt % 5 == 0:
            logger.info(
                "Host endpoint attempt %s: guest-loopback=%s guest-address=%s "
                "host-to-guest=%s localhost=%s",
                attempt, guest_loop, guest_addr, host_guest, host_local,
            )

        if host_local == 200:
            return WebEndpointResult(
                guest_loopback_status=guest_loop,
                guest_address_status=guest_addr,
                host_guest_address_status=host_guest,
                host_localhost_status=host_local,
                vm_ip=vm_ip,
                failure_stage=None,
                last_error=None,
            )

        if guest_loop is None:
            last_error = "guest-loopback-failed"
        elif guest_addr is None:
            last_error = "guest-address-unreachable"
        elif host_guest is None:
            last_error = "host-bridge-unreachable"
        else:
            last_error = "host-exposure-failed"

        time.sleep(HOST_ENDPOINT_POLL_INTERVAL_SECONDS)

    return WebEndpointResult(
        guest_loopback_status=guest_loop if 'guest_loop' in dir() else None,
        guest_address_status=guest_addr if 'guest_addr' in dir() else None,
        host_guest_address_status=host_guest if 'host_guest' in dir() else None,
        host_localhost_status=host_local if 'host_local' in dir() else None,
        vm_ip=vm_ip,
        failure_stage=last_error,
        last_error=last_error,
    )
