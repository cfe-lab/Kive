"""Validation and smoke-check subcommands for Incus development instances."""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from pathlib import Path

from .kv_commands import Cmds
from .shared import configure_logging, default_root, instance_exists, instance_is_running


logger = logging.getLogger("kivedevel.checks")


def _device_exists(cmds: Cmds, instance: str, device: str) -> bool:
    out = cmds.incus.output(["config", "device", "show", instance])
    return bool(re.search(rf"^{re.escape(device)}:\s*$", out, re.MULTILINE))


def _required_device_value(cmds: Cmds, instance: str, device: str, key: str) -> str:
    value = cmds.incus.output(["config", "device", "get", instance, device, key]).strip()
    if not value:
        logger.error("Missing %s for device %s on %s.", key, device, instance)
        sys.exit(1)
    return value


def _run_validate_vm(args: argparse.Namespace) -> None:
    workdir: Path = args.workdir.resolve()
    configure_logging(args, workdir)

    cmds = Cmds.create()
    cmds.incus.require()

    instance = args.instance
    if not instance_exists(cmds, instance):
        logger.error("Instance %s does not exist.", instance)
        sys.exit(1)

    if not instance_is_running(cmds, instance):
        logger.error("Instance %s exists but is not running.", instance)
        sys.exit(1)

    if not _device_exists(cmds, instance, "kive-code"):
        logger.error("Instance %s is missing required device 'kive-code'.", instance)
        sys.exit(1)

    if args.instance_type == "container":
        # Container mode mounts repo source as a host directory at /mnt/kive-code.
        mount_path = _required_device_value(cmds, instance, "kive-code", "path")
        source_path = Path(_required_device_value(cmds, instance, "kive-code", "source"))
        if mount_path != "/mnt/kive-code":
            logger.error("Unexpected kive-code path %s (expected /mnt/kive-code).", mount_path)
            sys.exit(1)
        if not source_path.is_dir():
            logger.error("kive-code source directory does not exist: %s", source_path)
            sys.exit(1)

    logger.info("validate-vm checks passed for %s.", instance)


def _request_status(opener, url: str):
    try:
        with opener.open(url, timeout=5) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body
    except urllib.error.URLError:
        # Connection refused, timeout, or other network issues
        return None, None


def _is_url_reachable(base_url: str) -> bool:
    opener = urllib.request.build_opener()
    status, _ = _request_status(opener, f"{base_url}/login/")
    return status == 200


def _vm_ip_candidates(cmds: Cmds, instance: str) -> list[str]:
    out = cmds.incus.output(["list", instance, "--format", "json"])
    if not out:
        return []
    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        return []

    candidates: list[str] = []
    if not isinstance(payload, list) or not payload:
        return candidates

    state = payload[0].get("state", {})
    network = state.get("network", {}) if isinstance(state, dict) else {}
    for iface_data in network.values():
        addresses = iface_data.get("addresses", []) if isinstance(iface_data, dict) else []
        for addr in addresses:
            family = addr.get("family")
            scope = addr.get("scope")
            address = addr.get("address")
            if family == "inet" and scope == "global" and address:
                candidates.append(address)
    return candidates


def _instance_kind(cmds: Cmds, instance: str) -> str:
    out = cmds.incus.output(["list", instance, "--format", "json"])
    if not out:
        return ""
    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    kind = payload[0].get("type")
    if isinstance(kind, str):
        return kind.lower()
    return ""


def _resolve_base_url(cmds: Cmds, instance: str, port: int, explicit: str | None) -> str | None:
    if explicit:
        return explicit.rstrip("/")

    candidates = [f"http://127.0.0.1:{port}"]
    for ip in _vm_ip_candidates(cmds, instance):
        candidates.append(f"http://{ip}:{port}")

    for candidate in candidates:
        try:
            if _is_url_reachable(candidate):
                return candidate
        except Exception:
            continue

    logger.info("No pre-existing API server reachable at: %s", ", ".join(candidates))
    return None


def _vm_looks_provisioned_for_primary_api(ip: str) -> bool:
    ssh_common = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ConnectTimeout=8",
        f"ubuntu@{ip}",
    ]
    probe = (
        "test -f /etc/kive_dev_vars "
        "-o -f /usr/local/share/Kive/kive/manage.py "
        "-o -d /usr/local/share/Kive/kive"
    )
    try:
        result = subprocess.run(
            ssh_common + [probe],
            check=False,
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except Exception:
        return False


def _run_api_probe(base_url: str, username: str, password: str) -> dict:
    login_url = f"{base_url}/login/"
    datasets_url = f"{base_url}/api/datasets/?limit=1"

    anon_opener = urllib.request.build_opener()
    anon_status, _ = _request_status(anon_opener, datasets_url)

    jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    login_status, _ = _request_status(opener, login_url)
    if login_status != 200:
        logger.error("Login page returned %s (expected 200)", login_status)
        sys.exit(1)

    csrf = None
    for cookie in jar:
        if cookie.name == "csrftoken":
            csrf = cookie.value
            break
    if not csrf:
        logger.error("Missing csrftoken after GET %s", login_url)
        sys.exit(1)

    data = urllib.parse.urlencode(
        {
            "username": username,
            "password": password,
            "csrfmiddlewaretoken": csrf,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        login_url,
        data=data,
        headers={"Referer": login_url},
        method="POST",
    )
    with opener.open(req, timeout=5) as response:
        post_login_status = response.status

    auth_status, auth_body = _request_status(opener, datasets_url)
    result = {
        "login_page_status": login_status,
        "anon_datasets_status": anon_status,
        "post_login_status": post_login_status,
        "auth_datasets_status": auth_status,
        "auth_json_ok": False,
        "auth_count": None,
    }

    if auth_status == 200:
        parsed = json.loads(auth_body)
        if isinstance(parsed, dict):
            items = parsed.get("results", parsed)
            if isinstance(items, list):
                result["auth_count"] = len(items)
        elif isinstance(parsed, list):
            result["auth_count"] = len(parsed)
        result["auth_json_ok"] = True

    return result


def _run_test_api(args: argparse.Namespace) -> None:
    workdir: Path = args.workdir.resolve()
    configure_logging(args, workdir)

    cmds = Cmds.create()
    cmds.incus.require()

    instance = args.instance
    if not instance_exists(cmds, instance):
        logger.error("Instance %s does not exist.", instance)
        sys.exit(1)

    if not instance_is_running(cmds, instance):
        logger.error("Instance %s is not running.", instance)
        sys.exit(1)

    base_url = _resolve_base_url(cmds, instance, args.port, args.base_url)

    if not base_url and not args.base_url:
        kind = _instance_kind(cmds, instance)
        vm_ips = _vm_ip_candidates(cmds, instance)
        vm_ip = vm_ips[0] if vm_ips else ""
        if vm_ip and _vm_looks_provisioned_for_primary_api(vm_ip):
            logger.error(
                "Primary API appears provisioned on %s but is not reachable from host on port %s",
                vm_ip,
                args.port,
            )
        elif kind == "container":
            logger.error(
                "No host-reachable API endpoint for container instance %s. "
                "Publish the API to a host-reachable address and rerun test-api.",
                instance,
            )
        else:
            logger.error(
                "No host-reachable API endpoint for VM instance %s. "
                "Start a host-reachable API service and rerun test-api.",
                instance,
            )

    if not base_url:
        logger.error("No reachable API endpoint for instance %s", instance)
        sys.exit(1)

    logger.info("Running API probe against %s...", base_url)
    probe = _run_api_probe(base_url, username=args.username, password=args.password)

    logger.debug("Probe result: %s", probe)

    login_page_status = int(probe.get("login_page_status", 0))
    anon_status = int(probe.get("anon_datasets_status", 0))
    auth_status = int(probe.get("auth_datasets_status", 0))
    auth_json_ok = bool(probe.get("auth_json_ok", False))

    if login_page_status != 200:
        logger.error("Login page check failed: expected 200, got %s", login_page_status)
        sys.exit(1)

    # Observable effect: authenticated session should change API behavior.
    if anon_status == auth_status:
        logger.error(
            "Authentication had no observable effect on /api/datasets/: status stayed %s",
            auth_status,
        )
        sys.exit(1)

    if auth_status != 200:
        logger.error("Authenticated datasets request failed: expected 200, got %s", auth_status)
        sys.exit(1)

    if not auth_json_ok:
        logger.error("Authenticated /api/datasets/ response was not valid JSON")
        sys.exit(1)

    logger.info(
        "test-api checks passed for %s. anon=%s auth=%s dataset_count=%s",
        instance,
        anon_status,
        auth_status,
        probe.get("auth_count"),
    )


def register_subcommands(subparsers) -> None:  # type: ignore[type-arg]
    log_opts = {
        "quiet": dict(action="store_true", help="Only show errors"),
        "verbose": dict(action="store_true", help="Show informational progress messages"),
        "debug": dict(action="store_true", help="Show debug logging, including full command lines"),
    }

    validate = subparsers.add_parser(
        "validate-vm",
        help="Validate that a build-vm instance is running and properly mounted",
    )
    validate.add_argument("instance", nargs="?", default="kive-minimal", help="Incus instance name")
    validate.add_argument(
        "--instance-type",
        choices=("vm", "container"),
        default="vm",
        help="Expected instance type for mount checks (default: vm)",
    )
    validate.add_argument(
        "--workdir",
        type=Path,
        default=default_root() / "tmp~" / "build",
        help="Working directory used for logs",
    )
    for opt, kwargs in log_opts.items():
        validate.add_argument(f"--{opt}", **kwargs)
    validate.set_defaults(func=_run_validate_vm)

    test_api = subparsers.add_parser(
        "test-api",
        help="Run API server checks inside an instance (HTTP connectivity, endpoint responses, JSON serialization)",
    )
    test_api.add_argument("instance", nargs="?", default="kive-minimal", help="Incus instance name")
    test_api.add_argument(
        "--username",
        default="kive",
        help="Username for API auth probe (default: kive)",
    )
    test_api.add_argument(
        "--password",
        default="kive",
        help="Password for API auth probe (default: kive)",
    )
    test_api.add_argument(
        "--port",
        type=int,
        default=8000,
        help="HTTP port to probe (default: 8000)",
    )
    test_api.add_argument(
        "--base-url",
        default=None,
        help="Explicit API base URL override (example: http://127.0.0.1:8000)",
    )
    test_api.add_argument(
        "--workdir",
        type=Path,
        default=default_root() / "tmp~" / "build",
        help="Working directory used for logs",
    )
    for opt, kwargs in log_opts.items():
        test_api.add_argument(f"--{opt}", **kwargs)
    test_api.set_defaults(func=_run_test_api)
