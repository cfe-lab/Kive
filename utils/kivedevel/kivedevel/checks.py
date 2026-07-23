"""Validation and smoke-check subcommands for Incus development instances."""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from pathlib import Path

from .kv_commands import Cmds
from .shared import configure_logging, default_root, instance_exists, instance_is_running
from .slurm_health import SLURM_HEALTHCHECK_SCRIPT


logger = logging.getLogger("kivedevel.checks")

SMOKE_DEADLINE_SECONDS = 1800


def _device_exists(cmds: Cmds, instance: str, device: str) -> bool:
    out = cmds.incus.output(["config", "device", "show", instance])
    return bool(re.search(rf"^{re.escape(device)}:\s*$", out, re.MULTILINE))


def _required_device_value(cmds: Cmds, instance: str, device: str, key: str) -> str:
    value = cmds.incus.output(["config", "device", "get", instance, device, key]).strip()
    if not value:
        logger.error("Missing %s for device %s on %s.", key, device, instance)
        sys.exit(1)
    return value


def _run_slurm_probe(cmds: Cmds, instance: str) -> None:
    script = SLURM_HEALTHCHECK_SCRIPT.strip()
    result = cmds.incus.run(
        ["exec", instance, "--", "sh", "-c", script],
        check=False, capture_output=True, timeout=SMOKE_DEADLINE_SECONDS,
    )
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        logger.error(
            "Slurm check failed on %s (rc=%s).\nstdout:\n%s\nstderr:\n%s",
            instance, result.returncode, stdout, stderr,
        )
        _print_slurm_diagnostics(cmds, instance)
        sys.exit(1)
    logger.info("Slurm health check passed on %s.\n%s", instance, stdout)


def _print_slurm_diagnostics(cmds: Cmds, instance: str) -> None:
    logger.error("=== Slurm diagnostics for %s ===", instance)
    logger.error("--- service state ---")
    for svc in ("munge", "mariadb", "slurmdbd", "slurmctld", "slurmd"):
        cmds.incus.run(
            ["exec", instance, "--", "systemctl", "status", svc, "--no-pager"],
            check=False,
        )
    logger.error("--- journal ---")
    cmds.incus.run(
        ["exec", instance, "--",
         "journalctl", "-u", "munge", "-u", "mariadb",
         "-u", "slurmdbd", "-u", "slurmctld", "-u", "slurmd",
         "--no-pager", "--lines=300"],
        check=False,
    )
    logger.error("--- controller ---")
    cmds.incus.run(["exec", instance, "--", "scontrol", "ping"], check=False)
    cmds.incus.run(["exec", instance, "--", "scontrol", "show", "node", "head", "-o"], check=False)
    logger.error("--- partitions and jobs ---")
    cmds.incus.run(["exec", instance, "--", "sinfo", "-Nel"], check=False)
    cmds.incus.run(["exec", instance, "--", "squeue", "-a"], check=False)
    logger.error("--- worker hardware ---")
    cmds.incus.run(["exec", instance, "--", "slurmd", "-C"], check=False)
    logger.error("--- Slurm logs ---")
    for logf in ("slurmdbd.log", "slurmctld.log", "slurmd.log"):
        cmds.incus.run(
            ["exec", instance, "--", "tail", "-200", f"/var/log/slurm/{logf}"],
            check=False,
        )


_SINGULARITY_PROBE_SCRIPT = """
set -e
echo '=== singularity version ==='
singularity --version 2>&1 || echo 'not installed'
echo '=== singularity exec test ==='
if command -v singularity >/dev/null 2>&1; then
  singularity exec docker://alpine:latest /bin/true 2>&1 && echo 'exec OK' || echo 'exec FAILED'
else
  echo 'singularity not available'
fi
"""


def _run_singularity_probe(cmds: Cmds, instance: str, fatal: bool = False) -> None:
    try:
        result = cmds.incus.run(
            ["exec", instance, "--", "sh", "-c", _SINGULARITY_PROBE_SCRIPT],
            check=False,
            capture_output=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        msg = "Singularity probe timed out on %s" % instance
        if fatal:
            logger.error(msg)
            sys.exit(1)
        logger.warning(msg)
        return
    except Exception as exc:
        logger.debug("Singularity probe skipped on %s: %s", instance, exc)
        return

    output = (result.stdout or "").strip()
    if not output:
        msg = "Singularity probe returned empty output on %s" % instance
        if fatal:
            logger.error(msg)
            sys.exit(1)
        logger.debug(msg)
        return

    if "not installed" in output:
        logger.error("Singularity is not installed in %s.", instance)
        if fatal:
            sys.exit(1)
        return

    if "exec FAILED" in output:
        logger.error("Singularity exec test failed in %s.", instance)
        if fatal:
            sys.exit(1)
        return

    if "exec OK" in output:
        logger.info("Singularity can execute containers in %s.", instance)

    logger.debug("Singularity probe output:\n%s", output)


def run_validate_vm(args: argparse.Namespace) -> None:
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

    _run_slurm_probe(cmds, instance)

    if args.instance_type == "vm":
        _run_singularity_probe(cmds, instance, fatal=True)

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


def _build_local_opener(cookie_jar: CookieJar | None = None):
    handlers: list = [urllib.request.ProxyHandler({})]
    if cookie_jar is not None:
        handlers.append(urllib.request.HTTPCookieProcessor(cookie_jar))
    return urllib.request.build_opener(*handlers)





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


def wait_for_http_200(
    url: str,
    *,
    deadline: float = 60,
    request_timeout: float = 5,
    poll_interval: float = 2,
) -> None:
    end = time.monotonic() + deadline
    last_error: Exception | None = None
    attempt = 0
    while time.monotonic() < end:
        attempt += 1
        try:
            opener = _build_local_opener()
            with opener.open(url, timeout=request_timeout) as r:
                if r.status == 200:
                    return
        except Exception as exc:
            last_error = exc
        if attempt % 5 == 0:
            logger.info("Waiting for %s (attempt %s)...", url, attempt)
        time.sleep(poll_interval)
    raise RuntimeError(
        f"URL {url} did not return 200 within {deadline}s. "
        f"Last error: {last_error!r}"
    )


def _resolve_base_url(explicit: str | None, port: int = 8000) -> str:
    if explicit:
        return explicit.rstrip("/")
    return f"http://127.0.0.1:{port}"


def _run_api_probe(base_url: str, username: str, password: str) -> dict:
    login_url = f"{base_url}/login/"
    datasets_url = f"{base_url}/api/datasets/?limit=1"

    anon_opener = _build_local_opener()
    anon_status, _ = _request_status(anon_opener, datasets_url)

    jar = CookieJar()
    opener = _build_local_opener(cookie_jar=jar)

    login_status, _ = _request_status(opener, login_url)

    csrf = None
    for cookie in jar:
        if cookie.name == "csrftoken":
            csrf = cookie.value
            break
    csrf_available = csrf is not None

    post_login_status = None
    if csrf:
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
        try:
            with opener.open(req, timeout=5) as response:
                post_login_status = response.status
        except Exception:
            post_login_status = None

    auth_status, auth_body = _request_status(opener, datasets_url)
    result = {
        "login_page_status": login_status,
        "csrf_available": csrf_available,
        "post_login_status": post_login_status,
        "anon_datasets_status": anon_status,
        "auth_datasets_status": auth_status,
        "auth_json_ok": False,
        "auth_count": None,
    }

    if auth_status == 200:
        try:
            parsed = json.loads(auth_body)
        except json.JSONDecodeError:
            return result
        if isinstance(parsed, dict):
            items = parsed.get("results", parsed)
            if isinstance(items, list):
                result["auth_count"] = len(items)
        elif isinstance(parsed, list):
            result["auth_count"] = len(parsed)
        result["auth_json_ok"] = True

    return result


def _print_vm_api_diagnostics(cmds: Cmds, instance: str) -> None:
    logger.error("=== VM diagnostics for %s ===", instance)
    import subprocess as _sp
    result = cmds.incus.run(["list", instance, "--format", "json"], check=False, capture_output=True, timeout=10)
    if result.returncode == 0:
        logger.error("VM list:\n%s", result.stdout or "")
    cmds.incus.run(["network", "list-leases", "kive-lab-br", "--format", "json"], check=False)
    vm_ip = _get_vm_ip_from_state(cmds, instance)
    if vm_ip:
        _sp.run(["ip", "route", "get", vm_ip], timeout=10, check=False)
    logger.error("=== GUEST service ===")
    cmds.incus.run(["exec", instance, "--", "systemctl", "status", "kive-dev-web.service", "--no-pager"], check=False)
    cmds.incus.run(["exec", instance, "--", "journalctl", "-u", "kive-dev-web.service", "--no-pager", "--lines=200"], check=False)
    logger.error("=== GUEST network ===")
    cmds.incus.run(["exec", instance, "--", "ss", "-ltnp"], check=False)
    cmds.incus.run(["exec", instance, "--", "curl", "-v", "--connect-timeout", "3", "--max-time", "5", "http://127.0.0.1:8000/login/"], check=False)
    if vm_ip:
        _sp.run(["curl", "--noproxy", "*", "--connect-timeout", "3", "--max-time", "5", f"http://{vm_ip}:8000/login/"], check=False)


def _get_vm_ip_from_state(cmds: Cmds, instance: str) -> str | None:
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


def _check_api_probe_results(results: dict, instance: str) -> None:
    """Validate probe results and log/exit on failure."""
    login_page_status = results.get("login_page_status")
    csrf_available = results.get("csrf_available", False)
    post_login_status = results.get("post_login_status")
    anon_status = results.get("anon_datasets_status")
    auth_status = results.get("auth_datasets_status")
    auth_json_ok = bool(results.get("auth_json_ok", False))

    if not login_page_status or login_page_status != 200:
        logger.error("Login page check failed: expected 200, got %s", login_page_status)
        sys.exit(1)

    if not csrf_available:
        logger.error("CSRF token not available after login page GET")
        sys.exit(1)

    if not post_login_status or post_login_status != 200:
        logger.error("Login POST failed: expected 200, got %s", post_login_status)
        sys.exit(1)

    if not anon_status or anon_status == auth_status:
        logger.error(
            "Authentication had no observable effect on /api/datasets/: anon=%s auth=%s",
            anon_status, auth_status,
        )
        sys.exit(1)

    if not auth_status or auth_status != 200:
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
        results.get("auth_count"),
    )


def run_test_api(args: argparse.Namespace) -> None:
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

    base_url = _resolve_base_url(args.base_url, args.port)
    logger.info("Waiting for API at %s/login/ ...", base_url)
    try:
        wait_for_http_200(f"{base_url}/login/")
    except RuntimeError:
        _print_vm_api_diagnostics(cmds, instance)
        sys.exit(1)

    logger.info("Running API probe against %s...", base_url)
    probe = _run_api_probe(base_url, username=args.username, password=args.password)
    _check_api_probe_results(probe, instance)


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
    validate.set_defaults(func=run_validate_vm)

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
    test_api.set_defaults(func=run_test_api)
