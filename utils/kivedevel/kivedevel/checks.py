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


_SLURM_PROBE_SCRIPT = """
set -e
echo '=== hostname ==='
hostname -s
echo '=== getent hosts head ==='
getent hosts head || echo 'not found'
echo '=== Slurm services ==='
for svc in slurmdbd slurmctld slurmd; do
  if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "$svc" 2>/dev/null; then
    echo "active: $svc"
  elif command -v systemctl >/dev/null 2>&1; then
    echo "inactive: $svc"
  else
    echo "unknown: $svc (no systemctl)"
  fi
done
echo '=== Slurm commands ==='
command -v squeue sinfo 2>&1 || true
"""


def _run_slurm_probe(cmds: Cmds, instance: str) -> None:
    try:
        result = cmds.incus.run(
            ["exec", instance, "--", "sh", "-c", _SLURM_PROBE_SCRIPT],
            check=False,
            capture_output=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        logger.warning("Slurm probe timed out on %s (instance may still be booting)", instance)
        return
    except Exception as exc:
        logger.debug("Slurm probe skipped on %s: %s", instance, exc)
        return

    output = (result.stdout or "").strip()
    if not output:
        logger.debug("Slurm probe returned empty output on %s", instance)
        return

    hostname_match = re.search(r"^=== hostname ===\s*\n(.+)", output, re.MULTILINE)
    got_hostname = hostname_match.group(1).strip() if hostname_match else ""
    if got_hostname and got_hostname != "head":
        logger.warning("Hostname is %r (expected 'head')", got_hostname)

    hosts_match = re.search(r"^=== getent hosts head ===\s*\n(.+)", output, re.MULTILINE)
    hosts_line = hosts_match.group(1).strip() if hosts_match else ""
    if hosts_line and hosts_line != "not found":
        if "127.0.0.1" not in hosts_line:
            logger.warning("'head' resolves to %s (expected 127.0.0.1)", hosts_line)
    elif hosts_line == "not found":
        logger.warning("'head' does not resolve via getent hosts")

    services_text = output[output.find("=== Slurm services ==="):output.find("=== Slurm commands ===")] if "=== Slurm commands ===" in output else output[output.find("=== Slurm services ==="):]
    inactive_svcs = re.findall(r"^inactive: (.+)$", services_text, re.MULTILINE)
    if inactive_svcs:
        logger.warning("Slurm service(s) not active: %s", ", ".join(inactive_svcs))

    cmds_text = output[output.find("=== Slurm commands ==="):] if "=== Slurm commands ===" in output else ""
    if cmds_text:
        logger.debug("Slurm commands found:\n%s", cmds_text)


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


def _run_singularity_probe(cmds: Cmds, instance: str) -> None:
    try:
        result = cmds.incus.run(
            ["exec", instance, "--", "sh", "-c", _SINGULARITY_PROBE_SCRIPT],
            check=False,
            capture_output=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        logger.warning("Singularity probe timed out on %s", instance)
        return
    except Exception as exc:
        logger.debug("Singularity probe skipped on %s: %s", instance, exc)
        return

    output = (result.stdout or "").strip()
    if not output:
        logger.debug("Singularity probe returned empty output on %s", instance)
        return

    if "not installed" in output:
        logger.warning("Singularity is not installed in %s.", instance)
        return

    if "exec FAILED" in output:
        logger.warning("Singularity exec test failed in %s.", instance)

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
        _run_singularity_probe(cmds, instance)

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


def _resolve_base_url(
    cmds: Cmds,
    instance: str,
    port: int,
    explicit: str | None,
    fallback_ports: list[int] | None = None,
) -> str | None:
    if explicit:
        return explicit.rstrip("/")

    ports_to_try = [port]
    for fallback_port in fallback_ports or []:
        if fallback_port not in ports_to_try:
            ports_to_try.append(fallback_port)

    candidates: list[str] = []
    for candidate_port in ports_to_try:
        candidates.append(f"http://127.0.0.1:{candidate_port}")
        for ip in _vm_ip_candidates(cmds, instance):
            candidates.append(f"http://{ip}:{candidate_port}")

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


def _vm_start_api_via_ssh(ip: str, port: int, max_wait: int = 30) -> bool:
    """Attempt to start the Kive API on the VM via SSH and wait for it to become reachable.
    
    Returns True if API becomes reachable on the given port, False otherwise.
    """
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
    
    # Start the API in the background
    start_cmd = (
        "cd /usr/local/share/Kive/kive && "
        "source /etc/kive_dev_vars 2>/dev/null || true && "
        "source $HOME/.venv_kive/bin/activate 2>/dev/null || true && "
        "(python manage.py runserver 0.0.0.0:8000 >/tmp/kive_api.log 2>&1 &) && "
        "sleep 2 && echo 'started'"
    )
    
    try:
        result = subprocess.run(
            ssh_common + [start_cmd],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            logger.debug("SSH API startup failed: %s", (result.stderr or "").strip())
            return False
    except Exception as e:
        logger.debug("SSH API startup exception: %s", e)
        return False
    
    # Wait for API to become HTTP-reachable on the given port
    url = f"http://{ip}:{port}/login/"
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            opener = urllib.request.build_opener()
            with opener.open(url, timeout=2) as response:
                if response.status == 200:
                    logger.info("API started and is reachable at %s:%s", ip, port)
                    return True
        except Exception:
            pass
        time.sleep(1)
    
    logger.debug("API did not become reachable at %s:%s within %s seconds", ip, port, max_wait)
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

    fallback_ports = [80, 8080] if args.port == 8000 else []
    base_url = _resolve_base_url(
        cmds,
        instance,
        args.port,
        args.base_url,
        fallback_ports=fallback_ports,
    )

    if not base_url and not args.base_url:
        kind = _instance_kind(cmds, instance)
        vm_ips = _vm_ip_candidates(cmds, instance)
        vm_ip = vm_ips[0] if vm_ips else ""
        
        if kind == "virtual-machine" and vm_ip:
            # For unprov machines, attempt SSH startup if provisioned
            if _vm_looks_provisioned_for_primary_api(vm_ip):
                logger.info(
                    "API not HTTP-reachable on %s, attempting SSH-based startup...",
                    vm_ip,
                )
                if _vm_start_api_via_ssh(vm_ip, args.port):
                    base_url = f"http://{vm_ip}:{args.port}"
                    logger.info("API started successfully, resuming test-api checks...")
                else:
                    logger.error(
                        "Failed to start API via SSH on %s. "
                        "Verify provisioning and dependencies, then try again.",
                        vm_ip,
                    )
            else:
                logger.error(
                    "No host-reachable API endpoint for VM instance %s. "
                    "Run provisioning steps and verify Kive is installed, then rerun test-api.",
                    instance,
                )
        elif kind == "container":
            logger.error(
                "No host-reachable API endpoint for container instance %s. "
                "Publish the API to a host-reachable address and rerun test-api.",
                instance,
            )
        else:
            logger.error(
                "No host-reachable API endpoint for instance %s. "
                "Verify the instance is properly provisioned and the API is running.",
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
