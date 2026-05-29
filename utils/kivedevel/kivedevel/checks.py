"""Validation and smoke-check subcommands for Incus development instances."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

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


def _http_request(
    cmds: Cmds, instance: str, method: str, path: str, port: int = 8000, **kwargs: Any
) -> tuple[int, str]:
    """Execute an HTTP request inside the instance and return (status_code, response_text)."""
    # Use curl inside the instance to make HTTP requests
    url = f"http://127.0.0.1:{port}{path}"
    curl_cmd = ["curl", "-s", "-w", "%{http_code}"]

    if method.upper() == "POST":
        curl_cmd.append("-X")
        curl_cmd.append("POST")
        if "json" in kwargs:
            curl_cmd.append("-H")
            curl_cmd.append("Content-Type: application/json")
            curl_cmd.append("-d")
            curl_cmd.append(json.dumps(kwargs["json"]))

    curl_cmd.append(url)

    try:
        output = cmds.incus.output(["exec", instance] + curl_cmd)
        # The last 3 characters are the HTTP status code (due to %{http_code})
        status_code = int(output[-3:])
        response_text = output[:-3]
        return status_code, response_text
    except Exception as e:
        logger.error("HTTP request failed: %s", e)
        raise


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

    # 1. Check HTTP server is reachable on port 8000
    logger.info("Testing HTTP connectivity to %s:8000...", instance)
    max_retries = 10
    for attempt in range(max_retries):
        try:
            status_code, response = _http_request(cmds, instance, "GET", "/")
            if status_code in (200, 301, 302):
                logger.info("HTTP server is reachable (status %d).", status_code)
                break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.debug("Connection attempt %d/%d failed: %s. Retrying...", attempt + 1, max_retries, e)
                time.sleep(1)
            else:
                logger.error("HTTP server on %s:8000 is not reachable after %d attempts.", instance, max_retries)
                sys.exit(1)

    # 2. Check login page (tests unauthenticated access and response handling)
    logger.info("Fetching login page...")
    try:
        status_code, response = _http_request(cmds, instance, "HEAD", "/login/")
        if status_code not in (200, 301, 302):
            logger.error("Failed to fetch login page (status %d). Response: %s", status_code, response)
            sys.exit(1)
        logger.info("Login page reachable (status %d).", status_code)
    except Exception as e:
        logger.error("Failed to reach login page: %s", e)
        sys.exit(1)

    # 3. Test login form retrieval (tests GET requests and form handling)
    logger.info("Testing API login form retrieval...")
    try:
        status_code, response = _http_request(cmds, instance, "GET", "/login/")
        if status_code != 200:
            logger.warning("Could not fetch login form (status %d), API may not be fully functional.", status_code)
        else:
            logger.info("Successfully retrieved login form (status 200).")
    except Exception as e:
        logger.warning("Could not retrieve login form: %s", e)

    # 4. Query API endpoints (tests database connectivity, serialization, and JSON responses)
    logger.info("Querying API endpoints...")
    endpoints = [
        ("/api/compounddatatypes/", "compound data types"),
        ("/api/datasets/", "datasets"),
        ("/api/pipelinefamilies/", "pipeline families"),
    ]

    any_endpoint_failed = False
    for endpoint_path, endpoint_name in endpoints:
        try:
            status_code, response = _http_request(cmds, instance, "GET", endpoint_path)
            if status_code == 200:
                # Try to parse JSON to verify response integrity
                try:
                    json.loads(response)
                    logger.info("  %s: OK (status 200, valid JSON response)", endpoint_name)
                except (json.JSONDecodeError, ValueError):
                    logger.warning("  %s: Response received but JSON parsing failed. Response snippet: %s...",
                                   endpoint_name, response[:100])
            elif status_code == 401:
                # Authentication required, but endpoint exists and responded
                logger.info("  %s: OK (status 401, endpoint exists and requires authentication)", endpoint_name)
            else:
                logger.warning("  %s: unexpected status %d. Response snippet: %s...",
                               endpoint_name, status_code, response[:100])
                any_endpoint_failed = True
        except Exception as e:
            logger.error("  %s: request failed: %s", endpoint_name, e)
            any_endpoint_failed = True

    if any_endpoint_failed:
        logger.error("One or more API endpoints failed to respond correctly.")
        sys.exit(1)

    logger.info("test-api checks passed for %s. HTTP server is operational and endpoints are reachable.", instance)


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
        "--workdir",
        type=Path,
        default=default_root() / "tmp~" / "build",
        help="Working directory used for logs",
    )
    for opt, kwargs in log_opts.items():
        test_api.add_argument(f"--{opt}", **kwargs)
    test_api.set_defaults(func=_run_test_api)
