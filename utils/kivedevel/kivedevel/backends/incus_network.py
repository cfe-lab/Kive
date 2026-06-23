"""Check guest network egress for Incus backend."""

from __future__ import annotations

import argparse
import logging
import sys
import time

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel.backends.incus_network")

NETWORK_READY_TIMEOUT = 120
POLL_INTERVAL = 1


def _guest_diagnostics(cmds: Cmds, instance: str) -> None:
    """Print diagnostics from inside the guest."""
    cmds.incus.run(
        [
            "exec", instance, "--", "sh", "-lc", """
                echo "--- cloud-init ---"; cloud-init status --long 2>&1 || true
                echo "--- ip addr ---"; ip addr || true
                echo "--- ip route ---"; ip route || true
                echo "--- resolv.conf ---"; cat /etc/resolv.conf || true
                echo "--- systemd-networkd ---"; systemctl status systemd-networkd --no-pager || true
                echo "--- systemd-resolved ---"; systemctl status systemd-resolved --no-pager || true
                echo "--- journal network snippets ---"; journalctl -u systemd-networkd -u systemd-resolved --no-pager -n 100 || true
            """,
        ],
        check=False,
    )


def _guest_network_ready(cmds: Cmds, instance: str, host: str) -> bool:
    result = cmds.incus.run(
        [
            "exec", instance, "--", "sh", "-lc",
            f"test -e /etc/resolv.conf && "
            f"ip -4 addr show dev eth0 | grep -q \"inet \" && "
            f"ip route | grep -q \"^default \" && "
            f"getent ahostsv4 {host} >/dev/null",
        ],
        check=False,
    )
    return result.returncode == 0


def _tcp_connect_test(cmds: Cmds, instance: str, host: str, port: int) -> None:
    logger.info("=== guest TCP egress test ===")
    result = cmds.incus.run(
        [
            "exec", instance, "--", "sh", "-lc",
            f"""python3 -c "import socket,sys; addr=socket.getaddrinfo('{host}',{port},socket.AF_INET,socket.SOCK_STREAM)[0][4]; print('connecting to', addr); sock=socket.create_connection(addr, timeout=10); sock.close(); print('TCP egress ok')" """,
        ],
        check=False,
    )
    if result.returncode != 0:
        logger.error("TCP egress test FAILED for %s:%s", host, port)
        sys.exit(1)


def run_check_network(args: argparse.Namespace) -> None:
    # CLI is backend-generic; only incus is implemented today.
    assert args.backend == "incus", f"Unsupported backend: {args.backend}"

    instance: str = args.instance
    host: str = args.host
    port: int = args.port
    debug: bool = getattr(args, "debug", False)

    cmds = Cmds.create()
    cmds.incus.require()

    # Delete any stale temporary instance.
    cmds.incus.run(["delete", "-f", instance], check=False)

    try:
        cmds.incus.run(
            [
                "launch",
                "images:ubuntu/noble/cloud",
                instance,
                "--profile", "default",
                "--config", "limits.cpu=1",
                "--config", "limits.memory=512MB",
            ]
        )

        logger.info("=== wait for guest network readiness ===")
        for i in range(1, NETWORK_READY_TIMEOUT + 1):
            if _guest_network_ready(cmds, instance, host):
                logger.info("Guest network is ready")
                break
            if i == NETWORK_READY_TIMEOUT:
                logger.error("Guest network readiness timed out")
                _guest_diagnostics(cmds, instance)
                sys.exit(1)
            time.sleep(POLL_INTERVAL)

        if debug:
            logger.info("=== guest network diagnostics ===")
            cmds.incus.run(
                [
                    "exec", instance, "--", "sh", "-lc",
                    "ip addr; ip route; cat /etc/resolv.conf; getent hosts archive.ubuntu.com; getent ahostsv4 archive.ubuntu.com",
                ],
                check=False,
            )

        _tcp_connect_test(cmds, instance, host, port)
        logger.info("Guest network check passed.")
    finally:
        cmds.incus.run(["delete", "-f", instance], check=False)


def register_subcommand(subparsers) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "check-network",
        help=(
            "Check that a temporary guest can resolve DNS and "
            "make outbound TCP connections"
        ),
    )
    parser.add_argument(
        "--backend",
        default="incus",
        choices=("incus",),
        help="Backend to use (default: incus)",
    )
    parser.add_argument(
        "--instance",
        default="network-smoke",
        help="Temporary instance name (default: network-smoke)",
    )
    parser.add_argument(
        "--host",
        default="archive.ubuntu.com",
        help="Host to test DNS resolution and TCP connectivity against (default: archive.ubuntu.com)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=80,
        help="TCP port to test connectivity against (default: 80)",
    )
    parser.add_argument("--debug", action="store_true", help="Show debug logging")
    parser.set_defaults(func=run_check_network)
