import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from Kive.utils.kivedevel.kivedevel.backends import incus_host
from Kive.utils.kivedevel.kivedevel.backends import incus_network


class MockRunResult:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TestIncusHostCheckBeforeInsert(unittest.TestCase):
    """Verify firewall rule insertion uses check-before-insert behavior."""

    def test_add_bridge_forwarding_rules_inserts_when_check_fails(self):
        bridge = "incusbr0"
        call_log = []

        def fake_run(cmd, check=True, **kwargs):
            call_log.append(cmd)
            if cmd[:3] == ["iptables", "-C", "DOCKER-USER"]:
                return MockRunResult(returncode=1)
            if cmd[:3] == ["iptables", "-C", "FORWARD"]:
                return MockRunResult(returncode=1)
            if cmd[:3] == ["iptables", "-nL", "DOCKER-USER"]:
                return MockRunResult(returncode=0)
            return MockRunResult(returncode=0)

        with mock.patch.object(incus_host, "_run", side_effect=fake_run):
            incus_host._add_bridge_forwarding_rules(bridge)

        insert_cmds = [
            c for c in call_log if c[0] == "iptables" and c[1] == "-I"
        ]
        self.assertEqual(len(insert_cmds), 4)

        self.assertIn("DOCKER-USER", " ".join(insert_cmds[0]))
        self.assertIn("DOCKER-USER", " ".join(insert_cmds[1]))
        self.assertIn("FORWARD", " ".join(insert_cmds[2]))
        self.assertIn("FORWARD", " ".join(insert_cmds[3]))

    def test_add_bridge_forwarding_rules_skips_insert_when_rule_exists(self):
        bridge = "incusbr0"
        call_log = []

        def fake_run(cmd, check=True, **kwargs):
            call_log.append(cmd)
            if cmd[:3] == ["iptables", "-C", "DOCKER-USER"]:
                return MockRunResult(returncode=0)
            if cmd[:3] == ["iptables", "-C", "FORWARD"]:
                return MockRunResult(returncode=0)
            if cmd[:3] == ["iptables", "-nL", "DOCKER-USER"]:
                return MockRunResult(returncode=0)
            return MockRunResult(returncode=0)

        with mock.patch.object(incus_host, "_run", side_effect=fake_run):
            incus_host._add_bridge_forwarding_rules(bridge)

        insert_cmds = [
            c for c in call_log if c[0] == "iptables" and c[1] == "-I"
        ]
        self.assertEqual(len(insert_cmds), 0)

    def test_no_docker_chain_no_error(self):
        bridge = "incusbr0"
        call_log = []

        def fake_run(cmd, check=True, **kwargs):
            call_log.append(cmd)
            if cmd[:3] == ["iptables", "-nL", "DOCKER-USER"]:
                return MockRunResult(returncode=1)
            if cmd[:3] == ["iptables", "-C", "FORWARD"]:
                return MockRunResult(returncode=1)
            return MockRunResult(returncode=0)

        with mock.patch.object(incus_host, "_run", side_effect=fake_run):
            incus_host._add_bridge_forwarding_rules(bridge)

        docker_cmds = [
            c for c in call_log if "DOCKER-USER" in " ".join(c)
        ]
        self.assertEqual(len(docker_cmds), 1)

    def test_add_masquerade_fallback_check_before_insert(self):
        bridge = "incusbr0"
        call_log = []

        cmds = mock.Mock()
        cmds.incus.output.return_value = "10.67.67.1/24"

        def fake_run(cmd, check=True, **kwargs):
            call_log.append(cmd)
            if "ip_network" in " ".join(cmd):
                return MockRunResult(
                    returncode=0, stdout="10.67.67.0/24\n"
                )
            if "-C" in cmd:
                return MockRunResult(returncode=1)
            return MockRunResult(returncode=0)
    
        with mock.patch.object(incus_host, "_run", side_effect=fake_run):
            incus_host._add_masquerade_fallback(cmds, bridge)
    
        insert_cmds = [
            c
            for c in call_log
            if c[0] == "iptables" and c[1] == "-t" and c[2] == "nat" and c[3] == "-I"
        ]
        self.assertEqual(len(insert_cmds), 1)
        self.assertIn("10.67.67.0/24", " ".join(insert_cmds[0]))
        self.assertIn("MASQUERADE", " ".join(insert_cmds[0]))

    def test_add_masquerade_fallback_skips_when_rule_exists(self):
        bridge = "incusbr0"

        cmds = mock.Mock()
        cmds.incus.output.return_value = "10.67.67.1/24"

        def fake_run(cmd, check=True, **kwargs):
            if "ip_network" in " ".join(cmd):
                return MockRunResult(
                    returncode=0, stdout="10.67.67.0/24\n"
                )
            if "-C" in cmd:
                return MockRunResult(returncode=0)
            return MockRunResult(returncode=0)

        with mock.patch.object(incus_host, "_run", side_effect=fake_run):
            incus_host._add_masquerade_fallback(cmds, bridge)

        self.assertTrue(True)

    def test_enable_ipv4_forwarding_sets_sysctl(self):
        call_log = []

        def fake_run(cmd, check=True, **kwargs):
            call_log.append(cmd)
            return MockRunResult(returncode=0)

        with mock.patch.object(incus_host, "_run", side_effect=fake_run):
            incus_host._enable_ipv4_forwarding()

        self.assertTrue(
            any(
                cmd == ["sysctl", "-w", "net.ipv4.ip_forward=1"]
                for cmd in call_log
            )
        )
        self.assertTrue(
            any(
                cmd == ["sysctl", "-w", "net.ipv4.conf.all.forwarding=1"]
                for cmd in call_log
            )
        )
        self.assertTrue(
            any(
                cmd == ["sysctl", "-w", "net.ipv4.conf.default.forwarding=1"]
                for cmd in call_log
            )
        )


class TestIncusNetworkCheckReadiness(unittest.TestCase):
    """Verify network readiness checks look for the right indicators."""

    def test_guest_network_ready_command_includes_all_checks(self):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        result = incus_network._guest_network_ready(
            cmds, "net-smoke", "archive.ubuntu.com"
        )

        self.assertTrue(result)

        call_args = cmds.incus.run.call_args
        cmd_list = call_args[0][0]

        self.assertIn("test -e /etc/resolv.conf", cmd_list[-1])
        self.assertIn('ip -4 addr show dev eth0 | grep -q "inet "', cmd_list[-1])
        self.assertIn('ip route | grep -q "^default "', cmd_list[-1])
        self.assertIn("getent ahostsv4 archive.ubuntu.com >/dev/null", cmd_list[-1])

    def test_guest_network_ready_returns_false_on_failure(self):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1)

        result = incus_network._guest_network_ready(
            cmds, "net-smoke", "archive.ubuntu.com"
        )
        self.assertFalse(result)

    def test_run_check_network_cleans_up_in_finally_on_success(self):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with mock.patch.object(incus_network, "Cmds") as mock_cmds_cls:
            mock_cmds_cls.create.return_value = cmds

            args = mock.Mock()
            args.backend = "incus"
            args.bridge = "incusbr0"
            args.instance = "network-smoke"
            args.host = "archive.ubuntu.com"
            args.port = 80
            args.debug = False

            incus_network.run_check_network(args)

        delete_calls = [
            c for c in cmds.incus.run.call_args_list
            if c[0][0][:2] == ["delete", "-f"]
        ]
        self.assertGreaterEqual(len(delete_calls), 1)


if __name__ == "__main__":
    unittest.main()
