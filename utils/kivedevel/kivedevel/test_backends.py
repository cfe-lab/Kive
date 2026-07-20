import logging
import unittest
from unittest import mock


from kivedevel.backends import incus_host
from kivedevel.backends import incus_network
from kivedevel.shared import _log_level, configure_console_logging
from kivedevel._test_helpers import MockRunResult


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


class TestConsoleLogging(unittest.TestCase):
    def test_log_level_debug(self):
        args = mock.Mock()
        args.quiet = False
        args.verbose = False
        args.debug = True
        self.assertEqual(_log_level(args), logging.DEBUG)

    def test_log_level_verbose(self):
        args = mock.Mock()
        args.quiet = False
        args.verbose = True
        args.debug = False
        self.assertEqual(_log_level(args), logging.INFO)

    def test_log_level_quiet(self):
        args = mock.Mock()
        args.quiet = True
        args.verbose = False
        args.debug = False
        self.assertEqual(_log_level(args), logging.ERROR)

    def test_log_level_default(self):
        args = mock.Mock()
        args.quiet = False
        args.verbose = False
        args.debug = False
        self.assertEqual(_log_level(args), logging.INFO)

    def test_configure_console_logging_sets_debug_level(self):
        args = mock.Mock()
        args.quiet = False
        args.verbose = False
        args.debug = True
        try:
            configure_console_logging(args)
            root = logging.getLogger()
            self.assertTrue(root.isEnabledFor(logging.DEBUG))
        finally:
            logging.basicConfig(handlers=[logging.NullHandler()], force=True)

    def test_configure_console_logging_sets_error_level(self):
        args = mock.Mock()
        args.quiet = True
        args.verbose = False
        args.debug = False
        try:
            configure_console_logging(args)
            root = logging.getLogger()
            self.assertFalse(root.isEnabledFor(logging.INFO))
            self.assertTrue(root.isEnabledFor(logging.ERROR))
        finally:
            logging.basicConfig(handlers=[logging.NullHandler()], force=True)


class TestNeedsPrivilegedFallback(unittest.TestCase):
    def test_matches_uid_gid_configured(self):
        self.assertTrue(
            incus_network._needs_privileged_fallback(
                "Error: Failed instance creation: "
                "Failed creating instance record: "
                "Failed initializing instance: Invalid config: "
                "No uid/gid allocation configured"
            )
        )

    def test_matches_no_map_for_user(self):
        self.assertTrue(
            incus_network._needs_privileged_fallback(
                "some error: no map found for user"
            )
        )

    def test_matches_only_privileged_supported(self):
        self.assertTrue(
            incus_network._needs_privileged_fallback(
                "only privileged containers are supported"
            )
        )

    def test_does_not_match_unrelated_error(self):
        self.assertFalse(
            incus_network._needs_privileged_fallback(
                "Error: image not found"
            )
        )

    def test_does_not_match_empty_string(self):
        self.assertFalse(incus_network._needs_privileged_fallback(""))


class TestIncusNetworkCheckReadiness(unittest.TestCase):
    """Verify network readiness checks look for the right indicators."""

    def _make_cmds(self, side_effects: list) -> mock.Mock:
        cmds = mock.Mock()
        cmds.incus.run.side_effect = side_effects
        return cmds

    def _make_args(self, **overrides) -> mock.Mock:
        args = mock.Mock()
        args.backend = "incus"
        args.instance = "network-smoke"
        args.host = "archive.ubuntu.com"
        args.port = 80
        args.debug = False
        for k, v in overrides.items():
            setattr(args, k, v)
        return args

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
        cmds = self._make_cmds([
            MockRunResult(returncode=0),  # delete
            MockRunResult(returncode=0),  # launch
            MockRunResult(returncode=0),  # guest_network_ready poll
            MockRunResult(returncode=0),  # tcp connect
            MockRunResult(returncode=0),  # finally delete
        ])

        with mock.patch.object(incus_network, "Cmds") as mock_cmds_cls:
            mock_cmds_cls.create.return_value = cmds
            incus_network.run_check_network(self._make_args())

        delete_calls = [
            c for c in cmds.incus.run.call_args_list
            if c[0][0][:2] == ["delete", "-f"]
        ]
        self.assertGreaterEqual(len(delete_calls), 1)

    def test_retries_privileged_on_uid_gid_error(self):
        cmds = self._make_cmds([
            MockRunResult(returncode=0),  # delete
            MockRunResult(returncode=1, stderr="No uid/gid allocation configured"),  # launch fails
            MockRunResult(returncode=0),  # privileged launch succeeds
            MockRunResult(returncode=0),  # network ready poll
            MockRunResult(returncode=0),  # tcp connect
            MockRunResult(returncode=0),  # finally delete
        ])

        with mock.patch.object(incus_network, "Cmds") as mock_cmds_cls:
            mock_cmds_cls.create.return_value = cmds
            incus_network.run_check_network(self._make_args())

        launch_calls = [
            c for c in cmds.incus.run.call_args_list
            if c[0][0][0] == "launch"
        ]
        self.assertEqual(len(launch_calls), 2)
        second_launch = launch_calls[1][0][0]
        self.assertIn("security.privileged=true", second_launch)

    def test_retries_privileged_on_no_map_for_user(self):
        cmds = self._make_cmds([
            MockRunResult(returncode=0),  # delete
            MockRunResult(returncode=1, stderr="no map found for user"),  # launch fails
            MockRunResult(returncode=0),  # privileged launch succeeds
            MockRunResult(returncode=0),  # network ready poll
            MockRunResult(returncode=0),  # tcp connect
            MockRunResult(returncode=0),  # finally delete
        ])

        with mock.patch.object(incus_network, "Cmds") as mock_cmds_cls:
            mock_cmds_cls.create.return_value = cmds
            incus_network.run_check_network(self._make_args())

        launch_calls = [
            c for c in cmds.incus.run.call_args_list
            if c[0][0][0] == "launch"
        ]
        self.assertEqual(len(launch_calls), 2)
        second_launch = launch_calls[1][0][0]
        self.assertIn("security.privileged=true", second_launch)

    def test_does_not_retry_on_unrelated_error(self):
        cmds = self._make_cmds([
            MockRunResult(returncode=0),  # delete
            MockRunResult(returncode=1, stderr="Error: image not found"),  # launch fails
            MockRunResult(returncode=0),  # finally delete
        ])

        with mock.patch.object(incus_network, "Cmds") as mock_cmds_cls:
            mock_cmds_cls.create.return_value = cmds
            with self.assertRaises(SystemExit):
                incus_network.run_check_network(self._make_args())

        launch_calls = [
            c for c in cmds.incus.run.call_args_list
            if c[0][0][0] == "launch"
        ]
        self.assertEqual(len(launch_calls), 1)

    def test_cleanup_on_launch_failure(self):
        cmds = self._make_cmds([
            MockRunResult(returncode=0),  # delete
            MockRunResult(returncode=1, stderr="Error: image not found"),  # launch fails
            MockRunResult(returncode=0),  # finally delete
        ])

        with mock.patch.object(incus_network, "Cmds") as mock_cmds_cls:
            mock_cmds_cls.create.return_value = cmds
            with self.assertRaises(SystemExit):
                incus_network.run_check_network(self._make_args())

        delete_calls = [
            c for c in cmds.incus.run.call_args_list
            if c[0][0][:2] == ["delete", "-f"]
        ]
        self.assertGreaterEqual(len(delete_calls), 1)


 

class TestPrepareHostDefaults(unittest.TestCase):
    """Verify prepare-host CLI and preseed defaults."""

    def test_prepare_host_default_bridge_is_kive_lab_br(self):
        import argparse
        from kivedevel.backends.incus_host import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["prepare-host", "--backend", "incus"])
        self.assertEqual(args.bridge, "kive-lab-br")

    def test_prepare_host_preseed_has_no_network(self):
        import textwrap
        preseed = textwrap.dedent("""\\
        config: {}
        storage_pools:
        - name: default
          driver: dir
        """)
        self.assertNotIn("networks:", preseed)
        self.assertIn("storage_pools:", preseed)

    def test_prepare_host_default_bridge_is_not_incusbr0(self):
        import argparse
        from kivedevel.backends.incus_host import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["prepare-host", "--backend", "incus"])
        self.assertNotEqual(args.bridge, "incusbr0")


if __name__ == "__main__":
    unittest.main()
