"""Tests for provisioning with versioned status.json protocol."""

from __future__ import annotations

import subprocess
import unittest
from unittest import mock

from kivedevel._test_helpers import MockRunResult, make_cmds
from kivedevel.build_vm import provision

PROVISION_ID = "test-provision-id"


def make_status(state, phase, *, exit_code=None, message=None):
    return provision.ProvisionStatus(
        schema_version=1,
        provision_id=PROVISION_ID,
        state=state,
        phase=phase,
        pid=123,
        service_result=None,
        exit_code=str(exit_code) if exit_code is not None else None,
        message=message,
    )


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class TestProvisionIdContract(unittest.TestCase):
    def setUp(self):
        self.cmds = make_cmds()

    def test_disabled_returns_immediately(self):
        provision.maybe_provision_instance(
            self.cmds, "test", "vm", provision=False, timeout=10)

    def test_enabled_without_id_raises(self):
        with self.assertRaises(ValueError):
            provision.maybe_provision_instance(
                self.cmds, "test", "vm", provision=True, provision_id=None, timeout=10)

    def test_enabled_with_empty_id_raises(self):
        with self.assertRaises(ValueError):
            provision.maybe_provision_instance(
                self.cmds, "test", "vm", provision=True, provision_id="", timeout=10)


class TestProvisionStatusFromJson(unittest.TestCase):
    def _assert_ok(self, raw, expected):
        status = provision.ProvisionStatus.from_json(raw, PROVISION_ID)
        self.assertEqual(status.state, expected.state)
        self.assertEqual(status.phase, expected.phase)
        self.assertEqual(status.exit_code, expected.exit_code)

    def _assert_raises(self, raw):
        with self.assertRaises(provision.ProvisionProtocolError):
            provision.ProvisionStatus.from_json(raw, PROVISION_ID)

    def test_starting(self):
        self._assert_ok(
            '{"schema_version":1,"provision_id":"test-provision-id","state":"starting","phase":"starting","pid":1}',
            make_status("starting", "starting"),
        )

    def test_running(self):
        self._assert_ok(
            '{"schema_version":1,"provision_id":"test-provision-id","state":"running","phase":"apt-update","pid":1}',
            make_status("running", "apt-update"),
        )

    def test_succeeded(self):
        self._assert_ok(
            '{"schema_version":1,"provision_id":"test-provision-id","state":"succeeded","phase":"complete","pid":1,"exit_code":0}',
            make_status("succeeded", "complete", exit_code=0),
        )

    def test_failed(self):
        self._assert_ok(
            '{"schema_version":1,"provision_id":"test-provision-id","state":"failed","phase":"ansible","pid":1,"exit_code":1,"message":"Ansible failed"}',
            make_status("failed", "ansible", exit_code=1, message="Ansible failed"),
        )

    def test_malformed_json(self):
        self._assert_raises("{bad")

    def test_array_not_object(self):
        with self.assertRaises(provision.ProvisionProtocolError):
            provision.ProvisionStatus.from_json("[]", PROVISION_ID)

    def test_missing_schema_version(self):
        self._assert_raises('{"state":"running","phase":"x","provision_id":"test-provision-id"}')

    def test_noninteger_schema_version(self):
        self._assert_raises('{"schema_version":"1","state":"running","phase":"x","provision_id":"test-provision-id"}')

    def test_unsupported_schema_version(self):
        self._assert_raises('{"schema_version":2,"state":"running","phase":"x","provision_id":"test-provision-id"}')

    def test_missing_provision_id(self):
        self._assert_raises('{"schema_version":1,"state":"running","phase":"x"}')

    def test_empty_provision_id(self):
        self._assert_raises('{"schema_version":1,"provision_id":"","state":"running","phase":"x"}')

    def test_wrong_provision_id(self):
        with self.assertRaises(provision.ProvisionProtocolError):
            provision.ProvisionStatus.from_json(
                '{"schema_version":1,"provision_id":"wrong","state":"running","phase":"x"}',
                PROVISION_ID,
            )

    def test_missing_state(self):
        self._assert_raises('{"schema_version":1,"provision_id":"test-provision-id","phase":"x"}')

    def test_unknown_state(self):
        self._assert_raises('{"schema_version":1,"provision_id":"test-provision-id","state":"bogus","phase":"x"}')

    def test_missing_phase(self):
        self._assert_raises('{"schema_version":1,"provision_id":"test-provision-id","state":"running"}')

    def test_noninteger_pid(self):
        self._assert_raises('{"schema_version":1,"provision_id":"test-provision-id","state":"running","phase":"x","pid":"abc"}')

    def test_string_exit_code_accepted(self):
        status = provision.ProvisionStatus.from_json(
            '{"schema_version":1,"provision_id":"test-provision-id","state":"running","phase":"x","exit_code":"0"}',
            PROVISION_ID,
        )
        self.assertEqual(status.exit_code, "0")

    def test_succeeded_with_nonzero_exit_code(self):
        self._assert_raises('{"schema_version":1,"provision_id":"test-provision-id","state":"succeeded","phase":"x","exit_code":1}')


class TestIsTransportStderr(unittest.TestCase):
    def _assert(self, stderr, expected):
        self.assertEqual(provision._is_transport_stderr(stderr), expected)

    def test_empty(self):
        self._assert("", False)

    def test_connection_refused(self):
        self._assert("connection refused", True)

    def test_websocket_bad_handshake(self):
        self._assert("websocket: bad handshake", True)

    def test_vm_agent_not_running(self):
        self._assert("VM agent isn't currently running", True)

    def test_not_connected(self):
        self._assert("not connected", True)

    def test_permission_denied(self):
        self._assert("permission denied", False)

    def test_invalid_option(self):
        self._assert("invalid option", False)


class TestPullStatus(unittest.TestCase):
    def setUp(self):
        self.cmds = make_cmds()

    def _run(self, **overrides):
        return provision._pull_status(self.cmds, "test", PROVISION_ID)

    def test_valid_status(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout='{"schema_version":1,"provision_id":"test-provision-id","state":"running","phase":"apt-update","pid":1}',
        )
        status = self._run()
        self.assertIsInstance(status, provision.ProvisionStatus)
        self.assertEqual(status.state, "running")
        self.assertEqual(status.phase, "apt-update")

    def test_not_found(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1, stderr="Error: Not Found",
        )
        with self.assertRaises(provision.ProvisionStatusUnavailable):
            self._run()

    def test_empty_stdout(self):
        self.cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="")
        with self.assertRaises(provision.ProvisionStatusUnavailable):
            self._run()

    def test_agent_not_running_transport(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1, stderr="VM agent isn't currently running",
        )
        with self.assertRaises(provision.ProvisionTransportError):
            self._run()

    def test_websocket_bad_handshake_transport(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1, stderr="websocket: bad handshake",
        )
        with self.assertRaises(provision.ProvisionTransportError):
            self._run()

    def test_connection_refused_transport(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1, stderr="connection refused",
        )
        with self.assertRaises(provision.ProvisionTransportError):
            self._run()

    def test_not_connected_transport(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1, stderr="not connected",
        )
        with self.assertRaises(provision.ProvisionTransportError):
            self._run()

    def test_timeout_expired_transport(self):
        self.cmds.incus.run.side_effect = subprocess.TimeoutExpired(
            ["incus", "file", "pull"], timeout=30, output=b"",
        )
        with self.assertRaises(provision.ProvisionTransportError):
            self._run()

    def test_permission_denied_fatal(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1, stderr="permission denied",
        )
        with self.assertRaises(provision.ProvisionProtocolError):
            self._run()

    def test_malformed_json_fatal(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0, stdout="{bad",
        )
        with self.assertRaises(provision.ProvisionProtocolError):
            self._run()

    def test_stale_provision_id_fatal(self):
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout='{"schema_version":1,"provision_id":"other","state":"running","phase":"x","pid":1}',
        )
        with self.assertRaises(provision.ProvisionProtocolError):
            self._run()

    def test_invocation(self):
        self.cmds.incus.run.side_effect = subprocess.TimeoutExpired(
            ["incus", "file", "pull"], timeout=30, output=b"",
        )
        try:
            self._run()
        except provision.ProvisionTransportError:
            pass
        self.cmds.incus.run.assert_called_once_with(
            ["file", "pull", "test/var/lib/kive-provision/status.json", "-"],
            check=False, capture_output=True, timeout=provision.PROBE_TIMEOUT,
        )


class TestMaybeProvisionInstance(unittest.TestCase):
    def setUp(self):
        self.cmds = make_cmds()

    def test_successful_lifecycle(self):
        clock = FakeClock()
        with mock.patch.object(provision, "_pull_status") as mock_pull:
            mock_pull.side_effect = [
                make_status("starting", "starting"),
                make_status("running", "apt-update"),
                make_status("running", "ansible"),
                make_status("running", "slurm-readiness"),
                make_status("running", "slurm-job"),
                make_status("succeeded", "complete", exit_code=0),
            ]
            with mock.patch.object(provision, "_stop_service") as mock_stop:
                with mock.patch.object(provision, "_cloud_init_diagnostics") as mock_diag:
                    with mock.patch("time.monotonic", clock.monotonic):
                        with mock.patch("time.sleep", clock.sleep):
                            provision.maybe_provision_instance(
                                self.cmds, "test", "vm",
                                provision=True, provision_id=PROVISION_ID, timeout=600,
                            )
        mock_stop.assert_not_called()
        mock_diag.assert_not_called()

    def test_failed_lifecycle(self):
        clock = FakeClock()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0, stdout="log content: Ansible failed",
        )
        with mock.patch.object(provision, "_pull_status") as mock_pull:
            mock_pull.side_effect = [
                make_status("running", "ansible"),
                make_status("failed", "ansible", exit_code=1, message="Ansible failed"),
            ]
            with mock.patch.object(provision, "_cloud_init_diagnostics", return_value="diag"):
                with mock.patch("time.monotonic", clock.monotonic):
                    with mock.patch("time.sleep", clock.sleep):
                        with self.assertRaises(RuntimeError) as ctx:
                            provision.maybe_provision_instance(
                                self.cmds, "test", "vm",
                                provision=True, provision_id=PROVISION_ID, timeout=600,
                            )
        self.assertIn("Ansible failed", str(ctx.exception))

    def test_protocol_failure(self):
        clock = FakeClock()
        with mock.patch.object(provision, "_pull_status") as mock_pull:
            mock_pull.side_effect = provision.ProvisionProtocolError("stale provision id")
            with mock.patch.object(provision, "_stop_service") as mock_stop:
                with mock.patch.object(provision, "_cloud_init_diagnostics", return_value="diag"):
                    with mock.patch("time.monotonic", clock.monotonic):
                        with mock.patch("time.sleep", clock.sleep):
                            with self.assertRaises(RuntimeError):
                                provision.maybe_provision_instance(
                                    self.cmds, "test", "vm",
                                    provision=True, provision_id=PROVISION_ID, timeout=600,
                                )
        mock_stop.assert_called_once()
        self.assertEqual(clock.now, 0.0)

    def test_repeated_transport_failures(self):
        clock = FakeClock()
        with mock.patch.object(provision, "_pull_status") as mock_pull:
            mock_pull.side_effect = provision.ProvisionTransportError("agent unavailable")
            with mock.patch.object(provision, "_cloud_init_diagnostics", return_value="diag"):
                with mock.patch("time.monotonic", clock.monotonic):
                    with mock.patch("time.sleep", clock.sleep):
                        with self.assertRaises(RuntimeError) as ctx:
                            provision.maybe_provision_instance(
                                self.cmds, "test", "vm",
                                provision=True, provision_id=PROVISION_ID, timeout=600,
                            )
        self.assertIn("5", str(ctx.exception))

    def test_startup_deadline(self):
        clock = FakeClock()
        with mock.patch.object(provision, "_pull_status") as mock_pull:
            mock_pull.side_effect = provision.ProvisionStatusUnavailable("not yet")
            with mock.patch.object(provision, "_stop_service") as mock_stop:
                with mock.patch.object(provision, "_cloud_init_diagnostics", return_value="diag"):
                    with mock.patch("time.monotonic", clock.monotonic):
                        with mock.patch("time.sleep", clock.sleep):
                            with self.assertRaises(RuntimeError) as ctx:
                                provision.maybe_provision_instance(
                                    self.cmds, "test", "vm",
                                    provision=True, provision_id=PROVISION_ID, timeout=600,
                                )
        mock_stop.assert_called_once()
        self.assertIn(str(provision.STATUS_STARTUP_TIMEOUT), str(ctx.exception))
        self.assertIn(PROVISION_ID, str(ctx.exception))

    def test_overall_timeout(self):
        clock = FakeClock()
        with mock.patch.object(provision, "_pull_status") as mock_pull:
            mock_pull.return_value = make_status("running", "ansible")
            with mock.patch.object(provision, "_stop_service") as mock_stop:
                with mock.patch.object(provision, "_cloud_init_diagnostics", return_value="diag"):
                    with mock.patch("time.monotonic", clock.monotonic):
                        with mock.patch("time.sleep", clock.sleep):
                            with self.assertRaises(RuntimeError) as ctx:
                                provision.maybe_provision_instance(
                                    self.cmds, "test", "vm",
                                    provision=True, provision_id=PROVISION_ID, timeout=25,
                                )
        mock_stop.assert_called_once()
        self.assertIn("25s", str(ctx.exception))
