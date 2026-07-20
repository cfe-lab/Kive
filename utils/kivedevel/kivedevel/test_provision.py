"""Tests for provisioning state machine, marker probes, and transport errors."""

from __future__ import annotations

import unittest
from unittest import mock


from kivedevel._test_helpers import (
    MockRunResult,
    make_cmds,
)



_PROVISION_MODULE = "kivedevel.build_vm.provision"


class TestMaybeProvisionInstance(unittest.TestCase):
    """Provisioning state machine: done, failed, started timeout, transport failures."""

    def setUp(self):
        self.cmds = make_cmds()

    def _patch_probe(self, prov, marker, error=None):
        """Patch the appropriate probe function for the instance type."""
        prov._probe_markers_via_file_pull = mock.Mock(return_value=(marker, error))

    def test_returns_on_done_marker(self):
        prov = _import_provision()
        self._patch_probe(prov, "done")
        prov.maybe_provision_instance(self.cmds, "test", "vm", provision=True, timeout=10)

    def test_raises_on_failed_marker(self):
        prov = _import_provision()
        prov._probe_markers_via_file_pull = mock.Mock()
        prov._probe_markers_via_file_pull.side_effect = [("failed", None)]
        import time as _time
        with mock.patch.object(_time, "sleep"):
            with mock.patch.object(prov, "_cloud_init_diagnostics", return_value=""):
                with self.assertRaises(RuntimeError):
                    prov.maybe_provision_instance(self.cmds, "test", "vm", provision=True, timeout=10)

    def test_fails_on_stuck_started_marker(self):
        prov = _import_provision()
        self._patch_probe(prov, "started")
        monotonic_vals = iter([100000, 100000, 200000])
        with mock.patch("time.monotonic", side_effect=lambda: next(monotonic_vals)):
            with mock.patch("time.sleep"):
                with mock.patch.object(prov, "_cloud_init_diagnostics", return_value=""):
                    with self.assertRaises(RuntimeError):
                        prov.maybe_provision_instance(self.cmds, "test", "vm", provision=True, timeout=10)

    def test_continues_until_done_marker(self):
        prov = _import_provision()
        prov._probe_markers_via_file_pull = mock.Mock()
        prov._probe_markers_via_file_pull.side_effect = [("done", None)]
        prov.maybe_provision_instance(self.cmds, "test", "vm", provision=True, timeout=10)

    def test_skips_when_provision_false(self):
        prov = _import_provision()
        prov.maybe_provision_instance(self.cmds, "test", "vm", provision=False, timeout=10)


class TestProbeMarkersViaFilePull(unittest.TestCase):
    """File-pull based marker probing (VM mode)."""

    def setUp(self):
        self.cmds = make_cmds()

    def test_returns_done_when_file_pull_succeeds(self):
        prov = _import_provision()
        prov._pull_file = mock.Mock(return_value=(True, "done"))
        marker, error = prov._probe_markers_via_file_pull(self.cmds, "test")
        self.assertEqual(marker, "done")
        self.assertIsNone(error)

    def test_returns_none_when_none_exist(self):
        prov = _import_provision()
        prov._pull_file = mock.Mock(return_value=(False, "not found"))
        marker, error = prov._probe_markers_via_file_pull(self.cmds, "test")
        self.assertEqual(marker, "none")

    def test_returns_none_with_transport_error(self):
        prov = _import_provision()
        prov._pull_file = mock.Mock(return_value=(False, "VM agent isn't currently running"))
        marker, error = prov._probe_markers_via_file_pull(self.cmds, "test")
        self.assertIsNone(marker)
        self.assertIsNotNone(error)


class TestIsTransportFailure(unittest.TestCase):
    """Transport error classification."""

    def _assert_transport(self, stderr: str, expected: bool):
        prov = _import_provision()
        result = MockRunResult(returncode=1 if expected else 0, stderr=stderr)
        self.assertEqual(prov._is_transport_failure(result), expected)

    def test_connection_refused_is_transport_failure(self):
        self._assert_transport("connection refused", True)

    def test_return_code_zero_is_not_failure(self):
        self._assert_transport("connection refused", False)

    def test_guest_command_error_not_transport_failure(self):
        self._assert_transport("some script error", False)

    def test_websocket_bad_handshake_is_transport(self):
        self._assert_transport("websocket: bad handshake", True)

    def test_vm_agent_not_running_is_transport(self):
        self._assert_transport("VM agent isn't currently running", True)

    def test_not_connected_is_transport(self):
        self._assert_transport("not connected", True)

    def test_empty_stderr_is_not_transport(self):
        self._assert_transport("", False)


class TestIsTransportStderr(unittest.TestCase):
    """Transport error classification by stderr text."""

    def _assert(self, stderr: str, expected: bool):
        prov = _import_provision()
        self.assertEqual(prov._is_transport_stderr(stderr), expected)

    def test_empty_is_not_transport(self):
        self._assert("", False)

    def test_connection_refused_is_transport(self):
        self._assert("connection refused", True)

    def test_websocket_bad_handshake_is_transport(self):
        self._assert("websocket: bad handshake", True)

    def test_vm_agent_not_running_is_transport(self):
        self._assert("VM agent isn't currently running", True)

    def test_not_connected_is_transport(self):
        self._assert("not connected", True)

    def test_guest_error_not_transport(self):
        self._assert("some script error", False)


class TestPullFile(unittest.TestCase):
    def test_returns_stdout_on_success(self):
        prov = _import_provision()
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="file content")
        ok, body = prov._pull_file(cmds, "test", "/some/path")
        self.assertTrue(ok)
        self.assertEqual(body, "file content")

    def test_returns_stderr_on_not_found(self):
        prov = _import_provision()
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stderr="not found")
        ok, body = prov._pull_file(cmds, "test", "/nonexistent")
        self.assertFalse(ok)
        self.assertIn("not found", body)


def _import_provision():
    import importlib
    from kivedevel.build_vm import provision as p
    importlib.reload(p)
    return p
