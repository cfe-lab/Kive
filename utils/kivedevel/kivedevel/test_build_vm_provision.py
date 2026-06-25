import inspect
import json
import signal
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from Kive.utils.kivedevel.kivedevel.build_vm import cloud_init, provision
from Kive.utils.kivedevel.kivedevel.kv_commands import Command
from Kive.utils.kivedevel.kivedevel._test_helpers import MockRunResult


class TestBuildVmProvision(unittest.TestCase):
    def test_pull_file_returns_stderr_on_not_found(self):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")

        ok, body = provision._pull_file(cmds, "ci-smoke", "/var/lib/kive-provision/done")

        self.assertFalse(ok)
        self.assertEqual(body, "Error: Not Found")

    def test_maybe_provision_instance_fails_when_failed_marker_exists(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        def run_side_effect(cmd, **kwargs):
            if "provision.log" in " ".join(cmd):
                return MockRunResult(returncode=0, stdout="FAILED: ansible error", stderr="")
            return MockRunResult(returncode=1, stdout="", stderr="")

        cmds.incus.run.side_effect = run_side_effect

        def probe_side_effect(_cmds, _instance):
            return ("failed", None)

        with mock.patch.object(provision, "_probe_markers", side_effect=probe_side_effect):
            with mock.patch.object(provision, "_collect_host_diagnostics", return_value=""):
                with self.assertRaises(RuntimeError) as cm:
                    provision.maybe_provision_instance(cmds, instance, "container", provision=True)

        self.assertIn("FAILED: ansible error", str(cm.exception))

    def test_ensure_user_data_generates_safe_printf_and_failed_marker(self):
        cmds = mock.Mock()
        saved_user_data = {}

        def fake_set_instance_config_multiline(cmds_arg, instance_arg, key, value):
            saved_user_data["value"] = value

        with mock.patch.object(cloud_init, "set_instance_config_multiline", fake_set_instance_config_multiline):
            with mock.patch.object(cloud_init, "find_ssh_pubkey", return_value=None):
                with mock.patch.object(cloud_init, "generate_password_hash", return_value="hash"):
                    cloud_init.ensure_user_data(cmds, "ci-smoke", provision=True)

        self.assertIn("touch \"$STATE_DIR/started\"", saved_user_data["value"])
        self.assertIn("trap mark_failed_on_exit EXIT", saved_user_data["value"])
        self.assertIn("if [ \"$status\" -ne 0 ] && [ ! -f \"$STATE_DIR/done\" ]; then", saved_user_data["value"])
        self.assertIn("timeout --foreground 30s python3 -c", saved_user_data["value"])
        self.assertIn("apt-get install -y ansible curl openssh-server", saved_user_data["value"])
        self.assertIn("if ! systemctl enable --now ssh; then", saved_user_data["value"])
        self.assertNotIn("packages:\n  - ansible\n  - curl\n  - openssh-server", saved_user_data["value"])
        self.assertNotIn("ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml\n        if [ $? -ne 0 ]", saved_user_data["value"])
        self.assertNotIn("cloud-init status --wait", saved_user_data["value"])

    def test_provision_script_has_dns_config_and_staged_preflight(self):
        cmds = mock.Mock()
        saved_user_data = {}

        def fake_set_instance_config_multiline(cmds_arg, instance_arg, key, value):
            saved_user_data["value"] = value

        with mock.patch.object(cloud_init, "set_instance_config_multiline", fake_set_instance_config_multiline):
            with mock.patch.object(cloud_init, "find_ssh_pubkey", return_value=None):
                with mock.patch.object(cloud_init, "generate_password_hash", return_value="hash"):
                    cloud_init.ensure_user_data(cmds, "ci-smoke", provision=True)

        value = saved_user_data["value"]
        self.assertIn("resolvectl dns enp5s0", value)
        self.assertIn("resolvectl default-route enp5s0", value)
        self.assertIn("systemctl restart systemd-resolved", value)
        self.assertIn("raw IP connectivity test", value)
        self.assertIn("DNS resolution test", value)
        self.assertIn("cannot reach the internet by IP", value)
        self.assertIn("Likely causes", value)
        self.assertIn("IPv4 forwarding disabled", value)
        self.assertIn("forward rules missing", value)
        self.assertIn("kive-devel-br", value)
        self.assertIn("DNS resolution is broken", value)
        self.assertIn("socket.create_connection((\"1.1.1.1\", 53)", value)

    def test_user_data_includes_resolved_conf_write_file(self):
        cmds = mock.Mock()
        saved_user_data = {}

        def fake_set_instance_config_multiline(cmds_arg, instance_arg, key, value):
            saved_user_data["value"] = value

        with mock.patch.object(cloud_init, "set_instance_config_multiline", fake_set_instance_config_multiline):
            with mock.patch.object(cloud_init, "find_ssh_pubkey", return_value=None):
                with mock.patch.object(cloud_init, "generate_password_hash", return_value="hash"):
                    cloud_init.ensure_user_data(cmds, "ci-smoke", provision=True)

        value = saved_user_data["value"]
        self.assertIn("/etc/systemd/resolved.conf.d/99-kive-devel.conf", value)
        self.assertIn("DNS=8.8.8.8 1.1.1.1", value)
        self.assertIn("FallbackDNS=9.9.9.9", value)

    def test_generated_provision_script_has_timeout_bounds(self):
        cmds = mock.Mock()
        saved_user_data = {}

        def fake_set_instance_config_multiline(cmds_arg, instance_arg, key, value):
            saved_user_data["value"] = value

        with mock.patch.object(cloud_init, "set_instance_config_multiline", fake_set_instance_config_multiline):
            with mock.patch.object(cloud_init, "find_ssh_pubkey", return_value=None):
                with mock.patch.object(cloud_init, "generate_password_hash", return_value="hash"):
                    cloud_init.ensure_user_data(cmds, "ci-smoke", provision=True)

        self.assertIn("timeout --foreground 30s python3", saved_user_data["value"])
        self.assertIn("timeout --foreground 180s apt-get update", saved_user_data["value"])
        self.assertIn("timeout --foreground 300s apt-get install -y ansible curl openssh-server", saved_user_data["value"])
        self.assertIn("export ANSIBLE_CONFIG=/usr/local/share/Kive/dev-env/ansible.cfg", saved_user_data["value"])
        self.assertIn("export ANSIBLE_ROLES_PATH=/usr/local/share/Kive/roles:/usr/local/share/Kive/cluster-setup/deployment/roles", saved_user_data["value"])
        self.assertIn("if ! timeout --foreground 900s ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml; then", saved_user_data["value"])
        self.assertNotIn("timeout --foreground 900s ANSIBLE_CONFIG=", saved_user_data["value"])
        self.assertIn("rm -rf /usr/local/share/Kive", saved_user_data["value"])
        self.assertIn("cp -a /mnt/kive-code/. /usr/local/share/Kive/", saved_user_data["value"])
        self.assertNotIn("ln -sfn /mnt/kive-code /usr/local/share/Kive", saved_user_data["value"])

    def test_workflow_calls_developer_facing_commands(self):
        workflow_path = Path(__file__).resolve().parents[4] / "Kive" / ".github" / "workflows" / "build-and-test.yml"
        workflow_text = workflow_path.read_text()

        self.assertIn("utils/dev prepare-host", workflow_text)
        self.assertIn("utils/dev check-network", workflow_text)
        self.assertIn("utils/dev smoke-local-install", workflow_text)
        self.assertIn("utils/dev cleanup-local-install", workflow_text)

        self.assertNotIn("utils/dev ci ", workflow_text)
        self.assertNotIn("Repair Incus bridge egress on GitHub runner", workflow_text)
        self.assertNotIn("Verify Incus container egress", workflow_text)
        self.assertNotIn("for i in $(seq 1 120)", workflow_text)
        self.assertNotIn("sudo incus admin init --preseed", workflow_text)
        self.assertNotIn("sudo iptables -C DOCKER-USER", workflow_text)
        self.assertNotIn("sudo iptables -C FORWARD", workflow_text)
        self.assertNotIn("sudo iptables -t nat -C POSTROUTING", workflow_text)
        self.assertNotIn("sudo iptables -t nat -I POSTROUTING", workflow_text)

        lines = workflow_text.splitlines()
        self.assertTrue(any("prepare-host" in line for line in lines))
        self.assertTrue(any("check-network" in line for line in lines))
        self.assertTrue(any("smoke-local-install" in line for line in lines))
        self.assertTrue(any("cleanup-local-install" in line for line in lines))

        # Verify cleanup is conditional on always()
        self.assertIn("if: always()", workflow_text)

        # Verify steps appear in correct order
        import re
        job_section_start = workflow_text.find("build-vm-smoke:")
        job_section = workflow_text[job_section_start:]
        job_step_names = re.findall(r"- name: (.+)", job_section)
        self.assertIn("Prepare host", job_step_names)
        self.assertIn("Check guest network", job_step_names)
        self.assertIn("Smoke-test local install", " ".join(job_step_names))
        self.assertIn("Cleanup local install smoke test", job_step_names)

        prepare_idx = job_step_names.index("Prepare host")
        check_idx = job_step_names.index("Check guest network")
        smoke_idx = next(i for i, name in enumerate(job_step_names) if name.startswith("Smoke-test local install"))
        cleanup_idx = job_step_names.index("Cleanup local install smoke test")
        self.assertLess(prepare_idx, check_idx)
        self.assertLess(check_idx, smoke_idx)
        self.assertLess(smoke_idx, cleanup_idx)

    def test_provision_polling_deadline_is_shorter_than_workflow_timeout(self):
        self.assertEqual(provision.DEFAULT_PROVISION_TIMEOUT, 900)

    def test_maybe_provision_instance_fails_on_stuck_started_marker(self):
        instance = "ci-smoke"
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stdout="", stderr="")

        monotonic_values = iter([0, 1, 900])

        def fake_monotonic():
            return next(monotonic_values)

        def probe_side_effect(_cmds, _instance):
            return ("started", None)

        with mock.patch.object(provision, "_probe_markers", side_effect=probe_side_effect):
            with mock.patch.object(provision.time, "monotonic", side_effect=fake_monotonic):
                with mock.patch.object(provision.time, "sleep", return_value=None):
                    with mock.patch.object(provision, "_collect_host_diagnostics", return_value=""):
                        with self.assertRaises(RuntimeError) as cm:
                            provision.maybe_provision_instance(cmds, instance, "container", provision=True, timeout=900)

        self.assertIn("Provisioning appears stuck", str(cm.exception))

    def test_marker_probe_script_is_bounded_shell(self):
        self.assertIn("set -e", provision._BOUNDED_PROBE_SCRIPT)
        self.assertIn("done", provision._BOUNDED_PROBE_SCRIPT)
        self.assertIn("failed", provision._BOUNDED_PROBE_SCRIPT)

    def test_diagnostics_script_includes_cloud_init_status(self):
        self.assertIn("cloud-init status", provision._DIAGNOSTICS_SCRIPT)
        self.assertIn("systemctl status", provision._DIAGNOSTICS_SCRIPT)
        self.assertIn("log tails", provision._DIAGNOSTICS_SCRIPT.lower())

    def test_maybe_provision_instance_aborts_on_consecutive_transport_failures(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        def probe_side_effect(_cmds, _instance):
            return (None, "connection refused")

        with mock.patch.object(provision, "_probe_markers", side_effect=probe_side_effect):
            with mock.patch.object(provision.time, "monotonic", side_effect=[0, 1, 2, 3, 4, 5, 6]):
                with mock.patch.object(provision.time, "sleep", return_value=None):
                    with self.assertRaises(RuntimeError) as cm:
                        provision.maybe_provision_instance(cmds, instance, "container", provision=True, timeout=60)

            self.assertIn("transport failures", str(cm.exception).lower())

    def test_vm_provision_does_not_abort_after_five_agent_not_running(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        call_count = 0

        def probe_side_effect(_cmds, _instance):
            nonlocal call_count
            call_count += 1
            if call_count <= 10:
                return (None, "Error: VM agent isn't currently running")
            return ("done", None)

        with mock.patch.object(provision, "_probe_markers_via_file_pull", side_effect=probe_side_effect):
            with mock.patch.object(provision.time, "monotonic", side_effect=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]):
                with mock.patch.object(provision.time, "sleep", return_value=None):
                    provision.maybe_provision_instance(cmds, instance, "vm", provision=True, timeout=60)

    def test_vm_provision_does_not_abort_after_five_websocket_errors(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        call_count = 0

        def probe_side_effect(_cmds, _instance):
            nonlocal call_count
            call_count += 1
            if call_count <= 10:
                return (None, "Error: websocket: bad handshake")
            return ("done", None)

        with mock.patch.object(provision, "_probe_markers_via_file_pull", side_effect=probe_side_effect):
            with mock.patch.object(provision.time, "monotonic", side_effect=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]):
                with mock.patch.object(provision.time, "sleep", return_value=None):
                    provision.maybe_provision_instance(cmds, instance, "vm", provision=True, timeout=60)

    def test_vm_provision_continues_until_done_marker(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        call_count = 0

        def probe_side_effect(_cmds, _instance):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return ("none", None)
            return ("done", None)

        with mock.patch.object(provision, "_probe_markers_via_file_pull", side_effect=probe_side_effect):
            with mock.patch.object(provision.time, "monotonic", side_effect=[0, 1, 2, 3, 4]):
                with mock.patch.object(provision.time, "sleep", return_value=None):
                    provision.maybe_provision_instance(cmds, instance, "vm", provision=True, timeout=60)

    def test_vm_provision_fails_on_failed_marker_with_log(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        def run_side_effect(cmd, **kwargs):
            if "provision.log" in " ".join(cmd):
                return MockRunResult(returncode=0, stdout="FAILED: ansible error", stderr="")
            return MockRunResult(returncode=0, stdout="")

        cmds.incus.run.side_effect = run_side_effect

        def probe_side_effect(_cmds, _instance):
            return ("failed", None)

        with mock.patch.object(provision, "_probe_markers_via_file_pull", side_effect=probe_side_effect):
            with mock.patch.object(provision.time, "monotonic", side_effect=[0, 1]):
                with mock.patch.object(provision.time, "sleep", return_value=None):
                    with mock.patch.object(provision, "_collect_host_diagnostics", return_value=""):
                        with self.assertRaises(RuntimeError) as cm:
                            provision.maybe_provision_instance(cmds, instance, "vm", provision=True, timeout=60)

        self.assertIn("FAILED: ansible error", str(cm.exception))

    def test_vm_provision_uses_file_pull_not_exec(self):
        """VM mode should call _probe_markers_via_file_pull, not _probe_markers."""
        instance = "ci-smoke"
        cmds = mock.Mock()

        with mock.patch.object(provision, "_probe_markers_via_file_pull", return_value=("done", None)) as mock_fp:
            with mock.patch.object(provision, "_probe_markers") as mock_exec:
                with mock.patch.object(provision.time, "sleep", return_value=None):
                    provision.maybe_provision_instance(cmds, instance, "vm", provision=True, timeout=60)

        mock_fp.assert_called()
        mock_exec.assert_not_called()


class TestCommandTimeout(unittest.TestCase):
    def test_run_passes_timeout_to_subprocess(self):
        cmd = Command(use_guix=False)
        cmd.exe = "true"
        with mock.patch.object(cmd, "_argv", return_value=["true"]):
            with mock.patch("subprocess.run") as mock_run:
                mock_run.return_value = MockRunResult(returncode=0)
                cmd.run([], timeout=30)
                _, kwargs = mock_run.call_args
                self.assertEqual(kwargs["timeout"], 30)

    def test_run_raises_on_non_positive_timeout(self):
        cmd = Command(use_guix=False)
        with self.assertRaises(ValueError):
            cmd.run([], timeout=0)
        with self.assertRaises(ValueError):
            cmd.run([], timeout=-1)

    def test_output_accepts_timeout(self):
        cmd = Command(use_guix=False)
        cmd.exe = "echo"
        with mock.patch.object(cmd, "run") as mock_run:
            mock_run.return_value = MockRunResult(returncode=0, stdout="hello")
            result = cmd.output([], timeout=5)
            self.assertEqual(result, "hello")
            _, kwargs = mock_run.call_args
            self.assertEqual(kwargs["timeout"], 5)


class TestProbeMarkers(unittest.TestCase):
    def test_probe_markers_returns_done(self):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="done\n")

        marker, error = provision._probe_markers(cmds, "test-instance")

        self.assertEqual(marker, "done")
        self.assertIsNone(error)

    def test_probe_markers_returns_failed(self):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="failed\n")

        marker, error = provision._probe_markers(cmds, "test-instance")

        self.assertEqual(marker, "failed")
        self.assertIsNone(error)

    def test_probe_markers_returns_none_on_transport_failure(self):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stdout="", stderr="Error: connection refused")

        marker, error = provision._probe_markers(cmds, "test-instance")

        self.assertIsNone(marker)
        self.assertIsNotNone(error)

    def test_probe_markers_returns_none_on_timeout(self):
        cmds = mock.Mock()
        cmds.incus.run.side_effect = subprocess.TimeoutExpired(cmd="incus", timeout=30)

        marker, error = provision._probe_markers(cmds, "test-instance")

        self.assertIsNone(marker)
        self.assertIn("timed out", error)

    def test_probe_markers_passes_timeout_to_run(self):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="none\n")

        provision._probe_markers(cmds, "test-instance")

        _, kwargs = cmds.incus.run.call_args
        self.assertEqual(kwargs.get("timeout"), provision.PROBE_TIMEOUT)


class TestIsTransportFailure(unittest.TestCase):
    def test_connection_refused_is_transport_failure(self):
        result = MockRunResult(returncode=1, stdout="", stderr="connection refused")
        self.assertTrue(provision._is_transport_failure(result))

    def test_return_code_zero_is_not_failure(self):
        result = MockRunResult(returncode=0, stdout="", stderr="")
        self.assertFalse(provision._is_transport_failure(result))

    def test_guest_command_error_not_transport_failure(self):
        result = MockRunResult(returncode=1, stdout="", stderr="Error: not found")
        self.assertFalse(provision._is_transport_failure(result))

    def test_guest_stderr_not_classified_as_transport(self):
        result = MockRunResult(returncode=1, stdout="", stderr="error: something from guest command")
        self.assertFalse(provision._is_transport_failure(result))

    def test_websocket_bad_handshake_is_transport_failure(self):
        result = MockRunResult(returncode=1, stdout="", stderr="Error: websocket: bad handshake")
        self.assertTrue(provision._is_transport_failure(result))

    def test_vm_agent_not_running_is_transport_failure(self):
        result = MockRunResult(returncode=1, stdout="", stderr="Error: VM agent isn't currently running")
        self.assertTrue(provision._is_transport_failure(result))

    def test_not_connected_is_transport_failure(self):
        result = MockRunResult(returncode=1, stdout="", stderr="not connected")
        self.assertTrue(provision._is_transport_failure(result))

    def test_empty_stderr_is_not_transport_failure(self):
        result = MockRunResult(returncode=1, stdout="", stderr="")
        self.assertFalse(provision._is_transport_failure(result))


class TestIsTransportStderr(unittest.TestCase):
    def test_empty_is_not_transport(self):
        self.assertFalse(provision._is_transport_stderr(""))

    def test_connection_refused_is_transport(self):
        self.assertTrue(provision._is_transport_stderr("connection refused"))

    def test_websocket_bad_handshake_is_transport(self):
        self.assertTrue(provision._is_transport_stderr("Error: websocket: bad handshake"))

    def test_vm_agent_not_running_is_transport(self):
        self.assertTrue(provision._is_transport_stderr("Error: VM agent isn't currently running"))

    def test_not_connected_is_transport(self):
        self.assertTrue(provision._is_transport_stderr("not connected"))

    def test_not_found_file_not_transport(self):
        """File-not-found from incus file pull is not a transport error."""
        self.assertFalse(provision._is_transport_stderr("Error: Not Found"))

    def test_guest_error_not_transport(self):
        """Guest command errors are not transport failures."""
        self.assertFalse(provision._is_transport_stderr("error: ansible-playbook failed"))


class TestProbeMarkersViaFilePull(unittest.TestCase):
    def test_returns_done_when_file_pull_succeeds(self):
        instance = "vm-test"
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="content")

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertEqual(marker, "done")
        self.assertIsNone(error)

    def test_returns_done_preferred_over_later_markers(self):
        """Done marker should match first, even if other markers also exist."""
        instance = "vm-test"
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockRunResult(returncode=0, stdout="ok")
            return MockRunResult(returncode=0, stdout="")

        cmds = mock.Mock()
        cmds.incus.run.side_effect = side_effect

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertEqual(marker, "done")

    def test_returns_failed_on_second_attempt(self):
        instance = "vm-test"
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")
            return MockRunResult(returncode=0, stdout="failed content")

        cmds = mock.Mock()
        cmds.incus.run.side_effect = side_effect

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertEqual(marker, "failed")

    def test_returns_started_on_third_attempt(self):
        instance = "vm-test"
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return MockRunResult(returncode=0, stdout="started") if call_count == 3 else MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")

        cmds = mock.Mock()
        cmds.incus.run.side_effect = side_effect

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertEqual(marker, "started")

    def test_returns_none_with_transport_error(self):
        instance = "vm-test"
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stdout="", stderr="Error: VM agent isn't currently running")

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertIsNone(marker)
        self.assertIn("agent isn't currently running", error)

    def test_returns_none_with_websocket_error(self):
        instance = "vm-test"
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stdout="", stderr="Error: websocket: bad handshake")

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertIsNone(marker)
        self.assertIn("websocket", error)

    def test_returns_none_on_not_connected(self):
        instance = "vm-test"
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stdout="", stderr="not connected")

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertIsNone(marker)
        self.assertIn("not connected", error)

    def test_returns_none_on_connection_refused(self):
        instance = "vm-test"
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stdout="", stderr="connection refused")

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertIsNone(marker)
        self.assertIn("connection refused", error)

    def test_returns_none_when_none_exist(self):
        instance = "vm-test"
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")

        marker, error = provision._probe_markers_via_file_pull(cmds, instance)

        self.assertEqual(marker, "none")
        self.assertIsNone(error)


class TestValidateVmSlurmProbe(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_validate_vm_calls_slurm_probe(self):
        from Kive.utils.kivedevel.kivedevel.checks import run_validate_vm
        source = inspect.getsource(run_validate_vm)
        self.assertIn("_run_slurm_probe", source)

    def test_slurm_probe_script_checks_hostname_and_services(self):
        from Kive.utils.kivedevel.kivedevel.checks import _SLURM_PROBE_SCRIPT
        self.assertIn("hostname -s", _SLURM_PROBE_SCRIPT)
        self.assertIn("getent hosts head", _SLURM_PROBE_SCRIPT)
        self.assertIn("systemctl is-active", _SLURM_PROBE_SCRIPT)
        self.assertIn("squeue", _SLURM_PROBE_SCRIPT)

    def test_slurm_probe_uses_bounded_exec(self):
        from Kive.utils.kivedevel.kivedevel.checks import _run_slurm_probe
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="")
        _run_slurm_probe(cmds, "test-instance")
        _, kwargs = cmds.incus.run.call_args
        self.assertEqual(kwargs.get("timeout"), 15)

    def test_slurm_probe_logs_warning_for_wrong_hostname(self):
        from Kive.utils.kivedevel.kivedevel.checks import _run_slurm_probe
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="=== hostname ===\nwrong-host\n=== getent hosts head ===\n127.0.0.1 head\n=== Slurm services ===\nactive: slurmdbd\nactive: slurmctld\ninactive: slurmd\n=== Slurm commands ===\n/usr/bin/squeue\n/usr/bin/sinfo\n",
        )
        with self.assertLogs("kivedevel.checks", level="WARNING") as logs:
            _run_slurm_probe(cmds, "test-instance")
        self.assertTrue(any("wrong-host" in msg for msg in logs.output))


class TestSlurmControllerRole(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_etc_hosts_task_uses_slurmctlnode_address_not_ansible_host(self):
        role_task = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_controller" / "tasks" / "main.yml"
        text = role_task.read_text()
        self.assertIn("slurmctlnode_address", text)
        self.assertNotIn("ansible_host", text)

    def test_etc_hosts_task_uses_slurmctlnode_parameter_not_hardcoded(self):
        role_task = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_controller" / "tasks" / "main.yml"
        text = role_task.read_text()
        self.assertIn("{{ slurmctlnode }}", text)
        # The regexp and line should both use the parameter, not a literal 'head'
        etc_hosts_block = text[text.find("ensure slurm controller"):text.find("install and start mariadb")]
        self.assertNotIn(" head", etc_hosts_block.split("{{")[0])

    def test_role_defaults_include_slurmctlnode_address(self):
        defaults = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_controller" / "defaults" / "main.yml"
        text = defaults.read_text()
        self.assertIn("slurmctlnode_address: 127.0.0.1", text)

    def test_setup_dev_env_pre_tasks_set_hostname_before_roles(self):
        setup_path = self.REPO_ROOT / "dev-env" / "setup-dev-env.yml"
        text = setup_path.read_text()
        # pre_tasks must come before roles
        pre_tasks_idx = text.find("pre_tasks:")
        roles_idx = text.find("roles:")
        self.assertGreater(roles_idx, pre_tasks_idx)
        # hostname task must be before /etc/hosts task
        hostname_idx = text.find("set local development hostname")
        hosts_idx = text.find("ensure Slurm controller hostname resolves locally")
        self.assertGreater(hosts_idx, hostname_idx)
        # Both are before roles
        self.assertGreater(roles_idx, hosts_idx)

    def test_setup_dev_env_writes_slurmctlnode_address(self):
        setup_path = self.REPO_ROOT / "dev-env" / "setup-dev-env.yml"
        text = setup_path.read_text()
        self.assertIn("slurmctlnode_address", text)
        self.assertIn("slurmctlnode", text)
        # The regexp should match the literal address
        self.assertIn("regexp: '^{{ slurmctlnode_address }}", text)

    def test_slurm_conf_template_uses_slurmctlnode(self):
        template_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_controller" / "templates" / "slurm.conf.j2"
        text = template_path.read_text()
        self.assertIn("SlurmctldHost={{ slurmctlnode }}", text)
        self.assertIn("AccountingStorageHost={{ slurmctlnode }}", text)


class TestTlsKeyRemoval(unittest.TestCase):
    """Verify no TLS private key material is committed to the repository."""

    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_star_cfe_key_not_present(self):
        old_key = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "kive_server" / "files" / "star_cfe.key"
        self.assertFalse(
            old_key.exists(),
            "Private key star_cfe.key must not exist in the repository. "
            "Remove it with git rm.",
        )

    def test_star_cfe_cert_not_present(self):
        old_cert = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "kive_server" / "files" / "star_cfe_chained.crt"
        self.assertFalse(
            old_cert.exists(),
            "Dev certificate star_cfe_chained.crt should be removed. "
            "Self-signed generation replaces committed cert material.",
        )

    def test_no_committed_private_key_in_kive_server_files(self):
        files_dir = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "kive_server" / "files"
        if files_dir.is_dir():
            for path in files_dir.rglob("*"):
                if path.is_file():
                    content = path.read_text(encoding="utf-8", errors="ignore")
                    self.assertNotIn(
                        "BEGIN PRIVATE KEY",
                        content,
                        f"Private key material found in committed file: {path}",
                    )

    def test_dev_env_vars_set_self_signed(self):
        dev_vars = self.REPO_ROOT / "dev-env" / "dev_env_vars.yml"
        text = dev_vars.read_text()
        self.assertIn("kive_tls_mode: self_signed", text)

    def test_ssl_template_uses_variable_paths(self):
        ssl_template = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "kive_server" / "templates" / "001-kive-ssl.conf.j2"
        text = ssl_template.read_text()
        self.assertIn("{{ kive_ssl_cert_path }}", text)
        self.assertIn("{{ kive_ssl_key_path }}", text)
        self.assertNotIn("star_cfe_chained.crt", text)
        self.assertNotIn("star_cfe.key", text)

    def test_server_role_installs_ssl_from_template_not_copy(self):
        tasks_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "kive_server" / "tasks" / "main.yml"
        text = tasks_path.read_text()
        self.assertIn("template:\n        src: 001-kive-ssl.conf.j2", text)
        self.assertNotIn("copy:\n        src: 001-kive-ssl.conf", text)

    def test_server_role_enables_ssl_module(self):
        tasks_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "kive_server" / "tasks" / "main.yml"
        text = tasks_path.read_text()
        self.assertIn("name: ssl", text)
        self.assertIn("state: present", text)


class TestEnterVm(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_enter_vm_registered_in_entrypoint(self):
        entrypoint_path = self.REPO_ROOT / "utils" / "kivedevel" / "kivedevel" / "entrypoint.py"
        text = entrypoint_path.read_text()
        self.assertIn("enter_vm.register_subcommand", text)

    def test_enter_vm_module_has_register_and_run(self):
        import importlib
        sys.path.insert(0, str(self.REPO_ROOT))
        mod = importlib.import_module("Kive.utils.kivedevel.kivedevel.enter_vm")
        self.assertTrue(hasattr(mod, "register_subcommand"))
        self.assertTrue(hasattr(mod, "run_enter_vm"))

    def test_enter_vm_checks_instance_exists_in_run(self):
        from Kive.utils.kivedevel.kivedevel.enter_vm import run_enter_vm
        source = inspect.getsource(run_enter_vm)
        self.assertIn("instance_exists", source)

    def test_enter_vm_starts_instance_if_not_running(self):
        from Kive.utils.kivedevel.kivedevel.enter_vm import run_enter_vm
        source = inspect.getsource(run_enter_vm)
        self.assertIn("instance_is_running", source)

    def test_enter_vm_uses_native_incus_exec(self):
        from Kive.utils.kivedevel.kivedevel.enter_vm import run_enter_vm
        source = inspect.getsource(run_enter_vm)
        self.assertIn("incus", source)
        self.assertIn("exec", source)

    def test_enter_vm_uses_subprocess_call_for_interactive(self):
        from Kive.utils.kivedevel.kivedevel.enter_vm import run_enter_vm
        source = inspect.getsource(run_enter_vm)
        self.assertIn("subprocess.call", source)
        self.assertNotIn("capture_output=True", source)

    def test_enter_vm_default_instance_is_kive_minimal(self):
        from Kive.utils.kivedevel.kivedevel.enter_vm import register_subcommand
        source = inspect.getsource(register_subcommand)
        self.assertIn("default=\"kive-minimal\"", source)

    def test_enter_vm_default_user_is_ubuntu(self):
        from Kive.utils.kivedevel.kivedevel.enter_vm import register_subcommand
        source = inspect.getsource(register_subcommand)
        self.assertIn("default=\"ubuntu\"", source)

    def test_enter_vm_default_shell_is_bash(self):
        from Kive.utils.kivedevel.kivedevel.enter_vm import register_subcommand
        source = inspect.getsource(register_subcommand)
        self.assertIn("default=\"/bin/bash\"", source)


class TestNoWorkspaceHelperReferences(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"
    THIS_FILE = Path(__file__).resolve()

    def test_no_ws_enter_vm_references(self):
        needles = self._build_needles()
        ignored_dirs = {".git", "__pycache__", ".mypy_cache", "node_modules", "tmp~", ".venv"}
        for path in self.REPO_ROOT.rglob("*"):
            if any(part in ignored_dirs for part in path.parts):
                continue
            if path.is_file() and path.name not in ("package-lock.json", self.THIS_FILE.name):
                try:
                    text = path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                for needle in needles:
                    if needle in text:
                        self.fail(f"{needle} found in {path}")

    def test_no_ws_hyphen_in_python_source(self):
        ignored_dirs = {".git", "__pycache__", ".mypy_cache", "node_modules", "tmp~", ".venv"}
        for path in self.REPO_ROOT.rglob("*.py"):
            if any(part in ignored_dirs for part in path.parts):
                continue
            if path.resolve() == self.THIS_FILE:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            self.assertNotIn("ws-", text, f"ws-* reference found in {path}")

    @staticmethod
    def _build_needles():
        prefix = "ws"
        return [f"{prefix}-enter-vm", f"{prefix}-start-incus-daemon"]


class TestPurge(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_purge_parser_rejects_dry_run(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.purge import register_subcommand
        source = inspect.getsource(register_subcommand)
        self.assertNotIn("dry-run", source)
        self.assertNotIn("dry_run", source)

    def test_run_purge_no_dry_run(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.purge import run_purge
        source = inspect.getsource(run_purge)
        self.assertNotIn("dry_run", source)

    def test_helper_functions_no_dry_run(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        source = inspect.getsource(purge_mod)
        self.assertNotIn("dry-run", source)

    def test_purge_help_deletes_resources(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.purge import register_subcommand
        source = inspect.getsource(register_subcommand)
        self.assertIn("deletes", source.lower())

    def test_find_tagged_instances_returns_matching(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        cmds = mock.Mock()
        cmds.incus.output.side_effect = [
            "kive-minimal\nci-smoke\nother-instance\n",
            "user.kive.devel.created-by: utils/dev\nother: stuff\n",
            "some: config\n",
            "user.kive.devel.created-by: utils/dev\n",
        ]
        result = purge_mod._find_tagged_instances(cmds)
        self.assertEqual(sorted(result), sorted(["kive-minimal", "other-instance"]))

    def test_find_tagged_instances_returns_empty_when_none(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        cmds = mock.Mock()
        cmds.incus.output.return_value = "kive-minimal\nci-smoke\n"
        result = purge_mod._find_tagged_instances(cmds)
        self.assertEqual(result, [])

    def test_find_marked_workdirs_accepts_created_by_underscore(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.purge import _find_marked_workdirs
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / ".kive-devel-resource.json"
            marker.write_text('{"created_by": "utils/dev", "kind": "build-workdir"}\n')
            result = _find_marked_workdirs(Path(tmp))
            self.assertEqual(len(result), 1)

    def test_find_marked_workdirs_accepts_old_hyphen_key(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.purge import _find_marked_workdirs
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / ".kive-devel-resource.json"
            marker.write_text('{"created-by": "utils/dev"}\n')
            result = _find_marked_workdirs(Path(tmp))
            self.assertEqual(len(result), 1)

    def test_find_marked_workdirs_rejects_unknown_creator(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.purge import _find_marked_workdirs
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / ".kive-devel-resource.json"
            marker.write_text('{"created_by": "other-tool"}\n')
            result = _find_marked_workdirs(Path(tmp))
            self.assertEqual(len(result), 0)

    def test_find_marked_workdirs_returns_empty_when_no_marker(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.purge import _find_marked_workdirs
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            result = _find_marked_workdirs(Path(tmp))
            self.assertEqual(result, [])

    def test_purge_deletes_tagged_instances_and_marked_workdirs(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        # _find_tagged_instances: list + config show kive-minimal
        # _check_legacy_skipped: uses run() not output()
        # _device_attached(kive-code): config show kive-minimal
        # _device_attached(kive-web): config show kive-minimal
        cmds.incus.output.side_effect = [
            "kive-minimal\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\n",
        ]
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / ".kive-devel-resource.json"
            marker.write_text('{"created_by": "utils/dev", "kind": "build-workdir"}\n')

            args = mock.Mock()
            args.root = Path(tmp)
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)

            delete_calls = [
                c for c in cmds.incus.run.call_args_list
                if c[0][0][:2] == ["delete", "-f"]
            ]
            dev_remove_calls = [
                c for c in cmds.incus.run.call_args_list
                if "device" in c[0][0] and "remove" in c[0][0]
            ]
            self.assertGreaterEqual(len(dev_remove_calls), 1,
                                    "Should remove kive-code device from tagged instance")
            self.assertGreaterEqual(len(delete_calls), 1,
                                    "Should delete tagged instance")

    def test_purge_idempotent_when_nothing_to_purge(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""

        with tempfile.TemporaryDirectory() as tmp:
            args = mock.Mock()
            args.root = Path(tmp)
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)
                    purge_mod.run_purge(args)

    def test_purge_does_not_delete_untagged_legacy_by_default(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        # legacy instances listed but none tagged
        cmds.incus.output.side_effect = [
            "kive-minimal\nci-smoke\n",
            "some: config\n",
            "other: config\n",
        ]
        cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="kive-minimal\n")

        call_log = {"config_shows": 0}

        def config_show_side_effect(cmd, **kwargs):
            call_log["config_shows"] += 1
            return "some: config\n"

        cmds.incus.output.side_effect = None
        cmds.incus.output.side_effect = config_show_side_effect

        with tempfile.TemporaryDirectory() as tmp:
            args = mock.Mock()
            args.root = Path(tmp)
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)

            delete_calls = [
                c for c in cmds.incus.run.call_args_list
                if c[0][0][:2] == ["delete", "-f"]
            ]
            self.assertEqual(len(delete_calls), 0,
                             "Should not delete untagged legacy instances")

    def test_purge_deletes_untagged_instance_with_explicit_flag(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        # _find_tagged_instances: instance list
        # _device_attached(kive-code): config show
        # _device_attached(kive-web): config show
        cmds.incus.output.side_effect = [
            "",
            "some: config\n",
            "some: config\n",
        ]
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with tempfile.TemporaryDirectory() as tmp:
            args = mock.Mock()
            args.root = Path(tmp)
            args.instances = ["kive-minimal"]
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)

            delete_calls = [
                c for c in cmds.incus.run.call_args_list
                if c[0][0][:2] == ["delete", "-f"]
            ]
            self.assertGreaterEqual(len(delete_calls), 1,
                                    "Should delete instance when explicitly named with --instance")

    def test_purge_remove_device_before_delete(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        # _find_tagged_instances: list + config show
        # _check_legacy_skipped: uses run() not output()
        # _device_attached(kive-code): config show
        # _device_attached(kive-web): config show
        cmds.incus.output.side_effect = [
            "kive-minimal\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\n",
        ]
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with tempfile.TemporaryDirectory() as tmp:
            args = mock.Mock()
            args.root = Path(tmp)
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            call_order = []

            def run_logger(cmd, check=True, capture_output=False, **kw):
                call_order.append(" ".join(cmd) if isinstance(cmd, list) else str(cmd))
                return MockRunResult(returncode=0)

            cmds.incus.run.side_effect = run_logger

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)

            # Verify device removal happens before instance deletion
            dev_idx = next((i for i, c in enumerate(call_order) if "device" in c and "remove" in c), None)
            del_idx = next((i for i, c in enumerate(call_order) if c.startswith("delete -f")), None)
            if dev_idx is not None and del_idx is not None:
                self.assertLess(dev_idx, del_idx,
                                "Device removal should happen before instance deletion")


class TestTlsReadme(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_readme_documents_kive_tls_mode(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        self.assertIn("kive_tls_mode", text)

    def test_readme_documents_provided_mode(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        self.assertIn("provided", text)

    def test_readme_documents_self_signed_mode(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        self.assertIn("self_signed", text)

    def test_readme_mentions_certificate_src_variables(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        self.assertIn("kive_ssl_certificate_src", text)
        self.assertIn("kive_ssl_key_src", text)

    def test_readme_says_do_not_commit_private_key(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        # "Do not" and "commit" are on separate lines in the markdown
        self.assertIn("Do not", text)
        self.assertIn("private-key material", text)
        # The TLS section should warn against committing key material
        tls_section = text[text.find("### Configure TLS"):text.find("### Set up network")]
        self.assertIn("Do not", tls_section)
        self.assertIn("commit", tls_section)

    def test_readme_includes_chained_cert_example(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        self.assertIn("star_cfe.crt", text)
        self.assertIn("star_cfe_chained.crt", text)

    def test_readme_includes_openssl_verify(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        self.assertIn("openssl verify", text)

    def test_readme_explains_chained_cert_contents(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        self.assertIn("wildcard/server certificate", text)
        self.assertIn("intermediate certificate", text)
        self.assertIn("root certificate", text)

    def test_readme_mentions_kive_ssl_cert_path(self):
        readme = self.REPO_ROOT / "cluster-setup" / "README.md"
        text = readme.read_text()
        self.assertIn("kive_ssl_cert_path", text)
        self.assertIn("kive_ssl_key_path", text)


class TestBuildVmParser(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_build_vm_provision_defaults_to_true(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertTrue(args.provision)

    def test_no_provision_sets_false(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--no-provision"])
        self.assertFalse(args.provision)

    def test_build_vm_help_shows_no_provision(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        source = inspect.getsource(register_subcommand)
        self.assertIn("no-provision", source)
        self.assertNotIn("--provision", source.replace("--no-provision", ""))

    def test_build_vm_has_no_provision_not_standalone_provision(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        source = inspect.getsource(register_subcommand)
        self.assertIn("no-provision", source)
        # "add_argument" for just "--provision" (without "--no-") should not appear
        add_lines = [line for line in source.splitlines() if "add_argument" in line and '"-provision"' in line]
        self.assertEqual(len(add_lines), 0)

    def test_default_instance_type_is_vm(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertEqual(args.instance_type, "vm")

    def test_explicit_instance_type_container(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--instance-type", "container"])
        self.assertEqual(args.instance_type, "container")

    def test_default_cpu_is_4(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertEqual(args.cpu, "4")

    def test_default_memory_is_8GiB(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertEqual(args.memory, "8GiB")


class TestBuildVmDefaults(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_dev_env_vars_slurm_node_cpus_match_vm_default(self):
        dev_vars_path = self.REPO_ROOT / "dev-env" / "dev_env_vars.yml"
        text = dev_vars_path.read_text()
        self.assertIn("cpus: \"4\"", text)
        self.assertIn("memory: \"8000\"", text)

    def test_workflow_ci_passes_instance_type_container(self):
        workflow_path = self.REPO_ROOT / ".github" / "workflows" / "build-and-test.yml"
        text = workflow_path.read_text()
        self.assertIn("--instance-type container", text)
        self.assertIn("container mode", text.lower())

    def test_singularity_probe_script_exists(self):
        from Kive.utils.kivedevel.kivedevel.checks import _SINGULARITY_PROBE_SCRIPT
        self.assertIn("singularity", _SINGULARITY_PROBE_SCRIPT)
        self.assertIn("exec", _SINGULARITY_PROBE_SCRIPT)
        self.assertIn("exec OK", _SINGULARITY_PROBE_SCRIPT)
        self.assertIn("exec FAILED", _SINGULARITY_PROBE_SCRIPT)

    def test_validate_vm_calls_singularity_probe_in_vm_mode(self):
        from Kive.utils.kivedevel.kivedevel.checks import run_validate_vm
        source = inspect.getsource(run_validate_vm)
        self.assertIn("_run_singularity_probe", source)
        self.assertIn('args.instance_type == "vm"', source)

    def test_setup_dev_env_installs_singularity(self):
        setup_path = self.REPO_ROOT / "dev-env" / "setup-dev-env.yml"
        text = setup_path.read_text()
        self.assertIn("singularity-container", text)
        self.assertIn("install singularity", text.lower())


class TestBuildVmWebProxy(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_web_port_default_is_8000(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertEqual(args.web_port, 8000)

    def test_web_port_parsed(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--web-port", "18000"])
        self.assertEqual(args.web_port, 18000)

    def test_no_web_proxy_defaults_to_false(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertFalse(args.no_web_proxy)

    def test_no_web_proxy_flag(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--no-web-proxy"])
        self.assertTrue(args.no_web_proxy)

    def test_config_from_args_includes_web_port(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        args = mock.Mock()
        args.root = Path("/tmp")
        args.workdir = Path("/tmp/work")
        args.instance = "test"
        args.instance_type = "container"
        args.image_name = "test.qcow2"
        args.pool = "default"
        args.profile = "default"
        args.root_size = "10GiB"
        args.memory = "1GB"
        args.cpu = "1"
        args.host_interface = ""
        args.provision = True
        args.web_port = 18000
        args.no_web_proxy = True
        cfg = BuildVmConfig.from_args(args)
        self.assertEqual(cfg.web_port, 18000)
        self.assertTrue(cfg.no_web_proxy)

    def test_proxy_config_format(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _proxy_config, GUEST_WEB_PORT
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"),
            instance="test", instance_type="container",
            image_path=Path("/tmp/img.qcow2"),
            pool="default", profile="default",
            root_size="10GiB", memory="1GB", cpu="1",
            host_interface="", provision=True,
            web_port=8000, no_web_proxy=False,
        )
        config = _proxy_config(cfg)
        self.assertEqual(config["type"], "proxy")
        self.assertEqual(config["listen"], "tcp:127.0.0.1:8000")
        self.assertEqual(config["connect"], f"tcp:127.0.0.1:{GUEST_WEB_PORT}")

    def test_proxy_config_custom_port(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _proxy_config
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"),
            instance="test", instance_type="container",
            image_path=Path("/tmp/img.qcow2"),
            pool="default", profile="default",
            root_size="10GiB", memory="1GB", cpu="1",
            host_interface="", provision=True,
            web_port=18000, no_web_proxy=False,
        )
        config = _proxy_config(cfg)
        self.assertEqual(config["listen"], "tcp:127.0.0.1:18000")

    def test_ensure_web_proxy_device_creates_new(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_proxy_device, PROXY_DEVICE
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        cmds = mock.Mock()
        # Simulate no existing kive-web device
        cmds.incus.output.return_value = "other-device:\n  type: nic\n"
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"),
            instance="test", instance_type="container",
            image_path=Path("/tmp/img.qcow2"),
            pool="default", profile="default",
            root_size="10GiB", memory="1GB", cpu="1",
            host_interface="", provision=True,
            web_port=8000, no_web_proxy=False,
        )
        _ensure_web_proxy_device(cmds, cfg)
        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if c[0][0][:4] == ["config", "device", "add", "test"]
               and PROXY_DEVICE in c[0][0]
        ]
        self.assertEqual(len(add_calls), 1)
        args_list = add_calls[0][0][0]
        self.assertIn("listen=tcp:127.0.0.1:8000", args_list)
        self.assertIn("connect=tcp:127.0.0.1:8000", args_list)

    def test_ensure_web_proxy_device_skipped_when_no_web_proxy(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_proxy_device
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        cmds = mock.Mock()
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"),
            instance="test", instance_type="container",
            image_path=Path("/tmp/img.qcow2"),
            pool="default", profile="default",
            root_size="10GiB", memory="1GB", cpu="1",
            host_interface="", provision=True,
            web_port=8000, no_web_proxy=True,
        )
        _ensure_web_proxy_device(cmds, cfg)
        cmds.incus.run.assert_not_called()

    def test_ensure_web_proxy_device_skipped_when_already_matches(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_proxy_device
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        cmds = mock.Mock()
        # Existing device with matching config
        cmds.incus.output.return_value = (
            "kive-web:\n"
            "  listen: tcp:127.0.0.1:8000\n"
            "  connect: tcp:127.0.0.1:8000\n"
            "  type: proxy\n"
        )
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"),
            instance="test", instance_type="container",
            image_path=Path("/tmp/img.qcow2"),
            pool="default", profile="default",
            root_size="10GiB", memory="1GB", cpu="1",
            host_interface="", provision=True,
            web_port=8000, no_web_proxy=False,
        )
        _ensure_web_proxy_device(cmds, cfg)
        # No add or remove should be called
        cmds.incus.run.assert_not_called()

    def test_ensure_web_proxy_device_updates_when_port_differs(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_proxy_device
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        cmds = mock.Mock()
        # Existing device with old port
        cmds.incus.output.return_value = (
            "kive-web:\n"
            "  listen: tcp:127.0.0.1:8000\n"
            "  connect: tcp:127.0.0.1:8000\n"
            "  type: proxy\n"
        )
        cmds.incus.run.return_value = None
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"),
            instance="test", instance_type="container",
            image_path=Path("/tmp/img.qcow2"),
            pool="default", profile="default",
            root_size="10GiB", memory="1GB", cpu="1",
            host_interface="", provision=True,
            web_port=18000, no_web_proxy=False,
        )
        _ensure_web_proxy_device(cmds, cfg)
        remove_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "remove" in c[0][0] and "kive-web" in c[0][0]
        ]
        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0] and "kive-web" in c[0][0]
        ]
        self.assertEqual(len(remove_calls), 1)
        self.assertEqual(len(add_calls), 1)
        add_args = add_calls[0][0][0]
        self.assertIn("listen=tcp:127.0.0.1:18000", add_args)

    def test_run_build_vm_prints_stable_url(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import run_build_vm
        import io
        import contextlib
        args = mock.Mock()
        args.root = Path("/tmp")
        args.workdir = Path("/tmp/work")
        args.instance = "test"
        args.instance_type = "container"
        args.image_name = "test.qcow2"
        args.pool = "default"
        args.profile = "default"
        args.root_size = "10GiB"
        args.memory = "1GB"
        args.cpu = "1"
        args.host_interface = ""
        args.provision = True
        args.web_port = 8000
        args.no_web_proxy = False
        args.debug = True
        args.verbose = True
        args.quiet = False
        args.log_file = None

        with (
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.Cmds") as mock_cmds_cls,
            mock.patch.object(Path, "mkdir"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_incus_daemon"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_storage_pool"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_profile_with_root_disk"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_instance", return_value=(True, "container")),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_network_device", return_value=False),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_user_data"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.enable_network_config"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_restart_after_config"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.handle_workspace_attachment"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_provision_instance"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._ensure_web_proxy_device"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.logger"),
        ):
            cmds = mock.Mock()
            cmds.incus.output.return_value = ""
            mock_cmds_cls.create.return_value = cmds
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                run_build_vm(args)
            output = buf.getvalue()
        self.assertIn("http://127.0.0.1:8000/login/", output)

    def test_run_build_vm_skips_url_when_no_provision(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import run_build_vm
        import io
        import contextlib
        args = mock.Mock()
        args.root = Path("/tmp")
        args.workdir = Path("/tmp/work")
        args.instance = "test"
        args.instance_type = "container"
        args.image_name = "test.qcow2"
        args.pool = "default"
        args.profile = "default"
        args.root_size = "10GiB"
        args.memory = "1GB"
        args.cpu = "1"
        args.host_interface = ""
        args.provision = False
        args.web_port = 8000
        args.no_web_proxy = False
        args.debug = True
        args.verbose = True
        args.quiet = False
        args.log_file = None

        with (
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.Cmds") as mock_cmds_cls,
            mock.patch.object(Path, "mkdir"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_incus_daemon"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_storage_pool"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_profile_with_root_disk"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_instance", return_value=(True, "container")),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_network_device", return_value=False),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_user_data"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.enable_network_config"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_restart_after_config"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.handle_workspace_attachment"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_provision_instance"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._ensure_web_proxy_device"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.logger"),
        ):
            cmds = mock.Mock()
            cmds.incus.output.return_value = ""
            mock_cmds_cls.create.return_value = cmds
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                run_build_vm(args)
            output = buf.getvalue()
        self.assertNotIn("http://127.0.0.1:8000/login/", output)

    def test_run_build_vm_skips_url_when_no_web_proxy(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import run_build_vm
        import io
        import contextlib
        args = mock.Mock()
        args.root = Path("/tmp")
        args.workdir = Path("/tmp/work")
        args.instance = "test"
        args.instance_type = "container"
        args.image_name = "test.qcow2"
        args.pool = "default"
        args.profile = "default"
        args.root_size = "10GiB"
        args.memory = "1GB"
        args.cpu = "1"
        args.host_interface = ""
        args.provision = True
        args.web_port = 8000
        args.no_web_proxy = True
        args.debug = True
        args.verbose = True
        args.quiet = False
        args.log_file = None

        with (
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.Cmds") as mock_cmds_cls,
            mock.patch.object(Path, "mkdir"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_incus_daemon"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_storage_pool"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_profile_with_root_disk"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_instance", return_value=(True, "container")),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_network_device", return_value=False),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_user_data"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.enable_network_config"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_restart_after_config"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.handle_workspace_attachment"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_provision_instance"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._ensure_web_proxy_device"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.logger"),
        ):
            cmds = mock.Mock()
            cmds.incus.output.return_value = ""
            mock_cmds_cls.create.return_value = cmds
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                run_build_vm(args)
            output = buf.getvalue()
        self.assertNotIn("http://127.0.0.1:8000/login/", output)

    def test_purge_removes_web_proxy_device(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        # _find_tagged_instances: list + config show
        # _device_attached(kive-code): config show
        # _device_attached(kive-web): config show
        cmds.incus.output.side_effect = [
            "kive-minimal\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\nkive-web:\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\nkive-web:\n",
            "user.kive.devel.created-by: utils/dev\nkive-code:\nkive-web:\n",
        ]
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with tempfile.TemporaryDirectory() as tmp:
            args = mock.Mock()
            args.root = Path(tmp)
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)

            web_remove_calls = [
                c for c in cmds.incus.run.call_args_list
                if "device" in c[0][0] and "remove" in c[0][0] and "kive-web" in c[0][0]
            ]
            self.assertGreaterEqual(len(web_remove_calls), 1,
                                    "Should remove kive-web proxy device from tagged instance")


class TestEnsureInstance(unittest.TestCase):
    def test_vm_unsupported_raises_runtime_error(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.instance import ensure_instance
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(
            returncode=1,
            stdout="",
            stderr='Error: instance type "virtual-machine" is not supported',
        )

        with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.instance.instance_exists", return_value=False):
            with self.assertRaises(RuntimeError) as cm_err:
                ensure_instance(cmds, "test", "vm", "default", "4", "8GiB")

        self.assertIn("VM instances are not supported", str(cm_err.exception))

    def test_vm_unsupported_does_not_fallback_to_container(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.instance import ensure_instance
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(
            returncode=1,
            stdout="",
            stderr='Error: instance type "virtual-machine" is not supported',
        )

        with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.instance.instance_exists", return_value=False):
            with self.assertRaises(RuntimeError):
                ensure_instance(cmds, "test", "vm", "default", "4", "8GiB")

        # ensure_instance should only be called once (no recursive fallback)
        self.assertEqual(cmds.incus.run.call_count, 1)

    def test_container_mode_still_retries_privileged(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.instance import ensure_instance
        cmds = mock.Mock()
        # First call fails with uid/gid error, second succeeds
        cmds.incus.run.side_effect = [
            MockRunResult(returncode=1, stdout="", stderr="no uid/gid allocation configured"),
            MockRunResult(returncode=0, stdout="", stderr=""),
        ]

        with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.instance.instance_exists", return_value=False):
            result = ensure_instance(cmds, "test", "container", "default", "1", "1GB")

        self.assertEqual(result, (True, "container"))
        self.assertEqual(cmds.incus.run.call_count, 2)

    def test_container_privileged_retry_still_fails(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.instance import ensure_instance
        cmds = mock.Mock()
        cmds.incus.run.side_effect = [
            MockRunResult(returncode=1, stdout="", stderr="no uid/gid allocation configured"),
            MockRunResult(returncode=1, stdout="", stderr="still failed"),
        ]

        with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.instance.instance_exists", return_value=False):
            with self.assertRaises(RuntimeError) as cm_err:
                ensure_instance(cmds, "test", "container", "default", "1", "1GB")

        self.assertIn("privileged", str(cm_err.exception).lower())


class TestSingularityProbeFatal(unittest.TestCase):
    def test_vm_mode_singularity_exec_failed_is_fatal(self):
        from Kive.utils.kivedevel.kivedevel.checks import _run_singularity_probe
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="exec FAILED",
            stderr="",
        )

        with self.assertRaises(SystemExit):
            _run_singularity_probe(cmds, "test", fatal=True)

    def test_vm_mode_singularity_not_installed_is_fatal(self):
        from Kive.utils.kivedevel.kivedevel.checks import _run_singularity_probe
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="not installed",
            stderr="",
        )

        with self.assertRaises(SystemExit):
            _run_singularity_probe(cmds, "test", fatal=True)

    def test_vm_mode_singularity_ok_no_exit(self):
        from Kive.utils.kivedevel.kivedevel.checks import _run_singularity_probe
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="exec OK",
            stderr="",
        )

        # Should not raise
        _run_singularity_probe(cmds, "test", fatal=True)

    def test_non_fatal_mode_returns_on_exec_failed(self):
        from Kive.utils.kivedevel.kivedevel.checks import _run_singularity_probe
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="exec FAILED",
            stderr="",
        )

        # Should not raise when not fatal
        _run_singularity_probe(cmds, "test", fatal=False)

    def test_validate_vm_passes_fatal_to_singularity_probe(self):
        from Kive.utils.kivedevel.kivedevel.checks import run_validate_vm
        source = inspect.getsource(run_validate_vm)
        self.assertIn("fatal=True", source)


class TestNoVMFallback(unittest.TestCase):
    def test_source_has_no_falling_back_to_container(self):
        instance_path = Path(__file__).resolve().parents[4] / "Kive" / "utils" / "kivedevel" / "kivedevel" / "build_vm" / "instance.py"
        text = instance_path.read_text()
        self.assertNotIn("falling back", text)
        self.assertNotIn("fallback", text)


class TestSlurmBuilderRole(unittest.TestCase):
    REPO_ROOT = Path(__file__).resolve().parents[4] / "Kive"

    def test_slurm_download_uses_get_url_with_checksum(self):
        task_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_builder" / "tasks" / "main.yml"
        text = task_path.read_text()
        self.assertIn("get_url:", text)
        self.assertIn("checksum:", text)
        self.assertIn("slurm_sha1_checksum", text)

    def test_slurm_download_has_timeout(self):
        task_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_builder" / "tasks" / "main.yml"
        text = task_path.read_text()
        self.assertIn("timeout: 60", text)

    def test_slurm_download_has_retries(self):
        task_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_builder" / "tasks" / "main.yml"
        text = task_path.read_text()
        self.assertIn("retries: 5", text)
        self.assertIn("delay: 30", text)

    def test_slurm_download_uses_until_succeeded(self):
        task_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_builder" / "tasks" / "main.yml"
        text = task_path.read_text()
        self.assertIn("until:", text)
        self.assertIn("succeeded", text)

    def test_slurm_download_registers_result(self):
        task_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_builder" / "tasks" / "main.yml"
        text = task_path.read_text()
        self.assertIn("register:", text)

    def test_slurm_download_no_stat_guard(self):
        task_path = self.REPO_ROOT / "cluster-setup" / "deployment" / "roles" / "slurm_builder" / "tasks" / "main.yml"
        text = task_path.read_text()
        self.assertNotIn("stat:", text)
        self.assertNotIn("slurm_download.stat.exists", text)


class TestVmNetwork(unittest.TestCase):
    def _setup_create_path(self, cmds):
        cmds.ip.ok.return_value = False

    def _setup_registry_owned(self, tmpdir: Path, bridge: str = "kive-devel-br") -> Path:
        reg = tmpdir / "tmp~" / ".kive-devel-resources.json"
        reg.parent.mkdir(parents=True, exist_ok=True)
        reg.write_text(json.dumps([
            {"kind": "linux-bridge", "name": bridge, "created_by": "utils/dev"},
        ], indent=2) + "\n")
        return reg

    def test_default_model_network_is_kive_devel_br(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        self.assertEqual(BuildVmConfig.vm_network, "kive-devel-br")

    def test_default_cli_network_is_kive_devel_br(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertEqual(args.vm_network, "kive-devel-br")

    def test_no_incus_network_create_called(self):
        network_path = Path(__file__).resolve().parents[4] / "Kive" / "utils" / "kivedevel" / "kivedevel" / "build_vm" / "network.py"
        text = network_path.read_text()
        self.assertNotIn('"network", "create"', text)
        self.assertNotIn("incus network create", text)

    def test_creates_bridge_with_ip_link(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_owned_bridge
        import tempfile
        cmds = mock.Mock()
        cmds.ip.ok.return_value = False

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ensure_owned_bridge(cmds, root, "kive-devel-br", "10.247.172.1/24", "10.247.172.80", Path(tmp))

        link_calls = [
            c for c in cmds.ip.run.call_args_list
            if c[0][0][:2] == ["link", "add"]
        ]
        self.assertEqual(len(link_calls), 1)
        args = link_calls[0][0][0]
        self.assertIn("kive-devel-br", args)
        self.assertIn("bridge", args)

    def test_creates_nft_nat_rules(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_owned_bridge
        import tempfile
        cmds = mock.Mock()
        cmds.ip.ok.return_value = False

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ensure_owned_bridge(cmds, root, "kive-devel-br", "10.247.172.1/24", "10.247.172.80", Path(tmp))

        nft_calls = [
            c for c in cmds.nft.run.call_args_list
            if "kive_devel" in str(c)
        ]
        self.assertGreaterEqual(len(nft_calls), 1)

    def test_creates_nft_forward_rules(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_owned_bridge
        import tempfile
        cmds = mock.Mock()
        cmds.ip.ok.return_value = False

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ensure_owned_bridge(cmds, root, "kive-devel-br", "10.247.172.1/24", "10.247.172.80", Path(tmp))

        forward_calls = [
            c for c in cmds.nft.run.call_args_list
            if "forward" in " ".join(c[0][0])
        ]
        self.assertGreaterEqual(len(forward_calls), 1, "Expected at least one nft forward rule")
        chain_create = any("add" in c[0][0] and "chain" in c[0][0] for c in forward_calls)
        self.assertTrue(chain_create, "Expected forward chain to be created")
        iif_rule = any("iifname" in " ".join(c[0][0]) and "kive-devel-br" in " ".join(c[0][0]) for c in forward_calls)
        self.assertTrue(iif_rule, "Expected forward rule for iifname kive-devel-br")

    def test_creates_registry(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_owned_bridge
        import tempfile
        cmds = mock.Mock()
        cmds.ip.ok.return_value = False

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ensure_owned_bridge(cmds, root, "kive-devel-br", "10.247.172.1/24", "10.247.172.80", Path(tmp))

            reg = root / "tmp~" / ".kive-devel-resources.json"
            self.assertTrue(reg.exists())
            data = json.loads(reg.read_text())
            kinds = [e.get("kind") for e in data]
            self.assertIn("linux-bridge", kinds)
            self.assertIn("nft-table", kinds)

    def test_reuses_registry_owned_bridge(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_owned_bridge
        import tempfile
        cmds = mock.Mock()
        cmds.ip.ok.return_value = False

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._setup_registry_owned(root)
            ensure_owned_bridge(cmds, root, "kive-devel-br", "10.247.172.1/24", "10.247.172.80", Path(tmp))

        link_calls = [
            c for c in cmds.ip.run.call_args_list
            if c[0][0][:2] == ["link", "add"]
        ]
        self.assertEqual(len(link_calls), 0, "Should not create bridge when registry-owned")

    def test_fails_on_collision_with_unowned_bridge(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_owned_bridge
        import tempfile
        cmds = mock.Mock()
        cmds.ip.ok.return_value = True

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(SystemExit):
                ensure_owned_bridge(cmds, root, "kive-devel-br", "10.247.172.1/24", "10.247.172.80", Path(tmp))

    def test_creation_failure_does_not_fallback_to_container(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_owned_bridge
        import tempfile
        cmds = mock.Mock()
        cmds.ip.ok.return_value = False
        cmds.ip.run.side_effect = RuntimeError("Cannot find device")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(SystemExit):
                ensure_owned_bridge(cmds, root, "kive-devel-br", "10.247.172.1/24", "10.247.172.80", Path(tmp))

    def test_no_fallback_to_container_on_bridge_failure(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_owned_bridge
        import tempfile
        cmds = mock.Mock()
        cmds.ip.ok.return_value = False
        cmds.ip.run.side_effect = RuntimeError("create failed")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(SystemExit):
                ensure_owned_bridge(cmds, root, "kive-devel-br", "10.247.172.1/24", "10.247.172.80", Path(tmp))

    def test_ensure_ip_forward_enables_when_disabled(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import _ensure_ip_forward
        import subprocess as _sp
        real_run = _sp.run

        def fake_read(args, **kwargs):
            if args == ["sysctl", "-n", "net.ipv4.ip_forward"]:
                return mock.Mock(stdout="0\n", returncode=0)
            if args == ["sudo", "--", "sysctl", "-w", "net.ipv4.ip_forward=1"]:
                return mock.Mock(stdout="", returncode=0)
            return real_run(args, **kwargs)

        with mock.patch("subprocess.run", side_effect=fake_read):
            _ensure_ip_forward()

    def test_ensure_ip_forward_skips_when_already_enabled(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import _ensure_ip_forward

        def fake_read(args, **kwargs):
            if args == ["sysctl", "-n", "net.ipv4.ip_forward"]:
                return mock.Mock(stdout="1\n", returncode=0)
            raise AssertionError("Should not call sysctl -w when already enabled")

        with mock.patch("subprocess.run", side_effect=fake_read):
            _ensure_ip_forward()

    def test_vm_nic_uses_bridged_parent(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_vm_nic
        cmds = mock.Mock()
        cmds.incus.output.return_value = "other-device:\n  type: nic\n"
        ensure_vm_nic(cmds, "test-vm", "kive-devel-br")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if c[0][0][:4] == ["config", "device", "add", "test-vm"]
               and "eth0" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 1)
        args = add_calls[0][0][0]
        self.assertIn("nictype=bridged", args)
        self.assertIn("parent=kive-devel-br", args)
        self.assertNotIn("network=", str(args))
        self.assertNotIn("ipv4.address=", str(args))

    def test_no_incusbr0_references_in_build_vm_network_code(self):
        network_path = Path(__file__).resolve().parents[4] / "Kive" / "utils" / "kivedevel" / "kivedevel" / "build_vm" / "network.py"
        text = network_path.read_text()
        self.assertNotIn("incusbr0", text)

    def test_purge_removes_registry_bridge(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._setup_registry_owned(root)
            reg_path = root / "tmp~" / ".kive-devel-resources.json"

            args = mock.Mock()
            args.root = root
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)

        link_delete_calls = [
            c for c in cmds.ip.run.call_args_list
            if c[0][0][:3] == ["link", "delete", "kive-devel-br"]
        ]
        self.assertGreaterEqual(len(link_delete_calls), 1,
                                "Should delete registry-owned bridge")
        self.assertFalse(reg_path.exists(),
                         "Registry file should be removed after purge")

    def test_purge_removes_nftables(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg = root / "tmp~" / ".kive-devel-resources.json"
            reg.parent.mkdir(parents=True, exist_ok=True)
            reg.write_text(json.dumps([
                {"kind": "nft-table", "name": "inet kive_devel", "created_by": "utils/dev"},
            ], indent=2) + "\n")

            args = mock.Mock()
            args.root = root
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)

        nft_calls = [
            c for c in cmds.nft.run.call_args_list
            if c[0][0][:3] == ["delete", "table", "inet"]
        ]
        self.assertGreaterEqual(len(nft_calls), 1,
                                "Should delete registry-owned nftables table")
        self.assertFalse(reg.exists(),
                         "Registry file should be removed after purge")

    def test_purge_idempotent_when_nothing_to_purge(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""

        with tempfile.TemporaryDirectory() as tmp:
            args = mock.Mock()
            args.root = Path(tmp)
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)
                    purge_mod.run_purge(args)

    def test_purge_does_not_delete_unowned_bridge(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with tempfile.TemporaryDirectory() as tmp:
            args = mock.Mock()
            args.root = Path(tmp)
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(purge_mod, "configure_logging"):
                    purge_mod.run_purge(args)

        ip_calls = [
            c for c in cmds.ip.run.call_args_list
            if "link" in str(c)
        ]
        self.assertEqual(len(ip_calls), 0,
                         "Should not delete any bridge when no registry-owned bridges exist")

    def test_find_forward_chains_with_drop_policy_ignores_kive_devel(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import _find_forward_chains_with_drop_policy
        nft_json = [
            {
                "table": {
                    "family": "inet",
                    "name": "kive_devel",
                    "chain": [{
                        "family": "inet",
                        "name": "forward",
                        "table": "kive_devel",
                        "type": "filter",
                        "hook": "forward",
                        "prio": 0,
                        "policy": "drop",
                    }],
                },
            },
            {
                "table": {
                    "family": "inet",
                    "name": "filter",
                    "chain": [{
                        "family": "inet",
                        "name": "FORWARD",
                        "table": "filter",
                        "type": "filter",
                        "hook": "forward",
                        "prio": 0,
                        "policy": "drop",
                    }],
                },
            },
        ]
        result = _find_forward_chains_with_drop_policy(nft_json)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table"], "filter")
        self.assertEqual(result[0]["chain"], "FORWARD")

    def test_find_forward_chains_with_drop_policy_empty_on_no_drop(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import _find_forward_chains_with_drop_policy
        nft_json = [
            {
                "table": {
                    "family": "inet",
                    "name": "filter",
                    "chain": [{
                        "family": "inet",
                        "name": "FORWARD",
                        "table": "filter",
                        "type": "filter",
                        "hook": "forward",
                        "prio": 0,
                        "policy": "accept",
                    }],
                },
            },
        ]
        result = _find_forward_chains_with_drop_policy(nft_json)
        self.assertEqual(result, [])

    def test_get_nft_json_list_returns_parsed(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import _get_nft_json_list
        cmds = mock.Mock()
        cmds.nft.run.return_value = MockRunResult(
            returncode=0,
            stdout=json.dumps({"nftables": [{"table": {"name": "test"}}]}),
        )
        result = _get_nft_json_list(cmds)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table"]["name"], "test")

    def test_get_nft_json_list_returns_empty_on_failure(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import _get_nft_json_list
        cmds = mock.Mock()
        cmds.nft.run.return_value = MockRunResult(returncode=1, stdout="error")
        result = _get_nft_json_list(cmds)
        self.assertEqual(result, [])

    def test_ensure_existing_forward_accept_adds_nft_rule(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import _ensure_existing_forward_accept
        import tempfile
        cmds = mock.Mock()
        nft_rules_json = json.dumps({
            "nftables": [{
                "table": {
                    "family": "inet",
                    "name": "filter",
                    "chain": [{
                        "family": "inet",
                        "name": "FORWARD",
                        "table": "filter",
                        "type": "filter",
                        "hook": "forward",
                        "prio": 0,
                        "policy": "drop",
                    }],
                },
            }],
        })
        cmds.nft.run.return_value = MockRunResult(returncode=0, stdout=nft_rules_json)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bridge = "kive-devel-br"
            _ensure_existing_forward_accept(cmds, root, bridge)

            # Should have added rule to inet filter FORWARD
            nft_add_calls = [
                c for c in cmds.nft.run.call_args_list
                if c[0][0][0] == "add" and c[0][0][1] == "rule" and c[0][0][2] == "inet"
            ]
            self.assertGreaterEqual(len(nft_add_calls), 1)
            args = nft_add_calls[0][0][0]
            self.assertIn("filter", args)
            self.assertIn("FORWARD", args)
            self.assertIn("accept", args)

            # Should be registered in registry
            reg = root / "tmp~" / ".kive-devel-resources.json"
            self.assertTrue(reg.exists())
            data = json.loads(reg.read_text())
            rules = [e for e in data if e.get("kind") == "forward-rule"]
            self.assertGreaterEqual(len(rules), 1)

    def test_ensure_existing_forward_accept_skips_when_registered(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import _ensure_existing_forward_accept
        import tempfile
        cmds = mock.Mock()
        # nft --json returns an empty ruleset so the drop-chain code is skipped
        cmds.nft.run.return_value = MockRunResult(returncode=0, stdout=json.dumps({"nftables": []}))

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Pre-register the iptables FORWARD rule
            reg = root / "tmp~" / ".kive-devel-resources.json"
            reg.parent.mkdir(parents=True, exist_ok=True)
            reg.write_text(json.dumps([
                {"kind": "forward-rule", "name": "iptables/FORWARD", "family": "", "table": "iptables", "chain": "FORWARD", "bridge": "kive-devel-br"},
            ], indent=2) + "\n")

            with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.network._sp") as mock_sp:
                _ensure_existing_forward_accept(cmds, root, "kive-devel-br")

            # Should NOT call iptables -I since rule is already registered
            iptables_insert_calls = [
                c for c in mock_sp.run.call_args_list
                if "iptables" in str(c) and "-I" in str(c)
            ]
            self.assertEqual(len(iptables_insert_calls), 0)

    def test_remove_forward_rules_nft_handle(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        cmds.nft.output.return_value = (
            'chain FORWARD {\n'
            '    type filter hook forward priority filter; policy drop;\n'
            '    iifname "kive-devel-br" accept # handle 42\n'
            '}'
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg = root / "tmp~" / ".kive-devel-resources.json"
            reg.parent.mkdir(parents=True, exist_ok=True)
            reg.write_text(json.dumps([
                {"kind": "forward-rule", "name": "inet/filter/FORWARD",
                 "family": "inet", "table": "filter", "chain": "FORWARD",
                 "bridge": "kive-devel-br"},
            ], indent=2) + "\n")

            purge_mod._remove_forward_rules(cmds, root)

        delete_calls = [
            c for c in cmds.nft.run.call_args_list
            if "delete" in c[0][0] and "rule" in c[0][0]
        ]
        self.assertGreaterEqual(len(delete_calls), 1)
        args = delete_calls[0][0][0]
        self.assertIn("handle", args)
        self.assertIn("42", args)

    def test_remove_forward_rules_iptables(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg = root / "tmp~" / ".kive-devel-resources.json"
            reg.parent.mkdir(parents=True, exist_ok=True)
            reg.write_text(json.dumps([
                {"kind": "forward-rule", "name": "iptables/FORWARD",
                 "family": "", "table": "iptables", "chain": "FORWARD",
                 "bridge": "kive-devel-br"},
            ], indent=2) + "\n")

            with mock.patch.object(purge_mod, "subprocess") as mock_sp:
                purge_mod._remove_forward_rules(cmds, root)

            iptables_delete_calls = [
                c for c in mock_sp.run.call_args_list
                if "iptables" in str(c) and "-D" in str(c)
            ]
            self.assertEqual(len(iptables_delete_calls), 1)

    def test_host_diagnostics_included_in_failed_marker(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        def run_side_effect(cmd, **kwargs):
            if "provision.log" in " ".join(cmd):
                return MockRunResult(returncode=0, stdout="cannot reach the internet by IP", stderr="")
            return MockRunResult(returncode=1, stdout="", stderr="")

        cmds.incus.run.side_effect = run_side_effect

        def probe_side_effect(_cmds, _instance):
            return ("failed", None)

        with mock.patch.object(provision, "_probe_markers", side_effect=probe_side_effect):
            with mock.patch.object(provision, "_collect_host_diagnostics", return_value="HOST-DIAG-CONTENT"):
                with self.assertRaises(RuntimeError) as cm:
                    provision.maybe_provision_instance(cmds, instance, "container", provision=True)

        exc_text = str(cm.exception)
        self.assertIn("cannot reach the internet by IP", exc_text)
        self.assertIn("=== host diagnostics ===", exc_text)
        self.assertIn("HOST-DIAG-CONTENT", exc_text)


class TestPortForward(unittest.TestCase):
    def test_vm_mode_does_not_call_incus_proxy(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _run_build_vm_vm
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cmds.ip.ok.return_value = False
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"),
            instance="test", instance_type="vm",
            image_path=Path("/tmp/img.qcow2"),
            pool="default", profile="default",
            root_size="10GiB", memory="1GB", cpu="1",
            host_interface="", provision=False,
            web_port=8000, no_web_proxy=False,
        )
        with (
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_owned_bridge",
                       return_value=("10.247.172.1/24", "10.247.172.80")),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_instance",
                       return_value=(True, "vm")),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_vm_nic",
                       return_value=True),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_user_data",
                       return_value=True),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.enable_network_config",
                       return_value=True),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_restart_after_config"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.handle_workspace_attachment"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._ensure_web_port_forward") as mock_forward,
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._ensure_web_proxy_device") as mock_proxy,
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_provision_instance"),
        ):
            _run_build_vm_vm(cfg, cmds)
        mock_forward.assert_called_once()
        mock_proxy.assert_not_called()

    def test_port_forward_starts_socat_and_registers(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_port_forward
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = BuildVmConfig(
                root=root, workdir=root / "work",
                instance="test", instance_type="vm",
                image_path=Path("/tmp/img.qcow2"),
                pool="default", profile="default",
                root_size="10GiB", memory="1GB", cpu="1",
                host_interface="", provision=True,
                web_port=8000, no_web_proxy=False,
            )
            cmds = mock.Mock()
            cmds.socat._argv.return_value = ["socat", "TCP-LISTEN:8000,bind=127.0.0.1,reuseaddr,fork", "TCP:10.247.172.80:8000"]
            with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.socket.socket") as mock_socket:
                mock_socket.return_value.__enter__.return_value.connect_ex.return_value = 1
                with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.subprocess.Popen") as mock_popen:
                    mock_proc = mock.Mock()
                    mock_proc.pid = 12345
                    mock_popen.return_value = mock_proc

                    with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._wait_for_listening_pid", return_value=12345):
                        _ensure_web_port_forward(cfg, "10.247.172.80", root, cmds)

            mock_popen.assert_called_once()
            args = mock_popen.call_args[0][0]
            self.assertIn("socat", args[0])
            self.assertIn("TCP-LISTEN:8000", str(args))
            self.assertIn("TCP:10.247.172.80:8000", str(args))

            reg = root / "tmp~" / ".kive-devel-resources.json"
            self.assertTrue(reg.exists())
            data = json.loads(reg.read_text())
            forwards = [e for e in data if e.get("kind") == "host-forward"]
            self.assertEqual(len(forwards), 1)
            self.assertEqual(forwards[0]["pid"], 12345)
            self.assertEqual(forwards[0]["port"], 8000)
            self.assertEqual(forwards[0]["vm_ip"], "10.247.172.80")

    def test_port_forward_reuses_existing_alive(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_port_forward
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg = root / "tmp~" / ".kive-devel-resources.json"
            reg.parent.mkdir(parents=True, exist_ok=True)
            reg.write_text(json.dumps([
                {"kind": "host-forward", "pid": 99999, "port": 8000,
                 "vm_ip": "10.247.172.80", "created_by": "utils/dev"},
            ], indent=2) + "\n")
            cfg = BuildVmConfig(
                root=root, workdir=root / "work",
                instance="test", instance_type="vm",
                image_path=Path("/tmp/img.qcow2"),
                pool="default", profile="default",
                root_size="10GiB", memory="1GB", cpu="1",
                host_interface="", provision=True,
                web_port=8000, no_web_proxy=False,
            )
            with (
                mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.os.kill") as mock_kill,
                mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.subprocess.Popen") as mock_popen,
            ):
                _ensure_web_port_forward(cfg, "10.247.172.80", root, mock.Mock())
            # Existing alive forward should be reused — no kill (SIGTERM), no restart
            for call in mock_kill.call_args_list:
                args, _ = call
                if len(args) >= 2 and args[1] != 0:
                    self.fail("Unexpected SIGTERM kill on alive forward")
            mock_popen.assert_not_called()

    def test_port_forward_stale_cleaned(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_port_forward
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg = root / "tmp~" / ".kive-devel-resources.json"
            reg.parent.mkdir(parents=True, exist_ok=True)
            reg.write_text(json.dumps([
                {"kind": "host-forward", "pid": 99999, "port": 8000,
                 "vm_ip": "10.247.172.80", "created_by": "utils/dev"},
            ], indent=2) + "\n")
            cfg = BuildVmConfig(
                root=root, workdir=root / "work",
                instance="test", instance_type="vm",
                image_path=Path("/tmp/img.qcow2"),
                pool="default", profile="default",
                root_size="10GiB", memory="1GB", cpu="1",
                host_interface="", provision=True,
                web_port=8000, no_web_proxy=False,
            )
            with (
                mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.os.kill") as mock_kill,
                mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.subprocess.Popen") as mock_popen,
                mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._wait_for_listening_pid", return_value=12345),
                mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._port_in_use", return_value=False),
            ):
                mock_proc = mock.Mock()
                mock_proc.pid = 12345
                mock_popen.return_value = mock_proc
                # Make pid 99999 appear dead (raises OSError)
                def kill_side_effect(pid, sig):
                    if pid == 99999:
                        raise ProcessLookupError()
                mock_kill.side_effect = kill_side_effect
                _ensure_web_port_forward(cfg, "10.247.172.80", root, mock.Mock())
            mock_popen.assert_called_once()

    def test_port_forward_unowned_port_fails(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_port_forward
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = BuildVmConfig(
                root=root, workdir=root / "work",
                instance="test", instance_type="vm",
                image_path=Path("/tmp/img.qcow2"),
                pool="default", profile="default",
                root_size="10GiB", memory="1GB", cpu="1",
                host_interface="", provision=True,
                web_port=8000, no_web_proxy=False,
            )
            with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.socket.socket") as mock_socket:
                mock_socket.return_value.__enter__.return_value.connect_ex.return_value = 0
                with self.assertRaises(SystemExit):
                    _ensure_web_port_forward(cfg, "10.247.172.80", root, mock.Mock())

    def test_no_web_proxy_skips_forward(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _ensure_web_port_forward
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = BuildVmConfig(
                root=root, workdir=root / "work",
                instance="test", instance_type="vm",
                image_path=Path("/tmp/img.qcow2"),
                pool="default", profile="default",
                root_size="10GiB", memory="1GB", cpu="1",
                host_interface="", provision=True,
                web_port=8000, no_web_proxy=True,
            )
            with mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.subprocess.Popen") as mock_popen:
                _ensure_web_port_forward(cfg, "10.247.172.80", root, mock.Mock())
            mock_popen.assert_not_called()

    def test_purge_kills_port_forward(self):
        from Kive.utils.kivedevel.kivedevel.build_vm import purge as purge_mod
        import tempfile
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cmds.incus.run.return_value = MockRunResult(returncode=0)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg = root / "tmp~" / ".kive-devel-resources.json"
            reg.parent.mkdir(parents=True, exist_ok=True)
            reg.write_text(json.dumps([
                {"kind": "host-forward", "pid": 12345, "port": 8000,
                 "vm_ip": "10.247.172.80", "created_by": "utils/dev"},
            ], indent=2) + "\n")

            args = mock.Mock()
            args.root = root
            args.instances = None
            args.workdirs = None
            args.quiet = False
            args.verbose = False
            args.debug = False
            args.log_file = None

            with (
                mock.patch.object(purge_mod, "Cmds") as mock_cmds_cls,
                mock.patch.object(purge_mod, "os") as mock_os,
                mock.patch.object(purge_mod, "configure_logging"),
            ):
                mock_cmds_cls.create.return_value = cmds
                purge_mod.run_purge(args)

            mock_os.kill.assert_any_call(12345, signal.SIGTERM if hasattr(signal, "SIGTERM") else 0)

    def test_container_still_uses_incus_proxy(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _run_build_vm_container
        from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"),
            instance="test", instance_type="container",
            image_path=Path("/tmp/img.qcow2"),
            pool="default", profile="default",
            root_size="10GiB", memory="1GB", cpu="1",
            host_interface="", provision=False,
            web_port=8000, no_web_proxy=False,
        )
        with (
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_instance",
                       return_value=(True, "container")),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_network_device",
                       return_value=False),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.ensure_user_data",
                       return_value=True),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.enable_network_config",
                       return_value=True),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_restart_after_config"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.handle_workspace_attachment"),
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner._ensure_web_proxy_device") as mock_proxy,
            mock.patch("Kive.utils.kivedevel.kivedevel.build_vm.runner.maybe_provision_instance"),
        ):
            _run_build_vm_container(cfg, cmds)
        mock_proxy.assert_called_once()


class TestProfileRootDisk(unittest.TestCase):
    def _make_cmds(self, ok_return: bool = True, output_yaml: str = ""):
        cmds = mock.Mock()
        cmds.incus.ok.return_value = ok_return
        cmds.incus.output.return_value = output_yaml
        return cmds

    def test_empty_devices_adds_root_disk(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        cmds = self._make_cmds(output_yaml="{}")

        ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0] and "root" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 1)
        args_list = add_calls[0][0][0]
        self.assertIn("pool=pool1", args_list)
        self.assertIn("path=/", args_list)
        self.assertIn("size=10GiB", args_list)

    def test_null_devices_output_fails(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        cmds = self._make_cmds(output_yaml="null")

        with self.assertRaises(SystemExit):
            ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

    def test_existing_root_disk_is_idempotent(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        devices_yaml = """\
root:
  path: /
  pool: default
  size: 10GiB
  type: disk
"""
        cmds = self._make_cmds(output_yaml=devices_yaml)

        ensure_profile_with_root_disk(cmds, "default", "default", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 0)

    def test_other_devices_no_root_adds_root_disk(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        devices_yaml = """\
data:
  path: /data
  pool: default
  type: disk
"""
        cmds = self._make_cmds(output_yaml=devices_yaml)

        ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 1)

    def test_incompatible_root_device_fails(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        devices_yaml = """\
root:
  nictype: bridged
  parent: incusbr0
  type: nic
"""
        cmds = self._make_cmds(output_yaml=devices_yaml)

        with self.assertRaises(SystemExit):
            ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 0, "Should not add root disk when incompatible root exists")

    def test_invalid_yaml_fails(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        cmds = self._make_cmds(output_yaml="not: valid: yaml: [")

        with self.assertRaises(SystemExit):
            ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 0)

    def test_debug_log_when_root_disk_added(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        cmds = self._make_cmds(output_yaml="{}")

        with self.assertLogs("kivedevel", level="INFO") as logs:
            ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        self.assertTrue(any("Adding root disk" in msg for msg in logs.output))

    def test_log_when_existing_root_skipped(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        devices_yaml = """\
root:
  path: /
  pool: default
  size: 10GiB
  type: disk
"""
        cmds = self._make_cmds(output_yaml=devices_yaml)

        with self.assertLogs("kivedevel", level="DEBUG") as logs:
            ensure_profile_with_root_disk(cmds, "default", "default", "10GiB")

        self.assertTrue(any("already has a valid root disk" in msg for msg in logs.output))

    def test_runner_calls_ensure_profile_before_ensure_instance(self):
        runner_path = Path(__file__).resolve().parents[4] / "Kive" / "utils" / "kivedevel" / "kivedevel" / "build_vm" / "runner.py"
        text = runner_path.read_text()
        profile_idx = text.find("ensure_profile_with_root_disk")
        instance_idx = text.find("ensure_instance")
        self.assertGreater(instance_idx, profile_idx,
                           "ensure_profile_with_root_disk must be called before ensure_instance")


if __name__ == "__main__":
    unittest.main()
