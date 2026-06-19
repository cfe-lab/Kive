import inspect
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from Kive.utils.kivedevel.kivedevel.build_vm import cloud_init, provision


class MockRunResult:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


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

        def run_side_effect(cmd, check=False, capture_output=False):
            if cmd[:3] == ["file", "pull", f"{instance}/var/lib/kive-provision/done"]:
                return MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")
            if cmd[:3] == ["file", "pull", f"{instance}/var/lib/kive-provision/failed"]:
                return MockRunResult(returncode=0, stdout="", stderr="")
            if cmd[:3] == ["exec", instance, "--"] and cmd[3:] == ["cloud-init", "status", "--long"]:
                return MockRunResult(returncode=0, stdout="status: error\n", stderr="")
            if cmd[:2] == ["file", "pull"]:
                return MockRunResult(returncode=0, stdout="", stderr="")
            return MockRunResult(returncode=0, stdout="", stderr="")

        cmds.incus.run.side_effect = run_side_effect

        with self.assertRaises(RuntimeError) as cm:
            provision.maybe_provision_instance(cmds, instance, "container", provision=True)

        self.assertIn("provisioning failed inside instance", str(cm.exception).lower())

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
        self.assertIn("if ! python3 -c 'import socket,sys; addr=socket.getaddrinfo(\"archive.ubuntu.com\", 80, socket.AF_INET, socket.SOCK_STREAM)[0][4]; sock=socket.create_connection(addr, timeout=10); sock.close()'", saved_user_data["value"])
        self.assertIn("apt-get install -y ansible curl openssh-server", saved_user_data["value"])
        self.assertNotIn("packages:\n  - ansible\n  - curl\n  - openssh-server", saved_user_data["value"])
        self.assertNotIn("ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml\n        if [ $? -ne 0 ]", saved_user_data["value"])

    def test_periodic_probe_uses_safe_printf(self):
        source = inspect.getsource(provision)
        self.assertIn("printf '%s\\n' '--- file layout ---'", source)

    def test_maybe_provision_instance_raises_on_cloud_init_error_status(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        def run_side_effect(cmd, check=False, capture_output=False):
            if cmd[:3] == ["file", "pull", f"{instance}/var/lib/kive-provision/done"]:
                return MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")
            if cmd[:3] == ["file", "pull", f"{instance}/var/lib/kive-provision/failed"]:
                return MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")
            if cmd[:3] == ["exec", instance, "--"] and cmd[3:] == ["sh", "-c", "cloud-init status --long 2>&1 || true"]:
                return MockRunResult(returncode=0, stdout="status: error\n", stderr="")
            if cmd[:2] == ["file", "pull"]:
                return MockRunResult(returncode=0, stdout="", stderr="")
            return MockRunResult(returncode=0, stdout="", stderr="")

        cmds.incus.run.side_effect = run_side_effect

        with self.assertRaises(RuntimeError) as cm:
            provision.maybe_provision_instance(cmds, instance, "container", provision=True)

        self.assertIn("cloud-init reported error status", str(cm.exception).lower())


if __name__ == "__main__":
    unittest.main()
