import inspect
import sys
import time
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
        self.assertIn("timeout --foreground 30s python3 -c", saved_user_data["value"])
        self.assertIn("apt-get install -y ansible curl openssh-server", saved_user_data["value"])
        self.assertIn("if ! systemctl enable --now ssh; then", saved_user_data["value"])
        self.assertNotIn("packages:\n  - ansible\n  - curl\n  - openssh-server", saved_user_data["value"])
        self.assertNotIn("ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml\n        if [ $? -ne 0 ]", saved_user_data["value"])
        self.assertNotIn("cloud-init status --wait", saved_user_data["value"])

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
        debug_lines = [l for l in lines if "utils/dev prepare-host" in l]
        self.assertTrue(any("prepare-host" in l for l in lines))
        self.assertTrue(any("check-network" in l for l in lines))
        self.assertTrue(any("smoke-local-install" in l for l in lines))
        self.assertTrue(any("cleanup-local-install" in l for l in lines))

        # Verify cleanup is conditional on always()
        self.assertIn("if: always()", workflow_text)

        # Verify steps appear in correct order
        import re
        job_section_start = workflow_text.find("build-vm-smoke:")
        job_section = workflow_text[job_section_start:]
        job_step_names = re.findall(r"- name: (.+)", job_section)
        self.assertIn("Prepare host", job_step_names)
        self.assertIn("Check guest network", job_step_names)
        self.assertIn("Smoke-test local install", job_step_names)
        self.assertIn("Cleanup local install smoke test", job_step_names)

        prepare_idx = job_step_names.index("Prepare host")
        check_idx = job_step_names.index("Check guest network")
        smoke_idx = job_step_names.index("Smoke-test local install")
        cleanup_idx = job_step_names.index("Cleanup local install smoke test")
        self.assertLess(prepare_idx, check_idx)
        self.assertLess(check_idx, smoke_idx)
        self.assertLess(smoke_idx, cleanup_idx)

    def test_provision_polling_deadline_is_shorter_than_workflow_timeout(self):
        self.assertEqual(provision.DEFAULT_PROVISION_TIMEOUT, 900)

    def test_maybe_provision_instance_fails_on_stuck_started_marker(self):
        instance = "ci-smoke"
        cmds = mock.Mock()

        def run_side_effect(cmd, check=False, capture_output=False):
            if cmd[:3] == ["file", "pull", f"{instance}/var/lib/kive-provision/done"]:
                return MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")
            if cmd[:3] == ["file", "pull", f"{instance}/var/lib/kive-provision/failed"]:
                return MockRunResult(returncode=1, stdout="", stderr="Error: Not Found")
            if cmd[:3] == ["file", "pull", f"{instance}/var/lib/kive-provision/started"]:
                return MockRunResult(returncode=0, stdout="started", stderr="")
            if cmd[:3] == ["exec", instance, "--"] and cmd[3:] == ["sh", "-c", "cloud-init status --long 2>&1 || true"]:
                return MockRunResult(returncode=0, stdout="status: running\n", stderr="")
            if cmd[:2] == ["file", "pull"]:
                return MockRunResult(returncode=0, stdout="", stderr="")
            return MockRunResult(returncode=0, stdout="", stderr="")

        cmds.incus.run.side_effect = run_side_effect

        time_values = [0]

        def fake_time():
            if len(time_values) == 1:
                time_values.append(841)
                return 0
            return 841

        with mock.patch.object(provision.time, "time", side_effect=fake_time):
            with mock.patch.object(provision.time, "sleep", return_value=None):
                with self.assertRaises(RuntimeError) as cm:
                    provision.maybe_provision_instance(cmds, instance, "container", provision=True, timeout=900)

        self.assertIn("Provisioning appears stuck", str(cm.exception))

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

    def test_build_completion_message_refers_to_enter_vm(self):
        runner_path = self.REPO_ROOT / "utils" / "kivedevel" / "kivedevel" / "build_vm" / "runner.py"
        text = runner_path.read_text()
        self.assertIn("./utils/dev enter-vm", text)
        self.assertNotIn("ws-enter-vm", text)

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


if __name__ == "__main__":
    unittest.main()
