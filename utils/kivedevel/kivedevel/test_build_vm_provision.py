import inspect
import json
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

    def test_command_not_found_is_transport_failure(self):
        result = MockRunResult(returncode=1, stdout="", stderr="not found")
        self.assertTrue(provision._is_transport_failure(result))

    def test_empty_stderr_is_not_transport_failure(self):
        result = MockRunResult(returncode=1, stdout="", stderr="")
        self.assertFalse(provision._is_transport_failure(result))


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
        config = _proxy_config(8000)
        self.assertEqual(config["type"], "proxy")
        self.assertEqual(config["listen"], "tcp:127.0.0.1:8000")
        self.assertEqual(config["connect"], f"tcp:127.0.0.1:{GUEST_WEB_PORT}")

    def test_proxy_config_custom_port(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.runner import _proxy_config
        config = _proxy_config(18000)
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


class TestProfileRootDisk(unittest.TestCase):
    def _make_cmds(self, ok_return: bool = True, output_json: str = ""):
        cmds = mock.Mock()
        cmds.incus.ok.return_value = ok_return
        cmds.incus.output.return_value = output_json
        return cmds

    def test_empty_profile_adds_root_disk(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        empty_profile = '{"name": "default", "devices": {}, "config": {}}'
        cmds = self._make_cmds(output_json=empty_profile)

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

    def test_null_devices_output_adds_root_disk(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        empty_output = '{"name": "default", "devices": null, "config": {}}'
        cmds = self._make_cmds(output_json=empty_output)

        ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 1)

    def test_existing_root_disk_is_idempotent(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        valid_profile = json.dumps({
            "name": "default",
            "devices": {
                "root": {
                    "path": "/",
                    "pool": "default",
                    "size": "10GiB",
                    "type": "disk",
                }
            },
            "config": {},
        })
        cmds = self._make_cmds(output_json=valid_profile)

        ensure_profile_with_root_disk(cmds, "default", "default", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 0)

    def test_other_devices_no_root_adds_root_disk(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        profile_with_data = json.dumps({
            "name": "default",
            "devices": {
                "data": {
                    "path": "/data",
                    "pool": "default",
                    "type": "disk",
                }
            },
            "config": {},
        })
        cmds = self._make_cmds(output_json=profile_with_data)

        ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 1)

    def test_incompatible_root_device_fails(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        nic_root = json.dumps({
            "name": "default",
            "devices": {
                "root": {
                    "nictype": "bridged",
                    "parent": "incusbr0",
                    "type": "nic",
                }
            },
            "config": {},
        })
        cmds = self._make_cmds(output_json=nic_root)

        with self.assertRaises(SystemExit):
            ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        add_calls = [
            c for c in cmds.incus.run.call_args_list
            if "device" in c[0][0] and "add" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 0, "Should not add root disk when incompatible root exists")

    def test_debug_log_when_root_disk_added(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.incus import ensure_profile_with_root_disk
        empty_profile = '{"name": "default", "devices": {}, "config": {}}'
        cmds = self._make_cmds(output_json=empty_profile)

        with self.assertLogs("kivedevel", level="INFO") as logs:
            ensure_profile_with_root_disk(cmds, "default", "pool1", "10GiB")

        self.assertTrue(any("Adding root disk" in msg for msg in logs.output))

    def test_runner_calls_ensure_profile_before_ensure_instance(self):
        runner_path = Path(__file__).resolve().parents[4] / "Kive" / "utils" / "kivedevel" / "kivedevel" / "build_vm" / "runner.py"
        text = runner_path.read_text()
        profile_idx = text.find("ensure_profile_with_root_disk")
        instance_idx = text.find("ensure_instance")
        self.assertGreater(instance_idx, profile_idx,
                           "ensure_profile_with_root_disk must be called before ensure_instance")


if __name__ == "__main__":
    unittest.main()
