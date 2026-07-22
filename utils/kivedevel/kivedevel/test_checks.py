"""Tests for validate-vm and test-api subcommands: probes, device checks, API validation."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock


from kivedevel._test_helpers import (
    MockRunResult,
    make_cmds,
)



class TestValidateVm(unittest.TestCase):
    """Instance validation logic for validate-vm."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        from kivedevel import checks as checks
        import importlib
        importlib.reload(checks)
        return checks

    def test_validate_vm_fails_when_instance_missing(self):
        checks = self._import()
        with mock.patch.object(checks, "instance_exists", return_value=False):
            args = mock.Mock(spec=[])
            args.workdir = Path("/tmp")
            args.instance = "test"
            args.instance_type = "vm"
            args.quiet = False
            args.verbose = False
            args.debug = False
            with self.assertRaises(SystemExit):
                checks.run_validate_vm(args)


class TestSingularityProbeFatal(unittest.TestCase):
    """Singularity probe fatal vs non-fatal modes."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        from kivedevel import checks as checks
        import importlib
        importlib.reload(checks)
        return checks

    def test_vm_mode_singularity_exec_failed_is_fatal(self):
        checks = self._import()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="=== singularity version ===\n3.0.0\n=== singularity exec test ===\nexec FAILED\n",
        )
        with self.assertRaises(SystemExit):
            checks._run_singularity_probe(self.cmds, "test", fatal=True)

    def test_vm_mode_singularity_not_installed_is_fatal(self):
        checks = self._import()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="=== singularity version ===\nnot installed\n",
        )
        with self.assertRaises(SystemExit):
            checks._run_singularity_probe(self.cmds, "test", fatal=True)

    def test_vm_mode_singularity_ok_no_exit(self):
        checks = self._import()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="=== singularity version ===\n3.0.0\n=== singularity exec test ===\nexec OK\n",
        )
        try:
            checks._run_singularity_probe(self.cmds, "test", fatal=True)
        except SystemExit:
            self.fail("_run_singularity_probe raised SystemExit when exec OK")

    def test_non_fatal_mode_returns_on_exec_failed(self):
        checks = self._import()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout="=== singularity version ===\n3.0.0\n=== singularity exec test ===\nexec FAILED\n",
        )
        checks._run_singularity_probe(self.cmds, "test", fatal=False)


class TestSlurmScript(unittest.TestCase):
    """Test _SLURM_CHECK_SCRIPT directly using stub executables."""

    def setUp(self):
        self.tmpdir = Path("/tmp/test_slurm_script")
        self.bindir = self.tmpdir / "bin"
        self.bindir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_stub(self, name, exit_code=0, stdout=""):
        path = self.bindir / name
        path.write_text(
            f"#!/bin/sh\necho '{stdout}'\nexit {exit_code}\n"
        )
        path.chmod(0o755)

    def _run_script(self):
        from kivedevel.checks import _SLURM_CHECK_SCRIPT
        import subprocess
        env = {"PATH": str(self.bindir) + ":/usr/bin:/bin"}
        return subprocess.run(
            ["sh", "-c", _SLURM_CHECK_SCRIPT.strip()],
            capture_output=True, text=True, env=env,
        )

    def test_healthy_cluster_succeeds(self):
        self._write_stub("command")
        self._write_stub("systemctl", stdout="")
        self._write_stub("scontrol", stdout="Slurmctld(primary) at head is UP")
        self._write_stub("sinfo", stdout="idle")
        self._write_stub("squeue", stdout="JOBID PARTITION NAME USER ST TIME NODES NODELIST(REASON)")
        self._write_stub("srun")
        result = self._run_script()
        self.assertEqual(0, result.returncode)

    def test_controller_down_fails(self):
        self._write_stub("command")
        self._write_stub("systemctl", stdout="")
        self._write_stub("scontrol", stdout="Slurmctld(primary) at head is DOWN")
        self._write_stub("sinfo", stdout="idle")
        self._write_stub("squeue", stdout="")
        self._write_stub("srun")
        result = self._run_script()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("is not UP", result.stderr)

    def test_empty_node_state_fails(self):
        self._write_stub("command")
        self._write_stub("systemctl", stdout="")
        self._write_stub("scontrol", stdout="Slurmctld(primary) at head is UP")
        self._write_stub("sinfo", stdout="")
        self._write_stub("squeue", stdout="")
        self._write_stub("srun")
        result = self._run_script()
        self.assertNotEqual(0, result.returncode)

    def test_non_idle_state_fails(self):
        self._write_stub("command")
        self._write_stub("systemctl", stdout="")
        self._write_stub("scontrol", stdout="Slurmctld(primary) at head is UP")
        self._write_stub("sinfo", stdout="down")
        self._write_stub("squeue", stdout="")
        self._write_stub("srun")
        result = self._run_script()
        self.assertNotEqual(0, result.returncode)

    def test_multiple_idle_states_normalize_to_one(self):
        self._write_stub("command")
        self._write_stub("systemctl", stdout="")
        self._write_stub("scontrol", stdout="Slurmctld(primary) at head is UP")
        self._write_stub("sinfo", stdout="idle\nidle\nidle")
        self._write_stub("squeue", stdout="")
        self._write_stub("srun")
        result = self._run_script()
        self.assertEqual(0, result.returncode)

    def test_mixed_states_fails(self):
        self._write_stub("command")
        self._write_stub("systemctl", stdout="")
        self._write_stub("scontrol", stdout="Slurmctld(primary) at head is UP")
        self._write_stub("sinfo", stdout="idle\ndown")
        self._write_stub("squeue", stdout="")
        self._write_stub("srun")
        result = self._run_script()
        self.assertNotEqual(0, result.returncode)

    def test_inactive_service_fails(self):
        self._write_stub("command")
        self._write_stub("systemctl", exit_code=1)
        self._write_stub("scontrol", stdout="")
        self._write_stub("sinfo", stdout="")
        self._write_stub("squeue", stdout="")
        self._write_stub("srun")
        result = self._run_script()
        self.assertNotEqual(0, result.returncode)


class TestRunSlurmProbe(unittest.TestCase):
    """Python orchestration of the Slurm probe."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        from kivedevel import checks as checks
        import importlib
        importlib.reload(checks)
        return checks

    def test_zero_return_passes(self):
        checks = self._import()
        self.cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="ok\n")
        with mock.patch.object(checks, "_print_slurm_diagnostics") as mock_diag:
            checks._run_slurm_probe(self.cmds, "test")
        mock_diag.assert_not_called()

    def test_nonzero_return_fails(self):
        checks = self._import()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1, stdout="", stderr="error",
        )
        with mock.patch.object(checks, "_print_slurm_diagnostics") as mock_diag:
            with self.assertRaises(SystemExit):
                checks._run_slurm_probe(self.cmds, "test")
        mock_diag.assert_called_once()

    def test_uses_smoke_deadline(self):
        checks = self._import()
        self.assertEqual(checks.SMOKE_DEADLINE_SECONDS, 1800)


class TestDeviceExists(unittest.TestCase):
    def test_device_exists_returns_true_when_present(self):
        from kivedevel.checks import _device_exists
        cmds = mock.Mock()
        cmds.incus.output.return_value = "kive-code:\n  type: disk\n"
        self.assertTrue(_device_exists(cmds, "test", "kive-code"))

    def test_device_exists_returns_false_when_absent(self):
        from kivedevel.checks import _device_exists
        cmds = mock.Mock()
        cmds.incus.output.return_value = "other:\n  type: disk\n"
        self.assertFalse(_device_exists(cmds, "test", "kive-code"))
