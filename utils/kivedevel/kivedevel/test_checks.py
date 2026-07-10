"""Tests for validate-vm and test-api subcommands: probes, device checks, API validation."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from Kive.utils.kivedevel.kivedevel._test_helpers import (
    MockRunResult,
    add_source_path,
    make_cmds,
)



class TestValidateVm(unittest.TestCase):
    """Instance validation logic for validate-vm."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        import Kive.utils.kivedevel.kivedevel.checks as checks
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
        import Kive.utils.kivedevel.kivedevel.checks as checks
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


class TestSlurmProbe(unittest.TestCase):
    """Slurm probe validation."""

    def setUp(self):
        self.cmds = make_cmds()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout=(
                "=== hostname ===\nhead\n"
                "=== getent hosts head ===\n127.0.0.1\n"
                "=== Slurm services ===\nactive: slurmdbd\nactive: slurmctld\nactive: slurmd\n"
                "=== Slurm commands ===\n/usr/bin/squeue\n/usr/bin/sinfo\n"
            ),
        )

    def _import(self):
        import Kive.utils.kivedevel.kivedevel.checks as checks
        import importlib
        importlib.reload(checks)
        return checks

    def test_valid_slurm_does_not_log_warning(self):
        checks = self._import()
        with mock.patch.object(checks.logger, "warning") as mock_warn:
            checks._run_slurm_probe(self.cmds, "test")
            mock_warn.assert_not_called()

    def test_slurm_probe_logs_warning_for_wrong_hostname(self):
        checks = self._import()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=0,
            stdout=(
                "=== hostname ===\nwrong\n"
                "=== getent hosts head ===\nnot found\n"
                "=== Slurm services ===\ninactive: slurmctld\n"
                "=== Slurm commands ===\n/usr/bin/squeue\n"
            ),
        )
        with mock.patch.object(checks.logger, "warning") as mock_warn:
            checks._run_slurm_probe(self.cmds, "test")
            self.assertGreaterEqual(mock_warn.call_count, 1)


class TestDeviceExists(unittest.TestCase):
    def test_device_exists_returns_true_when_present(self):
        from Kive.utils.kivedevel.kivedevel.checks import _device_exists
        cmds = mock.Mock()
        cmds.incus.output.return_value = "kive-code:\n  type: disk\n"
        self.assertTrue(_device_exists(cmds, "test", "kive-code"))

    def test_device_exists_returns_false_when_absent(self):
        from Kive.utils.kivedevel.kivedevel.checks import _device_exists
        cmds = mock.Mock()
        cmds.incus.output.return_value = "other:\n  type: disk\n"
        self.assertFalse(_device_exists(cmds, "test", "kive-code"))
