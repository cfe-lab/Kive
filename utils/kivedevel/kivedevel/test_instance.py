"""Tests for instance creation: VM/container lifecycle, QEMU capability, privileged fallback."""

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



class TestEnsureInstance(unittest.TestCase):
    """incus instance creation and validation."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        import Kive.utils.kivedevel.kivedevel.build_vm.instance as inst
        import importlib
        importlib.reload(inst)
        return inst

    def test_vm_unsupported_raises_runtime_error(self):
        inst = self._import()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1,
            stderr='Instance type "virtual-machine" is not supported',
        )
        with self.assertRaises(RuntimeError):
            inst.ensure_instance(self.cmds, "test", "vm", "default", "1", "1GB")

    def test_vm_unsupported_does_not_fallback_to_container(self):
        inst = self._import()
        self.cmds.incus.run.return_value = MockRunResult(
            returncode=1,
            stderr='Instance type "virtual-machine" is not supported',
        )
        with mock.patch.object(inst, "instance_exists", return_value=False):
            with self.assertRaises(RuntimeError):
                inst.ensure_instance(self.cmds, "test", "vm", "default", "1", "1GB")

    def test_container_mode_still_retries_privileged(self):
        inst = self._import()
        self.cmds.incus.run.side_effect = [
            MockRunResult(returncode=1, stderr="No uid/gid allocation configured"),
            MockRunResult(returncode=0),
        ]
        with mock.patch.object(inst, "instance_exists", return_value=False):
            result_type = inst.ensure_instance(self.cmds, "test", "container", "default", "1", "1GB")
        self.assertEqual(result_type[1], "container")

    def test_container_privileged_retry_still_fails(self):
        inst = self._import()
        self.cmds.incus.run.side_effect = [
            MockRunResult(returncode=1, stderr="No uid/gid allocation configured"),
            MockRunResult(returncode=1, stderr="still failed"),
        ]
        with mock.patch.object(inst, "instance_exists", return_value=False):
            with self.assertRaises(RuntimeError):
                inst.ensure_instance(self.cmds, "test", "container", "default", "1", "1GB")


class TestVmCapabilityCheck(unittest.TestCase):
    """QEMU system emulator detection for VM mode."""

    def test_qemu_found_for_x86_64(self):
        import Kive.utils.kivedevel.kivedevel.build_vm.runner as runner_mod
        with mock.patch.object(runner_mod, "shutil") as mock_shutil:
            mock_shutil.which.return_value = "/usr/bin/qemu-system-x86_64"
            with mock.patch("platform.machine", return_value="x86_64"):
                result = runner_mod.qemu_system_command_for_host()
                self.assertEqual(result, "qemu-system-x86_64")
                mock_shutil.which.assert_called_once_with("qemu-system-x86_64")

    def test_qemu_missing_for_x86_64(self):
        import Kive.utils.kivedevel.kivedevel.build_vm.runner as runner_mod
        with mock.patch.object(runner_mod, "shutil") as mock_shutil:
            mock_shutil.which.return_value = None
            with mock.patch("platform.machine", return_value="x86_64"):
                self.assertIsNone(runner_mod.qemu_system_command_for_host())

    def test_check_vm_capability_passes_when_qemu_present(self):
        import Kive.utils.kivedevel.kivedevel.build_vm.runner as runner_mod
        with mock.patch.object(runner_mod, "shutil") as mock_shutil:
            mock_shutil.which.return_value = "/usr/bin/qemu-system-x86_64"
            with mock.patch("platform.machine", return_value="x86_64"):
                try:
                    runner_mod._check_vm_capability()
                except SystemExit:
                    self.fail("_check_vm_capability raised SystemExit when QEMU is present")

    def test_check_vm_capability_fails_when_qemu_missing(self):
        import Kive.utils.kivedevel.kivedevel.build_vm.runner as runner_mod
        with mock.patch.object(runner_mod, "shutil") as mock_shutil:
            mock_shutil.which.return_value = None
            with mock.patch("platform.machine", return_value="x86_64"):
                with self.assertRaises(SystemExit):
                    runner_mod._check_vm_capability()
