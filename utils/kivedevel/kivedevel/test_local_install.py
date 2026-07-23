"""Tests for smoke-local-install orchestration: build, validate, test-api sequencing."""

from __future__ import annotations

import argparse
import unittest
from pathlib import Path
from unittest import mock

from kivedevel._test_helpers import (
    MockRunResult,
)


class TestSmokeLocalInstall(unittest.TestCase):
    """smoke-local-install orchestrates build-vm, validate-vm, test-api in sequence."""

    def _make_args(self, **overrides) -> mock.Mock:
        args = mock.Mock(spec=[])
        args.workdir = Path("/tmp")
        args.instance = "ci-smoke"
        args.instance_type = "vm"
        args.vm_network = None
        args.quiet = False
        args.verbose = False
        args.debug = False
        for k, v in overrides.items():
            setattr(args, k, v)
        return args

    def test_invokes_build_then_validate_then_test_api(self):
        from kivedevel import local_install as li
        from kivedevel._test_helpers import make_cmds
        from kivedevel.local_install import run_smoke_local_install
        args = self._make_args()
        call_order = []

        cmds = make_cmds()
        cmds.incus.run.side_effect = [
            MockRunResult(returncode=0),
            MockRunResult(returncode=1),
        ]

        with mock.patch(
            "kivedevel.local_install.get_vm_ipv4",
            return_value="10.77.77.100",
        ):
            with mock.patch(
                "kivedevel.local_install.run_build_vm",
                side_effect=lambda a: call_order.append("build"),
            ) as mock_build:
                with mock.patch(
                    "kivedevel.checks.run_validate_vm",
                    side_effect=lambda a: call_order.append("validate"),
                ) as mock_validate:
                    with mock.patch(
                        "kivedevel.checks.run_test_api",
                        side_effect=lambda a: call_order.append("test-api"),
                    ) as mock_test_api:
                        with mock.patch(
                            "kivedevel.local_install.reload_mod.run_reload",
                            side_effect=lambda a: call_order.append("reload"),
                        ) as mock_reload:
                            with mock.patch.object(
                                li, "Cmds",
                            ) as mock_cmds_cls:
                                mock_cmds_cls.create.return_value = cmds
                                run_smoke_local_install(args)

            self.assertEqual(call_order, ["build", "validate", "test-api", "reload", "test-api", "reload"])
            self.assertEqual(mock_build.call_count, 1)
            self.assertEqual(mock_validate.call_count, 1)
            self.assertEqual(mock_test_api.call_count, 2)
            self.assertEqual(mock_reload.call_count, 2)

    def test_failure_in_build_stops_sequence(self):
        from kivedevel.local_install import run_smoke_local_install
        args = self._make_args()

        with mock.patch(
            "kivedevel.local_install.run_build_vm",
            side_effect=RuntimeError("build failed"),
        ):
            with mock.patch(
                "kivedevel.checks.run_validate_vm",
            ) as mock_validate:
                with self.assertRaises(RuntimeError):
                    run_smoke_local_install(args)
                mock_validate.assert_not_called()

    def test_build_vm_args_host_interface_is_none_by_default(self):
        from kivedevel.local_install import _build_vm_args
        args = _build_vm_args("test", "vm", Path("/tmp"), debug=False)
        self.assertIsNone(args.host_interface)

    def test_build_vm_args_vm_network_is_none_by_default(self):
        from kivedevel.local_install import _build_vm_args
        args = _build_vm_args("test", "vm", Path("/tmp"), debug=False)
        self.assertIsNone(args.vm_network)

    def test_build_vm_args_container_host_interface_is_none(self):
        from kivedevel.local_install import _build_vm_args
        args = _build_vm_args("test", "container", Path("/tmp"), debug=False)
        self.assertIsNone(args.host_interface)

    def test_smoke_local_install_passes_vm_network_none_by_default(self):
        from kivedevel.local_install import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["smoke-local-install"])
        self.assertIsNone(args.vm_network)


class TestBuildVmCrossModeValidation(unittest.TestCase):
    """Default-mode args reach run_build_vm with correct types."""

    def test_default_vm_networking_reaches_run_build_vm(self):
        """Default VM smoke reaches run_build_vm with host_interface=None and vm_network=None."""
        from kivedevel.build_vm.runner import run_build_vm
        from kivedevel.local_install import _build_vm_args

        build_args = _build_vm_args("ci-smoke", "vm", Path("/tmp"), debug=False)
        self.assertIsNone(build_args.host_interface)
        self.assertIsNone(build_args.vm_network)

        with mock.patch("kivedevel.build_vm.runner.Cmds.create"):
            with mock.patch("kivedevel.build_vm.runner.ensure_incus_daemon"):
                with mock.patch("kivedevel.build_vm.runner.ensure_storage_pool"):
                    with mock.patch("kivedevel.build_vm.runner.ensure_profile_with_root_disk"):
                        with mock.patch("kivedevel.build_vm.runner._run_build_vm_vm") as mock_vm:
                            with mock.patch("kivedevel.build_vm.runner.logger"):
                                run_build_vm(build_args)

        cfg_arg = mock_vm.call_args[0][0]
        self.assertEqual(cfg_arg.vm_network, "kive-lab-br")
        self.assertIsNone(cfg_arg.host_interface)

    def test_default_container_networking_reaches_run_build_vm(self):
        """Default container smoke reaches run_build_vm with vm_network=None and host_interface=None."""
        from kivedevel.build_vm.runner import run_build_vm
        from kivedevel.local_install import _build_vm_args

        build_args = _build_vm_args("ci-smoke", "container", Path("/tmp"), debug=False)
        self.assertIsNone(build_args.host_interface)
        self.assertIsNone(build_args.vm_network)

        with mock.patch("kivedevel.build_vm.runner.Cmds.create"):
            with mock.patch("kivedevel.build_vm.runner.ensure_incus_daemon"):
                with mock.patch("kivedevel.build_vm.runner.ensure_storage_pool"):
                    with mock.patch("kivedevel.build_vm.runner.ensure_profile_with_root_disk"):
                        with mock.patch("kivedevel.build_vm.runner._run_build_vm_container") as mock_ctr:
                            with mock.patch("kivedevel.build_vm.runner.logger"):
                                run_build_vm(build_args)

        cfg_arg = mock_ctr.call_args[0][0]
        self.assertIsNone(cfg_arg.host_interface)
        self.assertIsNone(cfg_arg.vm_network)
