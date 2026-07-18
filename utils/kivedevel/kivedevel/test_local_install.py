"""Tests for smoke-local-install orchestration: build, validate, test-api sequencing."""

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
)



class TestSmokeLocalInstall(unittest.TestCase):
    """smoke-local-install orchestrates build-vm, validate-vm, test-api in sequence."""

    def _make_args(self, **overrides) -> mock.Mock:
        args = mock.Mock(spec=[])
        args.workdir = Path("/tmp")
        args.instance = "ci-smoke"
        args.instance_type = "vm"
        args.vm_network = ""
        args.quiet = False
        args.verbose = False
        args.debug = False
        for k, v in overrides.items():
            setattr(args, k, v)
        return args

    def test_invokes_build_then_validate_then_test_api(self):
        from Kive.utils.kivedevel.kivedevel import local_install as li
        from Kive.utils.kivedevel.kivedevel._test_helpers import MockRunResult, make_cmds
        from Kive.utils.kivedevel.kivedevel.local_install import run_smoke_local_install
        args = self._make_args()
        call_order = []

        cmds = make_cmds()
        # First exec tests for marker (reload 1 -> found), second (reload 2 -> gone).
        cmds.incus.run.side_effect = [
            MockRunResult(returncode=0),  # first reload: marker found
            MockRunResult(returncode=1),  # second reload: marker gone
        ]

        with mock.patch(
            "Kive.utils.kivedevel.kivedevel.local_install.run_build_vm",
            side_effect=lambda a: call_order.append("build"),
        ) as mock_build:
            with mock.patch(
                "Kive.utils.kivedevel.kivedevel.checks.run_validate_vm",
                side_effect=lambda a: call_order.append("validate"),
            ) as mock_validate:
                with mock.patch(
                    "Kive.utils.kivedevel.kivedevel.checks.run_test_api",
                    side_effect=lambda a: call_order.append("test-api"),
                ) as mock_test_api:
                    with mock.patch(
                        "Kive.utils.kivedevel.kivedevel.local_install.reload_mod.run_reload",
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
        from Kive.utils.kivedevel.kivedevel.local_install import run_smoke_local_install
        args = self._make_args()

        with mock.patch(
            "Kive.utils.kivedevel.kivedevel.local_install.run_build_vm",
            side_effect=RuntimeError("build failed"),
        ):
            with mock.patch(
                "Kive.utils.kivedevel.kivedevel.checks.run_validate_vm",
            ) as mock_validate:
                with self.assertRaises(RuntimeError):
                    run_smoke_local_install(args)
                mock_validate.assert_not_called()
