"""Tests for smoke-local-install orchestration: build, validate, test-api sequencing."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from Kive.utils.kivedevel.kivedevel._test_helpers import (
    MockRunResult,
    add_source_path,
)

add_source_path()


class TestSmokeLocalInstall(unittest.TestCase):
    """smoke-local-install orchestrates build-vm, validate-vm, test-api in sequence."""

    def test_invokes_build_then_validate_then_test_api(self):
        from Kive.utils.kivedevel.kivedevel.local_install import run_smoke_local_install
        args = mock.Mock(
            workdir=Path("/tmp"),
            instance="ci-smoke",
            instance_type="vm",
            vm_network="",
            quiet=False,
            verbose=False,
            debug=False,
        )
        call_order = []

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
                    run_smoke_local_install(args)

        self.assertEqual(call_order, ["build", "validate", "test-api"])
        self.assertEqual(mock_build.call_count, 1)
        self.assertEqual(mock_validate.call_count, 1)
        self.assertEqual(mock_test_api.call_count, 1)

    def test_failure_in_build_stops_sequence(self):
        from Kive.utils.kivedevel.kivedevel.local_install import run_smoke_local_install
        args = mock.Mock(
            workdir=Path("/tmp"),
            instance="ci-smoke",
            instance_type="vm",
            vm_network="",
            quiet=False,
            verbose=False,
            debug=False,
        )

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
