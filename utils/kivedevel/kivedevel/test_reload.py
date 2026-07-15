"""Tests for utils/dev reload subcommand."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from Kive.utils.kivedevel.kivedevel._test_helpers import (
    MockRunResult,
    make_cmds,
)

_RELOAD = "Kive.utils.kivedevel.kivedevel.reload"


class TestReloadCli(unittest.TestCase):
    """CLI registration and defaults."""

    def test_reload_subcommand_registered(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.reload import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        with self.assertRaises(SystemExit):
            parser.parse_args(["reload", "--help"])

    def test_default_instance_is_kive_minimal(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.reload import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["reload"])
        self.assertEqual(args.instance, "kive-minimal")

    def test_default_root_is_repo_root(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.reload import register_subcommand
        from Kive.utils.kivedevel.kivedevel.shared import default_root
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["reload"])
        self.assertEqual(args.root, default_root())

    def test_default_workdir_under_root(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.reload import register_subcommand
        from Kive.utils.kivedevel.kivedevel.shared import default_root
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["reload"])
        self.assertEqual(args.workdir, default_root() / "tmp~" / "build")

    def test_explicit_instance(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.reload import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["reload", "my-instance"])
        self.assertEqual(args.instance, "my-instance")

    def test_logging_flags(self):
        import argparse
        from Kive.utils.kivedevel.kivedevel.reload import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        for flag in ("--quiet", "--verbose", "--debug"):
            args = parser.parse_args(["reload", flag])
            self.assertTrue(getattr(args, flag.replace("--", "")))


class TestReloadPreconditions(unittest.TestCase):
    """Missing instance, stopped instance, unprovisioned instance."""

    def _make_args(self, **overrides) -> mock.Mock:
        args = mock.Mock(spec=[])
        args.instance = "test-vm"
        args.root = Path("/tmp")
        args.workdir = Path("/tmp")
        args.quiet = False
        args.verbose = False
        args.debug = False
        for k, v in overrides.items():
            setattr(args, k, v)
        return args

    def _import_reload(self):
        import Kive.utils.kivedevel.kivedevel.reload as r
        import importlib
        importlib.reload(r)
        return r

    def test_fails_when_instance_missing(self):
        rl = self._import_reload()
        cmds = make_cmds()
        with mock.patch.object(rl, "instance_exists", return_value=False):
            with self.assertRaises(SystemExit):
                rl.run_reload(self._make_args())

    def test_fails_when_instance_stopped(self):
        rl = self._import_reload()
        cmds = make_cmds()
        with mock.patch.object(rl, "instance_exists", return_value=True):
            with mock.patch.object(rl, "instance_is_running", return_value=False):
                with self.assertRaises(SystemExit):
                    rl.run_reload(self._make_args())

    def test_fails_when_not_provisioned(self):
        rl = self._import_reload()
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=1)
        with mock.patch.object(rl, "instance_exists", return_value=True):
            with mock.patch.object(rl, "instance_is_running", return_value=True):
                from Kive.utils.kivedevel.kivedevel.shared import instance_exists, instance_is_running
                with mock.patch.object(rl, "instance_exists", return_value=True):
                    with mock.patch.object(rl, "instance_is_running", return_value=True):
                        with self.assertRaises(SystemExit):
                            rl.run_reload(self._make_args())

    def test_fails_when_dev_service_missing(self):
        rl = self._import_reload()
        cmds = make_cmds()
        se = iter([
            MockRunResult(returncode=0),  # test -d /usr/local/share/Kive
            MockRunResult(returncode=1, stderr="not-found"),  # systemctl cat kive-dev-web
        ])
        cmds.incus.run.side_effect = se
        with mock.patch.object(rl, "instance_exists", return_value=True):
            with mock.patch.object(rl, "instance_is_running", return_value=True):
                with self.assertRaises(SystemExit):
                    rl.run_reload(self._make_args())


class TestReloadSnapshot(unittest.TestCase):
    """Host snapshot and transfer."""

    def test_host_source_snapshot_uses_rsync(self):
        rl = self._import_reload()
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            with mock.patch("subprocess.run") as mock_run:
                mock_run.return_value = MockRunResult(returncode=0)
                snapshot = rl._host_source_snapshot(workdir, workdir)
            self.assertTrue(str(snapshot).startswith(str(workdir)))
            call_args = mock_run.call_args[0][0]
            self.assertIn("rsync", call_args)
            self.assertIn("--delete", call_args)

    def test_transfer_snapshot_uses_incus_file_push(self):
        rl = self._import_reload()
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=0)
        guest_path = rl._transfer_snapshot(cmds, "test-vm", Path("/tmp/snap"))
        self.assertIn(".Kive.reload-", guest_path)
        push_calls = [c for c in cmds.incus.run.call_args_list if "push" in str(c)]
        self.assertGreaterEqual(len(push_calls), 1)

    def _import_reload(self):
        import Kive.utils.kivedevel.kivedevel.reload as r
        import importlib
        importlib.reload(r)
        return r


class TestReloadOrdering(unittest.TestCase):
    """Correct ordering: stage → validate → stop → switch → start → health."""

    def test_run_reload_ordering(self):
        rl = self._import_reload()
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=0)
        call_log = []
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            args = mock.Mock(spec=[])
            args.instance = "test"
            args.root = workdir
            args.workdir = workdir
            args.quiet = True
            args.verbose = False
            args.debug = False
            with mock.patch.object(rl, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(rl, "instance_exists", return_value=True):
                    with mock.patch.object(rl, "instance_is_running", return_value=True):
                        with mock.patch.object(rl, "_host_source_snapshot",
                                                return_value=workdir / "snap"):
                            with mock.patch.object(rl, "_transfer_snapshot",
                                                   return_value="/staging"):
                                with mock.patch.object(rl, "_validate_guest_tree"):
                                    with mock.patch.object(rl, "_fix_ownership"):
                                        with mock.patch.object(rl, "_stop_services",
                                                                side_effect=lambda *a: call_log.append("stop")):
                                            with mock.patch.object(rl, "_switch_tree",
                                                                    side_effect=lambda *a: call_log.append("switch") or "/backup"):
                                                with mock.patch.object(rl, "_start_services",
                                                                        side_effect=lambda *a: call_log.append("start")):
                                                    with mock.patch.object(rl, "_health_check",
                                                                            side_effect=lambda *a: call_log.append("health")):
                                                        with mock.patch.object(rl, "_cleanup_stale"):
                                                            rl.run_reload(args)
        self.assertEqual(call_log, ["stop", "switch", "start", "health"])

    def _import_reload(self):
        import Kive.utils.kivedevel.kivedevel.reload as r
        import importlib
        importlib.reload(r)
        return r


class TestReloadNoInstanceLifecycleCommands(unittest.TestCase):
    """Assert that reload never restarts, stops, or recreates the instance."""

    def test_no_instance_lifecycle_commands(self):
        rl = self._import_reload()
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=0)
        dangerous = ["stop", "restart", "delete", "create"]
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            args = mock.Mock(spec=[])
            args.instance = "test"
            args.root = workdir
            args.workdir = workdir
            args.quiet = True
            args.verbose = False
            args.debug = False
            before_count = len(cmds.incus.run.call_args_list)
            with mock.patch.object(rl, "Cmds") as mock_cmds_cls:
                mock_cmds_cls.create.return_value = cmds
                with mock.patch.object(rl, "instance_exists", return_value=True):
                    with mock.patch.object(rl, "instance_is_running", return_value=True):
                        with mock.patch.object(rl, "_host_source_snapshot",
                                                return_value=workdir / "snap"):
                            with mock.patch.object(rl, "_transfer_snapshot",
                                                   return_value="/staging"):
                                with mock.patch.object(rl, "_validate_guest_tree"):
                                    with mock.patch.object(rl, "_fix_ownership"):
                                        with mock.patch.object(rl, "_stop_services"):
                                            with mock.patch.object(rl, "_switch_tree",
                                                                    return_value="/backup"):
                                                with mock.patch.object(rl, "_start_services"):
                                                    with mock.patch.object(rl, "_health_check"):
                                                        with mock.patch.object(rl, "_cleanup_stale"):
                                                            rl.run_reload(args)
            after_calls = cmds.incus.run.call_args_list[before_count:]
            for call in after_calls:
                argv = " ".join(call[0][0]) if call[0] else ""
                for cmd in dangerous:
                    self.assertNotIn(cmd, argv.split()[0] if argv else "",
                                     f"reload issued dangerous command: {argv}")

    def _import_reload(self):
        import Kive.utils.kivedevel.kivedevel.reload as r
        import importlib
        importlib.reload(r)
        return r


class TestCloudInitSystemdUnit(unittest.TestCase):
    """Generated cloud-init contains the systemd unit and not the old nohup."""

    def test_provision_cloud_init_has_systemd_unit(self):
        source_path = Path(__file__).resolve().parents[4] / "Kive" / "utils" / "kivedevel" / "kivedevel" / "build_vm" / "cloud_init.py"
        text = source_path.read_text()
        self.assertIn("kive-dev-web.service", text)
        self.assertIn("--noreload", text)
        self.assertNotIn("nohup.*manage.py runserver", text)
