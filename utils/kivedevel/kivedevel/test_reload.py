"""Tests for utils/dev reload subcommand."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from kivedevel._test_helpers import MockRunResult, make_cmds


class TestReloadCli(unittest.TestCase):
    """CLI registration and defaults."""

    def test_reload_subcommand_registered(self):
        import argparse
        from kivedevel.reload import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        with self.assertRaises(SystemExit):
            parser.parse_args(["reload", "--help"])

    def test_default_instance_is_kive_minimal(self):
        import argparse
        from kivedevel.reload import register_subcommand
        from kivedevel.shared import default_root
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["reload"])
        self.assertEqual(args.instance, "kive-minimal")
        self.assertEqual(args.root, default_root())
        self.assertEqual(args.workdir, default_root() / "tmp~" / "build")

    def test_explicit_instance(self):
        import argparse
        from kivedevel.reload import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["reload", "my-instance"])
        self.assertEqual(args.instance, "my-instance")


class TestReloadPreconditions(unittest.TestCase):
    """Missing instance, stopped instance, unprovisioned instance."""

    def _call_reload(self, **patches):
        import kivedevel.reload as rl
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=0)
        args = mock.Mock(spec=[])
        args.instance = "test"
        args.root = Path("/tmp")
        args.workdir = Path("/tmp")
        args.quiet = True
        args.verbose = False
        args.debug = False
        with mock.patch.object(rl, "Cmds") as mc:
            mc.create.return_value = cmds
            for key, val in patches.items():
                if val is None:
                    continue
                getattr(rl, key)
            rl.run_reload(args)
        return cmds

    def _make_args(self, wd):
        args = mock.Mock(spec=[])
        args.instance = "test"
        args.root = wd
        args.workdir = wd
        args.quiet = True
        args.verbose = False
        args.debug = False
        return args

    def test_fails_when_instance_missing(self):
        import kivedevel.reload as rl
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            wd = Path(tmp)
            with mock.patch.object(rl, "instance_exists", return_value=False):
                with mock.patch.object(rl, "Cmds") as mc:
                    mc.create.return_value = make_cmds()
                    with self.assertRaises(SystemExit):
                        rl.run_reload(self._make_args(wd))

    def test_fails_when_instance_stopped(self):
        import kivedevel.reload as rl
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            wd = Path(tmp)
            with mock.patch.object(rl, "instance_exists", return_value=True):
                with mock.patch.object(rl, "instance_is_running", return_value=False):
                    with mock.patch.object(rl, "Cmds") as mc:
                        mc.create.return_value = make_cmds()
                        with self.assertRaises(SystemExit):
                            rl.run_reload(self._make_args(wd))

    def test_fails_when_not_provisioned(self):
        import kivedevel.reload as rl
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=1)
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            wd = Path(tmp)
            with mock.patch.object(rl, "instance_exists", return_value=True):
                with mock.patch.object(rl, "instance_is_running", return_value=True):
                    with mock.patch.object(rl, "Cmds") as mc:
                        mc.create.return_value = cmds
                        with self.assertRaises(SystemExit):
                            rl.run_reload(self._make_args(wd))


class TestReloadOrdering(unittest.TestCase):
    """Verify correct operation ordering."""

    def test_successful_ordering(self):
        import kivedevel.reload as rl
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=0)
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
            call_log = []
            with mock.patch.object(rl, "Cmds") as mc:
                mc.create.return_value = cmds
                with mock.patch.object(rl, "instance_exists", return_value=True):
                    with mock.patch.object(rl, "instance_is_running", return_value=True):
                        with mock.patch.object(rl, "_host_source_snapshot",
                                                side_effect=lambda *a: call_log.append("snapshot")):
                            with mock.patch.object(rl, "_transfer_snapshot",
                                                    return_value="/staging",
                                                    side_effect=lambda *a: call_log.append("transfer") or "/staging"):
                                with mock.patch.object(rl, "_validate_staged_tree",
                                                        side_effect=lambda *a: call_log.append("validate")):
                                    with mock.patch.object(rl, "_fix_ownership",
                                                            side_effect=lambda *a: call_log.append("chown")):
                                        with mock.patch.object(rl, "_stop_services",
                                                                side_effect=lambda *a: call_log.append("stop")):
                                            with mock.patch.object(rl, "_install_tree",
                                                                    side_effect=lambda *a: call_log.append("install")):
                                                with mock.patch.object(rl, "_start_services",
                                                                        side_effect=lambda *a: call_log.append("start")):
                                                    with mock.patch.object(rl, "_health_check",
                                                                            side_effect=lambda *a: call_log.append("health")):
                                                        rl.run_reload(args)
        self.assertEqual(call_log, ["snapshot", "transfer", "validate", "chown", "stop", "install", "start", "health"])


class TestReloadNoBackupPaths(unittest.TestCase):
    """No .Kive.backup-* path or backup helper remains."""

    def test_no_backup_paths(self):
        import kivedevel.reload as rl
        src = Path(rl.__file__).read_text()
        self.assertNotIn(".Kive.backup-", src)
        self.assertNotIn("_restore_tree", src)
        self.assertNotIn("_move_active_to_backup", src)
        self.assertNotIn("_cleanup_stale", src)
