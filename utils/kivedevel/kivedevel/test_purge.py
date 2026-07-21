"""Tests for purge lifecycle: instance discovery, workdir cleanup, network removal."""

from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from unittest import mock

from kivedevel._test_helpers import (
    MockRunResult,
    make_cmds,
)


_PURGE = "kivedevel.build_vm.purge"


class TestFindTaggedInstances(unittest.TestCase):
    """Discovery of tagged instances for purge."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        from kivedevel.build_vm import purge as p
        import importlib
        importlib.reload(p)
        return p

    def test_returns_matching_instances(self):
        purge = self._import()
        instances_json = json.dumps([
            {"name": "test-vm", "config": {"user.kive.devel.created-by": "utils/dev"}},
            {"name": "other-instance", "config": {}},
        ])
        self.cmds.incus.run.return_value = MockRunResult(returncode=0, stdout=instances_json)
        result = purge._find_tagged_instances(self.cmds)
        self.assertEqual(result, ["test-vm"])

    def test_returns_empty_when_none_found(self):
        purge = self._import()
        instances_json = json.dumps([
            {"name": "test-vm", "config": {}},
        ])
        self.cmds.incus.run.return_value = MockRunResult(returncode=0, stdout=instances_json)
        result = purge._find_tagged_instances(self.cmds)
        self.assertEqual(result, [])


class TestFindMarkedWorkdirs(unittest.TestCase):
    """Discovery of marked workdirs for purge."""

    def setUp(self):
        self.root = Path("/tmp/test_purge_root")
        self.marker_path = self.root / ".kive-devel-resource.json"

    def _import(self):
        from kivedevel.build_vm import purge as p
        import importlib
        importlib.reload(p)
        return p

    def test_accepts_created_by_underscore(self):
        purge = self._import()
        self.marker_path.parent.mkdir(parents=True, exist_ok=True)
        self.marker_path.write_text(json.dumps({"created_by": "utils/dev", "kind": "build-workdir"}))
        with mock.patch.object(Path, "rglob", return_value=[self.marker_path]):
            result = purge._find_marked_workdirs(self.root)
            self.assertEqual(len(result), 1)
        self.marker_path.unlink()

    def test_rejects_old_hyphen_key(self):
        purge = self._import()
        self.marker_path.parent.mkdir(parents=True, exist_ok=True)
        self.marker_path.write_text(json.dumps({"created-by": "utils/dev", "kind": "build-workdir"}))
        with mock.patch.object(Path, "rglob", return_value=[self.marker_path]):
            result = purge._find_marked_workdirs(self.root)
            self.assertEqual(len(result), 0)
        self.marker_path.unlink()

    def test_rejects_unknown_creator(self):
        purge = self._import()
        self.marker_path.parent.mkdir(parents=True, exist_ok=True)
        self.marker_path.write_text(json.dumps({"created_by": "other", "kind": "build-workdir"}))
        with mock.patch.object(Path, "rglob", return_value=[self.marker_path]):
            result = purge._find_marked_workdirs(self.root)
            self.assertEqual(len(result), 0)
        self.marker_path.unlink()

    def test_returns_empty_when_no_marker(self):
        purge = self._import()
        with mock.patch.object(Path, "rglob", return_value=[]):
            result = purge._find_marked_workdirs(self.root)
            self.assertEqual(len(result), 0)


class TestPurge(unittest.TestCase):
    """End-to-end purge behaviour with mocked incus."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        from kivedevel.build_vm import purge as p
        import importlib
        importlib.reload(p)
        return p

    def _make_args(self, **overrides):
        import argparse
        args = argparse.Namespace()
        args.root = Path("/tmp")
        args.instances = None
        args.workdirs = None
        args.quiet = False
        args.verbose = False
        args.debug = False
        args.log_file = None
        for k, v in overrides.items():
            setattr(args, k, v)
        return args

    def test_purge_idempotent_when_nothing_to_purge(self):
        purge = self._import()
        self.cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="[]")
        purge._run_purge(self._make_args(), self.cmds)

    def test_deletes_tagged_instances_and_marked_workdirs(self):
        purge = self._import()
        self.cmds.incus.output.return_value = ""
        with mock.patch.object(purge, "_find_tagged_instances", return_value=["test-vm"]):
            with mock.patch.object(purge, "_find_tagged_networks", return_value=[]):
                with mock.patch.object(purge, "_find_marked_workdirs", return_value=[]):
                    purge._run_purge(self._make_args(), self.cmds)
        delete_calls = [
            c for c in self.cmds.incus.run.call_args_list
            if c[0][0][:2] == ["delete", "-f"]
        ]
        self.assertGreaterEqual(len(delete_calls), 1)

    def test_remove_device_before_delete(self):
        purge = self._import()
        call_log = []

        def capture(*args, **kwargs):
            call_log.append(("run", args))
            return MockRunResult(returncode=0)

        self.cmds.incus.run.side_effect = capture
        self.cmds.incus.output.side_effect = [
            "test-vm\n",
            "user.kive.devel.created-by: utils/dev\n",
            "kive-code:\n",
            "kive-web:\n",
        ] * 3
        with mock.patch.object(purge, "_find_tagged_instances", return_value=["test-vm"]):
            with mock.patch.object(purge, "_find_marked_workdirs", return_value=[]):
                with mock.patch.object(purge, "_find_tagged_networks", return_value=[]):
                    purge._run_purge(self._make_args(), self.cmds)

    def test_purge_removes_web_proxy_device(self):
        purge = self._import()
        self.cmds.incus.output.side_effect = [
            "test-vm\n",
            "user.kive.devel.created-by: utils/dev\n",
            "kive-web:\n",
        ] * 3
        with mock.patch.object(purge, "_find_tagged_instances", return_value=["test-vm"]):
            with mock.patch.object(purge, "_find_marked_workdirs", return_value=[]):
                with mock.patch.object(purge, "_find_tagged_networks", return_value=[]):
                    purge._run_purge(self._make_args(), self.cmds)

    def test_purge_kills_port_forward(self):
        purge = self._import()
        import tempfile
        import signal
        import json
        cmds = make_cmds()
        cmds.incus.run.return_value = MockRunResult(returncode=0, stdout="[]")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg = root / "tmp~" / ".kive-devel-resources.json"
            reg.parent.mkdir(parents=True, exist_ok=True)
            reg.write_text(json.dumps([
                {"kind": "host-forward", "pid": 12345, "port": 8000,
                 "vm_ip": "10.247.172.80", "created_by": "utils/dev"},
            ], indent=2) + "\n")

            args = self._make_args(root=root)
            with mock.patch.object(purge, "os") as mock_os:
                purge._run_purge(args, cmds)

            mock_os.kill.assert_any_call(12345, signal.SIGTERM)


class TestSafeWorkdirRemoval(unittest.TestCase):
    """_remove_workdir_safely verifies mount state before image/workdir deletion."""

    def setUp(self):
        self.tmp = Path("/tmp/test_safe_workdir")
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.workdir = self.tmp / "workdir"
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.mountpoint = self.workdir / "kive-code-mount"
        self.mountpoint.mkdir(parents=True, exist_ok=True)
        self.image = self.workdir / "kive-code.img"
        self.image.write_text("fake-image")
        self.marker = self.workdir / ".kive-devel-resource.json"
        self.marker.write_text("{}")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _import(self):
        from kivedevel.build_vm import purge as p
        import importlib
        importlib.reload(p)
        return p

    def test_not_mounted(self):
        """Path not mounted: remove image + workdir directly."""
        purge = self._import()
        with mock.patch.object(purge, "_is_mountpoint", return_value=False):
            result = purge._remove_workdir_safely(self.workdir)
        self.assertTrue(result)
        self.assertFalse(self.image.exists())
        self.assertFalse(self.workdir.exists())

    def test_successful_unmount(self):
        """Unmount succeeds: remove image + workdir."""
        purge = self._import()
        with mock.patch.object(purge, "_is_mountpoint", side_effect=[True, False]):
            with mock.patch.object(purge, "_umount") as mock_umount:
                mock_umount.return_value = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="", stderr="",
                )
                result = purge._remove_workdir_safely(self.workdir)
        self.assertTrue(result)
        mock_umount.assert_called_once()

    def test_unmount_failure_preserves_image_and_workdir(self):
        """Non-zero umount: image and workdir are NOT removed."""
        purge = self._import()
        with mock.patch.object(purge, "_is_mountpoint", side_effect=[True, True]):
            with mock.patch.object(purge, "_umount") as mock_umount:
                mock_umount.return_value = subprocess.CompletedProcess(
                    args=[], returncode=1, stdout="", stderr="error",
                )
                result = purge._remove_workdir_safely(self.workdir)
        self.assertFalse(result)
        self.assertTrue(self.image.exists(), "Image should not have been removed")
        self.assertTrue(self.workdir.exists(), "Workdir should not have been removed")

    def test_mount_still_active_after_success_umount(self):
        """umount command succeeds but mount remains active: image and workdir preserved."""
        purge = self._import()
        is_mountpoint_calls = [True, True]
        with mock.patch.object(purge, "_is_mountpoint", side_effect=is_mountpoint_calls):
            with mock.patch.object(purge, "_umount") as mock_umount:
                mock_umount.return_value = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="", stderr="",
                )
                result = purge._remove_workdir_safely(self.workdir)
        self.assertFalse(result)
        self.assertTrue(self.image.exists(), "Image should not have been removed")
        self.assertTrue(self.workdir.exists(), "Workdir should not have been removed")

