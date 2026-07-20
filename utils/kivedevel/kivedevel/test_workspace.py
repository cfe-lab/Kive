"""Tests for workspace attachment, profile root disk sizing, and rsync exclusions."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock


from kivedevel._test_helpers import (
    MockRunResult,
    make_cmds,
)


class TestProfileRootDisk(unittest.TestCase):
    """Profile root disk creation and resizing."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        from kivedevel.build_vm import incus as incus
        import importlib
        importlib.reload(incus)
        return incus

    def test_empty_devices_adds_root_disk(self):
        incus = self._import()
        self.cmds.incus.output.return_value = "{}\n"
        incus.ensure_profile_with_root_disk(self.cmds, "default", "default", "60GiB")
        add_calls = [c for c in self.cmds.incus.run.call_args_list
                     if c[0][0][:4] == ["profile", "device", "add", "default"]]
        self.assertGreaterEqual(len(add_calls), 1)

    def test_existing_root_disk_is_idempotent_when_size_match(self):
        incus = self._import()
        self.cmds.incus.output.return_value = (
            "root:\n  path: /\n  pool: default\n  size: 60GiB\n  type: disk\n"
        )
        incus.ensure_profile_with_root_disk(self.cmds, "default", "default", "60GiB")
        set_calls = [c for c in self.cmds.incus.run.call_args_list
                     if c[0][0][:4] == ["profile", "device", "set", "default"]]
        self.assertEqual(len(set_calls), 0)

    def test_existing_root_disk_enlarged_when_too_small(self):
        incus = self._import()
        self.cmds.incus.output.return_value = (
            "root:\n  path: /\n  pool: default\n  size: 10GiB\n  type: disk\n"
        )
        incus.ensure_profile_with_root_disk(self.cmds, "default", "default", "60GiB")
        set_calls = [c for c in self.cmds.incus.run.call_args_list
                     if c[0][0][:4] == ["profile", "device", "set", "default"]]
        self.assertGreaterEqual(len(set_calls), 1)

    def test_incompatible_root_device_fails(self):
        incus = self._import()
        self.cmds.incus.output.return_value = (
            "root:\n  path: /\n  pool: different\n  type: not-disk\n"
        )
        with self.assertRaises(SystemExit):
            incus.ensure_profile_with_root_disk(self.cmds, "default", "default", "60GiB")


class TestWorkspaceRsyncExclusions(unittest.TestCase):
    """Workspace rsync excludes tmp and own workdir."""

    def test_workspace_rsync_excludes_tmp_and_workdir(self):
        from kivedevel.build_vm.workspace import _workspace_rsync_args
        root = Path("/repo")
        workdir = root / "tmp~" / "build"
        dest = Path("/dest")
        args = _workspace_rsync_args(root, workdir, dest)
        self.assertIn("--exclude=/tmp/", args)
