"""Tests for workspace attachment, profile root disk sizing, and rsync exclusions."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from Kive.utils.kivedevel.kivedevel._test_helpers import (
    MockRunResult,
    add_source_path,
    make_cmds,
)

add_source_path()


class TestProfileRootDisk(unittest.TestCase):
    """Profile root disk creation and resizing."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        import Kive.utils.kivedevel.kivedevel.build_vm.incus as incus
        import importlib
        importlib.reload(incus)
        return incus

    def _profile_yaml(self, devices_yaml="root:\n  path: /\n  pool: default\n  type: disk\n"):
        import yaml
        return yaml.safe_load(f"""config:
  limits.cpu: "4"
  limits.memory: 8GiB
description: Default Incus profile
devices:
  eth0:
    name: eth0
    network: kive-lab-br
    type: nic
  {devices_yaml}
name: default
""")

    def test_empty_devices_adds_root_disk(self):
        incus = self._import()
        self.cmds.incus.output.return_value = "name: default\ndevices: {}\n"
        incus.ensure_profile_with_root_disk(self.cmds, "default", "60GiB")
        create_calls = [c for c in self.cmds.incus.run.call_args_list if "create" in str(c)]
        self.assertEqual(len(create_calls), 0)

    def test_existing_root_disk_is_idempotent_when_size_match(self):
        incus = self._import()
        profile = self._profile_yaml()
        self.cmds.incus.output.return_value = (
            "devices:\n  root:\n    path: /\n    pool: default\n    size: 60GiB\n    type: disk\n"
        )
        incus.ensure_profile_with_root_disk(self.cmds, "default", "60GiB")
        self.assertIsNotNone(profile)

    def test_existing_root_disk_enlarged_when_too_small(self):
        incus = self._import()
        self.cmds.incus.output.return_value = (
            "devices:\n  root:\n    path: /\n    pool: default\n    size: 10GiB\n    type: disk\n"
        )
        incus.ensure_profile_with_root_disk(self.cmds, "default", "60GiB")
        set_calls = [c for c in self.cmds.incus.run.call_args_list
                     if c[0][0][:4] == ["profile", "device", "set", "default"]]
        self.assertGreaterEqual(len(set_calls), 1)

    def test_incompatible_root_device_fails(self):
        incus = self._import()
        self.cmds.incus.output.return_value = (
            "devices:\n  root:\n    path: /\n    pool: different\n    type: disk\n"
        )
        with self.assertRaises(RuntimeError):
            incus.ensure_profile_with_root_disk(self.cmds, "default", "60GiB")


class TestWorkspaceRsyncExclusions(unittest.TestCase):
    """Workspace rsync excludes tmp and own workdir."""

    def test_workspace_rsync_excludes_tmp_and_workdir(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.workspace import _workspace_rsync_args
        root = Path("/repo")
        workdir = root / "tmp~" / "build"
        dest = Path("/dest")
        args = _workspace_rsync_args(root, workdir, dest)
        self.assertIn("--exclude=/tmp/", args)
