"""Tests for container web proxy device: creation, update, skip."""

from __future__ import annotations

import unittest

from kivedevel._test_helpers import make_cmds, make_config


class TestContainerWebProxy(unittest.TestCase):
    """Container web proxy device lifecycle."""

    def setUp(self):
        self.cmds = make_cmds()
        self.cfg = make_config(instance_type="container")

    def _import_ensure(self):
        from kivedevel.build_vm.runner import _ensure_container_web_proxy_device
        return _ensure_container_web_proxy_device

    def test_creates_new_proxy_device(self):
        ensure = self._import_ensure()
        self.cmds.incus.output.return_value = ""
        ensure(self.cmds, self.cfg)
        add_calls = [
            c for c in self.cmds.incus.run.call_args_list
            if "add" in str(c) and "proxy" in str(c)
        ]
        self.assertGreaterEqual(len(add_calls), 1)

    def test_skipped_when_no_web_proxy(self):
        ensure = self._import_ensure()
        cfg = make_config(instance_type="container", no_web_proxy=True)
        ensure(self.cmds, cfg)
        add_calls = [
            c for c in self.cmds.incus.run.call_args_list
            if "add" in str(c) and "proxy" in str(c)
        ]
        self.assertEqual(len(add_calls), 0)

    def test_skipped_when_already_matches(self):
        ensure = self._import_ensure()
        self.cmds.incus.output.side_effect = lambda *a, **kw: (
            "proxy\n" if "type" in a[0] else
            "tcp:127.0.0.1:8000\n" if "listen" in a[0] else
            "tcp:127.0.0.1:8000\n" if "connect" in a[0] else
            ""
        )
        ensure(self.cmds, self.cfg)
        self.assertNotIn("add", str(self.cmds.incus.run.call_args_list))

    def test_updates_when_port_differs(self):
        ensure = self._import_ensure()
        self.cmds.incus.output.side_effect = lambda *a, **kw: (
            "proxy\n" if "type" in a[0] else
            "tcp:127.0.0.1:9000\n" if "listen" in a[0] else
            "tcp:127.0.0.1:8000\n" if "connect" in a[0] else
            ""
        )
        ensure(self.cmds, self.cfg)
        remove_calls = [
            c for c in self.cmds.incus.run.call_args_list
            if "remove" in str(c)
        ]
        self.assertGreaterEqual(len(remove_calls), 1)

    def test_container_add_command_no_nat(self):
        ensure = self._import_ensure()
        self.cmds.incus.output.return_value = ""
        ensure(self.cmds, self.cfg)
        add_call = next(
            c for c in self.cmds.incus.run.call_args_list
            if "add" in str(c) and "proxy" in str(c)
        )
        args = " ".join(add_call[0][0])
        self.assertNotIn("nat=", args)
        self.assertIn("connect=tcp:127.0.0.1:8000", args)
