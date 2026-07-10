"""Tests for managed Incus bridge network: creation, validation, NIC attachment, DHCP."""

from __future__ import annotations

import json
import unittest
from unittest import mock

from Kive.utils.kivedevel.kivedevel._test_helpers import (
    MockRunResult,
    add_source_path,
    make_cmds,
)

add_source_path()

_NET = "Kive.utils.kivedevel.kivedevel.build_vm.network"


class TestEnsureManagedVmNetwork(unittest.TestCase):
    """Bridge selection, creation, and validation."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        import Kive.utils.kivedevel.kivedevel.build_vm.network as n
        import importlib
        importlib.reload(n)
        return n

    def test_creates_when_missing(self):
        net = self._import()
        self.cmds.incus.output.return_value = json.dumps([])
        self.cmds.incus.run.return_value = MockRunResult(returncode=0)
        result = net.ensure_managed_vm_network(self.cmds, None)
        self.assertEqual(result.name, net.DEFAULT_VM_BRIDGE)
        self.assertTrue(result.managed)

    def test_prefers_kive_lab_br(self):
        net = self._import()
        self._mock_existing_bridges(net, ["kive-lab-br", "incusbr0"])
        with mock.patch(f"{_NET}.validate_live_bridge_address"):
            result = net.ensure_managed_vm_network(self.cmds, None)
        self.assertEqual(result.name, "kive-lab-br")

    def test_ignores_incusbr0_by_default(self):
        net = self._import()
        self.cmds.incus.output.return_value = json.dumps([
            {"name": "incusbr0", "type": "bridge", "managed": True},
        ])
        with mock.patch(f"{_NET}.validate_live_bridge_address"):
            with mock.patch(f"{_NET}._ensure_incus_network_create"):
                result = net.ensure_managed_vm_network(self.cmds, None)
        self.assertEqual(result.name, net.DEFAULT_VM_BRIDGE)

    def test_respects_requested_bridge(self):
        net = self._import()
        self.cmds.incus.output.return_value = json.dumps([
            {"name": "mybr", "type": "bridge", "managed": True},
        ])
        with mock.patch(f"{_NET}.validate_incus_bridge_config"):
            with mock.patch(f"{_NET}.validate_live_bridge_address"):
                result = net.ensure_managed_vm_network(self.cmds, "mybr")
        self.assertEqual(result.name, "mybr")

    def test_explicit_incusbr0_still_works(self):
        net = self._import()
        self.cmds.incus.output.return_value = json.dumps([
            {"name": "incusbr0", "type": "bridge", "managed": True},
        ])
        with mock.patch(f"{_NET}.validate_incus_bridge_config"):
            with mock.patch(f"{_NET}.validate_live_bridge_address"):
                result = net.ensure_managed_vm_network(self.cmds, "incusbr0")
        self.assertEqual(result.name, "incusbr0")

    def test_fails_on_unmanaged_bridge(self):
        net = self._import()
        self.cmds.incus.output.return_value = json.dumps([
            {"name": "kive-lab-br", "type": "bridge", "managed": False},
        ])
        with self.assertRaises(SystemExit):
            net.ensure_managed_vm_network(self.cmds, "kive-lab-br")

    def test_repairs_mutable_keys(self):
        net = self._import()
        bridges_json = json.dumps([{"name": "kive-lab-br", "type": "bridge", "managed": True}])
        se = [bridges_json] * 2
        se.append(net.DEFAULT_VM_BRIDGE_CIDR)
        for key in net._MUTABLE_KEYS:
            se.append("false" if key == "ipv4.nat" else "true")
        se.append("none")
        self.cmds.incus.output.side_effect = se
        with mock.patch(f"{_NET}.validate_live_bridge_address"):
            net.ensure_managed_vm_network(self.cmds, None)
        set_calls = [c for c in self.cmds.incus.run.call_args_list
                     if c[0][0][:3] == ["network", "set", "kive-lab-br"]]
        self.assertGreaterEqual(len(set_calls), 1)

    def test_get_existing_bridges_returns_parsed(self):
        net = self._import()
        self.cmds.incus.output.return_value = json.dumps([
            {"name": "incusbr0", "type": "bridge", "managed": True},
        ])
        result = net.get_existing_bridges(self.cmds)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "incusbr0")

    def test_get_existing_bridges_returns_empty_on_no_output(self):
        net = self._import()
        self.cmds.incus.output.return_value = ""
        result = net.get_existing_bridges(self.cmds)
        self.assertEqual(result, [])

    def _mock_existing_bridges(self, net, names):
        bridges = []
        for name in names:
            bridges.append({"name": name, "type": "bridge", "managed": True})
        bridges_json = json.dumps(bridges)
        se = [bridges_json] * 2
        se.append(net.DEFAULT_VM_BRIDGE_CIDR)
        for _ in net._MUTABLE_KEYS:
            se.append("true")
        se.append("none")
        self.cmds.incus.output.side_effect = se


class TestEnsureVmNic(unittest.TestCase):
    """VM NIC attachment to managed bridge."""

    def setUp(self):
        self.cmds = make_cmds()

    def _target(self, name="kive-lab-br"):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import VmNicTarget
        return VmNicTarget(name=name)

    def test_adds_with_network(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.return_value = "other-device:\n  type: nic\n"
        ensure_vm_nic(self.cmds, "test-vm", self._target())
        add_calls = [
            c for c in self.cmds.incus.run.call_args_list
            if c[0][0][:4] == ["config", "device", "add", "test-vm"] and "eth0" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 1)
        self.assertIn("network=kive-lab-br", str(add_calls[0][0][0]))

    def test_noop_when_matching_network(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.return_value = "eth0:\n  network: kive-lab-br\n  type: nic\n"
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertFalse(result)

    def test_replaces_wrong_network(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.return_value = "eth0:\n  network: wrong-br\n  type: nic\n"
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertTrue(result)

    def test_replaces_macvlan(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.return_value = (
            "eth0:\n"
            "  nictype: macvlan\n"
            "  parent: enp1s0\n"
            "  type: nic\n"
        )
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertTrue(result)

    def test_overrides_profile_macvlan(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.side_effect = [
            "",
            "eth0:\n  nictype: macvlan\n  parent: enp1s0\n  type: nic\n",
        ]
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertTrue(result)

    def test_profile_matching_network_noop(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.side_effect = [
            "",
            "eth0:\n  network: kive-lab-br\n  type: nic\n",
        ]
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertFalse(result)


class TestWaitVmDhcpLease(unittest.TestCase):
    """DHCP lease detection for VM NIC."""

    def setUp(self):
        self.cmds = make_cmds()

    def test_returns_ip_by_mac(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import wait_vm_dhcp_lease
        self.cmds.incus.output.side_effect = [
            "eth0:\n  hwaddr: aa:bb:cc:dd:ee:ff\n  type: nic\n",
            json.dumps([
                {"hwaddr": "aa:bb:cc:dd:ee:ff", "address": "10.77.77.42"},
            ]),
        ]
        ip = wait_vm_dhcp_lease(self.cmds, "test-vm", "kive-lab-br", timeout=1)
        self.assertEqual(ip, "10.77.77.42")

    def test_returns_none_on_timeout(self):
        from Kive.utils.kivedevel.kivedevel.build_vm.network import wait_vm_dhcp_lease
        self.cmds.incus.output.side_effect = [
            "eth0:\n  hwaddr: aa:bb:cc:dd:ee:ff\n  type: nic\n",
            json.dumps([]),
        ]
        ip = wait_vm_dhcp_lease(self.cmds, "test-vm", "kive-lab-br", timeout=1)
        self.assertIsNone(ip)
