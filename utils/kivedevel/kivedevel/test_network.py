"""Tests for managed Incus bridge network: creation, validation, NIC attachment, DHCP."""

from __future__ import annotations

import json
import unittest
from unittest import mock


from kivedevel._test_helpers import (
    MockRunResult,
    make_cmds,
    make_config,
)

_NET = "kivedevel.build_vm.network"


class TestEnsureManagedVmNetwork(unittest.TestCase):
    """Bridge selection, creation, and validation."""

    def setUp(self):
        self.cmds = make_cmds()

    def _import(self):
        from kivedevel.build_vm import network as n
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
        se = [bridges_json]
        se.append("utils/dev")  # user.kive.devel.created-by
        se.append(net.DEFAULT_VM_BRIDGE_CIDR)  # ipv4.address
        for key in net._MUTABLE_KEYS:
            se.append("false" if key == "ipv4.nat" else "true")
        se.append("none")  # ipv6.address
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
        se = [bridges_json]
        se.append("utils/dev")  # user.kive.devel.created-by
        se.append(net.DEFAULT_VM_BRIDGE_CIDR)  # ipv4.address
        for _ in net._MUTABLE_KEYS:
            se.append("true")
        se.append("none")  # ipv6.address
        self.cmds.incus.output.side_effect = se


class TestEnsureVmNic(unittest.TestCase):
    """VM NIC attachment to managed bridge."""

    def setUp(self):
        self.cmds = make_cmds()

    def _target(self, name="kive-lab-br"):
        from kivedevel.build_vm.network import VmNicTarget
        return VmNicTarget(name=name)

    def test_adds_with_network(self):
        from kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.return_value = "other-device:\n  type: nic\n"
        ensure_vm_nic(self.cmds, "test-vm", self._target())
        add_calls = [
            c for c in self.cmds.incus.run.call_args_list
            if c[0][0][:4] == ["config", "device", "add", "test-vm"] and "eth0" in c[0][0]
        ]
        self.assertEqual(len(add_calls), 1)
        self.assertIn("network=kive-lab-br", str(add_calls[0][0][0]))

    def test_noop_when_matching_network(self):
        from kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.return_value = "eth0:\n  network: kive-lab-br\n  type: nic\n"
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertFalse(result)

    def test_replaces_wrong_network(self):
        from kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.return_value = "eth0:\n  network: wrong-br\n  type: nic\n"
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertTrue(result)

    def test_replaces_macvlan(self):
        from kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.return_value = (
            "eth0:\n"
            "  nictype: macvlan\n"
            "  parent: enp1s0\n"
            "  type: nic\n"
        )
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertTrue(result)

    def test_overrides_profile_macvlan(self):
        from kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.side_effect = [
            "",
            "eth0:\n  nictype: macvlan\n  parent: enp1s0\n  type: nic\n",
        ]
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertTrue(result)

    def test_profile_matching_network_noop(self):
        from kivedevel.build_vm.network import ensure_vm_nic
        self.cmds.incus.output.side_effect = [
            "",
            "eth0:\n  network: kive-lab-br\n  type: nic\n",
        ]
        result = ensure_vm_nic(self.cmds, "test-vm", self._target())
        self.assertFalse(result)


class TestPortForward(unittest.TestCase):
    """VM mode does not use incus proxy device."""

    def test_vm_mode_does_not_call_incus_proxy(self):
        from kivedevel.build_vm.runner import _run_build_vm_vm
        from kivedevel.build_vm.network import VmNicTarget
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cmds.incus.run.return_value.returncode = 0
        cfg = make_config()
        nic_target = VmNicTarget(name="kive-lab-br", managed=True)
        with (
            mock.patch("kivedevel.build_vm.runner.wait_vm_dhcp_lease",
                       return_value="10.77.77.100"),
            mock.patch("kivedevel.build_vm.runner._reserve_vm_nic_address"),
            mock.patch("kivedevel.build_vm.runner._check_vm_egress"),
            mock.patch("kivedevel.build_vm.runner.ensure_managed_vm_network",
                       return_value=nic_target),
            mock.patch("kivedevel.build_vm.runner.ensure_instance",
                       return_value=(True, "vm")),
            mock.patch("kivedevel.build_vm.runner.ensure_vm_nic",
                       return_value=True),
            mock.patch("kivedevel.build_vm.runner.ensure_user_data",
                       return_value=True),
            mock.patch("kivedevel.build_vm.runner.enable_network_config",
                       return_value=True),
            mock.patch("kivedevel.build_vm.runner.maybe_restart_after_config"),
            mock.patch("kivedevel.build_vm.runner.handle_workspace_attachment"),
            mock.patch("kivedevel.build_vm.runner.maybe_provision_instance"),
        ):
            _run_build_vm_vm(cfg, cmds)

    def test_vm_mode_calls_ensure_managed_vm_network(self):
        from kivedevel.build_vm.runner import _run_build_vm_vm
        from kivedevel.build_vm.network import VmNicTarget
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cmds.incus.run.return_value.returncode = 0
        cfg = make_config()
        nic_target = VmNicTarget(name="kive-lab-br", managed=True)
        with (
            mock.patch("kivedevel.build_vm.runner.wait_vm_dhcp_lease",
                       return_value="10.77.77.100"),
            mock.patch("kivedevel.build_vm.runner._reserve_vm_nic_address"),
            mock.patch("kivedevel.build_vm.runner._check_vm_egress"),
            mock.patch("kivedevel.build_vm.runner.ensure_managed_vm_network",
                       return_value=nic_target) as mock_choose,
            mock.patch("kivedevel.build_vm.runner.ensure_instance",
                       return_value=(True, "vm")),
            mock.patch("kivedevel.build_vm.runner.ensure_vm_nic",
                       return_value=True),
            mock.patch("kivedevel.build_vm.runner.ensure_user_data",
                       return_value=True),
            mock.patch("kivedevel.build_vm.runner.enable_network_config",
                       return_value=True),
            mock.patch("kivedevel.build_vm.runner.maybe_restart_after_config"),
            mock.patch("kivedevel.build_vm.runner.handle_workspace_attachment"),
            mock.patch("kivedevel.build_vm.runner.maybe_provision_instance"),
        ):
            _run_build_vm_vm(cfg, cmds)
        mock_choose.assert_called_once()

    def test_container_still_uses_web_proxy(self):
        from kivedevel.build_vm.runner import _run_build_vm_container
        cmds = mock.Mock()
        cmds.incus.output.return_value = ""
        cmds.incus.run.return_value.returncode = 0
        cfg = make_config(instance_type="container")
        with (
            mock.patch("kivedevel.build_vm.runner.ensure_instance",
                       return_value=(True, "container")),
            mock.patch("kivedevel.build_vm.runner.ensure_network_device",
                       return_value=False),
            mock.patch("kivedevel.build_vm.runner.ensure_user_data",
                       return_value=True),
            mock.patch("kivedevel.build_vm.runner.enable_network_config",
                       return_value=True),
            mock.patch("kivedevel.build_vm.runner.maybe_restart_after_config"),
            mock.patch("kivedevel.build_vm.runner.handle_workspace_attachment"),
            mock.patch("kivedevel.build_vm.runner._ensure_web_proxy_device") as mock_proxy,
            mock.patch("kivedevel.build_vm.runner.maybe_provision_instance"),
        ):
            _run_build_vm_container(cfg, cmds)
        mock_proxy.assert_called_once()


class TestVmEgressCheck(unittest.TestCase):
    """Egress check transport error handling and success/failure."""

    def _make_cmds(self, returncode=0, stdout="", stderr=""):
        cmds = mock.Mock()
        cmds.incus.run.return_value = MockRunResult(
            returncode=returncode, stdout=stdout, stderr=stderr,
        )
        return cmds

    def test_transport_error_websocket_skips(self):
        from kivedevel.build_vm.runner import _check_vm_egress
        cmds = self._make_cmds(returncode=1, stderr="Error: websocket: bad handshake")
        _check_vm_egress(cmds, "test-vm")

    def test_transport_error_agent_not_running_skips(self):
        from kivedevel.build_vm.runner import _check_vm_egress
        cmds = self._make_cmds(returncode=1, stderr="Error: VM agent isn't currently running")
        _check_vm_egress(cmds, "test-vm")

    def test_transport_error_connection_refused_skips(self):
        from kivedevel.build_vm.runner import _check_vm_egress
        cmds = self._make_cmds(returncode=1, stderr="Error: connection refused")
        _check_vm_egress(cmds, "test-vm")

    def test_transport_error_not_connected_skips(self):
        from kivedevel.build_vm.runner import _check_vm_egress
        cmds = self._make_cmds(returncode=1, stderr="Error: not connected")
        _check_vm_egress(cmds, "test-vm")

    def test_non_transport_error_does_not_abort(self):
        from kivedevel.build_vm.runner import _check_vm_egress
        cmds = self._make_cmds(returncode=1, stdout="some network output", stderr="exit code 1")
        _check_vm_egress(cmds, "test-vm")

    def test_successful_egress_returns(self):
        from kivedevel.build_vm.runner import _check_vm_egress
        stdout = (
            "1: lo: ...\n"
            "2: enp5s0: ... inet 10.166.248.88/24 ...\n"
            "default via 10.166.248.1 dev enp5s0\n"
            "raw IPv4 egress OK\n"
            "archive.ubuntu.com:80 OK\n"
        )
        cmds = self._make_cmds(returncode=0, stdout=stdout)
        _check_vm_egress(cmds, "test-vm")

    def test_fails_on_missing_raw_ipv4(self):
        from kivedevel.build_vm.runner import _check_vm_egress
        stdout = "1: lo: ...\nraw IPv4 egress missing\n"
        cmds = self._make_cmds(returncode=0, stdout=stdout)
        with self.assertRaises(SystemExit):
            _check_vm_egress(cmds, "test-vm")


class TestWaitVmDhcpLease(unittest.TestCase):
    """DHCP lease detection for VM NIC."""

    def setUp(self):
        self.cmds = make_cmds()

    def test_returns_ip_by_mac(self):
        from kivedevel.build_vm.network import wait_vm_dhcp_lease
        self.cmds.incus.output.side_effect = [
            "eth0:\n  hwaddr: aa:bb:cc:dd:ee:ff\n  type: nic\n",
            json.dumps([
                {"hwaddr": "aa:bb:cc:dd:ee:ff", "address": "10.77.77.42"},
            ]),
        ]
        ip = wait_vm_dhcp_lease(self.cmds, "test-vm", "kive-lab-br", timeout=1)
        self.assertEqual(ip, "10.77.77.42")

    def test_returns_none_on_timeout(self):
        from kivedevel.build_vm.network import wait_vm_dhcp_lease
        self.cmds.incus.output.side_effect = [
            "eth0:\n  hwaddr: aa:bb:cc:dd:ee:ff\n  type: nic\n",
            json.dumps([]),
        ]
        ip = wait_vm_dhcp_lease(self.cmds, "test-vm", "kive-lab-br", timeout=1)
        self.assertIsNone(ip)
