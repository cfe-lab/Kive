"""Tests for CLI argument parsing and defaults."""

from __future__ import annotations

import argparse
from pathlib import Path
import unittest
from unittest import mock



class TestBuildVmParser(unittest.TestCase):
    """Build-vm argument defaults."""

    @classmethod
    def setUpClass(cls):
        from kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        cls.args = parser.parse_args(["build-vm"])

    def test_default_instance_type_is_vm(self):
        self.assertEqual(self.args.instance_type, "vm")

    def test_default_cpu_is_4(self):
        self.assertEqual(self.args.cpu, "4")

    def test_default_memory_is_8GiB(self):
        self.assertEqual(self.args.memory, "8GiB")

    def test_default_web_port_is_8000(self):
        self.assertEqual(self.args.web_port, 8000)

    def test_no_web_proxy_defaults_to_false(self):
        self.assertFalse(self.args.no_web_proxy)

    def test_build_vm_provision_defaults_to_true(self):
        self.assertTrue(self.args.provision)

    def test_no_provision_sets_false(self):
        from kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--no-provision"])
        self.assertFalse(args.provision)

    def test_default_vm_network_is_none(self):
        from kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertIsNone(args.vm_network)

    def test_vm_mode_resolves_network_to_kive_lab_br(self):
        from pathlib import Path
        from kivedevel.build_vm.models import BuildVmConfig
        cfg = BuildVmConfig(
            root=Path("/tmp"), workdir=Path("/tmp"), instance="test",
            image_path=Path("/tmp/img.img"), instance_type="vm",
        )
        self.assertIsNone(cfg.vm_network)

    def test_default_model_network_is_none(self):
        from kivedevel.build_vm.models import BuildVmConfig
        self.assertIsNone(BuildVmConfig.__dataclass_fields__["vm_network"].default)

    def test_explicit_instance_type_container(self):
        from kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--instance-type", "container"])
        self.assertEqual(args.instance_type, "container")

    def test_host_interface_flag(self):
        from kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--host-interface", "docker0"])
        self.assertEqual(args.host_interface, "docker0")


    def test_vm_mode_rejects_host_interface(self):
        from kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--host-interface", "docker0"])
        self.assertEqual(args.host_interface, "docker0")

    def test_container_mode_rejects_vm_network(self):
        from kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm", "--instance-type", "container", "--vm-network", "kive-lab-br"])
        self.assertEqual(args.vm_network, "kive-lab-br")


class TestBuildVmCrossModeRejection(unittest.TestCase):
    """run_build_vm rejects cross-mode flags."""

    def test_vm_host_interface_rejected(self):
        from kivedevel.build_vm.runner import run_build_vm
        args = argparse.Namespace(
            root=Path("/tmp"), workdir=Path("/tmp"), instance="test",
            image_name="kive-code.img", instance_type="vm",
            pool="default", profile="default", root_size="60GiB",
            memory="8GiB", cpu="4", host_interface="docker0",
            provision=True, web_port=8000, no_web_proxy=False,
            quiet=False, verbose=False, debug=False, log_file=None,
            vm_network=None,
        )
        with mock.patch("kivedevel.build_vm.runner.Cmds.create") as m_cmds:
            with mock.patch("kivedevel.build_vm.runner.logger") as mock_logger:
                with self.assertRaises(SystemExit) as ctx:
                    run_build_vm(args)
        self.assertEqual(ctx.exception.code, 1)
        mock_logger.error.assert_called_once()
        self.assertIn("not supported in VM mode", mock_logger.error.call_args[0][0])

    def test_container_vm_network_rejected(self):
        from kivedevel.build_vm.runner import run_build_vm
        args = argparse.Namespace(
            root=Path("/tmp"), workdir=Path("/tmp"), instance="test",
            image_name="kive-code.img", instance_type="container",
            pool="default", profile="default", root_size="60GiB",
            memory="8GiB", cpu="4", host_interface=None,
            provision=True, web_port=8000, no_web_proxy=False,
            quiet=False, verbose=False, debug=False, log_file=None,
            vm_network="kive-lab-br",
        )
        with mock.patch("kivedevel.build_vm.runner.Cmds.create") as m_cmds:
            with mock.patch("kivedevel.build_vm.runner.logger") as mock_logger:
                with self.assertRaises(SystemExit) as ctx:
                    run_build_vm(args)
        self.assertEqual(ctx.exception.code, 1)
        mock_logger.error.assert_called_once()
        self.assertIn("not supported in container mode", mock_logger.error.call_args[0][0])


class TestSmokeLocalInstallParser(unittest.TestCase):
    def test_default_instance_type_is_vm(self):
        from kivedevel.local_install import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["smoke-local-install"])
        self.assertEqual(args.instance_type, "vm")

    def test_default_vm_network_is_none(self):
        from kivedevel.local_install import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["smoke-local-install"])
        self.assertIsNone(args.vm_network)


class TestPrepareHostParser(unittest.TestCase):
    def test_default_bridge_is_kive_lab_br(self):
        from kivedevel.backends.incus_host import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["prepare-host", "--backend", "incus"])
        self.assertEqual(args.bridge, "kive-lab-br")

    def test_default_backend_is_incus(self):
        from kivedevel.backends.incus_host import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["prepare-host"])
        self.assertEqual(args.backend, "incus")
