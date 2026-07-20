"""Tests for CLI argument parsing and defaults."""

from __future__ import annotations

import argparse
import unittest



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

    def test_default_vm_network_is_kive_lab_br(self):
        from kivedevel.build_vm import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["build-vm"])
        self.assertEqual(args.vm_network, "kive-lab-br")

    def test_default_model_network_is_kive_lab_br(self):
        from kivedevel.build_vm.models import BuildVmConfig
        self.assertEqual(BuildVmConfig.__dataclass_fields__["vm_network"].default, "kive-lab-br")

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


class TestSmokeLocalInstallParser(unittest.TestCase):
    def test_default_instance_type_is_vm(self):
        from kivedevel.local_install import register_subcommand
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()
        register_subcommand(subparsers)
        args = parser.parse_args(["smoke-local-install"])
        self.assertEqual(args.instance_type, "vm")


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
