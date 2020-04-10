# CfE Cluster Setup

This repository contains code and instructions for setting up a multi-host compute cluster.


# Test Environment

This directory contains a Vagrantfile that describes two VMs (a head node and a
worker node) that can be used to test Ansible playbooks or practice performing
cluster management tasks. Ansible is installed on the `head` node, and this directory
is mounted at `/vagrant`. Playbooks can be edited from the host machine, but should
be run from the `head` node.


# Quickstart

This will guide you through setting up your test environment and running your
first Ansible commands. You'll need to have [Vagrant] and [VirtualBox] installed.

To being, bring up the Vagrant VMs. This will create two VMs (`head` and
`worker`) and install Ansible on `head`.

    vagrant up

Next, log in to `head` and move into the test environment directory. This is where
we'll do most of our testing and practice.

    vagrant ssh head
    cd /vagrant/testenv

`ansible.cfg` contains holds the configuration for the test environment. Most
importantly, it directs ansible to load it's inventory from
`testenvironment/inventory.ini` instead of from the default location under `/etc`.

From `./testenvironment`, you can run Ansible commands against the inventoried
hosts (including the head node).

This command runs the Ansible's `ping` module against all hosts, which checks that
they can be accessed.

    ansible -m ping all


[Vagrant]: https://www.vagrantup.com/downloads.html
[VirtualBox]: https://www.virtualbox.org/wiki/Downloads


# Ansible Docs

Essential:

- [Concepts](https://docs.ansible.com/ansible/latest/user_guide/basic_concepts.html)
- [Quickstart](https://docs.ansible.com/ansible/latest/user_guide/quickstart.html)

Thorough:

- [Playbooks](https://docs.ansible.com/ansible/2.3/playbooks.html)
- [How to build your inventory](https://docs.ansible.com/ansible/latest/user_guide/intro_inventory.html#intro-inventory)
- [Creating Reusable Playbooks](https://docs.ansible.com/ansible/latest/user_guide/playbooks_reuse.html)
- [Module Index](https://docs.ansible.com/ansible/latest/modules/list_of_all_modules.html)

Extended:

- [Installation](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html#installation-guide)
- [Become (privesc)](https://docs.ansible.com/ansible/2.3/become.html)
- ["Dry Run" mode](https://docs.ansible.com/ansible/2.3/playbooks_checkmode.html)
- [Asynchronous Actions and Polling](https://docs.ansible.com/ansible/2.3/playbooks_async.html)
- [Vault](https://docs.ansible.com/ansible/2.3/playbooks_vault.html)
