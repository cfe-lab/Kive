# CfE Cluster Setup

This repository contains code and instructions for setting up a multi-host compute cluster.

# Test Environment

This directory contains a Vagrantfile that describes two VMs (a head node and a
worker node) that can be used to test Ansible playbooks or practice performing
cluster management tasks. Ansible is installed on the `head` node, and this directory
is mounted at `/vagrant`. Playbooks can be edited from the host machine, but should
be run from the `head` node.

# Ansible Docs

Essential:

- [Concepts](https://docs.ansible.com/ansible/latest/user_guide/basic_concepts.html)
- [Quickstart](https://docs.ansible.com/ansible/latest/user_guide/quickstart.html)

Thorough:

- [Playbooks](https://docs.ansible.com/ansible/2.3/playbooks.html)
- [How to build your inventory](https://docs.ansible.com/ansible/latest/user_guide/intro_inventory.html#intro-inventory)

Extended:

- [Installation](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html#installation-guide)
- [Become (privesc)](https://docs.ansible.com/ansible/2.3/become.html)
- ["Dry Run" mode](https://docs.ansible.com/ansible/2.3/playbooks_checkmode.html)
- [Asynchronous Actions and Polling](https://docs.ansible.com/ansible/2.3/playbooks_async.html)
- [Vault](https://docs.ansible.com/ansible/2.3/playbooks_vault.html)
