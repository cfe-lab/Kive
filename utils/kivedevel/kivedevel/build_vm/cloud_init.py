from __future__ import annotations

import logging

import yaml

from ..kv_commands import Cmds
from .helpers import find_ssh_pubkey, generate_password_hash, get_instance_ipv4_for_bridge, set_instance_config_multiline
from .network import get_bridge_cidr


logger = logging.getLogger("kivedevel")


def ensure_user_data(cmds: Cmds, instance: str, provision: bool = False) -> bool:
    logger.info("Configuring cloud-init user data for %s...", instance)
    pubkey = find_ssh_pubkey()

    password_hash = generate_password_hash("kive1234")
    ssh_key_block = f"\n    ssh_authorized_keys:\n      - {pubkey}" if pubkey else ""

    provision_write_files = ""
    provision_runcmd = ""
    if provision:
        provision_write_files = """
  - path: /usr/local/bin/kive-provision.sh
    owner: root:root
    permissions: '0755'
    content: |
      #!/usr/bin/env bash
      set -euo pipefail
      STATE_DIR=/var/lib/kive-provision
      LOG_FILE=/var/log/kive-provision.log
      mkdir -p "$STATE_DIR"
      rm -f "$STATE_DIR/done" "$STATE_DIR/failed"
      {
        for _ in $(seq 1 300); do
          if [ -f /mnt/kive-code/dev-env/setup-dev-env.yml ]; then
            break
          fi
          sleep 2
        done
        if [ ! -f /mnt/kive-code/dev-env/setup-dev-env.yml ]; then
          echo "Workspace mount not ready at /mnt/kive-code/dev-env/setup-dev-env.yml"
          exit 1
        fi

        export DEBIAN_FRONTEND=noninteractive
        ln -sfn /mnt/kive-code /usr/local/share/Kive
        cd /usr/local/share/Kive/dev-env
        printf '%s\\n' 'head ansible_connection=local ansible_python_interpreter=/usr/bin/python3' > /tmp/dev_inv.ini
        ANSIBLE_CONFIG=/usr/local/share/Kive/dev-env/ansible.cfg \
        ANSIBLE_ROLES_PATH=/usr/local/share/Kive/roles:/usr/local/share/Kive/cluster-setup/deployment/roles \
        ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml

        PYTHON_BIN=/opt/venv_kive/bin/python
        if [ ! -x "$PYTHON_BIN" ]; then
          PYTHON_BIN=/usr/bin/python3
        fi
        if [ ! -x "$PYTHON_BIN" ]; then
          echo "Missing Python interpreter after provisioning"
          exit 1
        fi
        export PYTHON_BIN

        cd /usr/local/share/Kive/kive
        source /tmp/kive_dev_vars 2>/dev/null || true
        source /etc/kive_dev_vars 2>/dev/null || true
        nohup bash -lc ". /tmp/kive_dev_vars 2>/dev/null || true; . /etc/kive_dev_vars 2>/dev/null || true; exec \"$PYTHON_BIN\" manage.py runserver 0.0.0.0:8000" >/var/log/kive-api-smoke.log 2>&1 &

        for _ in $(seq 1 180); do
          if curl -fsS http://127.0.0.1:8000/login/ >/dev/null 2>&1; then
            break
          fi
          sleep 1
        done
        if ! curl -fsS http://127.0.0.1:8000/login/ >/dev/null 2>&1; then
          echo "API did not become reachable on 127.0.0.1:8000/login/"
          exit 1
        fi

        touch "$STATE_DIR/done"
      } >>"$LOG_FILE" 2>&1 || {
        touch "$STATE_DIR/failed"
        exit 1
      }
"""
        provision_runcmd = "\n  - [sh, -c, '/usr/local/bin/kive-provision.sh || true']"

    userdata = f"""\
#cloud-config
users:
  - name: ubuntu
    gecos: Ubuntu
    sudo: ALL=(ALL) NOPASSWD:ALL
    groups: sudo
    shell: /bin/bash
    lock_passwd: false
    plain_text_passwd: kive1234
    passwd: {password_hash}{ssh_key_block}
ssh_pwauth: true
package_update: false
package_upgrade: false
packages:
  - ansible
  - curl
  - openssh-server
write_files:
  - path: /etc/systemd/system/serial-getty@ttyS0.service.d/override.conf
    owner: root:root
    permissions: '0644'
    content: |
      [Service]
      ExecStart=
      ExecStart=-/sbin/agetty --autologin ubuntu --keep-baud 115200,38400,9600 %I $TERM
  - path: /usr/local/bin/mount-kive-code.sh
    owner: root:root
    permissions: '0755'
    content: |
      #!/bin/sh
      set -eu
      mkdir -p /mnt/kive-code
      if [ -e /dev/disk/by-label/KIVE_CODE ]; then
        mountpoint -q /mnt/kive-code || mount -t ext4 -o defaults /dev/disk/by-label/KIVE_CODE /mnt/kive-code
      fi
  - path: /etc/apt/apt.conf.d/99force-ipv4
    owner: root:root
    permissions: '0644'
    content: |
      Acquire::ForceIPv4 "true";
      Acquire::Retries "3";
  - path: /etc/systemd/system/mount-kive-code.service
    owner: root:root
    permissions: '0644'
    content: |
      [Unit]
      Description=Mount Kive workspace disk
      After=local-fs.target

      [Service]
      Type=oneshot
      ExecStart=/usr/local/bin/mount-kive-code.sh
      RemainAfterExit=true

      [Install]
      WantedBy=multi-user.target
{provision_write_files}
runcmd:
  - [systemctl, daemon-reload]
  - [systemctl, enable, --now, ssh]
  - [systemctl, restart, serial-getty@ttyS0]
  - [systemctl, enable, --now, mount-kive-code.service]
  - [sh, -c, 'systemctl enable --now incus-agent || true']
  - [sh, -c, 'systemctl enable --now lxd-agent || true']
{provision_runcmd}
"""
    try:
        yaml.safe_load(userdata)
    except Exception as exc:
        raise RuntimeError(f"Generated invalid cloud-init user-data for {instance}: {exc}") from exc

    set_instance_config_multiline(cmds, instance, "user.user-data", userdata)
    return True


def enable_network_config(cmds: Cmds, instance: str, host_interface: str, instance_type: str) -> bool:
    logger.info("Configuring cloud-init network config for %s...", instance)
    if instance_type == "container":
        network_config = """\
version: 2
ethernets:
  eth0:
    dhcp4: true
    dhcp6: false
    nameservers:
      addresses: [8.8.8.8,1.1.1.1]
"""
        try:
            yaml.safe_load(network_config)
        except Exception as exc:
            raise RuntimeError(f"Generated invalid cloud-init network config for {instance}: {exc}") from exc

        set_instance_config_multiline(cmds, instance, "user.network-config", network_config)
        return True

    network_cidr = get_bridge_cidr(cmds, host_interface)
    if host_interface == "docker0" and network_cidr:
        gateway = network_cidr.split("/")[0]
        prefix = network_cidr.split("/")[1]
        vm_ipv4 = get_instance_ipv4_for_bridge(instance, network_cidr)
        logger.info("Using static IPv4 %s/%s for %s on docker0", vm_ipv4, prefix, instance)
        network_config = f"""\
version: 2
ethernets:
  enp5s0:
    dhcp4: false
    addresses: [{vm_ipv4}/{prefix}]
    gateway4: {gateway}
    nameservers:
      addresses: [8.8.8.8,1.1.1.1]
"""
    else:
        network_config = """\
version: 2
ethernets:
  enp5s0:
    dhcp4: true
    dhcp6: false
    nameservers:
      addresses: [8.8.8.8,1.1.1.1]
"""

    try:
        yaml.safe_load(network_config)
    except Exception as exc:
        raise RuntimeError(f"Generated invalid cloud-init network config for {instance}: {exc}") from exc

    set_instance_config_multiline(cmds, instance, "user.network-config", network_config)
    return True
