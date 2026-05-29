from __future__ import annotations

import logging

from ..kv_commands import Cmds
from .helpers import find_ssh_pubkey, generate_password_hash, get_instance_ipv4_for_bridge, set_instance_config_multiline
from .network import get_bridge_cidr


logger = logging.getLogger("kivedevel")


def ensure_user_data(cmds: Cmds, instance: str) -> bool:
    logger.info("Configuring cloud-init user data for %s...", instance)
    pubkey = find_ssh_pubkey()
    password_hash = generate_password_hash("kive1234")
    ssh_key_block = f"\n    ssh_authorized_keys:\n      - {pubkey}" if pubkey else ""
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
runcmd:
  - [systemctl, daemon-reload]
  - [systemctl, enable, --now, ssh]
  - [systemctl, restart, serial-getty@ttyS0]
  - [systemctl, enable, --now, mount-kive-code.service]
  - [sh, -c, 'systemctl enable --now incus-agent || true']
  - [sh, -c, 'systemctl enable --now lxd-agent || true']
"""
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
    set_instance_config_multiline(cmds, instance, "user.network-config", network_config)
    return True
