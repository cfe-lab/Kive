from __future__ import annotations

import logging

import yaml

from ..kv_commands import Cmds
from .helpers import find_ssh_pubkey, generate_password_hash, set_instance_config_multiline


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
  - path: /etc/systemd/system/kive-dev-web.service
    owner: root:root
    permissions: '0644'
    content: |
      [Unit]
      Description=Kive development web server
      After=network.target

      [Service]
      User=kive
      Group=kive
      WorkingDirectory=/usr/local/share/Kive/kive
      EnvironmentFile=/etc/kive/kive-dev.env
      ExecStart=/opt/venv_kive/bin/python manage.py runserver --noreload 0.0.0.0:8000
      Restart=on-failure
      RestartSec=5

      [Install]
      WantedBy=multi-user.target
  - path: /usr/local/bin/kive-provision.sh
    owner: root:root
    permissions: '0755'
    content: |
      #!/usr/bin/env bash
      set -euo pipefail
      STATE_DIR=/var/lib/kive-provision
      LOG_FILE=/var/log/kive-provision.log
      mkdir -p "$STATE_DIR"
      rm -f "$STATE_DIR/done" "$STATE_DIR/failed" "$STATE_DIR/started"
      touch "$STATE_DIR/started"

      mark_failed_on_exit() {
        status=$?
        if [ "$status" -ne 0 ] && [ ! -f "$STATE_DIR/done" ]; then
          touch "$STATE_DIR/failed" || true
        fi
        exit "$status"
      }
      trap mark_failed_on_exit EXIT

      exec >>"$LOG_FILE" 2>&1
      set -x
      date
      uname -a
      echo "HOSTNAME=$(hostname)"
      echo "PWD=$(pwd)"
      echo "USER=$(id)"
      echo "ENVIRONMENT:"
      env | sort
      echo "--- initial file checks ---"
      ls -la /mnt/kive-code /mnt/kive-code/dev-env/setup-dev-env.yml /usr/local/share/Kive || true
      echo "=== cloud-init status check ==="
      cloud_init_status=$(cloud-init status --long 2>&1 || true)
      echo "$cloud_init_status"
      if printf '%s\\n' "$cloud_init_status" | grep -qi 'status: error'; then
        echo "cloud-init reported an error status"
        cat /var/log/cloud-init.log /var/log/cloud-init-output.log 2>/dev/null || true
        exit 1
      fi
      echo "=== network preflight ==="
      ip addr || true
      ip route || true
      resolvectl status || true
      cat /etc/resolv.conf || true

      if ! timeout --foreground 15s python3 -c 'import socket; sock=socket.create_connection(("1.1.1.1",443), timeout=10); sock.close()'; then
        echo "raw IPv4 egress failed; likely host forwarding/NAT/firewall." >&2
        exit 1
      fi
      echo "--- raw IPv4 egress OK ---"

      getent ahostsv4 archive.ubuntu.com || true
      if ! timeout --foreground 15s python3 -c 'import socket; addr=socket.getaddrinfo("archive.ubuntu.com",80,socket.AF_INET,socket.SOCK_STREAM)[0][4]; sock=socket.create_connection(addr, timeout=10); sock.close()'; then
        echo "DNS resolution worked but TCP egress to archive.ubuntu.com:80 failed." >&2
        exit 1
      fi
      echo "--- TCP egress to archive.ubuntu.com OK ---"
      echo "=== apt install prerequisites ==="
      export DEBIAN_FRONTEND=noninteractive
      if ! timeout --foreground 180s apt-get update; then
        echo "apt-get update failed or timed out" >&2
        exit 1
      fi
      if ! timeout --foreground 300s apt-get install -y ansible curl openssh-server; then
        echo "apt-get install failed or timed out" >&2
        exit 1
      fi
      if ! systemctl enable --now ssh; then
        echo "Failed to enable/start ssh after installing openssh-server" >&2
        exit 1
      fi
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
        rm -rf /usr/local/share/Kive
        mkdir -p /usr/local/share/Kive
        cp -a /mnt/kive-code/. /usr/local/share/Kive/
        chown -R root:root /usr/local/share/Kive
        cd /usr/local/share/Kive/dev-env
        printf '%s\\n' 'head ansible_connection=local ansible_python_interpreter=/usr/bin/python3' > /tmp/dev_inv.ini
        export ANSIBLE_CONFIG=/usr/local/share/Kive/dev-env/ansible.cfg
        export ANSIBLE_ROLES_PATH=/usr/local/share/Kive/roles:/usr/local/share/Kive/cluster-setup/deployment/roles
        if ! timeout --foreground 1800s ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml; then
          echo "ansible-playbook failed or timed out" >&2
          exit 1
        fi
        ls -la /opt/venv_kive/bin /usr/bin/python3 /tmp/kive_dev_vars /etc/kive_dev_vars || true
        cat /tmp/kive_dev_vars | sed -n '1,80p' || true


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
        pwd
        ls -la . || true
        echo "Using PYTHON_BIN=$PYTHON_BIN"
        if [ -s /tmp/kive_dev_vars ]; then
          source /tmp/kive_dev_vars
        fi
        if [ -s /etc/kive_dev_vars ]; then
          source /etc/kive_dev_vars
        fi
        if [ ! -s /tmp/kive_dev_vars ] && [ ! -s /etc/kive_dev_vars ]; then
          echo "Missing kive_dev_vars configuration files"
          ls -la /tmp/kive_dev_vars /etc/kive_dev_vars || true
          exit 1
        fi
        echo "ENV after sourcing dev vars:"
        env | sort
        mkdir -p /etc/kive
        sed 's/^export //' /tmp/kive_dev_vars > /etc/kive/kive-dev.env
        sed -i 's|EnvironmentFile=/tmp/kive_dev_vars|EnvironmentFile=/etc/kive/kive-dev.env|' \
          /etc/systemd/system/kive-dev-web.service
        systemctl daemon-reload
        systemctl enable --now kive-dev-web.service
        echo "kive-dev-web.service started"

        for _ in $(seq 1 60); do
          if systemctl is-active kive-dev-web.service >/dev/null 2>&1 && \
             curl -fsS http://127.0.0.1:8000/login/ >/dev/null 2>&1; then
            break
          fi
          sleep 2
        done
        if ! systemctl is-active kive-dev-web.service >/dev/null 2>&1; then
          echo "kive-dev-web.service failed to start:"
          systemctl status kive-dev-web.service --no-pager || true
          journalctl -u kive-dev-web.service --no-pager --lines=50 || true
          exit 1
        fi
        if ! curl -fsS http://127.0.0.1:8000/login/ >/dev/null 2>&1; then
          echo "API did not become reachable on 127.0.0.1:8000/login/"
          exit 1
        fi

        echo "=== Slurm readiness check ==="
        SINFO="$(sinfo -Nel 2>&1 || true)"
        SCONTROL="$(scontrol show node head 2>&1 || true)"
        echo "$SCONTROL"
        echo "$SINFO"

        if echo "$SCONTROL" | grep -qiE 'down|drain|invalid_reg'; then
          echo "Slurm node head is not usable:" >&2
          echo "$SCONTROL" >&2
          echo "$SINFO" >&2
          grep -nE 'NodeName|PartitionName' /usr/local/etc/slurm/slurm.conf 2>/dev/null || true >&2
          tail -20 /var/log/slurm/slurmd.log /var/log/slurm/slurmctld.log 2>/dev/null || true >&2
          exit 1
        fi

        echo "Running test Slurm job via srun..."
        if ! timeout --foreground 120s srun --partition=debug --nodes=1 --ntasks=1 /bin/true 2>&1; then
          echo "Slurm test job failed:" >&2
          scontrol show node head >&2 || true
          sinfo -Nel >&2 || true
          squeue -a >&2 || true
          sacct --format=JobID,State,ExitCode --brief 2>/dev/null | tail -5 >&2 || true
          grep -nE 'NodeName|PartitionName' /usr/local/etc/slurm/slurm.conf 2>/dev/null || true >&2
          tail -20 /var/log/slurm/slurmd.log /var/log/slurm/slurmctld.log 2>/dev/null || true >&2
          exit 1
        fi
        echo "--- Slurm readiness OK ---"

        touch "$STATE_DIR/done"
      } >>"$LOG_FILE" 2>&1
"""
        provision_runcmd = "\n  - [sh, -c, '/usr/local/bin/kive-provision.sh']"

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


def enable_network_config(
    cmds: Cmds,
    instance: str,
    host_interface: str,
    instance_type: str,
    static_ip: str | None = None,
    cidr: str | None = None,
    gateway: str | None = None,
) -> bool:
    """Set cloud-init network config for the instance.

    For VM mode: skip network config entirely and let the Ubuntu cloud image
    use its default DHCP behavior on the attached NIC.  The static IP path
    (``--vm-ip`` / ``--vm-cidr``) has been removed — VMs always use DHCP.

    For container mode: write a DHCP config for ``eth0``.

    Returns True if a change was made.
    """
    if instance_type == "vm":
        if static_ip or cidr or gateway:
            logger.error(
                "Static VM IP configuration (--vm-ip / --vm-cidr) is no longer supported. "
                "VMs always use DHCP from the Incus managed bridge."
            )
            sys.exit(1)
        current = cmds.incus.output(["config", "get", instance, "user.network-config"]).strip()
        if not current:
            return False
        logger.debug("Removing stale network-config from %s.", instance)
        cmds.incus.run(["config", "unset", instance, "user.network-config"])
        return True

    # Container mode: write DHCP config for eth0
    network_config = """\
version: 2
ethernets:
  eth0:
    dhcp4: true
    dhcp6: false
    nameservers:
      addresses: [8.8.8.8,1.1.1.1]
"""
    current = cmds.incus.output(["config", "get", instance, "user.network-config"]).strip()
    if current.rstrip() == network_config.rstrip():
        return False
    _set_network_config(cmds, instance, network_config)
    return True


def _set_network_config(cmds: Cmds, instance: str, config: str) -> None:
    try:
        yaml.safe_load(config)
    except Exception as exc:
        raise RuntimeError(f"Generated invalid cloud-init network config for {instance}: {exc}") from exc
    set_instance_config_multiline(cmds, instance, "user.network-config", config)
