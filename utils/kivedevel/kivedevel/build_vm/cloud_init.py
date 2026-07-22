from __future__ import annotations

import logging

import yaml

from ..kv_commands import Cmds
from ..slurm_health import SLURM_HEALTHCHECK_SCRIPT
from .helpers import find_ssh_pubkey, generate_password_hash, set_instance_config_multiline


logger = logging.getLogger("kivedevel")


def ensure_user_data(cmds: Cmds, instance: str, *, provision: bool = False, provision_id: str = "") -> bool:
    logger.info("Configuring cloud-init user data for %s...", instance)
    pubkey = find_ssh_pubkey()

    password_hash = generate_password_hash("kive1234")
    ssh_key_block = f"\n    ssh_authorized_keys:\n      - {pubkey}" if pubkey else ""

    provision_write_files = ""
    provision_runcmd = ""
    if provision:
        kive_slurm_healthcheck = "\n".join(
            "      " + line for line in SLURM_HEALTHCHECK_SCRIPT.strip().splitlines()
        )
        provision_write_files = f"""
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
  - path: /usr/local/bin/kive-slurm-healthcheck
    owner: root:root
    permissions: '0755'
    content: |
{kive_slurm_healthcheck}
  - path: /usr/local/bin/kive-provision-status
    owner: root:root
    permissions: '0755'
    content: |
      #!/usr/bin/env python3
      import json, os, sys, tempfile

      STATE_DIR = "/var/lib/kive-provision"
      STATUS_PATH = os.path.join(STATE_DIR, "status.json")

      def write_status(provision_id, state, phase, pid=0, service_result=None, exit_code=None, message=None):
          doc = {{
              "schema_version": 1,
              "provision_id": provision_id,
              "state": state,
              "phase": phase,
              "pid": pid or os.getpid(),
              "service_result": service_result,
              "exit_code": exit_code,
              "message": message,
          }}
          os.makedirs(STATE_DIR, exist_ok=True)
          fd, tmp = tempfile.mkstemp(dir=STATE_DIR)
          try:
              with os.fdopen(fd, "w") as f:
                  json.dump(doc, f, indent=2)
                  f.write("\\n")
                  f.flush()
                  os.fsync(fd)
              os.rename(tmp, STATUS_PATH)
          except BaseException:
              try:
                  os.unlink(tmp)
              except OSError:
                  pass
              raise

      if __name__ == "__main__":
          prov_id = os.environ.get("KIVE_PROVISION_ID", "")
          if not prov_id:
              print("KIVE_PROVISION_ID is empty or unset", file=sys.stderr)
              sys.exit(1)
          sys.argv.pop(0)
          if len(sys.argv) < 2:
              print("Usage: kive-provision-status <state> <phase> [options]", file=sys.stderr)
              sys.exit(1)
          state = sys.argv.pop(0)
          if state not in ("starting", "running", "succeeded", "failed"):
              print(f"Unknown state: {{state}}", file=sys.stderr)
              sys.exit(1)
          phase = sys.argv.pop(0)
          if not phase:
              print("Phase is required", file=sys.stderr)
              sys.exit(1)
          extra = {{}}
          if "--service-result" in sys.argv:
              idx = sys.argv.index("--service-result")
              extra["service_result"] = sys.argv[idx + 1]
          if "--exit-code" in sys.argv:
              idx = sys.argv.index("--exit-code")
              val = sys.argv[idx + 1]
              try:
                  extra["exit_code"] = int(val)
              except ValueError:
                  extra["exit_code"] = val
          if "--message" in sys.argv:
              idx = sys.argv.index("--message")
              extra["message"] = sys.argv[idx + 1]
          write_status(prov_id, state, phase, **extra)
  - path: /etc/systemd/system/kive-provision.service
    owner: root:root
    permissions: '0644'
    content: |
      [Unit]
      Description=Provision the Kive development environment
      After=network-online.target mount-kive-code.service
      Wants=network-online.target
      Requires=mount-kive-code.service

      [Service]
      Type=oneshot
      Environment=KIVE_PROVISION_ID={provision_id}
      ExecStart=/usr/local/bin/kive-provision.sh
      ExecStopPost=/usr/local/bin/kive-provision-finalize
      TimeoutStartSec=3000
      TimeoutStopSec=30
      KillMode=control-group
      SendSIGKILL=yes
      RemainAfterExit=yes
      StandardOutput=append:/var/log/kive-provision.log
      StandardError=append:/var/log/kive-provision.log

      [Install]
      WantedBy=multi-user.target
  - path: /usr/local/bin/kive-provision-finalize
    owner: root:root
    permissions: '0755'
    content: |
      #!/usr/bin/env python3
      import json, os, sys
      STATUS_FILE = "/var/lib/kive-provision/status.json"
      if os.path.isfile(STATUS_FILE):
          try:
              with open(STATUS_FILE) as f:
                  doc = json.load(f)
              if isinstance(doc, dict) and doc.get("state") == "succeeded":
                  sys.exit(0)
          except (json.JSONDecodeError, OSError):
              pass
      exit_code = os.environ.get("EXIT_STATUS", "1")
      try:
          exit_code = int(exit_code)
      except ValueError:
          exit_code = 1
      os.execvp("/usr/local/bin/kive-provision-status", [
          "kive-provision-status", "failed", "finalize",
          "--service-result", os.environ.get("SERVICE_RESULT", "unknown"),
          "--exit-code", str(exit_code),
          "--message", "Provisioning terminated before success",
      ])
  - path: /usr/local/bin/kive-provision.sh
    owner: root:root
    permissions: '0755'
    content: |
      #!/usr/bin/env bash
      set -euo pipefail
      STATE_DIR=/var/lib/kive-provision
      LOG_FILE=/var/log/kive-provision.log
      mkdir -p "$STATE_DIR"
      exec >>"$LOG_FILE" 2>&1
      set -x

      kive_status() {{
        /usr/local/bin/kive-provision-status running "$1"
      }}

      run_bounded() {{
        phase="$1"
        duration="$2"
        shift 2
        kive_status "$phase"
        set +e
        timeout --signal=TERM --kill-after=30s "$duration" "$@"
        rc=$?
        set -e
        case "$rc" in
          0) return 0 ;;
          124|137)
            echo "$phase exceeded its deadline" >&2
            return "$rc"
            ;;
          *)
            echo "$phase failed with status $rc" >&2
            return "$rc"
            ;;
        esac
      }}

      /usr/local/bin/kive-provision-status running starting

      echo "=== cloud-init status check ==="
      cloud_init_status=$(cloud-init status --long 2>&1 || true)
      echo "$cloud_init_status"
      if printf '%s\\n' "$cloud_init_status" | grep -qi 'status: error'; then
        echo "cloud-init reported an error status"
        cat /var/log/cloud-init.log /var/log/cloud-init-output.log 2>/dev/null || true
        exit 1
      fi

      echo "=== network preflight ==="
      run_bounded network-preflight 30s python3 -c '
      import socket
      sock=socket.create_connection(("1.1.1.1",443), timeout=10); sock.close()
      '

      echo "=== apt install prerequisites ==="
      export DEBIAN_FRONTEND=noninteractive
      run_bounded apt-update 1800s apt-get \
        -o Acquire::ForceIPv4=true \
        -o Acquire::Retries=10 \
        update
      run_bounded apt-install 1800s apt-get \
        -o Acquire::ForceIPv4=true \
        -o Acquire::Retries=10 \
        install -y ansible curl openssh-server
      systemctl enable --now ssh

      echo "=== workspace wait ==="
      run_bounded workspace-wait 600s sh -c '
        for _ in $(seq 1 300); do
          if [ -f /mnt/kive-code/dev-env/setup-dev-env.yml ]; then
            exit 0
          fi
          sleep 2
        done
        echo "Workspace mount not ready" >&2
        exit 1
      '

      echo "=== workspace copy and ansible ==="
      rm -rf /usr/local/share/Kive
      mkdir -p /usr/local/share/Kive
      cp -a /mnt/kive-code/. /usr/local/share/Kive/
      chown -R root:root /usr/local/share/Kive
      cd /usr/local/share/Kive/dev-env
      printf '%s\\n' 'head ansible_connection=local ansible_python_interpreter=/usr/bin/python3' > /tmp/dev_inv.ini
      export ANSIBLE_CONFIG=/usr/local/share/Kive/dev-env/ansible.cfg
      export ANSIBLE_ROLES_PATH=/usr/local/share/Kive/roles:/usr/local/share/Kive/cluster-setup/deployment/roles
      run_bounded ansible 1800s ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml

      echo "=== web config ==="
      PYTHON_BIN=/opt/venv_kive/bin/python
      if [ ! -x "$PYTHON_BIN" ]; then PYTHON_BIN=/usr/bin/python3; fi
      if [ ! -x "$PYTHON_BIN" ]; then echo "Missing Python interpreter" >&2; exit 1; fi

      cd /usr/local/share/Kive/kive
      if [ -s /tmp/kive_dev_vars ]; then source /tmp/kive_dev_vars; fi
      if [ -s /etc/kive_dev_vars ]; then source /etc/kive_dev_vars; fi
      if [ ! -s /tmp/kive_dev_vars ] && [ ! -s /etc/kive_dev_vars ]; then
        echo "Missing kive_dev_vars" >&2; exit 1
      fi
      mkdir -p /etc/kive
      sed 's/^export //' /tmp/kive_dev_vars > /etc/kive/kive-dev.env
      sed -i 's|EnvironmentFile=/tmp/kive_dev_vars|EnvironmentFile=/etc/kive/kive-dev.env|' \
        /etc/systemd/system/kive-dev-web.service
      systemctl daemon-reload
      systemctl enable --now kive-dev-web.service

      echo "=== web readiness ==="
      kive_status web-readiness
      for _ in $(seq 1 60); do
        if systemctl is-active kive-dev-web.service >/dev/null 2>&1 && \
           curl -fsS http://127.0.0.1:8000/login/ >/dev/null 2>&1; then
          break
        fi
        sleep 2
      done
      if ! curl -fsS http://127.0.0.1:8000/login/ >/dev/null 2>&1; then
        echo "API did not become reachable" >&2; exit 1
      fi

      echo "=== Slurm readiness check ==="
      kive_status slurm-readiness
      /usr/local/bin/kive-slurm-healthcheck

      echo "=== Slurm smoke job ==="
      kive_status slurm-job
      job_id="$(sbatch --parsable --partition=debug --nodes=1 --ntasks=1 --wrap=/bin/true)"
      if [ -z "$job_id" ] || ! [ "$job_id" -eq "$job_id" ] 2>/dev/null; then
        echo "Invalid job ID from sbatch: $job_id" >&2; exit 1
      fi
      JOB_DEADLINE=300
      JOB_END=$(( $(date +%s) + JOB_DEADLINE ))
      while [ $(date +%s) -lt $JOB_END ]; do
        job_state="$(sacct -n -X -j "$job_id" --format=State,ExitCode 2>/dev/null || true)"
        case "$job_state" in
          *COMPLETED*0:0*) break ;;
          *FAILED*|*CANCELLED*|*TIMEOUT*|*NODE_FAIL*|*OUT_OF_MEMORY*|*PREEMPTED*|*BOOT_FAIL*|*DEADLINE*|*REVOKED*)
            echo "Slurm job failed: $job_state" >&2; exit 1 ;;
        esac
        sleep 2
      done
      if ! echo "$job_state" | grep -q 'COMPLETED.*0:0'; then
        scancel "$job_id" 2>/dev/null || true
        echo "Slurm job did not complete within ${{JOB_DEADLINE}}s" >&2; exit 1
      fi
      echo "--- Slurm readiness OK ---"

      /usr/local/bin/kive-provision-status succeeded complete --exit-code 0
"""
        provision_runcmd = """
  - [sh, -c, 'systemctl daemon-reload']
  - [sh, -c, 'systemctl enable --now --no-block kive-provision.service']"""

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
    instance_type: str,
) -> bool:
    """Set cloud-init network config for the instance.

    VM mode: no network config needed (DHCP from the managed Incus bridge).

    Container mode: write a DHCP config for ``eth0``.

    Returns True if a change was made.
    """
    if instance_type == "vm":
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
