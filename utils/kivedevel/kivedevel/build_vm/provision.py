from __future__ import annotations

import logging

from ..kv_commands import Cmds
from .remote import resolve_instance_ip, run_ssh_script, wait_for_ssh


logger = logging.getLogger("kivedevel")


_PROVISION_SCRIPT = """\
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

if command -v cloud-init >/dev/null 2>&1; then
    sudo cloud-init status --wait || true
fi

if ! command -v ansible-playbook >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install -y ansible
fi

if [ -L /usr/local/share/Kive ]; then
    sudo ln -sfn /mnt/kive-code /usr/local/share/Kive
elif [ ! -e /usr/local/share/Kive ]; then
    sudo mkdir -p /usr/local/share
    sudo ln -s /mnt/kive-code /usr/local/share/Kive
fi

cd /usr/local/share/Kive/dev-env
printf 'head ansible_connection=local ansible_python_interpreter=/usr/bin/python3\n' > /tmp/dev_inv.ini
ANSIBLE_CONFIG=/usr/local/share/Kive/dev-env/ansible.cfg \\
ANSIBLE_ROLES_PATH=/usr/local/share/Kive/roles:/usr/local/share/Kive/cluster-setup/deployment/roles \\
ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml
"""


def maybe_provision_instance(
    cmds: Cmds,
    instance: str,
    instance_type: str,
    *,
    provision: bool,
    ssh_identity_file: str | None = None,
) -> None:
    if not provision:
        return

    logger.info("Provisioning %s instance %s via SSH...", instance_type, instance)
    ip = resolve_instance_ip(cmds, instance)
    wait_for_ssh(ip, identity_file=ssh_identity_file)
    run_ssh_script(ip, _PROVISION_SCRIPT, identity_file=ssh_identity_file)
