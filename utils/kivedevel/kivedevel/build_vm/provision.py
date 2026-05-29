from __future__ import annotations

import logging

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")


def maybe_provision_instance(cmds: Cmds, instance: str, instance_type: str, *, provision: bool) -> None:
    if not provision:
        return

    if instance_type != "container":
        logger.error("--provision is currently supported only for container instances.")
        raise RuntimeError("Unsupported provisioning target")

    logger.info("Provisioning container instance %s for API smoke tests...", instance)
    cmds.incus.run(
        [
            "exec",
            instance,
            "--",
            "bash",
            "-lc",
            """
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ansible
ln -sfn /mnt/kive-code /usr/local/share/Kive
cd /usr/local/share/Kive/dev-env
printf 'head ansible_connection=local ansible_python_interpreter=/usr/bin/python3\\n' > /tmp/dev_inv.ini
ANSIBLE_CONFIG=/usr/local/share/Kive/dev-env/ansible.cfg \\
ANSIBLE_ROLES_PATH=/usr/local/share/Kive/roles:/usr/local/share/Kive/cluster-setup/deployment/roles \\
ansible-playbook --become -i /tmp/dev_inv.ini setup-dev-env.yml
""",
        ]
    )
