from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")

_DEFAULT_KIVE_PASSWORD_HASH = "$6$w7nYcSFmaLbIxt0X$ru7i8S1R8KghBGY7RuclLvDE4ik6C9WZ89HeZeC2LxWJJBBOOkmoWFjSc7viAlL/4Yop9l28Ylw0DyOqmuhbR1"


def default_development_password_hash() -> str:
    return _DEFAULT_KIVE_PASSWORD_HASH


def set_instance_config_multiline(cmds: Cmds, instance: str, key: str, value: str) -> None:
    result = cmds.incus.run(["config", "set", instance, key, "-"], input=value, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Failed to set {key}")


def instance_is_cloud_variant(cmds: Cmds, instance: str) -> bool:
    out = cmds.incus.output(["config", "show", instance])
    return bool(re.search(r"^\s*image\.variant:\s*cloud\s*$", out, re.MULTILINE))


def find_ssh_pubkey() -> str:
    home = Path.home()
    for name in ("id_ed25519.pub", "id_rsa.pub"):
        pubkey = home / ".ssh" / name
        if pubkey.exists():
            return pubkey.read_text().strip()
    return ""


def get_instance_ipv4_for_bridge(instance: str, bridge_cidr: str) -> str:
    if not bridge_cidr or "/" not in bridge_cidr:
        return "172.30.0.50"
    gateway = bridge_cidr.split("/")[0]
    octets = gateway.split(".")
    if len(octets) != 4:
        return "172.30.0.50"
    hashed = int(hashlib.sha256(instance.encode()).hexdigest()[:8], 16)
    host = 50 + (hashed % 180)
    return f"{octets[0]}.{octets[1]}.{octets[2]}.{host}"
