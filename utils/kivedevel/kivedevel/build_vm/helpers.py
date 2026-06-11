from __future__ import annotations

import hashlib
import logging
import random
import re
import string
import subprocess
from pathlib import Path

from ..kv_commands import Cmds


logger = logging.getLogger("kivedevel")

_DEFAULT_KIVE_PASSWORD_HASH = "$6$w7nYcSFmaLbIxt0X$ru7i8S1R8KghBGY7RuclLvDE4ik6C9WZ89HeZeC2LxWJJBBOOkmoWFjSc7viAlL/4Yop9l28Ylw0DyOqmuhbR1"


def generate_password_hash(password: str) -> str:
    try:
        import crypt
        return crypt.crypt(password, crypt.mksalt(crypt.METHOD_SHA512))
    except (AttributeError, ValueError):
        salt = "$6$" + "".join(random.choices(string.ascii_letters + string.digits, k=16))
        return crypt.crypt(password, salt)
    except ModuleNotFoundError:
        if password == "kive1234":
            logger.warning("Python crypt module is unavailable; using precomputed SHA-512 password hash.")
            return _DEFAULT_KIVE_PASSWORD_HASH
        raise


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


def ensure_ssh_identity(workdir: Path) -> tuple[Path, str]:
    home = Path.home()

    ed25519 = home / ".ssh" / "id_ed25519"
    ed25519_pub = home / ".ssh" / "id_ed25519.pub"
    if ed25519.exists() and ed25519_pub.exists():
        return ed25519, ed25519_pub.read_text().strip()

    rsa = home / ".ssh" / "id_rsa"
    rsa_pub = home / ".ssh" / "id_rsa.pub"
    if rsa.exists() and rsa_pub.exists():
        return rsa, rsa_pub.read_text().strip()

    key_path = workdir / "kive_build_ssh_ed25519"
    pub_path = workdir / "kive_build_ssh_ed25519.pub"
    if not key_path.exists() or not pub_path.exists():
        logger.info("No local SSH key found; generating temporary build key at %s", key_path)
        result = subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            err = (result.stderr or "").strip()
            raise RuntimeError(err or "Failed to generate SSH key")

    return key_path, pub_path.read_text().strip()


def get_instance_ipv4_for_bridge(instance: str, bridge_cidr: str) -> str:
    if not bridge_cidr or "/" not in bridge_cidr:
        return "172.30.0.50"
    gateway = bridge_cidr.split("/")[0]
    octets = gateway.split(".")
    if len(octets) != 4:
        return "172.30.0.50"
    hashed = int(hashlib.sha256(instance.encode()).hexdigest()[:8], 16)
    host = 50 + (hashed % 180)
    if host in (0, 1, 255):
        host = 50
    return f"{octets[0]}.{octets[1]}.{octets[2]}.{host}"
