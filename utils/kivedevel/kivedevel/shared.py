from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

from .kv_commands import Cmds


def default_root() -> Path:
    return Path(__file__).resolve().parents[3]


def configure_logging(args, workdir: Path, *, default_log_name: str = "build-vm.log") -> None:
    if getattr(args, "quiet", False):
        level = logging.ERROR
    elif getattr(args, "debug", False):
        level = logging.DEBUG
    elif getattr(args, "verbose", False):
        level = logging.INFO
    else:
        level = logging.INFO

    log_file = getattr(args, "log_file", None) or (workdir / default_log_name)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    handlers: list[logging.Handler] = [logging.FileHandler(log_file, encoding="utf-8")]
    if getattr(args, "verbose", False) or getattr(args, "debug", False):
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.DEBUG if getattr(args, "debug", False) else logging.INFO)
        handlers.append(console)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )


def instance_exists(cmds: Cmds, instance: str) -> bool:
    out = cmds.incus.output(["list", instance, "--format", "csv"])
    for line in out.splitlines():
        if line.split(",", 1)[0] == instance:
            return True
    return False


def instance_is_running(cmds: Cmds, instance: str) -> bool:
    out = cmds.incus.output(["info", instance])
    return bool(re.search(r"^Status:\s+Running$", out, re.IGNORECASE | re.MULTILINE))
