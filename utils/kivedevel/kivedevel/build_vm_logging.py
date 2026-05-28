from __future__ import annotations

import logging
import sys
from pathlib import Path


def configure_logging(args, workdir: Path) -> None:
    if getattr(args, "quiet", False):
        level = logging.ERROR
    elif getattr(args, "debug", False):
        level = logging.DEBUG
    elif getattr(args, "verbose", False):
        level = logging.INFO
    else:
        level = logging.INFO

    log_file = getattr(args, "log_file", None) or (workdir / "build-vm.log")
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
