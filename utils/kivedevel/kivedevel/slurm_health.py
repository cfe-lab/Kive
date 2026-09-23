from __future__ import annotations

from importlib.resources import files


SLURM_HEALTHCHECK_SCRIPT = (
    files("kivedevel")
    .joinpath("slurm_healthcheck.sh")
    .read_text(encoding="utf-8")
)
