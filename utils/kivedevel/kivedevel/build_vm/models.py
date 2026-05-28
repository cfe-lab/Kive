from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BuildVmConfig:
    root: Path
    workdir: Path
    instance: str
    instance_type: str
    image_path: Path
    pool: str
    profile: str
    root_size: str
    memory: str
    cpu: str
    host_interface: str

    @classmethod
    def from_args(cls, args) -> "BuildVmConfig":
        root = args.root.resolve()
        workdir = args.workdir.resolve()
        return cls(
            root=root,
            workdir=workdir,
            instance=args.instance,
            instance_type=args.instance_type,
            image_path=workdir / args.image_name,
            pool=args.pool,
            profile=args.profile,
            root_size=args.root_size,
            memory=args.memory,
            cpu=args.cpu,
            host_interface=args.host_interface,
        )
