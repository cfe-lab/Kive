from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class BuildVmConfig:
    root: Path
    workdir: Path
    instance: str
    image_path: Path
    instance_type: str = "vm"
    pool: str = "default"
    profile: str = "default"
    root_size: str = "60GiB"
    memory: str = "8GiB"
    cpu: str = "4"
    host_interface: str | None = None
    provision: bool = True
    web_port: int = 8000
    no_web_proxy: bool = False
    vm_network: str | None = None
    provision_id: str = field(default_factory=lambda: uuid.uuid4().hex)

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
            provision=bool(args.provision),
            web_port=args.web_port,
            no_web_proxy=bool(args.no_web_proxy),
            vm_network=args.vm_network,
        )
