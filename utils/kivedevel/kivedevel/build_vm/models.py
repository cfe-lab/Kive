from __future__ import annotations

from dataclasses import dataclass
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
    host_interface: str = ""
    provision: bool = True
    web_port: int = 8000
    no_web_proxy: bool = False
    vm_network: str = ""
    vm_cidr: str = ""
    vm_ip: str = ""

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
            vm_network=getattr(args, "vm_network", ""),
            vm_cidr=getattr(args, "vm_cidr", ""),
            vm_ip=getattr(args, "vm_ip", ""),
        )
