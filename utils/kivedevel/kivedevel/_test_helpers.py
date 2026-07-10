from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock


class MockRunResult:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def make_cmds(incus_output: str = "[]", **kwargs) -> mock.Mock:
    cmds = mock.Mock()
    cmds.incus.output.return_value = incus_output
    cmds.incus.run.return_value = MockRunResult(returncode=0)
    for key, val in kwargs.items():
        setattr(cmds, key, val)
    return cmds


def make_config(instance: str = "test", instance_type: str = "vm", **overrides) -> Path:
    from Kive.utils.kivedevel.kivedevel.build_vm.models import BuildVmConfig
    params = dict(
        root=Path("/tmp"),
        workdir=Path("/tmp"),
        instance=instance,
        instance_type=instance_type,
        image_path=Path("/tmp/img.qcow2"),
        pool="default",
        profile="default",
        root_size="60GiB",
        memory="1GB",
        cpu="1",
        host_interface="",
        provision=False,
        web_port=8000,
        no_web_proxy=False,
        vm_network="kive-lab-br",
        image_name="kive-code.qcow2",
    )
    params.update(overrides)
    return BuildVmConfig(**params)


def add_source_path() -> None:
    root = Path(__file__).resolve().parents[4]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
