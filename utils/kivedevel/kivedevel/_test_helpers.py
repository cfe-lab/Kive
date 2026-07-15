from __future__ import annotations

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
