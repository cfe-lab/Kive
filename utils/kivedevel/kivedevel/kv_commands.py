from __future__ import annotations

import dataclasses
import logging
import shlex
import shutil
import subprocess
import sys


logger = logging.getLogger("kivedevel")


class Command:
    """Base class for an external command wrapper."""

    exe: str
    guix_package: str

    def __init__(self, use_guix: bool) -> None:
        self._use_guix = use_guix

    def is_available(self) -> bool:
        if self._use_guix:
            return True
        return shutil.which(self.exe) is not None

    def require(self) -> None:
        if not self.is_available():
            print(f"Required command {self.exe!r} not found in PATH.", file=sys.stderr)
            sys.exit(1)

    def _argv(self, args: list[str], *, sudo: bool) -> list[str]:
        if self._use_guix:
            cmd: list[str] = [
                "guix",
                "environment",
                "--pure",
                "--ad-hoc",
                self.guix_package,
                "--",
                self.exe,
            ] + args
        else:
            cmd = [self.exe] + args
        if sudo:
            cmd = ["sudo", "--"] + cmd
        return cmd

    def run(
        self,
        args: list[str],
        *,
        sudo: bool = False,
        check: bool = True,
        capture_output: bool = False,
        input: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        logger.debug("Running command: %s", shlex.join(self._argv(args, sudo=sudo)))
        if capture_output:
            result = subprocess.run(
                self._argv(args, sudo=sudo),
                check=False,
                capture_output=True,
                input=input,
                text=True,
            )
        else:
            if logger.isEnabledFor(logging.DEBUG):
                result = subprocess.run(
                    self._argv(args, sudo=sudo),
                    check=False,
                    input=input,
                    text=True,
                )
            else:
                result = subprocess.run(
                    self._argv(args, sudo=sudo),
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    input=input,
                    text=True,
                )

        if check and result.returncode != 0:
            logger.error("Command failed with exit code %s", result.returncode)
            raise subprocess.CalledProcessError(result.returncode, result.args)

        if logger.isEnabledFor(logging.DEBUG):
            if result.stdout:
                logger.debug("stdout:\n%s", result.stdout.rstrip())
            if result.stderr:
                logger.debug("stderr:\n%s", result.stderr.rstrip())

        return result

    def output(self, args: list[str], *, sudo: bool = False) -> str:
        result = self.run(args, sudo=sudo, check=False, capture_output=True)
        return result.stdout.strip()

    def ok(self, args: list[str], *, sudo: bool = False) -> bool:
        return self.run(args, sudo=sudo, check=False).returncode == 0


class Incus(Command):
    exe = "incus"
    guix_package = "incus"


class Rsync(Command):
    exe = "rsync"
    guix_package = "rsync"


class QemuImg(Command):
    exe = "qemu-img"
    guix_package = "qemu"


class QemuNbd(Command):
    exe = "qemu-nbd"
    guix_package = "qemu"


class Ip(Command):
    exe = "ip"
    guix_package = "iproute2"


@dataclasses.dataclass(frozen=True)
class Cmds:
    incus: Incus
    rsync: Rsync
    qemu_img: QemuImg
    qemu_nbd: QemuNbd
    ip: Ip
    use_guix: bool

    @classmethod
    def create(cls) -> "Cmds":
        use_guix = shutil.which("guix") is not None
        if use_guix:
            logger.info("guix found — all commands will run via guix environment --pure.")
        else:
            logger.info("guix not found — all commands will run directly.")
        return cls(
            incus=Incus(use_guix),
            rsync=Rsync(use_guix),
            qemu_img=QemuImg(use_guix),
            qemu_nbd=QemuNbd(use_guix),
            ip=Ip(use_guix),
            use_guix=use_guix,
        )

    def require_all(self) -> None:
        for cmd in (self.incus, self.rsync, self.qemu_img, self.qemu_nbd):
            cmd.require()
