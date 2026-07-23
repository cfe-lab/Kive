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
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if timeout is not None and timeout <= 0:
            raise ValueError(f"timeout must be positive, got {timeout}")
        logger.debug("Running command: %s", shlex.join(self._argv(args, sudo=sudo)))
        try:
            if capture_output:
                result = subprocess.run(
                    self._argv(args, sudo=sudo),
                    check=False,
                    capture_output=True,
                    input=input,
                    text=True,
                    timeout=timeout,
                )
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    result = subprocess.run(
                        self._argv(args, sudo=sudo),
                        check=False,
                        input=input,
                        text=True,
                        timeout=timeout,
                    )
                else:
                    result = subprocess.run(
                        self._argv(args, sudo=sudo),
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        input=input,
                        text=True,
                        timeout=timeout,
                    )
        except subprocess.TimeoutExpired:
            logger.error("Command timed out after %ss: %s", timeout, shlex.join(self._argv(args, sudo=sudo)))
            raise

        if check and result.returncode != 0:
            logger.error("Command failed with exit code %s", result.returncode)
            if result.stderr:
                stderr_body = result.stderr.rstrip()
                logger.error("stderr:\n%s", stderr_body)
            if result.stdout:
                stdout_body = result.stdout.rstrip()
                logger.error("stdout:\n%s", stdout_body)
            raise subprocess.CalledProcessError(
                result.returncode, result.args,
                output=result.stdout, stderr=result.stderr,
            )

        if logger.isEnabledFor(logging.DEBUG):
            if result.stdout:
                logger.debug("stdout:\n%s", result.stdout.rstrip())
            if result.stderr:
                logger.debug("stderr:\n%s", result.stderr.rstrip())

        return result

    def output(self, args: list[str], *, sudo: bool = False, timeout: float | None = None) -> str:
        result = self.run(args, sudo=sudo, check=False, capture_output=True, timeout=timeout)
        return result.stdout.strip()

    def ok(self, args: list[str], *, sudo: bool = False) -> bool:
        return self.run(args, sudo=sudo, check=False).returncode == 0


class Incus(Command):
    exe = "incus"
    guix_package = "incus"

    def __init__(self, use_guix: bool) -> None:
        super().__init__(use_guix)
        self._needs_sudo = self._check_needs_sudo()

    @staticmethod
    def _check_needs_sudo() -> bool:
        result = subprocess.run(
            ["incus", "info"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return False
        result = subprocess.run(
            ["sudo", "--", "incus", "info"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return True
        return False

    def _argv(self, args: list[str], *, sudo: bool) -> list[str]:
        effective_sudo = sudo or self._needs_sudo
        return super()._argv(args, sudo=effective_sudo)


class Rsync(Command):
    exe = "rsync"
    guix_package = "rsync"


class Ip(Command):
    exe = "ip"
    guix_package = "iproute2"


class Nft(Command):
    exe = "nft"
    guix_package = "nftables"


class Socat(Command):
    exe = "socat"
    guix_package = "socat"


@dataclasses.dataclass(frozen=True)
class Cmds:
    incus: Incus
    rsync: Rsync
    ip: Ip
    nft: Nft
    socat: Socat
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
            ip=Ip(use_guix),
            nft=Nft(use_guix),
            socat=Socat(use_guix),
            use_guix=use_guix,
        )

    def require_all(self) -> None:
        for cmd in (self.incus, self.rsync, self.socat):
            cmd.require()
