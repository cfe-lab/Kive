"""
build-vm subcommand — build and configure Incus VMs with cloud-init.

Every external tool is represented by its own Command subclass.  Each class
declares the guix package that provides it via the ``guix_package`` attribute.

When ``guix`` is found in PATH, every command is run as:
    guix environment --pure --ad-hoc <guix_package> -- <exe> <args…>

When ``guix`` is absent, every command is run directly (the caller must have
all tools in PATH).

No shell=True is used anywhere.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import logging
import random
import re
import shutil
import shlex
import string
import subprocess
import sys
import time
from pathlib import Path


logger = logging.getLogger("kivedevel.build_vm")

_DEFAULT_KIVE_PASSWORD_HASH = "$6$w7nYcSFmaLbIxt0X$ru7i8S1R8KghBGY7RuclLvDE4ik6C9WZ89HeZeC2LxWJJBBOOkmoWFjSc7viAlL/4Yop9l28Ylw0DyOqmuhbR1"


# ---------------------------------------------------------------------------
# Command abstraction — one class per external tool
# ---------------------------------------------------------------------------

class Command:
    """Base class for an external command.

    Subclasses set *exe* (the binary name) and *guix_package* (the Guix
    package that provides it).  When *use_guix* is True every invocation is
    wrapped in ``guix environment --pure --ad-hoc <guix_package> --``.
    """

    exe: str
    guix_package: str

    def __init__(self, use_guix: bool) -> None:
        self._use_guix = use_guix

    # ------------------------------------------------------------------
    # availability check
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """True when the command can be run (via guix or directly)."""
        if self._use_guix:
            return True  # guix will supply it
        return shutil.which(self.exe) is not None

    def require(self) -> None:
        """Exit with an error message if the command is not available."""
        if not self.is_available():
            print(
                f"Required command {self.exe!r} not found in PATH.",
                file=sys.stderr,
            )
            sys.exit(1)

    # ------------------------------------------------------------------
    # build argv
    # ------------------------------------------------------------------

    def _argv(self, args: list[str], *, sudo: bool) -> list[str]:
        if self._use_guix:
            cmd: list[str] = [
                "guix", "environment", "--pure",
                "--ad-hoc", self.guix_package,
                "--", self.exe,
            ] + args
        else:
            cmd = [self.exe] + args
        if sudo:
            cmd = ["sudo", "--"] + cmd
        return cmd

    # ------------------------------------------------------------------
    # run helpers
    # ------------------------------------------------------------------

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
        """Run and return stdout, empty string on failure."""
        result = self.run(args, sudo=sudo, check=False, capture_output=True)
        return result.stdout.strip()

    def ok(self, args: list[str], *, sudo: bool = False) -> bool:
        """Return True if the command exits 0."""
        return self.run(args, sudo=sudo, check=False).returncode == 0


# ------------------------------------------------------------------
# Concrete command classes
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Bundle of all command instances
# ------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_password_hash(password: str) -> str:
    try:
        import crypt  # stdlib on Linux
        return crypt.crypt(password, crypt.mksalt(crypt.METHOD_SHA512))
    except (AttributeError, ValueError):
        salt = "$6$" + "".join(
            random.choices(string.ascii_letters + string.digits, k=16)
        )
        return crypt.crypt(password, salt)
    except ModuleNotFoundError:
        if password == "kive1234":
            logger.warning(
                "Python crypt module is unavailable; using precomputed SHA-512 password hash."
            )
            return _DEFAULT_KIVE_PASSWORD_HASH
        raise


def _set_instance_config_multiline(
    cmds: Cmds, instance: str, key: str, value: str
) -> None:
    """Set an Incus config key by piping value through stdin."""
    result = cmds.incus.run(
        ["config", "set", instance, key, "-"],
        input=value,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Failed to set {key}")


def _instance_exists(cmds: Cmds, instance: str) -> bool:
    out = cmds.incus.output(["list", instance, "--format", "csv"])
    return out.startswith(instance)


def _instance_is_cloud_variant(cmds: Cmds, instance: str) -> bool:
    out = cmds.incus.output(["config", "show", instance])
    return bool(re.search(r"^\s*image\.variant:\s*cloud\s*$", out, re.MULTILINE))


def _get_default_host_interface(cmds: Cmds) -> str:
    if cmds.ip.ok(["link", "show", "docker0"]):
        return "docker0"
    out = cmds.ip.output(["route", "get", "8.8.8.8"])
    m = re.search(r"dev\s+(\S+)", out)
    if m:
        return m.group(1)
    out = cmds.ip.output(["route"])
    for line in out.splitlines():
        m = re.search(r"default.*dev\s+(\S+)", line)
        if m:
            return m.group(1)
    return ""


def _get_bridge_cidr(cmds: Cmds, iface: str) -> str:
    out = cmds.ip.output(["-o", "-f", "inet", "addr", "show", "--", iface])
    parts = out.split()
    return parts[3] if len(parts) >= 4 else ""


def _find_ssh_pubkey() -> str:
    home = Path.home()
    for name in ("id_ed25519.pub", "id_rsa.pub"):
        p = home / ".ssh" / name
        if p.exists():
            return p.read_text().strip()
    return ""


def _get_instance_ipv4_for_bridge(instance: str, bridge_cidr: str) -> str:
    if not bridge_cidr or "/" not in bridge_cidr:
        return "172.30.0.50"
    gateway = bridge_cidr.split("/")[0]
    octets = gateway.split(".")
    if len(octets) != 4:
        return "172.30.0.50"
    hashed = int(hashlib.sha256(instance.encode()).hexdigest()[:8], 16)
    host = 50 + (hashed % 180)
    if host in (0, 1, 255):
        host = 50
    return f"{octets[0]}.{octets[1]}.{octets[2]}.{host}"


# ---------------------------------------------------------------------------
# Cloud-init config
# ---------------------------------------------------------------------------

def _ensure_user_data(cmds: Cmds, instance: str) -> bool:
    logger.info("Configuring cloud-init user data for %s...", instance)
    pubkey = _find_ssh_pubkey()
    password_hash = _generate_password_hash("kive1234")
    ssh_key_block = (
        f"\n    ssh_authorized_keys:\n      - {pubkey}" if pubkey else ""
    )
    userdata = f"""\
#cloud-config
users:
  - name: ubuntu
    gecos: Ubuntu
    sudo: ALL=(ALL) NOPASSWD:ALL
    groups: sudo
    shell: /bin/bash
    lock_passwd: false
    plain_text_passwd: kive1234
    passwd: {password_hash}{ssh_key_block}
ssh_pwauth: true
package_update: false
package_upgrade: false
packages:
  - openssh-server
write_files:
  - path: /etc/systemd/system/serial-getty@ttyS0.service.d/override.conf
    owner: root:root
    permissions: '0644'
    content: |
      [Service]
      ExecStart=
      ExecStart=-/sbin/agetty --autologin ubuntu --keep-baud 115200,38400,9600 %I $TERM
runcmd:
  - [systemctl, daemon-reload]
  - [systemctl, enable, --now, ssh]
  - [systemctl, restart, serial-getty@ttyS0]
  - [sh, -c, 'systemctl enable --now incus-agent || true']
  - [sh, -c, 'systemctl enable --now lxd-agent || true']
"""
    _set_instance_config_multiline(cmds, instance, "user.user-data", userdata)
    return True


def _enable_network_config(cmds: Cmds, instance: str, host_interface: str) -> bool:
    logger.info("Configuring cloud-init network config for %s...", instance)
    network_cidr = _get_bridge_cidr(cmds, host_interface)
    if host_interface == "docker0" and network_cidr:
        gateway = network_cidr.split("/")[0]
        prefix = network_cidr.split("/")[1]
        vm_ipv4 = _get_instance_ipv4_for_bridge(instance, network_cidr)
        logger.info("Using static IPv4 %s/%s for %s on docker0", vm_ipv4, prefix, instance)
        network_config = f"""\
version: 2
ethernets:
  enp5s0:
    dhcp4: false
    addresses: [{vm_ipv4}/{prefix}]
    gateway4: {gateway}
    nameservers:
      addresses: [8.8.8.8,1.1.1.1]
"""
    else:
        network_config = """\
version: 2
ethernets:
  enp5s0:
    dhcp4: true
    dhcp6: false
    nameservers:
      addresses: [8.8.8.8,1.1.1.1]
"""
    _set_instance_config_multiline(cmds, instance, "user.network-config", network_config)
    return True


def _ensure_network_device(cmds: Cmds, instance: str, host_interface: str) -> bool:
    out = cmds.incus.output(["config", "device", "list", instance])
    if re.search(r"^eth0\s*$", out, re.MULTILINE):
        return False
    if not host_interface:
        host_interface = _get_default_host_interface(cmds)
        if not host_interface:
            logger.error("Unable to determine host network interface for VM network device.")
            sys.exit(1)
    nictype = (
        "bridged"
        if (host_interface == "docker0" or host_interface.startswith("br-"))
        else "macvlan"
    )
    logger.info("Adding network device eth0 on %s (nictype=%s)...", host_interface, nictype)
    cmds.incus.run(
        [
            "config", "device", "add",
            instance, "eth0", "nic",
            f"nictype={nictype}",
            f"parent={host_interface}",
        ],
        check=False,
    )
    return True


# ---------------------------------------------------------------------------
# Workspace disk
# ---------------------------------------------------------------------------

def _sudo(*args: str) -> subprocess.CompletedProcess[str]:
    """Run a privileged system command directly (no guix wrapping needed)."""
    return subprocess.run(
        ["sudo", "--"] + list(args),
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def _handle_workspace_disk(
    cmds: Cmds, instance: str, image_path: Path, root: Path, workdir: Path
) -> None:
    logger.info("Building workspace disk image at %s...", image_path)

    if not image_path.exists():
        logger.info("Creating workspace disk image with qemu-img and nbd...")

        # Load nbd kernel module
        _sudo("modprobe", "nbd", "max_part=8")

        # Clean stale mountpoint
        mount_dir = workdir / "kive-code-mount"
        if mount_dir.exists():
            logger.info("Cleaning stale mountpoint %s...", mount_dir)
            _sudo("umount", "--", str(mount_dir))

        # Clean stale nbd0 attachment
        nbd_pid_file = Path("/sys/block/nbd0/pid")
        if nbd_pid_file.exists():
            try:
                pid = int(nbd_pid_file.read_text().strip())
                if pid != 0:
                    logger.info("Cleaning stale /dev/nbd0 attachment...")
                    cmds.qemu_nbd.run(["-d", "/dev/nbd0"], sudo=True, check=False)
                    _sudo("kill", "--", str(pid))
                    time.sleep(1)
            except (ValueError, OSError):
                pass

        # Create qcow2 image
        cmds.qemu_img.run(["create", "-f", "qcow2", "--", str(image_path), "2G"])

        # Connect with nbd
        cmds.qemu_nbd.run(["-c", "/dev/nbd0", "--", str(image_path)], sudo=True)

        # Format and mount
        _sudo("mkfs.ext4", "-F", "/dev/nbd0")
        mount_dir.mkdir(parents=True, exist_ok=True)
        _sudo("mount", "--", "/dev/nbd0", str(mount_dir))

        # Copy files, excluding workdir if it lives inside root
        rsync_args = ["-a"]
        if str(workdir).startswith(str(root) + "/"):
            rel_path = str(workdir)[len(str(root)) + 1:]
            rsync_args += [f"--exclude={rel_path}"]
        rsync_args += ["--", str(root) + "/", str(mount_dir) + "/"]
        cmds.rsync.run(rsync_args, sudo=True)
        subprocess.run(["sync"], check=True)

        # Unmount and disconnect
        _sudo("umount", "--", str(mount_dir))
        cmds.qemu_nbd.run(["-d", "/dev/nbd0"], sudo=True)
        try:
            mount_dir.rmdir()
        except OSError:
            pass
    else:
        logger.info("Using existing image %s.", image_path)

    logger.info("Attaching workspace disk to %s...", instance)
    cmds.incus.run(
        [
            "config", "device", "add",
            instance, "kive-code", "disk",
            f"source={image_path}",
        ],
        check=False,
    )


# ---------------------------------------------------------------------------
# Incus daemon
# ---------------------------------------------------------------------------

def _ensure_incus_daemon(cmds: Cmds, root: Path, workdir: Path) -> None:
    if cmds.incus.ok(["info"]):
        return

    if not shutil.which("ws-start-incus-daemon"):
        logger.error("Incus daemon is not running and ws-start-incus-daemon is not available.")
        logger.error("Start the daemon manually or install the helper script.")
        sys.exit(1)

    logger.info("Starting Incus daemon using ws-start-incus-daemon...")
    subprocess.run(
        ["sudo", "--", "pkill", "-f", "incusd --group incus-admin"],
        check=False,
    )

    log_file = workdir / "incusd_restart.log"
    with open(log_file, "w") as f:
        subprocess.Popen(
            ["ws-start-incus-daemon", "--", str(root)],
            stdout=f,
            stderr=f,
        )

    time.sleep(5)
    if not cmds.incus.ok(["info"]):
        logger.error("Failed to start Incus daemon. See %s.", log_file)
        sys.exit(1)


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main(args: argparse.Namespace) -> None:
    cmds = Cmds.create()

    root: Path = args.root
    workdir: Path = args.workdir
    instance: str = args.instance
    image_path: Path = workdir / args.image_name
    pool: str = args.pool
    profile: str = args.profile
    root_size: str = args.root_size
    memory: str = args.memory
    cpu: str = args.cpu
    host_interface: str = args.host_interface

    workdir.mkdir(parents=True, exist_ok=True)

    cmds.require_all()

    _ensure_incus_daemon(cmds, root, workdir)

    # Storage pool
    try:
        out = cmds.incus.output(["storage", "list", "--format=csv"])
        if not out or not re.search(
            f"^{re.escape(pool)},", out, re.MULTILINE
        ):
            logger.info("Creating storage pool %s...", pool)
            cmds.incus.run(["storage", "create", pool, "dir"], check=False)
    except Exception as e:
        logger.warning("Could not create storage pool: %s", e)

    # Profile
    if not cmds.incus.ok(["profile", "show", profile]):
        logger.info("Profile %s not found. Creating it...", profile)
        cmds.incus.run(["profile", "create", profile])

    # Root disk in profile
    if not cmds.incus.ok(["profile", "device", "show", profile]):
        logger.info("Adding root disk to profile %s...", profile)
        cmds.incus.run(
            [
                "profile", "device", "add",
                profile, "root", "disk",
                f"pool={pool}",
                "path=/",
                f"size={root_size}",
            ]
        )

    # Create VM instance
    created_new_instance = False
    if not _instance_exists(cmds, instance):
        logger.info("Creating VM instance %s...", instance)
        cmds.incus.run(
            [
                "create", "images:ubuntu/noble/cloud", instance,
                "--vm",
                "--config", f"limits.cpu={cpu}",
                "--config", f"limits.memory={memory}",
                "--profile", profile,
            ]
        )
        created_new_instance = True
    else:
        if not _instance_is_cloud_variant(cmds, instance):
            logger.error(
                f"Existing VM '{instance}' was not created from a cloud image "
                "and won't reliably apply login/agent config.",
            )
            logger.error(
                f"Please run: incus delete -f -- {instance} && "
                f"./utils/dev build-vm {instance}",
            )
            sys.exit(2)
        logger.info("Instance %s already exists. Skipping creation.", instance)
        out = cmds.incus.output(["info", instance])
        if out and not re.search(
            r"^Status:\s+Running$", out, re.IGNORECASE | re.MULTILINE
        ):
            logger.info("Starting instance %s...", instance)
            cmds.incus.run(["start", instance])

    # Configure networking and cloud-init
    restart_required = False

    if _ensure_network_device(cmds, instance, host_interface):
        if not host_interface:
            host_interface = _get_default_host_interface(cmds)
        restart_required = True

    if _ensure_user_data(cmds, instance):
        restart_required = True

    if _enable_network_config(cmds, instance, host_interface):
        restart_required = True

    if not created_new_instance:
        logger.info(
            "Note: cloud-init usually runs only on first boot. "
            "Existing VMs may require recreation to apply updated login/agent settings."
        )

    if restart_required:
        try:
            out = cmds.incus.output(["info", instance])
            if out and re.search(
                r"^Status:\s+Running$", out, re.IGNORECASE | re.MULTILINE
            ):
                logger.info("Restarting %s to apply configuration changes...", instance)
                cmds.incus.run(["restart", instance])
            elif out:
                logger.info("Starting %s after configuration changes...", instance)
                cmds.incus.run(["start", instance])
        except Exception as e:
            logger.warning("Could not restart instance: %s", e)

    # Workspace disk
    try:
        out = cmds.incus.output(["config", "show", instance])
        if out and "kive-code:" not in out:
            _handle_workspace_disk(cmds, instance, image_path, root, workdir)
        elif out and "kive-code:" in out:
            logger.info("Device 'kive-code' is already attached to %s.", instance)
    except Exception as e:
        logger.warning("Could not check workspace disk: %s", e)

    logger.info(
        "Build step complete. Use ws-enter-vm (or ./utils/dev enter-vm) to connect to %s.",
        instance,
    )


# ---------------------------------------------------------------------------
# Argument parser (also used by entrypoint.py)
# ---------------------------------------------------------------------------

def _default_root() -> Path:
    # build_vm.py is at  Kive/utils/kivedevel/kivedevel/build_vm.py
    # Kive root is 4 levels up
    return Path(__file__).resolve().parents[3]


def register_subcommand(subparsers) -> None:  # type: ignore[type-arg]
    """Register the build-vm subcommand with an argparse subparsers object."""
    default_root = _default_root()
    p = subparsers.add_parser(
        "build-vm",
        help="Build and configure an Incus VM with cloud-init",
    )
    log_group = p.add_mutually_exclusive_group()
    log_group.add_argument(
        "--quiet",
        action="store_true",
        help="Only show errors",
    )
    log_group.add_argument(
        "--verbose",
        action="store_true",
        help="Show informational progress messages",
    )
    log_group.add_argument(
        "--debug",
        action="store_true",
        help="Show debug logging, including full command lines",
    )
    p.add_argument(
        "--log-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="Write a persistent log file (default: <workdir>/build-vm.log)",
    )
    p.add_argument(
        "instance",
        nargs="?",
        default="kive-minimal",
        help="Incus instance name (default: kive-minimal)",
    )
    p.add_argument(
        "--root",
        type=Path,
        default=default_root,
        metavar="DIR",
        help=f"Repository root to sync into the workspace disk (default: {default_root})",
    )
    p.add_argument(
        "--workdir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Working directory for build artefacts (default: <root>/tmp/build)",
    )
    p.add_argument(
        "--image-name",
        default="kive-code.qcow2",
        metavar="NAME",
        help="Filename of the workspace qcow2 image (default: kive-code.qcow2)",
    )
    p.add_argument("--pool", default="default", help="Incus storage pool (default: default)")
    p.add_argument("--profile", default="default", help="Incus profile (default: default)")
    p.add_argument("--root-size", default="10GiB", help="Root disk size (default: 10GiB)")
    p.add_argument("--memory", default="1GB", help="VM memory limit (default: 1GB)")
    p.add_argument("--cpu", default="1", help="VM CPU count (default: 1)")
    p.add_argument(
        "--host-interface",
        default="",
        metavar="IFACE",
        help="Host network interface for the VM NIC (auto-detected by default)",
    )
    p.set_defaults(func=_run)


def _run(args: argparse.Namespace) -> None:
    """Resolve defaults that depend on other args, then call main()."""
    if args.workdir is None:
        args.workdir = args.root / "tmp" / "build"
    configure_logging(args, args.workdir)
    main(args)


def configure_logging(args: argparse.Namespace, workdir: Path) -> None:
    """Configure console and file logging for a build run."""
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

    handlers: list[logging.Handler] = [
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
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
    logger.debug("Logging configured at %s; log file=%s", logging.getLevelName(level), log_file)
