# Local development environment

This guide covers setting up and using the **recommended local development
environment** for Kive. It uses [Incus](https://linuxcontainers.org/incus/)
to manage an isolated environment (VM or container) where Kive and its
dependencies are automatically installed.

For production deployment, see [cluster-setup/README.md](../cluster-setup/README.md).
For contributing, see [CONTRIBUTING.md](../CONTRIBUTING.md).

---

## Overview

The development tooling (`utils/dev`) provisions an environment from the
current source checkout. The environment includes:

- **Kive** itself (from the checkout)
- **PostgreSQL** — relational database
- **Slurm** — workload manager (single-node)
- **Apache** — web server with TLS
- **Development TLS** — self-signed certificate for HTTPS
- **Singularity** (optional, VM mode only) — container runtime for pipelines

The tooling manages the full lifecycle: host preparation, instance creation,
provisioning, validation, API testing, and cleanup.

---

## Supported hosts

| Requirement | Details |
|-------------|---------|
| Operating system | Linux (tested on Ubuntu 24.04) |
| Architecture | x86\_64 (amd64) |
| Nested virtualisation | Required for VM mode |
| Other distributions | May work but not tested |
| macOS / Windows | Not supported |

---

## Host prerequisites

### Required for all modes

| Tool | Purpose | Verification |
|------|---------|-------------|
| Incus client + daemon | Instance management | `incus info` |
| `uv` | Python project manager | `uv --version` |
| `rsync` | File synchronisation | `rsync --version` |
| `iproute2` | Network configuration | `ip --version` |
| `socat` | Port forwarding | `socat -V` |
| `sudo` | Privileged operations | — |
| UID/GID subordinate mappings | Unprivileged containers | `incus info` |

### Required for VM mode (default)

| Tool | Purpose | Verification |
|------|---------|-------------|
| QEMU system emulator | VM execution | `qemu-system-x86_64 --version` |
| KVM | Hardware acceleration | `ls -l /dev/kvm` |
| OVMF | UEFI firmware | `/usr/share/ovmf/OVMF.fd` |
| `qemu-utils` | Disk image tools | `qemu-img --version` |

### Installation commands

Ubuntu 24.04:

```sh
sudo apt-get update
# Install system packages (curl is needed for the uv installer)
sudo apt-get install -y incus rsync qemu-utils qemu-system-x86 ovmf \
  iproute2 socat curl

# Install uv (Python project manager) — https://docs.astral.sh/uv/
curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to the current shell's PATH (or log out and back in)
export PATH="$HOME/.local/bin:$PATH"
```

### Incus permissions

After installing Incus, add your user to the `incus-admin` group and start
a new login session:

```sh
sudo usermod -aG incus-admin "$USER"
exec newgrp incus-admin
```

> **Warning:** Members of `incus-admin` have effectively root-level control
> over the Incus daemon.

**Note:** The development tooling (`utils/dev`) itself manages its Python
dependencies via `uv` — you do not need to install Python packages manually.

---

## First-time setup

The recommended sequence for setting up a development environment:

> **Note:** `prepare-host` requires `sudo` for systemctl and iptables commands.
> All other commands run without privilege escalation.

```sh
# 0. Sync development tool dependencies as your user first, so the venv
#    (utils/kivedevel/.venv) is user-owned and can be updated later.
uv sync --project utils/kivedevel

# 1. Prepare the host: initialise Incus, create the Kive bridge, configure
#    IP forwarding and firewall rules.
sudo --preserve-env=PATH utils/dev prepare-host

# 2. Build the development instance (creates a VM by default).
utils/dev build-vm

# 3. Validate the instance: check services, devices, and probes.
utils/dev validate-vm

# 4. Test the API: verify HTTP access, authentication, and JSON responses.
utils/dev test-api

# 5. Enter the instance interactively.
utils/dev enter-vm
```

### What each command does

| Command | Purpose |
|---------|---------|
| `prepare-host` | Starts Incus, creates the `kive-lab-br` bridge (`10.77.77.1/24`), enables IP forwarding, installs firewall rules. |
| `build-vm` | Creates an Incus instance (VM or container), attaches the network, runs cloud-init provisioning. |
| `validate-vm` | Checks instance state, required devices, Slurm and Singularity probes. |
| `test-api` | Probes the Kive API: login page, anonymous vs authenticated access, dataset endpoint. |
| `enter-vm` | Opens an interactive shell inside the instance. |

### First-time setup with container mode

If your host does not support VM mode (no KVM/QEMU), use container mode:

```sh
utils/dev build-vm --instance-type container
utils/dev validate-vm --instance-type container
```

Container mode provides the same services but has limitations (see comparison
below).

---

## Fast smoke-test workflow

For a quick end-to-end check without running individual commands:

```sh
utils/dev smoke-local-install
```

This runs `build-vm`, `validate-vm`, and `test-api` in sequence. By default it
creates a VM instance named `ci-smoke`. The smoke test does **not** perform
cleanup — you must run `utils/dev purge` separately to remove resources.

### Selecting VM vs container mode

```sh
utils/dev smoke-local-install                           # VM mode (default)
utils/dev smoke-local-install --instance-type container  # Container mode
```

---

## VM vs container mode

| Aspect | VM mode (default) | Container mode |
|--------|-------------------|----------------|
| Isolation | Full virtual machine | OS-level container |
| KVM required | Yes | No |
| Startup time | 30–60 s | 5–10 s |
| Slurm | Runs inside VM | Runs inside container |
| Singularity | Supported | Not available |
| Filesystem sharing | Workspace disk image | Host directory mount |
| Privileged fallback | N/A | Automatic on UID/GID errors |
| CI suitability | Full testing | Lightweight smoke tests |
| Realistic local testing | Recommended | Limited |

**Recommendation:** Use VM mode for local development. Use container mode only
when nested virtualisation is unavailable.

---

## Entering and using the environment

### Getting a shell

```sh
utils/dev enter-vm                  # default instance (kive-minimal)
utils/dev enter-vm my-instance      # named instance
```

The shell opens as `ubuntu` user. The Kive source tree appears at
`/usr/local/share/Kive/kive/`.

### Source synchronisation

- **VM mode:** The workspace is a qcow2 disk image attached to the VM.
  Changes on the host are synchronised via `rsync` during build; they are
  **not** live-synced. Re-run `utils/dev build-vm` to update.
- **Container mode:** The workspace is a host directory bind-mounted at
  `/mnt/kive-code/`. Source changes on the host are immediately visible.

### Starting Kive services

Services are started automatically by cloud-init during provisioning.
To inspect or restart:

```sh
# Inside the instance:
systemctl status apache2
systemctl status slurmctld
systemctl status postgresql
```

### Accessing the web interface

Once provisioned, Kive is available at:

- **http://127.0.0.1:8000/** (via proxy device)
- **https://[instance-ip]:443/** (if TLS is configured)

Default credentials: `kive` / `kive`.

### TLS and browser access

The development environment uses a self-signed certificate. Your browser will
warn about the untrusted certificate — this is expected. You may need to add a
security exception or use HTTP on port 8000 for local access.

---

## Rebuilding and updating

| Situation | Action |
|-----------|--------|
| Application source changed | `utils/dev build-vm` — re-runs provisioning |
| Ansible / cloud-init changed | `utils/dev build-vm` — reprovisions |
| Dependencies changed | `utils/dev build-vm` — reprovisions |
| Instance already exists | `build-vm` detects it and reconfigures |
| Stale provisioning state | `utils/dev purge && utils/dev build-vm` |

The tool does **not** automatically detect stale provisioning. When in doubt,
run `utils/dev purge` and rebuild from scratch.

---

## Cleanup

```sh
utils/dev purge
```

This removes:

- **Incus instances** tagged with `user.kive.devel.created-by: utils/dev`
- **Build directories** marked with `.kive-devel-resource.json`
- **Managed networks** tagged via marker files with `kind: network`
- **Port forwards** registered in the resource registry

### What is intentionally not removed

- The `kive-lab-br` bridge (not tagged by `prepare-host`; remove manually if needed)
- The `incusbr0` default bridge
- The `default` Incus storage pool and profile
- Firewall rules (IP forwarding, iptables `FORWARD`, `DOCKER-USER`)
- `sysctl` changes
- System packages

### Removing the bridge manually

Before deleting the bridge, remove or reconfigure the default profile's NIC
that references it:

```sh
incus profile device remove default eth0
incus network delete kive-lab-br
sudo ip link delete kive-lab-br
```

### Idempotence

Purge is safe to run repeatedly. If no resources are found, it exits without
errors.

### Removing explicitly named resources

```sh
utils/dev purge --instance my-instance --workdir /path/to/workdir
```

---

## Host changes and security considerations

> **Warning:** `utils/dev prepare-host` modifies host networking and firewall
> configuration. Review the changes below before running on a shared or
> production machine.

### Changes made by `prepare-host`

| Change | Detail | Persists after purge |
|--------|--------|----------------------|
| IP forwarding | `net.ipv4.ip_forward=1` | Yes |
| Firewall: `FORWARD` | Accept rules for bridge traffic | Yes |
| Firewall: `DOCKER-USER` | Accept rules for bridge traffic | Yes |
| Firewall: `POSTROUTING` | MASQUERADE rule for bridge CIDR | Yes |
| Incus bridge | `kive-lab-br` (10.77.77.1/24) | Yes |
| Incus storage pool | `default` (dir-backed) | Yes |
| Default profile | NIC, root disk | Yes |

### Viewing current state

```sh
sysctl net.ipv4.ip_forward
iptables -S FORWARD
iptables -S DOCKER-USER
iptables -t nat -S POSTROUTING
incus network show kive-lab-br
```

---

## Troubleshooting

### Incus daemon unavailable

**Symptom:** `Error: Failed to connect to Incus`

**Diagnostic:** `incus info`

**Fix:** `sudo systemctl enable --now incus.service incus.socket`

### VM support unavailable

**Symptom:** `Instance type "virtual-machine" is not supported`

**Diagnostic:** `qemu-system-x86_64 --version`

**Fix:** Install `qemu-system-x86` and `ovmf`, then re-run `prepare-host`.

### `/dev/kvm` unavailable

**Symptom:** VMs are extremely slow

**Diagnostic:** `ls -l /dev/kvm`

**Fix:** Enable nested virtualisation in the host BIOS or hypervisor, or use
container mode (`--instance-type container`).

### Missing subordinate UID/GID mappings

**Symptom:** `No uid/gid allocation configured` when creating containers

**Diagnostic:** `incus info` shows no UID/GID map

**Fix:** Configure `/etc/subuid` and `/etc/subgid`, then restart Incus.

### Container UID/GID mapping failures

**Symptom:** The tool retries with `security.privileged=true`

**Diagnostic:** Check `incus info` for subordinate mapping.

**Fix:** The automatic privileged fallback usually succeeds. If not, fix the
subordinate mapping.

### Incompatible existing bridge

**Symptom:** `Existing Incus network kive-lab-br has ipv4.address=X, but Kive
expects 10.77.77.1/24`

**Fix:** Delete the stale bridge: `incus network delete kive-lab-br`, then
re-run the tool.

### CIDR collision

**Symptom:** `CIDR 10.77.77.1/24 overlaps with existing route`

**Fix:** Remove the conflicting route or choose a different subnet.

### Missing DHCP lease

**Symptom:** The tool waits indefinitely for a DHCP lease

**Diagnostic:** `incus network list-leases kive-lab-br`

**Fix:** Check bridge configuration and Incus daemon health.

### Docker forwarding policy

**Symptom:** VMs get DHCP and DNS but cannot reach the internet

**Diagnostic:** `iptables -S FORWARD | grep DROP`

**Fix:** Configure Docker to preserve the `FORWARD` policy:
```json
{ "ip-forward-no-drop": true }
```
Then restart Docker.

### Cloud-init provisioning failed

**Symptom:** `RuntimeError: Provisioning failed inside instance.`

**Diagnostic:** Inside the instance:
```sh
cat /var/log/kive-provision.log
cat /var/log/cloud-init-output.log
cloud-init status --long
```

**Fix:** Check the Ansible playbook for errors, fix, rebuild.

### Slurm not registering

**Symptom:** `squeue` shows no nodes

**Diagnostic:** `sinfo -a`

**Fix:** Check `/etc/slurm-llnl/slurm.conf` and restart `slurmd` + `slurmctld`.

### API unavailable

**Symptom:** `utils/dev test-api` fails

**Diagnostic:**
```sh
curl -k https://127.0.0.1:8000/login/
```

**Fix:** Check that Apache is running inside the instance and that the port
forward is active.

### Self-signed TLS warning

**Symptom:** Browser shows security warning

**Explanation:** The development self-signed certificate is expected. Accept the
warning or configure your own certificate (see `cluster-setup/README.md`).

---

## See also

- [utils/kivedevel/README.md](../utils/kivedevel/README.md) — Full CLI reference
- [CONTRIBUTING.md](../CONTRIBUTING.md) — Contributing guide
- [INSTALL.md](../INSTALL.md) — Manual and legacy installation
- [cluster-setup/README.md](../cluster-setup/README.md) — Production deployment
