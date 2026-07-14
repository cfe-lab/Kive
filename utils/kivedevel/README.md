# `utils/dev` — Kive development environment manager

This is the reference manual for the `utils/dev` command-line tool. For the
step-by-step local development guide, see [dev-env/README.md](../../dev-env/README.md).

---

## Command overview

```
utils/dev [--purge] COMMAND [options]
```

| Subcommand | Purpose |
|------------|---------|
| `build-vm` | Create and provision an Incus instance (VM or container) |
| `validate-vm` | Check instance state, devices, Slurm and Singularity probes |
| `test-api` | Probe the Kive API (login, authentication, dataset endpoint) |
| `prepare-host` | Initialise Incus, create bridge, configure host networking |
| `check-network` | Verify guest network readiness in a temporary instance |
| `enter-vm` | Open an interactive shell in an existing instance |
| `smoke-local-install` | Run `build-vm` + `validate-vm` + `test-api` in sequence |
| `cleanup-local-install` | Delete an instance and remove its workdir |
| `purge` | Remove all resources created by `utils/dev` |
| `--purge` | Shortcut for the `purge` subcommand |

---

## Global options

| Option | Description |
|--------|-------------|
| `--purge` | Purge all resources (shortcut, overrides subcommand) |

Most subcommands accept `--quiet`, `--verbose`, and `--debug` for log control.
(Exceptions are noted in the individual command descriptions.)

---

## Subcommand reference

### `build-vm`

Create and provision an Incus development instance.

```
utils/dev build-vm [instance] [options]
```

| Argument / Option | Default | Description |
|-------------------|---------|-------------|
| `instance` | `kive-minimal` | Instance name (positional, optional) |
| `--instance-type` | `vm` | `vm` or `container` |
| `--vm-network` | `kive-lab-br` | Managed Incus bridge for VM NIC |
| `--host-interface` | (auto-detect) | Host interface for container NIC |
| `--no-provision` | (provision enabled) | Skip provisioning after creation |
| `--web-port` | `8000` | Host port for the web proxy |
| `--no-web-proxy` | `false` | Skip proxy device creation |
| `--workdir` | `<root>/tmp~/build` | Working directory |
| `--cpu` | `4` | CPU limit |
| `--memory` | `8GiB` | Memory limit |
| `--root-size` | `60GiB` | Root disk size |
| `--profile` | `default` | Incus profile |
| `--pool` | `default` | Incus storage pool |
| `--image-name` | `kive-code.qcow2` | Workspace image filename |
| `--root` | (repository root) | Kive source checkout path |

**Requires `sudo`:** No (Incus operations are user-level).

**Resources created:** Incus instance, managed bridge (if missing), workspace
image (VM mode) or host directory (container mode), proxy device, provisioning
marker files.

**Exit behaviour:** Returns 0 on success. Raises `RuntimeError` on provisioning
failure, `SystemExit` on configuration errors.

**Examples:**

```sh
utils/dev build-vm
utils/dev build-vm --instance-type container --no-provision
utils/dev build-vm my-dev --vm-network my-bridge
```

---

### `validate-vm`

Validate an existing development instance.

```
utils/dev validate-vm [instance] [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `instance` | `kive-minimal` | Instance name (positional) |
| `--instance-type` | `vm` | Expected instance type |
| `--workdir` | `<root>/tmp~/build` | Working directory |

**Checks performed:**
- Instance exists and is running
- Required device `kive-code` is attached
- Slurm services and hostname (logged as warnings, not fatal)
- Singularity availability (fatal in VM mode, warning in container mode)
- Container-only: `kive-code` device path and source directory

**Examples:**

```sh
utils/dev validate-vm
utils/dev validate-vm --instance-type container
```

---

### `test-api`

Probe the Kive API in an existing instance.

```
utils/dev test-api [instance] [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `instance` | `kive-minimal` | Instance name (positional) |
| `--username` | `kive` | API username |
| `--password` | `kive` | API password |
| `--instance-type` | (auto-detect) | Instance type hint for VM fallback |
| `--port` | `8000` | HTTP port to probe |
| `--base-url` | (auto-detect) | Explicit API base URL |
| `--workdir` | `<root>/tmp~/build` | Working directory |

**Checks performed:**
- Login page returns HTTP 200
- Anonymous `/api/datasets/` returns a different status than authenticated
- Authenticated `/api/datasets/` returns HTTP 200 with valid JSON
- CSRF token extraction and cookie-based session

**Fallback paths (VM mode):**
1. HTTP probe via proxy device (`127.0.0.1:8000`)
2. SSH-based API startup if the VM looks provisioned
3. `incus exec`-based API probe from within the VM

**Examples:**

```sh
utils/dev test-api
utils/dev test-api my-instance --port 8080 --username admin --password secret
```

---

### `prepare-host`

Prepare the local machine for running development instances.

```
utils/dev prepare-host [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--backend` | `incus` | Backend type (only `incus` is supported) |
| `--bridge` | `kive-lab-br` | Bridge network name |

**Requires `sudo`:** Yes (for systemctl and iptables).

**Resources modified:**
- Enables `incus.service` and `incus.socket`
- Initialises Incus (no-op if already initialised)
- Creates/ensures `kive-lab-br` bridge with `10.77.77.1/24`
- Sets `ipv4.nat=true`, `ipv4.routing=true`, `ipv4.firewall=true`
- Enables `net.ipv4.ip_forward=1`
- Adds iptables ACCEPT rules to `DOCKER-USER` and `FORWARD`
- Adds MASQUERADE rule to `POSTROUTING`

**Idempotent:** Yes. Safe to run multiple times.

**Examples:**

```sh
sudo --preserve-env=PATH utils/dev prepare-host
sudo --preserve-env=PATH utils/dev prepare-host --bridge my-bridge
```

---

### `check-network`

Verify guest network readiness in a temporary instance.

```
utils/dev check-network [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--backend` | `incus` | Backend type |
| `--instance` | `network-smoke` | Temporary instance name |
| `--host` | `archive.ubuntu.com` | Connectivity test target |

**Creates a temporary instance, runs connectivity checks, then deletes it.**

---

### `enter-vm`

Open an interactive shell in an existing development instance.

```
utils/dev enter-vm [instance]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `instance` | `kive-minimal` | Instance to enter |

If the instance is stopped, it is started first. The shell runs as `ubuntu`.

**Examples:**

```sh
utils/dev enter-vm
utils/dev enter-vm my-instance
```

---

### `smoke-local-install`

Run the end-to-end smoke test.

```
utils/dev smoke-local-install [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--instance` | `ci-smoke` | Instance name |
| `--instance-type` | `vm` | `vm` or `container` |
| `--workdir` | `<root>/tmp~/build` | Working directory |
| `--vm-network` | (auto-detect) | Managed bridge for VM NIC |

**What it runs:**
1. `build-vm` with the given options
2. `validate-vm` to check instance health
3. `test-api` to verify the API

**Does not clean up.** Run `utils/dev purge` after the test.

**Examples:**

```sh
utils/dev smoke-local-install
utils/dev smoke-local-install --instance-type container
```

---

### `cleanup-local-install`

Delete a smoke-test instance and remove its workdir.

```
utils/dev cleanup-local-install [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--instance` | `ci-smoke` | Instance to delete |
| `--workdir` | `<root>/tmp~/build` | Workdir to remove |

---

### `purge`

Remove all resources created by `utils/dev`.

```
utils/dev purge [options]
```

| Option | Description |
|--------|-------------|
| `--instance` | Explicit instance to purge (may be repeated) |
| `--workdir` | Explicit workdir to purge (may be repeated) |
| `--root` | Repository root for discovery |

**What is removed:**
- Tagged Incus instances (`user.kive.devel.created-by: utils/dev`)
- Marked build directories (containing `.kive-devel-resource.json`)
- Tagged managed networks
- Port forwards and resource registry

**What is NOT removed:**
- Untagged legacy instances (logged as warnings)
- Firewall rules, `sysctl` changes, system packages

**Idempotent:** Yes.

**Examples:**

```sh
utils/dev purge
utils/dev purge --instance my-instance
```

---

## Resource ownership

Resources created by `utils/dev` are identified as follows:

| Resource | Ownership marker |
|----------|-----------------|
| Incus instances | Config key `user.kive.devel.created-by: utils/dev` |
| Build directories | File `.kive-devel-resource.json` with `created_by: utils/dev` |
| Managed networks | Same marker file with `kind: network` |
| Port forwards | Resource registry at `<root>/tmp~/.kive-devel-resources.json` |

Resources without these markers are **not** automatically removed by `purge`,
unless explicitly named with `--instance` or `--workdir`.

---

## Provisioning state model

Provisioning uses marker files at `/var/lib/kive-provision/` inside the
instance:

| Marker | Meaning |
|--------|---------|
| `started` | Provisioning script has started |
| `done` | Provisioning completed successfully |
| `failed` | Provisioning failed (log at `/var/log/kive-provision.log`) |

The host polls for these markers. In VM mode, polling uses `incus file pull`;
in container mode it uses `incus exec`.

| Condition | Behaviour |
|-----------|-----------|
| `done` found | Success, provisioning complete |
| `failed` found | RuntimeError with log and diagnostics |
| `started` > 14 min | RuntimeError (stuck) |
| 5 consecutive transport errors | RuntimeError (container mode only) |
| Timeout (default 900 s) | RuntimeError |

---

## Workspace model

| Aspect | VM mode | Container mode |
|--------|---------|----------------|
| Type | qcow2 disk image | Host directory bind-mount |
| Create | `qemu-img create -f qcow2` | `mkdir` + `rsync` |
| Mount | `qemu-nbd` + `sudo mount` | Incus disk device |
| Update | Rebuild required (`rsync` on next build) | Live (bind mount) |
| Location | `<workdir>/kive-code.qcow2` | `<workdir>/kive-code-host/` |
| Exclusion | `tmp/` and own workdir | Same |

---

## Networking model

The default managed bridge is `kive-lab-br`:

| Property | Value |
|----------|-------|
| Address | `10.77.77.1/24` |
| NAT | Enabled (`ipv4.nat=true`) |
| DHCP | Enabled (`ipv4.dhcp=true`) |
| DNS | `1.1.1.1, 8.8.8.8` |
| Firewall | Enabled (`ipv4.firewall=true`) |
| Routing | Enabled (`ipv4.routing=true`) |

### Network flow (VM mode)

```
VM eth0 → kive-lab-br (10.77.77.1) → host NIC → internet
           |-- DHCP: 10.77.77.x
           |-- NAT: MASQUERADE
           |-- DNS: 1.1.1.1 / 8.8.8.8
```

### Mutable vs immutable bridge settings

Incus bridge settings that are safe to repair automatically:
`ipv4.nat`, `ipv4.routing`, `ipv4.firewall`, `ipv4.dhcp`, `ipv6.address`,
`raw.dnsmasq`.

The bridge **IPv4 address** is never changed automatically. If the bridge
exists with a different address, the tool exits with an error.

---

## Validation and API probes

### Instance validation (`validate-vm`)

1. Instance exists → running → has `kive-code` device
2. Slurm probe (hostname, services, commands) — warnings only
3. Singularity probe (version, exec test) — fatal in VM mode

### API testing (`test-api`)

1. Resolve base URL (proxy device → VM IP → explicit override)
2. GET `/login/` — expect 200, extract CSRF token
3. GET `/api/datasets/?limit=1` anonymous — expect non-200
4. POST `/login/` with credentials + CSRF token
5. GET `/api/datasets/?limit=1` authenticated — expect 200 + valid JSON

---

## Diagnostics and logs

| Source | Location / command |
|--------|-------------------|
| Provisioning log | Instance: `/var/log/kive-provision.log` |
| Cloud-init output | Instance: `/var/log/cloud-init-output.log` |
| Ansible output | Instance: `/var/log/kive-provision.log` |
| Apache logs | Instance: `/var/log/apache2/` |
| Kive logs | Instance: `/usr/local/share/Kive/kive/logs/` |
| Slurm logs | Instance: `/var/log/slurm-llnl/` |
| Incus metadata | `incus info <instance>` |
| DHCP leases | `incus network list-leases kive-lab-br` |
| Bridge config | `incus network show kive-lab-br` |
| Host firewall | `iptables -S FORWARD DOCKER-USER` |

---

## Development of `kivedevel`

### Python dependencies

```sh
uv sync --project utils/kivedevel
```

### Running tests

```sh
# All unit tests
uv run --project utils/kivedevel --extra test --frozen \
  python -m pytest utils/kivedevel/kivedevel/

# Specific test file
uv run --project utils/kivedevel --extra test --frozen \
  python -m pytest utils/kivedevel/kivedevel/test_network.py

# Fast unit tests only (no database, no Incus)
uv run --project utils/kivedevel --extra test --frozen \
  python -m pytest utils/kivedevel/kivedevel/ -m "not slow"
```

### Linting and type checking

```sh
uv run --project utils/kivedevel --extra dev --frozen \
  ruff check utils/kivedevel/kivedevel/

uv run --project utils/kivedevel --extra dev --frozen \
  python -m mypy utils/kivedevel/kivedevel/
```

### Project structure

```
utils/kivedevel/
  pyproject.toml        # Package metadata, dependencies, tool config
  kivedevel/
    entrypoint.py       # CLI entry point and subcommand dispatch
    build_vm/           # Instance creation, provisioning, networking
    backends/           # Incus host preparation
    checks.py           # Validate-vm and test-api subcommands
    local_install.py    # Smoke-local-install orchestration
    enter_vm.py         # Interactive shell
    kv_commands.py      # Command execution wrappers (incus, ip, rsync)
    shared.py           # Logging, instance helpers
    test_*.py           # Unit tests (no Incus daemon required)
```

### Test categories

| Category | Description | Run command |
|----------|-------------|-------------|
| Unit tests | Mock-based, no external deps | `pytest utils/kivedevel/kivedevel/` |
| Database-backed | Require PostgreSQL | `pytest kive/` (Django tests) |
| Smoke tests | Require Incus daemon | `utils/dev smoke-local-install` |

---

## Compatibility

| Component | Version |
|-----------|---------|
| Host OS | Ubuntu 24.04 (Noble) |
| Guest OS | Ubuntu 24.04 (Noble) cloud image |
| `kivedevel` Python | 3.13 |
| Kive Python | 3.10 (provisioned) |
| PostgreSQL | 16 (guest) |
| Slurm | 23.02.5 (source-built) |
| Incus | Latest from Ubuntu repos |
| QEMU / KVM | 8.x |
| Ansible | 11.x |
| Singularity | 3.x (guest, VM mode only) |

---

## See also

- [dev-env/README.md](../../dev-env/README.md) — Local development guide
- [CONTRIBUTING.md](../../CONTRIBUTING.md) — Contributing guide
- [cluster-setup/README.md](../../cluster-setup/README.md) — Production deployment
