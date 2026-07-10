# Contributing to Kive

If you like this project and want to make it better, help out. You could report
a bug, or pitch in with some development work.

## Bug reports and enhancement requests

Please create issue descriptions [on GitHub](https://github.com/cfe-lab/Kive/issues).
Be as specific as possible. Which version are you using? What did you do? What
did you expect to happen? Are you planning to submit your own fix in a pull
request?

## Development setup

The recommended local development environment uses Incus to manage an isolated
VM or container. See [dev-env/README.md](dev-env/README.md) for the full setup
guide.

Quick start:

```sh
# Install host dependencies (Ubuntu 24.04)
sudo apt-get update
sudo apt-get install -y incus rsync qemu-utils qemu-system-x86 ovmf \
  iproute2 socat uv

# Prepare the host, build the instance, and enter it
utils/dev prepare-host
utils/dev build-vm
utils/dev enter-vm
```

### Editor workflow

Source code lives on the host at the repository checkout. Edits are visible
inside the development instance as follows:

- **VM mode:** Changes are synchronised on the next `utils/dev build-vm` run.
- **Container mode:** The source tree is bind-mounted at `/mnt/kive-code/`;
  changes are visible immediately.

### Dependency management

- **Python packages (Kive):** Listed in `requirements.txt`,
  `requirements-dev.txt`, `requirements-test.txt`. Install with `pip`.
- **Python packages (kivedevel):** Managed by `uv` with
  `utils/kivedevel/pyproject.toml`. Run `uv sync --project utils/kivedevel`.
- **Node.js packages:** Managed by `npm` with `package.json`.
- **Ansible roles:** Managed with `requirements.yml` under `dev-env/`.

## Running the application

Inside the development instance (via `utils/dev enter-vm`):

```sh
# Start the development server (if not already running)
cd /usr/local/share/Kive/kive
python manage.py runserver 0.0.0.0:8000
```

Or use the pre-configured Apache setup started by provisioning.

### Inspecting services

```sh
systemctl status postgresql   # Database
systemctl status slurmctld    # Slurm controller
systemctl status slurmd       # Slurm compute node
systemctl status apache2      # Web server
```

## Testing

### `kivedevel` unit tests (fast, no external dependencies)

```sh
uv run --project utils/kivedevel --extra test --frozen \
  python -m pytest utils/kivedevel/kivedevel/
```

These tests use mocks and do not require Incus, PostgreSQL, or any running
services.

### Django tests (require PostgreSQL)

```sh
cd kive
# Set up database credentials (see INSTALL.md for details)
pytest --flake8
```

### API tests

```sh
cd api
pytest
```

### Frontend tests

```sh
npm test
```

### End-to-end smoke test (requires Incus)

```sh
utils/dev smoke-local-install
```

This is the same test run by CI. It creates a VM, runs provisioning, validates
the instance, and tests the API. Run `utils/dev purge` afterward to clean up.

### Test categories

| Category | Requirements | When to run |
|----------|-------------|-------------|
| `kivedevel` unit tests | None | Any change to `utils/kivedevel/` |
| Django tests | PostgreSQL | Application-code change |
| API tests | PostgreSQL | API or serializer change |
| Frontend tests | Node.js | JavaScript/TypeScript change |
| End-to-end smoke | Incus + QEMU | Networking, provisioning, or release |

### CI pipeline

The CI workflow (`.github/workflows/build-and-test.yml`) runs:

1. `pip install` + Django tests with PostgreSQL
2. API tests
3. End-to-end smoke test: `prepare-host` + `smoke-local-install` (VM mode)

## Deploying a release

1. Make sure the code works in your development environment. Run the relevant
   tests from the table above, or check that CI passed.
2. Check if the `kiveapi` package needs a version bump by looking for new
   commits in the `api/` folder.
3. Check that all issues in the current milestone are closed.
4. Collect and package static files:
   ```sh
   cd kive
   ./manage.py collectstatic -c --no-input
   cd ..
   tar -czvf static_root.tar.gz static_root
   rm -rf static_root
   ```
5. Review changes in `kive/settings.py` since the last release for new
   environment variables.
6. [Create a release](https://github.com/cfe-lab/Kive/releases) on GitHub.
   Use `vX.Y` as the tag. Attach `static_root.tar.gz` to the release.
7. Deploy to the production server:
   ```sh
   ssh user@server
   cd /usr/local/share/Kive/kive
   git fetch
   git checkout tags/vX.Y
   ./manage.py migrate
   ```
   See `cluster-setup/README.md` for production deployment details.
8. Update the Kive API library if needed:
   ```sh
   cd /usr/local/share/Kive/api
   pip install -e .
   ```

## See also

- [dev-env/README.md](dev-env/README.md) — Local development setup
- [utils/kivedevel/README.md](utils/kivedevel/README.md) — CLI reference
- [INSTALL.md](INSTALL.md) — Manual / legacy installation
- [cluster-setup/README.md](cluster-setup/README.md) — Production deployment
