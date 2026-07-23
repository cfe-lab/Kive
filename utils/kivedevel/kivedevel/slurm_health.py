SLURM_HEALTHCHECK_SCRIPT = """set -eu

# Commands
command -v squeue
command -v sinfo
command -v scontrol
command -v srun

# Services (quiet assertions)
systemctl is-active --quiet munge
systemctl is-active --quiet mariadb
systemctl is-active --quiet slurmdbd
systemctl is-active --quiet slurmctld
systemctl is-active --quiet slurmd

# Controller
controller_status="$(scontrol ping)"
printf 'CONTROLLER_STATUS=%s\\n' "$controller_status"
case "$controller_status" in
  *"is UP"*) ;;
  *)
    echo "Slurm controller is not UP: $controller_status" >&2
    exit 1
    ;;
esac

# Node state
scontrol show node head -o
node_states="$(sinfo -h -N -n head -o '%T' | awk 'NF { print tolower($1) }' | sort -u)"
printf 'NODE_STATES=%s\\n' "$node_states"
if [ "$node_states" != "idle" ]; then
  echo "Node head is not exclusively idle: $node_states" >&2
  exit 1
fi

squeue -a
"""
