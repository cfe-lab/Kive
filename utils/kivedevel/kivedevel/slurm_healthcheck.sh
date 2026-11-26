#!/usr/bin/env bash
set -eu

SERVICE_DEADLINE=120
NODE_DEADLINE=120

# Commands
command -v squeue
command -v sinfo
command -v scontrol
command -v srun

# Bounded service wait with per-service diagnostics
wait_for_service() {
    service="$1"
    deadline="$2"
    end=$(( $(date +%s) + deadline ))

    while [ "$(date +%s)" -lt "$end" ]; do
        state="$(systemctl is-active "$service" 2>/dev/null || true)"

        case "$state" in
            active)
                printf 'SERVICE_%s=active\n' "$service"
                return 0
                ;;
            failed)
                echo "Required service $service entered failed state." >&2
                systemctl status "$service" --no-pager >&2 || true
                journalctl -u "$service" --no-pager --lines=200 >&2 || true
                return 1
                ;;
        esac

        sleep 2
    done

    echo "Required service $service did not become active." >&2
    systemctl status "$service" --no-pager >&2 || true
    journalctl -u "$service" --no-pager --lines=200 >&2 || true
    return 1
}

wait_for_service munge "$SERVICE_DEADLINE"
wait_for_service mariadb "$SERVICE_DEADLINE"
wait_for_service slurmdbd "$SERVICE_DEADLINE"
wait_for_service slurmctld "$SERVICE_DEADLINE"
wait_for_service slurmd "$SERVICE_DEADLINE"

# Bounded controller readiness
CONTROLLER_DEADLINE=120
CONTROLLER_END=$(( $(date +%s) + CONTROLLER_DEADLINE ))
while [ "$(date +%s)" -lt "$CONTROLLER_END" ]; do
    controller_status="$(scontrol ping 2>/dev/null || true)"
    case "$controller_status" in
        *"is UP"*)
            printf 'CONTROLLER_STATUS=%s\n' "$controller_status"
            break
            ;;
    esac
    sleep 2
done

if ! echo "$controller_status" | grep -qi 'is UP'; then
    echo "Slurm controller is not UP." >&2
    echo "$controller_status" >&2
    scontrol ping >&2 || true
    systemctl status slurmctld --no-pager >&2 || true
    journalctl -u slurmctld --no-pager --lines=200 >&2 || true
    exit 1
fi

# Node state
scontrol show node head -o
NODE_DEADLINE_END=$(( $(date +%s) + NODE_DEADLINE ))
while [ "$(date +%s)" -lt "$NODE_DEADLINE_END" ]; do
    node_states="$(sinfo -h -N -n head -o '%T' 2>/dev/null | awk 'NF { print tolower($1) }' | sort -u || true)"
    if [ "$node_states" = "idle" ]; then
        printf 'NODE_STATES=%s\n' "$node_states"
        break
    fi
    sleep 2
done

if [ "$node_states" != "idle" ]; then
    printf 'NODE_STATES=%s\n' "${node_states:-empty}" >&2
    echo "Node head is not exclusively idle." >&2
    scontrol show node head -o >&2 || true
    sinfo -N -l >&2 || true
    systemctl status slurmd --no-pager >&2 || true
    journalctl -u slurmd --no-pager --lines=200 >&2 || true
    exit 1
fi

squeue -a
