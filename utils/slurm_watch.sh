watch bash -c '~/slurm_queue.sh|head -n 42 && ~/slurm_queue.sh -h | grep "RUNNING" | wc -l && ~/slurm_queue.sh -h | wc -l'
