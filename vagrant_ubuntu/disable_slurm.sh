# Disable slurm commands to emulate the Travis environment.

mv /usr/bin/sbatch /usr/bin/sbatch_disabled
mv /usr/bin/sacct /usr/bin/sacct_disabled
mv /usr/bin/sinfo /usr/bin/sinfo_disabled
mv /usr/bin/scancel /usr/bin/scancel_disabled
