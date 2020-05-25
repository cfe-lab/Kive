This role creates users and deploys files that are common to the Slurm
Controller and Slurm Workers. It will:

- Create a user called `slurm` with a consistent UID
- Creates the directories that Slurmm might write files to
- Put copies of the shared configuration files (that `slurmd` and
  `slurmctld` both use) in the appropriate places