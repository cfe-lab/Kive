This role sets up `slurmctld` on the node it runs on.

Note that this does *not* set up `slurmd`; a node that should function
as a Slurm compute node should also run the [slurm node] role to set up `slurmd`.

Like the [slurm node] role, this node depends on:
- the [munge node] role to set up the MUNGE authentication service;
- the [slurm dependencies] role to install slurmctld's dependencies; and
- the [slurm configuration] role to create the `slurm` user and system directories
  used by slurmctld.

[slurm node]: ../slurm_node

To set up the Slurm controller and database daemons, it will:

- install and configure a MariaDB server;
- deploy required configuration files (including those needed for `slurmd`); and
- spin up `slurmctld`.

Note that the config files deployed by this role are the ones required for
`slurmd`, and in our cluster compute nodes will use these files via NFS mounts.
