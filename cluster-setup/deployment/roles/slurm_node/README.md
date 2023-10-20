This role confirms that configuration files required by slurmd are in place
and then spins up the slurmd service.  It depends on:
- the [munge node] role to set up the MUNGE authentication service;
- the [slurm dependencies] role to install slurmd's dependencies; and
- the [slurm configuration] role to create the `slurm` user and system directories
  used by slurmd.

In a typical cluster configuration, the configuration files required will be mounted
via NFS, so we don't actually install them in this role or in the dependencies.

[munge node]: ../munge_node
[slurm dependencies]: ../slurm_dependencies
[slurm configuration]: ../slurm_configuration
