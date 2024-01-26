This role fetches the Slurm source code and builds
Slurm.  It depends on the [slurm dependencies] role to install the dependencies
needed to build Slurm.

It's used by the [slurm controller] role to build Slurm and place it on /usr/local,
which is shared via NFS with the worker nodes.  The [slurm node] role fails if 
`/usr/local/lib/systemd/system/slurmd.service` isn't present (it should be if you've 
run this role on the head node).

[slurm node]: ../slurm_node
[slurm controller]: ../slurm_controller
[slurm dependencies]: ../slurm_dependencies
