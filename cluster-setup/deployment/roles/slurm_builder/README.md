This role fetches the Slurm source code, installs its dependencies, and builds
Slurm.

It's used by the [slurm controller] role to build Slurm and place it on /usr/local,
which is shared via NFS with the worker nodes.  The [slurm node] role fails if 
`/usr/local/lib/systemd/system/slurmd.service` isn't present, using that as a proxy
for whether this role has been run on the head node yet.

[slurm node]: ../slurm_node
[slurm controller]: ../slurm_controller