# Kive in a CentOS VirtualBox #
This folder contains scripts to create a full Kive installation in a CentOS VirtualBox. It can also be a helpful guide
for installing a Kive server on CentOS. To run the scripts, install [Vagrant] and its
[VirtualBox] provider. To install Kive on a regular CentOS server, just follow the same steps as the `bootstrap.sh`
script.

The most challenging tool to install is Slurm. See its documentation on [slurm.conf], [consumable resources],
[accounting], and [slurmdbd]. After changing the Slurm configuration, it may be easier to run these commands instead of
restarting all the daemons:

    sudo systemctl restart slurmctld
    sudo scontrol reconfigure

You can test the multi-machine Slurm deployment with the following command:

    srun -n2 python -c "import socket; print(socket.gethostname())"

[Vagrant]: https://www.vagrantup.com/downloads.html
[VirtualBox]: https://www.vagrantup.com/docs/virtualbox/
[slurm.conf]: https://slurm.schedmd.com/slurm.conf.html
[consumable resources]: https://slurm.schedmd.com/cons_res.html
[accounting]: https://slurm.schedmd.com/accounting.html
[slurmdbd]: https://slurm.schedmd.com/slurmdbd.conf.html

# Multi-machine mode

The `Vagrantfile` in this folder can be run in a multi-machine mode to simulate running
Slurm jobs on multiple nodes. The default VM (`head`) contains the Slurm control and database
daemons, a Slurm worker daemon, and the Kive application. The second VM (`worker`) contains
an additional Slurm worker daemon.

By default, the worker VM won't start; this arrangement is simpler, and probably suitable for most
development tasks. Slurm will see that the node is down and avoid scheduling jobs on it.

To start the extra worker node, run `vagrant up worker`.

You can check that the multi-machine Slurm deployment is operating by running the following
(on either node):

    srun -n2 python -c "import socket; print(socket.gethostname())"
