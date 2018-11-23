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

[Vagrant]: https://www.vagrantup.com/downloads.html
[VirtualBox]: https://www.vagrantup.com/docs/virtualbox/
[slurm.conf]: https://slurm.schedmd.com/slurm.conf.html
[consumable resources]: https://slurm.schedmd.com/cons_res.html
[accounting]: https://slurm.schedmd.com/accounting.html
[slurmdbd]: https://slurm.schedmd.com/slurmdbd.conf.html