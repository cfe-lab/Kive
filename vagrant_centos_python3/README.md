# Kive in a CentOS VirtualBox #
This folder contains scripts to create a full *Python 3*-based Kive installation in a CentOS VirtualBox, copied
and modified from the corresponding scripts for a Python 2-based Kive CentOS VirtualBox. 

As with the Python 2 scripts, they are intended to  
also be a helpful guide for installing a Kive server on CentOS. To run the scripts, install [Vagrant] and its
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