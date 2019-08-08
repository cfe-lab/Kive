# Kive in an Ubuntu VirtualBox #
This folder contains scripts to create a full Kive installation in an Ubuntu VirtualBox. It can also be a helpful guide
for developers configuring their Ubuntu workstations to work on Kive. To install Kive on a regular Ubuntu workstation, just follow the same steps as the `bootstrap.sh` script.

To install Kive using Vagrant, first install VirtualBox:

    sudo apt install virtualbox
    
Do not install Vagrant using `apt` or other package managers. Instead, download the binary from the [Vagrant website] directly and install using the package.

Then, install Kive from this directory:

    vagrant up
    
The Kive server should now be running on port 8080.

Troubleshooting
--------

The most challenging tool to install is Slurm. See its documentation on [slurm.conf], [consumable resources],
[accounting], and [slurmdbd]. After changing the Slurm configuration, it may be easier to run these commands instead of
restarting all the daemons:

    sudo systemctl restart slurmctld
    sudo scontrol reconfigure

[Vagrant website]: https://www.vagrantup.com/downloads.html
[VirtualBox]: https://www.vagrantup.com/docs/virtualbox/
[slurm.conf]: https://slurm.schedmd.com/slurm.conf.html
[consumable resources]: https://slurm.schedmd.com/cons_res.html
[accounting]: https://slurm.schedmd.com/accounting.html
[slurmdbd]: https://slurm.schedmd.com/slurmdbd.conf.html 
