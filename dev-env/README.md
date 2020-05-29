# Kive Development Environment

This directory contains the necessary code for setting up a Kive development environment. It uses [Vagrant](https://www.vagrantup.com/) (with [Virtualbox](https://www.virtualbox.org/)) to manage a VM where Kive's dependencies will be installed and run.

To set up the environment, run:

    vagrant up

in this directory. This will set up a VM and install a few basic prerequisites. Next, log in to the VM with `vagrant ssh` (again, in this directory) and run the Ansible playbook for setting up a dev environment:

    vagrant ssh
    cd /usr/local/share/Kive/dev-env
    ansible-playbook setup-dev-env.yml

This process installs the rest of the required software (including Kive itself), sets up the application database, copies files to the necessary locations, etc. It will take several minutes to run.

Once the playbooks have run, you should be able to access Kive on port 8080 of your development machine. The source code is mapped as well, so edits made to this directory will be reflected in the VM.