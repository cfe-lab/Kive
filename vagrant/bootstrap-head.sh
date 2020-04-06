#!/usr/bin/env bash
# Run on the Kive head node to set up Kive, the Slurm controller,
# and a Slurm worker.

set -eu -o pipefail
IFS=$"\n\t"

# shellcheck source="./setuplib.sh"
. "/usr/local/share/Kive/vagrant/setuplib.sh"

setuplib::python3
setuplib::vagrant_user
setuplib::kive_user
setuplib::postgres
setuplib::singularity
setuplib::mariadb
setuplib::munge
setuplib::slurm_user
setuplib::slurm_controller
setuplib::slurm_worker
setuplib::apache
setuplib::mod_wsgi
setuplib::kive_head

# Apache should be active on port 8080.
# Launch development server on port 8000 like this:
# sudo su kive
# cd /usr/local/share/Kive/kive
# . /opt/venv_kive/bin/activate
# . ../vagrant_ubuntu/envvars.conf
# ./manage.py runserver 0.0.0.0:8000
