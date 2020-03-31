#!/usr/bin/env bash
# Run on a Kive worker node to setup Kive worker requirements and
# a Slurm worker.

set -eu -o pipefail
IFS=$"\n\t"

# shellcheck source="./setuplib.sh"
. "/usr/local/share/Kive/vagrant/setuplib.sh"

setuplib::python3
setuplib::kive_user
setuplib::vagrant_user
setuplib::singularity
setuplib::munge
setuplib::slurm_user
setuplib::slurm_worker