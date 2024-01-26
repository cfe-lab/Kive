#!/bin/bash
set -eu -o pipefail
IFS=$'\t\n'

# # Enable extra repositories
# dnf install -q -y epel-release
# dnf config-manager --set-enabled PowerTools

# Install Python3
# dnf install -q -y python3
apt update
apt install -y python3 python3-pip

# Install Python packages
python3 -m pip install -r /vagrant/requirements.txt