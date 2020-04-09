#!/bin/bash
set -eu -o pipefail
IFS=$'\t\n'

yum install -q -y python3
sudo -u vagrant python3 -m pip install --user -r /vagrant/requirements.txt