#!/bin/bash
set -eu -o pipefail
IFS=$'\t\n'

dnf install -q -y python3
python3 -m pip install -r /vagrant/requirements.txt