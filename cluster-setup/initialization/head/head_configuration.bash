#! /usr/bin/bash

# Run this as root on a vanilla installation of Jammy.

apt update -y
apt upgrade -y
apt install -y python3 python3-pip

python3 -m pip install -r requirements.txt
ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""
cat /root/.ssh/id_ed25519.pub >> /root/.ssh/authorized_keys
cat cluster_hosts >> /etc/hosts
