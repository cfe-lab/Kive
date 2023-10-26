#! /usr/bin/bash

# Run this as root on a vanilla installation of Jammy on the compute nodes.

apt update -y
apt upgrade -y
apt install -y python3

cat head_node_root_id_ed25519.pub >> /root/.ssh/authorized_keys
