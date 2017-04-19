#!/usr/bin/env bash

# This script assumes you have the privileges to do all of this.

# First, install the required packages.
dpkg -i TravisSlurmPackages/slurm_16.05.10-3_amd64.deb
dpkg -i TravisSlurmPackages/slurm-plugins_16.05.10-3_amd64.deb
dpkg -i TravisSlurmPackages/slurm-munge_16.05.10-3_amd64.deb

adduser --system slurm

mkdir /var/run/slurm
mkdir /var/log/slurm
mkdir /var/lib/slurm

chown slurm /var/run/slurm
chown slurm /var/log/slurm
chown slurm /var/lib/slurm

cp travis_slurm.conf /etc/slurm/slurm.conf

slurmd
slurmctld