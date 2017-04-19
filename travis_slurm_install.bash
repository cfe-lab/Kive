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

mkdir /var/lib/slurm/slurmctld
mkdir /var/lib/slurm/slurmd
chown slurm /var/lib/slurm/slurmctld
chown slurm /var/lib/slurm/slurmd

touch /var/log/slurm/accounting
chown slurm /var/log/slurm/accounting
touch /var/log/slurm/job_completions
chown slurm /var/log/slurm/job_completions
touch /var/log/slurm/slurmctld.log
chown slurm /var/log/slurm/slurmctld.log

cp travis_slurm.conf /etc/slurm/slurm.conf

slurmd
slurmctld