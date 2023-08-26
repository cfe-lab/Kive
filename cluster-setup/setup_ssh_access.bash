#! /usr/bin/env bash

# Run this as root to set up passwordless SSH access.

cat /vagrant/setupfiles/vagrant_testkey.pub >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
