#! /usr/bin/env bash

# Run this as root (using sudo) to install our "stock" SSH keys.
if [ -f /root/.ssh/id_ed25519 ]
then
  cp /root/.ssh/id_ed25519 /root/.ssh/id_ed25519.bak
fi

if [ -f /root/.ssh/id_ed25519.pub ]
then
  cp /root/.ssh/id_ed25519.pub /root/.ssh/id_ed25519.pub.bak
fi

cp /vagrant/setupfiles/vagrant_testkey /root/.ssh/id_ed25519
cp /vagrant/setupfiles/vagrant_testkey.pub /root/.ssh/id_ed25519.pub
chmod 600 /root/.ssh/id_ed25519
chmod 644 /root/.ssh/id_ed25519.pub
