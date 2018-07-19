#!/usr/bin/env bash

yum install -y git libtool libarchive-devel squashfs-tools

git clone https://github.com/singularityware/singularity.git
cd singularity
git checkout tags/2.5.2
./autogen.sh
./configure --prefix=/usr/local
make
sudo make install
cd ..
rm -rf singularity
