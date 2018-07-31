#!/usr/bin/env bash

yum install -y git libtool libarchive-devel squashfs-tools python-devel sqlite-devel

# Singularity
git clone https://github.com/singularityware/singularity.git
cd singularity
git checkout -q tags/2.5.2
./autogen.sh
./configure --prefix=/usr/local
make
sudo make install
cd ..
rm -rf singularity

# pip
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py pip==9.0.1
rm get-pip.py

# Kive
cd /usr/local/share/Kive
pip install -r requirements-dev.txt
