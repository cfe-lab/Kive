#!/usr/bin/env bash

# Exit immediately if any untested command fails.
set -e

apt-get update -qq --fix-missing

echo ========== Installing PostgreSQL ==========
apt-get install -qq postgresql postgresql-contrib postgresql-client

echo  ========== Installing Singularity ==========
apt-get install -qq python dh-autoreconf build-essential libarchive-dev squashfs-tools
git clone https://github.com/singularityware/singularity.git
cd singularity
git checkout -q tags/2.5.2
./autogen.sh
./configure --prefix=/usr/local
make
make install
cd ..
rm -rf singularity

echo ========== Installing Docker ==========
apt-get install -qq \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt-get install -qq docker-ce

echo ========== Installing Slurm ==========
mkdir --parents /etc/slurm-llnl
cp  /usr/local/share/Kive/vagrant_ubuntu/slurm.conf \
    /usr/local/share/Kive/vagrant_ubuntu/cgroup.conf \
    /etc/slurm-llnl/
apt-get install -qq munge slurm-wlm slurmctld slurm-wlm-basic-plugins
chmod g+r,o+r /var/log/slurm-llnl/accounting

echo ========== Installing pip ==========
apt-get install -qq wget
wget -q https://bootstrap.pypa.io/get-pip.py
python get-pip.py pip==9.0.1
rm get-pip.py

echo ========== Installing Kive ==========
apt-get install -qq python-dev libsqlite3-dev wamerican
cd /usr/local/share/Kive/api
python setup.py install
cd ..
mkdir --parents \
    vagrant_ubuntu/media_root_backup/CodeResources \
    vagrant_ubuntu/media_root_backup/Datasets \
    vagrant_ubuntu/media_root_backup/Logs \
    /var/kive/media_root
chown vagrant:vagrant /var/kive/media_root
ln -s /usr/local/share/Kive/vagrant_ubuntu/media_root_backup/* \
    /var/kive/media_root/
ln -s /usr/local/share/Kive/kive/fleet/docker_wrap.py /usr/local/bin/docker_wrap.py
pip install -r requirements-dev.txt
sed -e 's/\[YOUR DB NAME HERE\]/kive/' \
    -e 's/\[YOUR DB USER NAME HERE\]/vagrant/' \
    -e 's/\[YOUR DB USER PASSWORD HERE\]//' \
    -e "s|MEDIA_ROOT = ''|MEDIA_ROOT = '/var/kive/media_root'|" \
    -e 's/SLURM_PRIO_KEYWORD = "priority"/SLURM_PRIO_KEYWORD = "prioritytier"/' \
    -e 's/SLURM_PRIO_COLNAME = "PRIORITY"/SLURM_PRIO_COLNAME = "PRIO_TIER"/' \
    kive/kive/settings_default.py > kive/kive/settings_vagrant.py
if [ ! -f kive/kive/settings.py ]; then
    cp kive/kive/settings_vagrant.py kive/kive/settings.py
fi

cd vagrant_ubuntu
sudo -u vagrant ./dbcreate.sh

# Launch server like this:
# cd /usr/local/share/Kive/kive
# ./manage.py runserver 0.0.0.0:8000
