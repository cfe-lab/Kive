#!/usr/bin/env bash

# Make apt-get select the default options
export DEBIAN_FRONTEND=noninteractive

# Exit immediately if any untested command fails.
set -e

apt-get update -qq --fix-missing

echo ========== Installing PostgreSQL ==========
apt-get install -qq postgresql postgresql-contrib postgresql-client libpq-dev

echo  ========== Installing Go ==========
export GO_VERSION=1.15.6 OS=linux ARCH=amd64
wget -q https://dl.google.com/go/go$GO_VERSION.$OS-$ARCH.tar.gz
tar -C /usr/local -xzf go$GO_VERSION.$OS-$ARCH.tar.gz
rm go$GO_VERSION.$OS-$ARCH.tar.gz

echo 'export GOPATH=${HOME}/go' >> ~/.bashrc
echo 'export PATH=/usr/local/go/bin:${PATH}:${GOPATH}/bin' >> ~/.bashrc
export GOPATH=${HOME}/go
export PATH=/usr/local/go/bin:${PATH}:${GOPATH}/bin

echo 'export GOPATH=${HOME}/go' >> ~vagrant/.bashrc
echo 'export PATH=/usr/local/go/bin:${PATH}:${GOPATH}/bin' >> ~vagrant/.bashrc

echo  ========== Installing Singularity ==========
apt-get install -qq python3-venv python3-dev build-essential uuid-dev libgpgme-dev squashfs-tools libseccomp-dev \
    wget pkg-config git cryptsetup-bin

git clone https://github.com/singularityware/singularity.git
cd singularity
git checkout -q tags/v3.6.4
./mconfig
make -C ./builddir
make -C ./builddir install
cd ..
rm -rf singularity

echo ========== Installing MySQL for Slurm ==========
apt-get install -qq mysql-server

echo ========== Installing Slurm ==========
mkdir --parents /etc/slurm-llnl
cp  /usr/local/share/Kive/vagrant_ubuntu/slurm.conf \
    /usr/local/share/Kive/vagrant_ubuntu/cgroup.conf \
    /usr/local/share/Kive/vagrant_ubuntu/slurmdbd.conf \
    /etc/slurm-llnl/
mysql --execute "create user slurm@localhost;"
mysql --execute "grant all on slurm_acct_db.* TO slurm@localhost;"
mysql --execute "create database slurm_acct_db;"
apt-get install -qq slurmdbd munge slurm-wlm slurmctld slurm-wlm-basic-plugins
chown -R slurm:slurm /etc/slurm-llnl/
chmod o-r /etc/slurm-llnl/slurmdbd.conf
sacctmgr -i add cluster localhost
systemctl restart slurmdbd
systemctl restart slurmctld

echo ========== Installing Apache ==========
apt-get install -qq apache2 libapache2-mod-wsgi-py3

useradd --system kive
mkdir /home/kive /etc/kive /var/log/kive
chown kive:kive /home/kive /etc/kive /var/log/kive
chmod go-rx /home/kive /etc/kive /var/log/kive
# Add vagrant user to kive group
usermod -a -G kive vagrant

cp /usr/local/share/Kive/vagrant_ubuntu/001-kive.conf /etc/apache2/sites-available/
a2ensite 001-kive
sed -ie 's/<VirtualHost \*:80>/<VirtualHost *:8080>/' /etc/apache2/sites-available/000-default.conf
sed -ie 's/Listen 80$/Listen 8080/' /etc/apache2/ports.conf
cat /usr/local/share/Kive/vagrant_ubuntu/envvars.conf >> /etc/apache2/envvars
# All users will now have the proper environment variables set
cat /usr/local/share/Kive/vagrant_ubuntu/envvars.conf | grep KIVE_MEDIA_ROOT >> /etc/environment
echo "
export APACHE_RUN_USER=kive
export APACHE_RUN_GROUP=kive" >> /etc/apache2/envvars
# KIVE_SECRET_KEY gets added to /etc/apache2/envvars in the Kive section below.

. /usr/local/share/Kive/vagrant_ubuntu/envvars.conf  # Lets this script run manage.py
systemctl restart apache2

echo ========== Installing virtualenv ==========
python3 -m venv /opt/venv_kive
. /opt/venv_kive/bin/activate

echo ========== Installing Kive ==========
apt-get install -qq python-dev libsqlite3-dev wamerican graphviz libgraphviz-dev pkg-config
cd /usr/local/share/Kive/api
python setup.py install
cd ..
mkdir --parents /var/kive/media_root
chown -R kive:kive /var/kive
chmod go-rx /var/kive

/opt/venv_kive/bin/python -m pip install -r requirements-dev.txt
cd kive
./manage.py collectstatic
./manage.py shell -c "
from django.core.management.utils import get_random_secret_key
print('export KIVE_SECRET_KEY='+repr(get_random_secret_key()))" >> /etc/apache2/envvars
systemctl restart apache2

echo ========== Installing Kive purge tasks ==========
cd /etc/systemd/system
cp /usr/local/share/Kive/vagrant/kive_purge.service .
cp /usr/local/share/Kive/vagrant/kive_purge.timer .
cp /usr/local/share/Kive/vagrant/kive_purge_synch.service .
cp /usr/local/share/Kive/vagrant/kive_purge_synch.timer .
cp /usr/local/share/Kive/vagrant/kive_purge.conf /etc/kive/
cp /usr/local/share/Kive/vagrant/kive_backup.service .
cp /usr/local/share/Kive/vagrant/kive_backup.timer .
cp /usr/local/share/Kive/vagrant/kive_backup.conf /etc/kive/
systemctl enable kive_purge.service
systemctl enable kive_purge.timer
systemctl start kive_purge.timer
systemctl enable kive_purge_synch.service
systemctl enable kive_purge_synch.timer
systemctl start kive_purge_synch.timer
systemctl enable kive_backup.service
systemctl enable kive_backup.timer
systemctl start kive_backup.timer

echo ========== Creating Kive database ==========
cd /usr/local/share/Kive/vagrant_ubuntu
./dbcreate.sh

# Apache should be active on port 8080.
# Launch development server on port 8000 like this:
# sudo su kive
# cd /usr/local/share/Kive/kive
# . /opt/venv_kive/bin/activate
# . ../vagrant_ubuntu/envvars.conf
# ./manage.py runserver 0.0.0.0:8000
