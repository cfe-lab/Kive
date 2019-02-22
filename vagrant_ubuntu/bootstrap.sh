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
apt-get install -qq apache2 libapache2-mod-wsgi

useradd --system kive
mkdir /home/kive /etc/kive /var/log/kive
chown kive:kive /home/kive /etc/kive /var/log/kive
chmod go-rx /home/kive /etc/kive /var/log/kive

cp /usr/local/share/Kive/vagrant_ubuntu/001-kive.conf /etc/apache2/sites-available/
a2ensite 001-kive
sed -ie 's/<VirtualHost \*:80>/<VirtualHost *:8080>/' /etc/apache2/sites-available/000-default.conf
sed -ie 's/Listen 80$/Listen 8080/' /etc/apache2/ports.conf
cat /usr/local/share/Kive/vagrant_ubuntu/envvars.conf >> /etc/apache2/envvars
echo "
export KIVE_LOG=/var/log/kive/kive_apache.log
export APACHE_RUN_USER=kive
export APACHE_RUN_GROUP=kive" >> /etc/apache2/envvars
# KIVE_SECRET_KEY gets added to /etc/apache2/envvars in the Kive section below.

. /usr/local/share/Kive/vagrant_ubuntu/envvars.conf  # Lets this script run manage.py
systemctl restart apache2

echo ========== Installing virtualenv ==========
curl -Ss --location --output virtualenv-15.1.0.tar.gz https://github.com/pypa/virtualenv/tarball/15.1.0
tar xfz virtualenv-15.1.0.tar.gz
python pypa-virtualenv-bcc2a4c/virtualenv.py /opt/venv_kive
rm -r pypa-virtualenv-bcc2a4c/ virtualenv-15.1.0.tar.gz
. /opt/venv_kive/bin/activate

echo ========== Installing pip ==========
apt-get install -qq wget
wget -q https://bootstrap.pypa.io/get-pip.py
python get-pip.py pip==9.0.1
rm get-pip.py

echo ========== Installing Kive ==========
apt-get install -qq python-dev libsqlite3-dev wamerican graphviz libgraphviz-dev pkg-config
cd /usr/local/share/Kive/api
python setup.py install
cd ..
mkdir --parents /var/kive/media_root
chown -R kive:kive /var/kive
chmod go-rx /var/kive

ln -s /usr/local/share/Kive/kive/fleet/docker_wrap.py /usr/local/bin/docker_wrap.py
pip install -r requirements-dev.txt
if [ ! -f kive/kive/settings.py ]; then
    cp kive/kive/settings_default.py kive/kive/settings.py
fi
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
systemctl enable kive_purge.service
systemctl enable kive_purge.timer
systemctl start kive_purge.timer
systemctl enable kive_purge_synch.service
systemctl enable kive_purge_synch.timer
systemctl start kive_purge_synch.timer

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
