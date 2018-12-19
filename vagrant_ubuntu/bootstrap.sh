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
cp /usr/local/share/Kive/vagrant_ubuntu/001-kive.conf /etc/apache2/sites-available/
a2ensite 001-kive
sed -ie 's/<VirtualHost \*:80>/<VirtualHost *:8080>/' /etc/apache2/sites-available/000-default.conf
sed -ie 's/Listen 80$/Listen 8080/' /etc/apache2/ports.conf
cp /usr/local/share/Kive/vagrant_ubuntu/.pam_environment /home/vagrant/.pam_environment
chown vagrant:vagrant /home/vagrant/.pam_environment
cat /usr/local/share/Kive/vagrant_ubuntu/envvars.conf >> /etc/apache2/envvars
. /usr/local/share/Kive/vagrant_ubuntu/envvars.conf
systemctl restart apache2

echo ========== Installing virtualenv ==========
curl --location --output virtualenv-15.1.0.tar.gz https://github.com/pypa/virtualenv/tarball/15.1.0
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
if [ ! -f kive/kive/settings.py ]; then
    cp kive/kive/settings_default.py kive/kive/settings.py
fi
cd kive
./manage.py collectstatic

cd ../vagrant_ubuntu
sudo -u vagrant ./dbcreate.sh

# Apache should be active on port 8080.
# Launch development server on port 8000 like this:
# cd /usr/local/share/Kive/kive
# ./manage.py runserver 0.0.0.0:8000
