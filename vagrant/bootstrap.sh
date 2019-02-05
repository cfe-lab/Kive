#!/usr/bin/env bash

# Exit immediately if any untested command fails.
set -e

cd /root

/usr/local/share/Kive/vagrant/centos_dependencies.bash

echo ========== Installing virtualenv ==========
curl --location --output virtualenv-15.1.0.tar.gz https://github.com/pypa/virtualenv/tarball/15.1.0
tar xfz virtualenv-15.1.0.tar.gz
python pypa-virtualenv-bcc2a4c/virtualenv.py /opt/venv_kive
rm -r pypa-virtualenv-bcc2a4c/ virtualenv-15.1.0.tar.gz
. /opt/venv_kive/bin/activate

echo ========== Installing pip ==========
wget -q https://bootstrap.pypa.io/get-pip.py
python get-pip.py pip==9.0.1
rm get-pip.py

cd /usr/local/share/Kive/vagrant
./kive_setup.bash requirements-dev.txt

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
