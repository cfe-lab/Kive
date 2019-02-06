#!/usr/bin/env bash

PYTHON=/usr/bin/python36

# Exit immediately if any untested command fails.
set -e

cd /usr/local/share/Kive/vagrant
./centos_dependencies.bash
sed -ie 's/python2.7/python3.6/' /etc/httpd/conf.d/001-kive.conf
systemctl restart httpd

echo ========== Installing Python 3 ==========
yum install -q -y python36 python36-devel
${PYTHON} -m venv /opt/venv_kive

./kive_setup.bash requirements-dev.py34.txt

echo ========== Creating Kive database ==========
cd /usr/local/share/Kive/vagrant_ubuntu
./dbcreate.sh

# Apache should be active on guest port 8080 (mapped to host port 8083).
# Launch development server on guest port 8003 (mapped to host port 8003) like this:
# sudo su kive
# cd /usr/local/share/Kive/kive
# . /opt/venv_kive/bin/activate
# . ../vagrant_ubuntu/envvars.conf
# ./manage.py runserver 0.0.0.0:8003
