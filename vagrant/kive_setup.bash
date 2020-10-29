#!/usr/bin/env bash

# Exit immediately if any untested command fails.
set -e

REQUIREMENTS=$1

# Use the virtualenv Python which is assumed to be set up prior to calling this script.
. /opt/venv_kive/bin/activate

echo ========== Installing Kive ==========
yum install -q -y python-devel libsqlite3x-devel words lsof graphviz graphviz-devel
cd /usr/local/share/Kive/api
python setup.py install
cd ..
usermod -a -G kive vagrant
pip install -r $REQUIREMENTS
if [ ! -f kive/kive/settings.py ]; then
    cp kive/kive/settings_default.py kive/kive/settings.py
fi

. vagrant_ubuntu/envvars.conf  # Lets this script run manage.py
cd kive
./manage.py collectstatic
systemctl restart httpd

echo ========== Installing Kive purge and backup tasks ==========
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
