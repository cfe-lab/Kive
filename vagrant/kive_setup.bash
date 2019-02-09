#!/usr/bin/env bash

REQUIREMENTS=$1

# Use the virtualenv Python which is assumed to be set up prior to calling this script.
. /opt/venv_kive/bin/activate

echo ========== Installing Kive ==========
yum install -q -y python-devel libsqlite3x-devel words lsof graphviz graphviz-devel
cd /usr/local/share/Kive/api
python setup.py install
cd ..
mkdir --parents /var/kive/media_root
chown -R kive:kive /var/kive
chmod -R 770 /var/kive
chmod -R g+s /var/kive
usermod -a -G kive vagrant
ln -s /usr/local/share/Kive/kive/fleet/docker_wrap.py /usr/local/bin/docker_wrap.py
pip install -r $REQUIREMENTS
if [ ! -f kive/kive/settings.py ]; then
    cp kive/kive/settings_default.py kive/kive/settings.py
fi

. vagrant_ubuntu/envvars.conf  # Lets this script run manage.py
cd kive
./manage.py collectstatic
./manage.py shell -c "
from django.core.management.utils import get_random_secret_key
print('KIVE_SECRET_KEY='+repr(get_random_secret_key()))" >> /etc/sysconfig/httpd
systemctl restart httpd

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
