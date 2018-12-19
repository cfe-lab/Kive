#!/usr/bin/env sh

# This script creates a new kive database and runs the migrations.

# Exit immediately if any untested command fails.
set -e

echo "Creating kive database."

sudo -u postgres createdb kive
sudo -u postgres createuser vagrant
sudo -u postgres psql -c 'GRANT ALL PRIVILEGES ON DATABASE kive TO vagrant;'
sudo -u postgres psql -c 'ALTER USER vagrant CREATEDB;'  # Only needed to run tests.

if [ -e db_data.sql ] ;then
    psql --set ON_ERROR_STOP=on kive < db_data.sql
fi
if [ -e media_root_backup/CodeResources ] ;then
    rsync -a media_root_backup/CodeResources /var/kive/media_root
fi

/opt/venv_kive/bin/python ../kive/manage.py migrate

echo "Created kive database."
