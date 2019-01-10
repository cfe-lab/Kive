#!/usr/bin/env sh

# This script dumps the database to a backup file and copies the code resources.

# Exit immediately if any untested command fails.
set -e

echo "Dumping kive database."

sudo -u kive pg_dump -n public kive > db_data.sql

echo "Dumping data files."
mkdir --parents media_root_backup

sudo chmod o+rx /var/kive
rsync -a --delete --exclude ContainerRuns --exclude Sandboxes \
    /var/kive/media_root/ \
    media_root_backup
sudo chmod o-rx /var/kive

echo "Dumped."
