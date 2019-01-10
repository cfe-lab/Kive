#!/usr/bin/env sh

# This script dumps the database to a backup file and copies the code resources.

# Exit immediately if any untested command fails.
set -e

echo "Dumping kive database."
mkdir --parents dumps

sudo -u kive pg_dump -n public kive > dumps/db_data.sql

echo "Dumping data files."

sudo chmod o+rx /var/kive
rsync -a --delete --exclude ContainerRuns --exclude Sandboxes \
    /var/kive/media_root/ \
    dumps/media_root
sudo chmod o-rx /var/kive

echo "Dumped."
