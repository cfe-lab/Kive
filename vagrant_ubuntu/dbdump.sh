#!/usr/bin/env sh

# This script dumps the database to a backup file and copies the code resources.

# Exit immediately if any untested command fails.
set -e

echo "Dumping kive database."

pg_dump -n public kive > db_data.sql

echo "Dumping CodeResources."
mkdir --parents media_root_backup/CodeResources
rsync -a --delete /var/kive/media_root/CodeResources media_root_backup

echo "Dumped."
