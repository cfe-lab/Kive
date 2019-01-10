#!/usr/bin/env sh

# This script drops the kive database.

# Exit immediately if any untested command fails.
set -e

if [ "$1" = "-f" ]; then
    confirmed=Y
else
    read -p "This will completely drop the database. Are you sure? y/[N] " confirmed
fi
if [ "$confirmed" = "${confirmed#[Yy]}" ] ;then
    # $confirmed did not start with Y or y, abort!
    exit
fi

sudo -u postgres dropdb kive
sudo -u postgres dropuser kive

echo "Database dropped."
