#!/usr/bin/env sh

# This script creates a new kive database and runs the migrations.

# Exit immediately if any untested command fails.
set -e

operation="$1"

if [ "$operation" = "create" ]
then
  echo "Creating user 'vagrant'"
  sudo -u postgres createuser vagrant
  echo "Granting privileges to user 'vagrant'"
  sudo -u postgres psql -c 'GRANT kive TO vagrant;'
  sudo -u postgres psql -c 'ALTER USER vagrant CREATEDB;'  # Only needed to run tests
elif [ "$operation" = "drop" ]
then
  echo "Dropping user 'vagrant'"
  sudo -u postgres dropuser vagrant
else
  echo "Must run ./dbcreate.sh prior to running this script. Please run like so: ./configure_vagrant_user.sh <create, drop>"
  exit 1
fi