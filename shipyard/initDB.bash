#!/bin/bash -e

export DJANGO_SETTINGS_MODULE=shipyard.settings

./initDB.expect
./nukeDB.bash
