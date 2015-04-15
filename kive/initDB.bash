#!/bin/bash -e

export DJANGO_SETTINGS_MODULE=kive.settings

./initDB.expect
./nukeDB.bash
