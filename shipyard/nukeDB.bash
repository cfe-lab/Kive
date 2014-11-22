#!/bin/bash -e

export DJANGO_SETTINGS_MODULE=shipyard.settings
./nukeDB.expect 
python2.7 manage.py loaddata initial_data 
python2.7 manage.py loaddata initial_user
python2.7 load_default_objects.py > /dev/null
python2.7 manage.py loaddata initial_pipeline
python2.7 setup_test_pipeline.py
