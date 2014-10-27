#!/bin/bash -e

./nukeDB.expect 
python2.7 manage.py loaddata initial_data 
python2.7 manage.py loaddata initial_user
python2.7 manage.py loaddata initial_groups
python manage.py shell < load_default_objects.py > /dev/null
