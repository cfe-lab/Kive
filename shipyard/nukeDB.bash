#!/bin/bash

./nukeDB.expect && python2.7 manage.py loaddata initial_data && python2.7 manage.py loaddata initial_user
python manage.py shell < load_default_objects.py > /dev/null

