#!/bin/bash

./nukeDB.expect
python manage.py loaddata initial_data
python manage.py loaddata initial_user
