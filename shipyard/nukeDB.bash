#!/bin/bash

./nukeDB.expect
python2.7 manage.py loaddata initial_data
