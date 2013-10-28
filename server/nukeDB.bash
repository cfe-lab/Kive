#!/bin/bash

./nukeDB.expect
./manage.py loaddata initial_data
