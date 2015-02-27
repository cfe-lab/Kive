#!/bin/bash

./manage.py dumpdata --indent=4 auth.user > user.json
./manage.py dumpdata --indent=4 --natural auth.group > group.json
./manage.py dumpdata --indent=4 metadata > metadata.json
