# This file overrides some of the defaults for testing, and does not quiet the
# console.
# Use it by running ./manage.py test --settings=kive.settings_test_pg_loud

import os
from settings import *  # @UnusedWildImport

# Avoid overwriting developer data files.
MEDIA_ROOT = os.path.join(MEDIA_ROOT, 'Testing')
HOST_MEDIA_ROOT = None if HOST_MEDIA_ROOT is None else os.path.join(HOST_MEDIA_ROOT, 'Testing')

# Disable logging to console so test output isn't polluted.
# LOGGING['handlers']['console']['level'] = 'CRITICAL'

# Speed up short runs during tests.
FLEET_POLLING_INTERVAL = 0.1
CONFIRM_COPY_RETRIES = 5
CONFIRM_COPY_WAIT_MIN = 0.1
CONFIRM_COPY_WAIT_MAX = 0.15
CONFIRM_FILE_CREATED_RETRIES = 5
CONFIRM_FILE_CREATED_WAIT_MIN = 0.1
CONFIRM_FILE_CREATED_WAIT_MAX = 0.15

# An alternate settings file for the fleet to use.
FLEET_SETTINGS = "kive.settings_test_fleet_pg"
