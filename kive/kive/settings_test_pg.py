# This file overrides some of the defaults to make the testing output quieter,
# while still using PostgreSQL for thoroughness.
# Use it by running ./manage.py test --settings=kive.settings_test_pg

import os
from settings import *  # @UnusedWildImport

MEDIA_ROOT = os.path.join(MEDIA_ROOT, 'Testing')  # Avoid overwriting developer data files.

# Disable logging to console so test output isn't polluted.
LOGGING['handlers']['console']['level'] = 'CRITICAL'

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
# Whether or not to use the dummy Slurm scheduler.
TEST_WITH_DUMMY_SLURM = False