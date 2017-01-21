# This file overrides some of the defaults to make the testing output quieter,
# while still using PostgreSQL for thoroughness.
# Use it by running ./manage.py test --settings=kive.muted_test_settings

import os
from settings import *  # @UnusedWildImport

# Disable logging to console so test output isn't polluted.
LOGGING['handlers']['console']['level'] = 'CRITICAL'

MEDIA_ROOT = os.path.join(MEDIA_ROOT, 'Testing')  # Avoid overwriting developer data files.

# Speed up short runs during tests.
FLEET_POLLING_INTERVAL = 0.1
CONFIRM_FILE_CREATED_WAIT_MIN = 0.01
CONFIRM_FILE_CREATED_WAIT_MAX = 0.02
