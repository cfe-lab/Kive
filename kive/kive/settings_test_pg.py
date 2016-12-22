# This file overrides some of the defaults to make the testing output quieter,
# while still using PostgreSQL for thoroughness.
# Use it by running ./manage.py test --settings=kive.muted_test_settings

from settings import *  # @UnusedWildImport

# Disable logging to console so test output isn't polluted.
LOGGING['handlers']['console']['level'] = 'CRITICAL'

MEDIA_ROOT += '_testing'  # Avoid overwriting developer data files.

FLEET_POLLING_INTERVAL = 0.1  # Speed up short runs during tests.
