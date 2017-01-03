# This file overrides some of the defaults to make the testing output quieter,
# while still using PostgreSQL for thoroughness.
# Use it by running ./manage.py test --settings=kive.settings_test_pg

from settings import *  # @UnusedWildImport

# Disable logging to console so test output isn't polluted.
LOGGING['handlers']['console']['level'] = 'CRITICAL'

CONFIRM_COPY_RETRIES = 5
CONFIRM_COPY_WAIT_MIN = 0.1
CONFIRM_COPY_WAIT_MAX = 0.15

CONFIRM_FILE_CREATED_RETRIES = 5
CONFIRM_FILE_CREATED_WAIT_MIN = 0.1
CONFIRM_FILE_CREATED_WAIT_MAX = 0.15