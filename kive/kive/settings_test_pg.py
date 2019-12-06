# This file overrides some of the defaults to make the testing output quieter,
# while still using PostgreSQL for thoroughness.
# Use it by running ./manage.py test --settings=kive.settings_test_pg

from kive.settings import *  # @UnusedWildImport

# Avoid overwriting developer data files
MEDIA_ROOT = os.path.join(MEDIA_ROOT, 'Testing')


# Disable logging to console so test output isn't polluted.
LOGGING['handlers']['console']['level'] = 'CRITICAL'
