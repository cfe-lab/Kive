# This file overrides some of the defaults for testing, and does not quiet the
# console.
# Use it by running ./manage.py test --settings=kive.settings_test_pg_loud

from settings_test_pg import *  # @UnusedWildImport

# Restore logging to console so we can see the details.
LOGGING['handlers']['console']['level'] = 'INFO'
