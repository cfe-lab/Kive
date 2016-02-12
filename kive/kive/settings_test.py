# This file overrides some of the defaults to make tests run faster.
# Use it by running ./manage.py test --settings=kive.test_settings

from settings import *  # @UnusedWildImport

# Run with an in-memory database: about twice as fast as PostgreSQL
DATABASES['default'] = {'ENGINE': 'django.db.backends.sqlite3',
                        'NAME': ':memory:'}

# Disable logging to console so test output isn't polluted.
LOGGING['handlers']['console']['level'] = 'CRITICAL'
