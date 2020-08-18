# This file is meant for use of the fleet while testing.
# flake8: noqa

from settings_test_pg import *  # @UnusedWildImport

# Point the default database to the test database.
DATABASES["default"]["NAME"] = "test_{}".format(DATABASES["default"]["NAME"])
# Spit out more details -- these will go into the log files produced during
# execution, so will not pollute the console.
LOGGING['handlers']['console']['level'] = 'DEBUG'
