# This file is meant for use of the fleet while testing.

from settings_test_pg import *  # @UnusedWildImport

# Point the default database to the test database.
DATABASES["default"]["NAME"] = "test_{}".format(DATABASES["default"]["NAME"])