# This file overrides some of the defaults for the runfleet command.

from settings import *

LOGGING['handlers']['file']['filename'] = (
    LOGGING['handlers']['file']['filename'].replace('kive.log', 'kive_fleet.log'))
