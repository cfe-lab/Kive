# This file overrides some of the defaults for the step_helper and cable_helper commands.

from settings import *

# Disable file logging, as this is running as a Slurm job anyway so the console
# logging will be captured in a file.
LOGGING['root']['handlers'] = ['console']
