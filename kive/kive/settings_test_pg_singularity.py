# This file is meant for use of the fleet while testing.

from settings_test_pg_slurm_docker import *  # @UnusedWildImport

# Enable the running of the Slurm tests.
RUN_SLURM_TESTS = False
RUN_DOCKER_TESTS = True
RUN_SINGULARITY_TESTS = True


