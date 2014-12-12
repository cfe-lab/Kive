from mpi4py import MPI
from optparse import make_option
import sys

from django.core.management.base import BaseCommand

import fleet.workers
        
class Command(BaseCommand):
    help = 'Launches the manager and workers to execute pipelines.'
    
    option_list = BaseCommand.option_list + (
        make_option('--workers', '-w', type='int', default=1), )
    
    def handle(self, *args, **options):
        worker_count = options['workers']
        manage_script = sys.argv[0]
        comm = MPI.COMM_SELF.Spawn(sys.executable,
                                   args=[manage_script, 'fleetworker'],
                                   maxprocs=worker_count).Merge()
        
        manager = fleet.workers.Manager(comm)
        manager.main_procedure()

        comm.Disconnect()
