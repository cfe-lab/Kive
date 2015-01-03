from mpi4py import MPI

from django.core.management.base import BaseCommand

import fleet.workers

class Command(BaseCommand):
    help = 'Worker process to execute pipelines.'
    
    def handle(self, *args, **options):
        comm = MPI.Comm.Get_parent().Merge()

        worker = fleet.workers.Worker(comm)
        worker.main_procedure()
            
        comm.Disconnect()
