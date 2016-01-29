from mpi4py import MPI
from django.core.management.base import BaseCommand
import fleet.workers


class Command(BaseCommand):
    help = 'Worker process to execute pipelines.'
    
    def handle(self, *args, **options):
        interface = fleet.workers.MPIWorkerInterface()

        worker = fleet.workers.Worker(interface)
        worker.main_procedure()
            
        interface.close()
