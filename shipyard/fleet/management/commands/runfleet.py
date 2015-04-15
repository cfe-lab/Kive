from optparse import make_option
from django.core.management.base import BaseCommand
import fleet.workers
import sys


class Command(BaseCommand):
    help = 'Launches the manager and workers to execute pipelines.'
    
    option_list = BaseCommand.option_list + (
        make_option('--workers', '-w', type='int', default=1), )
    
    def handle(self, *args, **options):
        worker_count = options['workers']
        manage_script = sys.argv[0]
        manager = fleet.workers.Manager(worker_count, manage_script)
        manager.main_procedure()
