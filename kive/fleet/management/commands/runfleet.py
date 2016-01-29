from optparse import make_option
from django.core.management.base import BaseCommand
import fleet.workers
import sys


class Command(BaseCommand):
    help = 'Launches the manager and worker_interfaces to execute pipelines.'

    option_list = BaseCommand.option_list + (
        make_option('--worker_interfaces', '-w', type='int', default=1),
        make_option('--quit-idle',
                    '-q',
                    action='store_true',
                    help='Shut down the fleet as soon as it is idle.'))

    def handle(self, *args, **options):
        manager_interface = fleet.workers.MPIManagerInterface(
            worker_count=options["worker_interfaces"],
            manage_script=sys.argv[0]
        )
        manager = fleet.workers.Manager(manager_interface, options["quit_idle"])
        manager.main_procedure()
