from optparse import make_option
from django.core.management.base import BaseCommand
import fleet.workers
import sys


class Command(BaseCommand):
    help = 'Launches the manager and worker_interfaces to execute pipelines.'

    def add_arguments(self, parser):
        parser.add_argument(
            "-w",
            "--worker_interfaces",
            dest="worker_interfaces",
            type="int",
            default=1
        )
        parser.add_argument(
            "-q",
            "--quit-idle",
            dest="quit_idle",
            action="store_true",
            help="Shut down the fleet as soon as it is idle."
        )

    def handle(self, *args, **options):
        manager_interface = fleet.workers.MPIManagerInterface(
            worker_count=options["worker_interfaces"],
            manage_script=sys.argv[0]
        )
        manager = fleet.workers.Manager(manager_interface, options["quit_idle"])
        manager.main_procedure()
