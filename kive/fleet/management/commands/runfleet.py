from django.core.management.base import BaseCommand
import fleet.workers
import sys


class Command(BaseCommand):
    help = 'Launches the manager and worker_interfaces to execute pipelines.'

    def add_arguments(self, parser):
        parser.add_argument(
            "-q",
            "--quit-idle",
            dest="quit_idle",
            action="store_true",
            help="Shut down the fleet as soon as it is idle."
        )

    def handle(self, *args, **options):
        manager = fleet.workers.Manager(options["quit_idle"])
        manager.main_procedure()
