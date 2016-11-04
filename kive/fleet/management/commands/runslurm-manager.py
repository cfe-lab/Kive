from django.core.management.base import BaseCommand

import fleet.workers
from fleet.slurminterface import SlurmManagerInterface


class Command(BaseCommand):
    help = 'Launches the SLURM manager to execute pipelines.'

    def add_arguments(self, parser):
        # parser.add_argument(
        #   "-w",
        #    "--workers",
        #    dest="worker_interfaces",
        #    type=int,
        #    default=1
        # )
        parser.add_argument(
            "-q",
            "--quit-idle",
            dest="quit_idle",
            action="store_true",
            help="Shut down the fleet as soon as it is idle."
        )

    def handle(self, *args, **options):
        manager_interface = SlurmManagerInterface()
        manager = fleet.workers.Manager(manager_interface, options["quit_idle"])
        manager.main_procedure()
