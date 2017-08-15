from django.core.management.base import BaseCommand, CommandError
import fleet.workers


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
        parser.add_argument(
            "-s",
            "--stop-user",
            dest="stop_user",
            help="Username for the user that should stop all running tasks "
                 "at start up."
        )
        parser.add_argument(
            "--no-stop",
            dest="no_stop",
            action="store_true",
            help="Do not stop running tasks at start up."
        )

    def handle(self, *args, **options):
        try:
            manager = fleet.workers.Manager(options["quit_idle"],
                                            stop_username=options["stop_user"],
                                            no_stop=options["no_stop"])
        except fleet.workers.ActiveRunsException as ex:
            raise CommandError(
                ('Found {} active runs. Use the --stop-user or '
                 '--no-stop option.').format(ex.count))

        manager.main_procedure()
