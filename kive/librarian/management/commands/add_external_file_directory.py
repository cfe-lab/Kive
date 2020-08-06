from django.core.management.base import BaseCommand

import os.path

from librarian.models import ExternalFileDirectory


class Command(BaseCommand):
    help = 'Registers an external directory to be used for Datasets.'

    def add_arguments(self, parser):
        parser.add_argument(
            "path",
            help="absolute path of external file directory"
        )
        parser.add_argument(
            "-n",
            "--name",
            help="human-readable label for this directory",
            default=""
        )

    def handle(self, *args, **options):
        efd = ExternalFileDirectory(path=os.path.abspath(options["path"]), name=options["name"])
        efd.clean()
        efd.save()
