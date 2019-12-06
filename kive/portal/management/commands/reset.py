import os
import shutil

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.conf import settings

import librarian.models
import container.models


class Command(BaseCommand):
    help = 'Resets the database and loads sample data.'
    TARGETS = (
        librarian.models.Dataset.UPLOAD_DIR,
        container.models.Container.UPLOAD_DIR,
        container.models.ContainerLog.UPLOAD_DIR,
        container.models.ContainerRun.SANDBOX_ROOT)

    def add_arguments(self, parser):
        parser.add_argument(
            "-l",
            "--load",
            dest="load",
            help="fixture name to load"
        )

    def handle(self, *args, **options):
        fixture = options['load']

        for target in self.TARGETS:
            target_path = os.path.join(settings.MEDIA_ROOT, target)
            if os.path.isdir(target_path):
                shutil.rmtree(target_path)

        call_command("flush", interactive=False)
        call_command("migrate")
        # flush truncates all tables, so we need to re-load this stuff.
        call_command("loaddata", "initial_groups")
        call_command("loaddata", "initial_user")

        if fixture:
            call_command("loaddata", fixture)
            fixture_folder = os.path.join("FixtureFiles", fixture)
            if os.path.isdir(fixture_folder):
                for child in os.listdir(fixture_folder):
                    source = os.path.join(fixture_folder, child)
                    if os.path.isdir(source):
                        destination = os.path.join(settings.MEDIA_ROOT, child)
                        if not os.path.exists(destination):
                            os.mkdir(destination)
                        for grandchild in os.listdir(source):
                            source_child = os.path.join(source, grandchild)
                            destination_child = os.path.join(destination,
                                                             grandchild)
                            if os.path.isdir(source_child):
                                shutil.copytree(source_child, destination_child)
                            else:
                                shutil.copy(source_child, destination_child)
