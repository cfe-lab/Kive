from __future__ import print_function
import logging
import os
import shutil
import sys
import tempfile
from argparse import ArgumentDefaultsHelpFormatter

from django.core.management.base import BaseCommand
from django.core.files import File
from django.conf import settings
from django.db import transaction

from container.models import Container


logger = logging.getLogger(__name__)


# This assumes we are using the default FileSystemStorage class.
class Command(BaseCommand):
    help = "Consolidates Container file locations into their default directory."

    container_directory = os.path.normpath(os.path.join(settings.MEDIA_ROOT, Container.UPLOAD_DIR))

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter

    def handle(self, **options):
        containers_to_move = self.identify_containers_to_move()
        prompt_message = "The following Container files will be moved to {}:\n".format(self.container_directory)
        for container in containers_to_move:
            prompt_message += "{} ({})\n".format(container, container.file.path)
        prompt_message += "Proceed?"
        proceed = self.confirm(prompt_message)
        if not proceed:
            return
        self.move_container_files(containers_to_move)

    def identify_containers_to_move(self):
        """
        Identify Containers whose files must move.
        :return:
        """
        containers_to_consolidate = []
        # Scan through all Containers and examine the filenames.
        for container in Container.objects.all():
            current_absolute_path = os.path.normpath(container.file.path)
            directory_in_storage = os.path.dirname(current_absolute_path)
            if directory_in_storage == self.container_directory:
                continue
            containers_to_consolidate.append(container)

        return containers_to_consolidate

    def move_container_files(self, containers_to_move):
        """
        Move Container files around as specified.

        :param containers_to_move: a list of Containers
        :return:
        """
        for container in containers_to_move:
            with transaction.atomic():
                print("Moving {} to {}....".format(container.file.path, self.container_directory))

                new_name = os.path.basename(container.file.name)
                # First make a temporary home for the contents.
                with tempfile.TemporaryFile() as f:
                    for chunk in container.file.chunks():
                        f.write(chunk)

                    f.seek(0)
                    container.file.delete()
                    container.file.save(new_name, File(f), save=True)

    # This is copied from the "purge.py" management command.
    def confirm(self, prompt):
        print(prompt, end=' ')
        confirmation = sys.stdin.readline().strip()
        is_confirmed = confirmation.lower() == 'y'
        return is_confirmed
