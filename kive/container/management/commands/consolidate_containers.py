from __future__ import print_function
import logging
import os
import sys
from argparse import ArgumentDefaultsHelpFormatter

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import transaction

from container.models import Container


logger = logging.getLogger(__name__)


# This assumes we are using the default FileSystemStorage class.
class Command(BaseCommand):
    help = "Consolidates Container file locations into their default directory."

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter

        parser.add_argument('--directory',
                            help='Directory to consolidate Container files in',
                            default=os.path.join(settings.MEDIA_ROOT, Container.UPLOAD_DIR))

    def handle(self, directory, **options):
        move_instructions = self.identify_containers_to_move(directory=directory)
        prompt_message = "The following Container files will be moved:\n"
        for container, curr_loc, new_loc in move_instructions:
            prompt_message += "{} ({} -> {})\n".format(container, curr_loc, new_loc)
        prompt_message += "Proceed?"
        proceed = self.confirm(prompt_message)
        if not proceed:
            return
        self.move_container_files(move_instructions)

    def identify_containers_to_move(self, directory):
        """
        Identify Containers whose files must move.
        :param directory:
        :return:
        """
        new_container_directory = os.path.normpath(directory)

        containers_to_consolidate = []  # this will be a list of 3-tuples containing Container, old path, and new path
        # Scan through all Containers and examine the filenames.
        for container in Container.objects.all():
            current_absolute_path = os.path.join(settings.MEDIA_ROOT, container.file.name)
            directory_in_storage = os.path.normpath(os.path.dirname(current_absolute_path))
            if directory_in_storage == new_container_directory:
                continue

            current_basename = os.path.basename(current_absolute_path)
            new_absolute_path = os.path.join(new_container_directory, current_basename)
            containers_to_consolidate.append((container, current_absolute_path, new_absolute_path))

        return containers_to_consolidate

    def move_container_files(self, move_instructions):
        """
        Move Container files around as specified.

        :param move_instructions: a list of 3-tuples (container, current location, new location)
        :return:
        """
        for container, curr_loc, new_loc in move_instructions:
            # This incantation is copied from
            # https://docs.djangoproject.com/en/1.11/topics/files/#using-files-in-models
            with transaction.atomic():
                print("Moving {} to {}....".format(curr_loc, new_loc))
                container.file.name = new_loc
                os.rename(curr_loc, new_loc)
                container.save()

    # This is copied from the "purge.py" management command.
    def confirm(self, prompt):
        print(prompt, end=' ')
        confirmation = sys.stdin.readline().strip()
        is_confirmed = confirmation.lower() == 'y'
        return is_confirmed
