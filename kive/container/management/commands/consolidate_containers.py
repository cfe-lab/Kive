from __future__ import print_function
import logging
import os
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
        self.consolidate_containers(directory=directory)

    def consolidate_containers(self, directory):
        """
        Consolidate container files in the specified directory.

        :param directory: an absolute path
        :return:
        """
        new_container_directory = os.path.normpath(directory)

        # Scan through all Containers and examine the filenames.
        for container in Container.objects.all():
            current_absolute_path = os.path.join(settings.MEDIA_ROOT, container.file.name)
            directory_in_storage = os.path.normpath(os.path.dirname(current_absolute_path))
            if directory_in_storage == new_container_directory:
                continue

            # This incantation is copied from
            # https://docs.djangoproject.com/en/1.11/topics/files/#using-files-in-models
            with transaction.atomic():
                current_basename = os.path.basename(current_absolute_path)
                new_absolute_path = os.path.join(new_container_directory, current_basename)
                print("Moving {} to {}....".format(current_absolute_path, new_absolute_path))
                container.file.name = new_absolute_path
                os.rename(current_absolute_path, new_absolute_path)
                container.save()
