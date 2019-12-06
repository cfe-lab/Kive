#! /opt/venv_kive/bin/python
from __future__ import print_function
import psycopg2
import argparse
import os
from django.core.management.base import BaseCommand
from librarian.models import Dataset
from container.models import ContainerDataset
from django.conf import settings
import sys


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('-df', '--delete_files', action='store_true', help='Delete orphaned files')
        parser.add_argument('-dr', '--delete_records', action='store_true', help='Delete database records for orphaned files')
        parser.add_argument('-da', '--delete_all', action='store_true', help='Combine options -df and -dr')
        parser.add_argument('-r', '--root_path', default=settings.MEDIA_ROOT, help='Path to kive media root folder')

    def handle(self, *args, **options):
        orphans = self.find_orphans()
        if not orphans:
            print('No orphan files found')
            sys.exit(0)
        self.display_orphans(orphans, root_path=options['root_path'])
        self.remove_orphans(
            orphans,
            delete_all=options['delete_all'],
            delete_files=options['delete_files'],
            delete_records=options['delete_records'],
            root_path=options['root_path']
        )

    @staticmethod
    def find_orphans():
        orphans = Dataset.objects.filter(
            is_uploaded__exact=False
        ).exclude(
            id__in=ContainerDataset.objects.all().values_list('dataset_id', flat=True)
        )
        return orphans

    @staticmethod
    def display_orphans(orphans, root_path=settings.MEDIA_ROOT, verbosity=1):
        norphans = len(orphans)
        if verbosity > 0:
            for orphan in orphans:
                print(os.path.join(root_path, str(orphan.dataset_file)))
        else:
            print(n_orphaned)

    @staticmethod
    def remove_orphans(orphans, delete_all=True, delete_files=True, delete_records=True, root_path=settings.MEDIA_ROOT):
        if any((delete_all, delete_records, delete_files)):
            for orphan in orphans:
                print('For orphan {}'.format(orphan))
                # path = os.path.join(options['root_path'], orphan.dataset_file.name)
                if delete_all or delete_files:
                    # print('Deleting file "{}"'.format(path))
                    print('Deleting file "{}"'.format(orphan.dataset_file.path))
                    orphan.dataset_file.delete()
                    print('File deleted successfully')
                if delete_all or delete_records:
                    print('Deleting database record')
                    orphan.delete()
                    print('Record deleted successfully')