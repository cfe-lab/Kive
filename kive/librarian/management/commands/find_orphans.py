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
import itertools


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('-f', '--delete_files', action='store_true', help='Delete orphaned files')
        parser.add_argument('-r', '--delete_records', action='store_true', help='Delete database records for orphaned files')
        parser.add_argument('-a', '--delete_all', action='store_true', help='Combine options -f and -r')
        parser.add_argument('-c', '--chunk_size', type=int, default=10, help='Specify how many orphans to find at a time (for optimization purposes)')

    def handle(self, *args, **options):
        orphans = self.grouper(options['chunk_size'], self.find_orphans())
        if not orphans:
            print('No orphan files found')
            sys.exit(0)
        for chunk in orphans:
            self.display_orphans(chunk)
            self.remove_orphans(
                chunk,
                delete_all=options['delete_all'],
                delete_files=options['delete_files'],
                delete_records=options['delete_records']
            )

    @staticmethod
    def find_orphans():
        orphans = Dataset.objects.filter(
            is_uploaded__exact=False
        ).exclude(
            id__in=ContainerDataset.objects.values('dataset_id')
        ).order_by(
            'id'
        )
        return orphans

    @staticmethod
    def display_orphans(orphans, verbosity=1):
        if verbosity > 0:
            for orphan in orphans:
                print(orphan.id)
        else:
            print(orphans.count())

    @staticmethod
    def remove_orphans(orphans, delete_all=True, delete_files=True, delete_records=True):
        if any((delete_all, delete_records, delete_files)):
            for orphan in orphans:
                print('For orphan "{}"'.format(orphan.id))
                if delete_all or delete_files:
                    print('Deleting file "{}"'.format(orphan.dataset_file.path))
                    try:
                        orphan.dataset_file.delete()
                    except ValueError:
                        print('File has already been deleted')
                    print('File deleted successfully')
                if delete_all or delete_records:
                    print('Deleting database record')
                    orphan.delete()
                    print('Record deleted successfully')

    @staticmethod
    def grouper(n, iterable):
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, n))
            if not chunk:
                return
            yield chunk