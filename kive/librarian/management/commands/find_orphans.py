#! /opt/venv_kive/bin/python
import logging

from django.core.management.base import BaseCommand
from librarian.models import Dataset
import sys


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('-f',
                            '--delete_files',
                            action='store_true',
                            help='Delete orphaned files')
        parser.add_argument('-r',
                            '--delete_records',
                            action='store_true',
                            help='Delete database records for orphaned files')
        parser.add_argument('-a',
                            '--delete_all',
                            action='store_true',
                            help='Combine options -f and -r')

    def handle(self, *args, **options):
        orphans = self.find_orphans()
        if not orphans:
            logger.error('No orphan files found')
            sys.exit(0)
        self.display_orphans(orphans, verbosity=options['verbosity'])
        self.remove_orphans(
            orphans,
            delete_all=options['delete_all'],
            delete_files=options['delete_files'],
            delete_records=options['delete_records']
        )

    @staticmethod
    def find_orphans():
        orphans = Dataset.objects.filter(
            is_uploaded=False,
            containers=None
        )
        return orphans

    @staticmethod
    def display_orphans(orphans, verbosity=1):
        if verbosity > 0:
            for orphan in orphans:
                logger.info(orphan.id)
        else:
            logger.info(orphans.count())

    @staticmethod
    def remove_orphans(orphans, delete_all=True, delete_files=True, delete_records=True):
        if any((delete_all, delete_records, delete_files)):
            for orphan in orphans:
                logger.info('For orphan "{}"'.format(orphan.id))
                if delete_all or delete_files:
                    try:
                        logger.info('Deleting file "{}"'.format(orphan.dataset_file.path))
                        orphan.dataset_file.delete()
                    except ValueError:
                        logger.error('File has already been deleted')
                    logger.info('File deleted successfully')
                if delete_all or delete_records:
                    logger.info('Deleting database record')
                    orphan.delete()
                    logger.info('Record deleted successfully')
