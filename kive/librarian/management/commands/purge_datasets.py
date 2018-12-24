import logging
from argparse import ArgumentDefaultsHelpFormatter

from django.core.management.base import BaseCommand
from django.conf import settings
from datetime import datetime, timedelta

from django.template.defaultfilters import filesizeformat

from librarian.models import Dataset

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Purge datasets until a target level of free space is attained.'

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter
        parser.add_argument(
            "--threshold",
            help="Threshold for triggering a purge (bytes)",
            type=int,
            default=settings.DATASET_MAX_STORAGE
        )
        parser.add_argument(
            "--target",
            type=int,
            help="Number of bytes to (attempt to) reduce Dataset consumption to",
            default=settings.DATASET_TARGET_STORAGE
        )
        parser.add_argument(
            "--grace_period",
            type=float,
            help="Only remove registered Dataset files older than this (hours)",
            default=settings.DATASET_GRACE_PERIOD_HRS
        )
        parser.add_argument(
            "--registered",
            help="Purge registered datasets",
            action="store_true",
            default=True
        )
        parser.add_argument(
            "--unregistered",
            help="Purge unregistered datasets",
            action="store_true",
            default=False
        )

    def handle(self, threshold, registered, unregistered, *args, **options):
        Dataset.set_dataset_sizes()
        if unregistered:
            total_storage_used = Dataset.total_storage_used()
        else:
            total_storage_used = Dataset.known_storage_used()
        if total_storage_used < threshold:
            logger.info("Total storage used (%s) is below the purge threshold "
                         "of %s; returning.",
                         filesizeformat(total_storage_used),
                         filesizeformat(threshold))
            return

        remove_older_than = datetime.now() - timedelta(hours=options["grace_period"])
        remove_this_many_bytes = total_storage_used - options["target"]

        # First remove unregistered datasets if requested, followed by registered.
        stray_bytes_purged = 0
        if unregistered:
            stray_bytes_purged, stray_files_purged, known_files, still_new = Dataset.purge_unregistered_datasets(
                bytes_to_purge=remove_this_many_bytes,
                date_cutoff=remove_older_than
            )
            logger.info("Unregistered files: %d files removed (%s); omitted "
                        "%d known files, %d new files",
                        stray_files_purged,
                        filesizeformat(stray_bytes_purged),
                        known_files,
                        still_new)

        if registered:
            registered_bytes_purged, registered_files_purged = Dataset.purge_registered_datasets(
                remove_this_many_bytes - stray_bytes_purged
            )
            logger.info("Registered files: %d files removed (%s)",
                        registered_files_purged,
                        filesizeformat(registered_bytes_purged))
