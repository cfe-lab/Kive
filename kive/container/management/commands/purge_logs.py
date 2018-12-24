from argparse import ArgumentDefaultsHelpFormatter

from django.core.management.base import BaseCommand
from django.conf import settings
from datetime import datetime, timedelta

from container.models import ContainerLog


class Command(BaseCommand):
    help = 'Purge logs until a target level of free space is attained.'

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter
        parser.add_argument(
            "--threshold",
            help="Threshold for triggering a purge (bytes)",
            default=settings.LOGFILE_MAX_STORAGE
        )
        parser.add_argument(
            "--target",
            help="Number of bytes to (attempt to) reduce Dataset consumption to",
            default=settings.LOGFILE_TARGET_STORAGE
        )
        parser.add_argument(
            "--grace_period",
            help="Only remove registered Dataset files older than this (hours)",
            default=settings.LOGFILE_GRACE_PERIOD_HRS
        )
        parser.add_argument(
            "--registered",
            help="Purge registered logs",
            action="store_true",
            default=True
        )
        parser.add_argument(
            "--unregistered",
            help="Purge unregistered logs",
            action="store_true",
            default=False
        )

    def handle(self, *args, **options):
        total_storage_used = ContainerLog.total_storage_used()
        if total_storage_used < options["threshold"]:
            print("Total storage used ({}) is below the purge threshold of {}; returning.")
            return

        remove_older_than = datetime.now() - timedelta(hours=options["grace_period"])
        remove_this_many_bytes = total_storage_used - options["target"]

        # First remove unregistered datasets if requested, followed by registered.
        stray_bytes_purged = 0
        if options["unregistered"]:
            stray_bytes_purged, stray_files_purged, known_files, still_new = ContainerLog.purge_unregistered_logs(
                bytes_to_purge=remove_this_many_bytes,
                date_cutoff=remove_older_than
            )
            print(
                "Unregistered files: {} files removed ({} bytes); omitted {} known files, {} new files".format(
                    stray_files_purged,
                    stray_bytes_purged,
                    known_files,
                    still_new
                )
            )

        if options["registered"]:
            registered_bytes_purged, registered_files_purged = ContainerLog.purge_registered_logs(
                remove_this_many_bytes - stray_bytes_purged
            )
            print(
                "Registered files: {} files removed ({} bytes)".format(
                    registered_files_purged,
                    registered_bytes_purged
                )
            )
