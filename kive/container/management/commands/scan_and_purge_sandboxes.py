from argparse import ArgumentDefaultsHelpFormatter

from django.core.management.base import BaseCommand
from django.conf import settings
from datetime import datetime, timedelta

from container.models import ContainerRun


class Command(BaseCommand):
    help = 'Scan through sandboxes, purging where appropriate and recording their size where appropriate.'

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter

        default_grace_period = (24 * 60 * settings.SANDBOX_PURGE_DAYS
                                + 60 * settings.SANDBOX_PURGE_HOURS
                                + settings.SANDBOX_PURGE_MINUTES)
        parser.add_argument(
            "--grace_period",
            help="Only remove Sandboxes older than this (minutes)",
            default=default_grace_period
        )
        parser.add_argument(
            "--registered",
            help="Purge registered Sandboxes",
            action="store_true",
            default=True
        )
        parser.add_argument(
            "--unregistered",
            help="Purge unregistered Sandboxes",
            action="store_true",
            default=False
        )
        parser.add_argument(
            "--compute_sizes",
            help="Compute sizes for Sandboxes with no stored size",
            action="store_true",
            default=True
        )

    def handle(self, *args, **options):
        remove_older_than = datetime.now() - timedelta(minutes=options["grace_period"])

        # Order of operations:
        # compute Sandbox sizes
        # remove unregistered Sandboxes
        # remove registered Sandboxes

        if options["compute_sizes"]:
            for run in ContainerRun.objects.filter(sandbox_size__isnull=True, sandbox_purged=False):
                run.set_sandbox_size()

        if options["unregistered"]:
            paths_removed = sorted(ContainerRun.scan_for_unaccounted_sandboxes_and_logs())
            if len(paths_removed) > 0:
                print("The following paths are unaccounted for and were removed:")
                for path in paths_removed:
                    print("- {}".format(path))

        if options["registered"]:
            registered_bytes_purged, registered_files_purged = ContainerLog.purge_registered_datasets(
                remove_this_many_bytes - stray_bytes_purged
            )
            print(
                "Registered files: {} files removed ({} bytes)".format(
                    registered_files_purged,
                    registered_bytes_purged
                )
            )
