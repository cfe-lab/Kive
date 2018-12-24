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
            runs_purged = ContainerRun.purge_sandboxes(remove_older_than, keep_most_recent=settings.SANDBOX_KEEP_RECENT)
            if len(runs_purged) == 0:
                print("No sandboxes were purged.")

            else:
                print("Removed {} sandboxes:".format(len(runs_purged)))
                total_bytes_removed = 0
                for run in runs_purged:
                    bytes_removed_string = "size was unknown"
                    if run.sandbox_size is not None:
                        bytes_removed_string = "{} bytes removed".format(run.sandbox_size)
                        total_bytes_removed += run.sandbox_size
                    print(
                        "Run {} ({}): {}".format(
                            run.pk,
                            bytes_removed_string,
                            run.sandbox_path
                        )
                    )
                print("Total bytes belonging to known runs removed: {}".format(total_bytes_removed))
