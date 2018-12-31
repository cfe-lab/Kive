import logging
from argparse import ArgumentDefaultsHelpFormatter

from django.core.management.base import BaseCommand
from django.conf import settings

from django.template.defaultfilters import filesizeformat
from django.utils import timezone
from django.utils.dateparse import parse_duration

from container.models import ContainerRun

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Scan through sandboxes, recording the sandbox size of newly ' \
           'finished runs, and purging sandboxes that are old enough.'

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter

        parser.add_argument(
            "--delay",
            help="Only purge sandboxes older than this.",
            default=settings.PURGE_SANDBOXES_DELAY,
            type=parse_duration)
        parser.add_argument(
            "--keep_recent",
            help="How many of the most recent runs to keep.",
            default=settings.SANDBOX_KEEP_RECENT,
            type=int)
        parser.add_argument(
            "--unregistered",
            help="Skip sandboxes in the database, and purge any sandboxes "
                 "that are older than the delay and have no entry in the "
                 "database.",
            action="store_true")

    def handle(self, delay, unregistered=False, keep_recent=0, **options):
        remove_older_than = timezone.now() - delay

        # Order of operations:
        # compute Sandbox sizes
        # remove unregistered Sandboxes
        # remove registered Sandboxes

        if unregistered:
            paths_removed = sorted(ContainerRun.scan_for_unaccounted_sandboxes_and_logs())
            if len(paths_removed) > 0:
                print("The following paths are unaccounted for and were removed:")
                for path in paths_removed:
                    print("- {}".format(path))
        else:
            for run in ContainerRun.objects.filter(sandbox_size__isnull=True, sandbox_purged=False):
                run.set_sandbox_size()

            runs_purged = ContainerRun.purge_sandboxes(remove_older_than, keep_most_recent=keep_recent)
            if len(runs_purged) == 0:
                logger.debug("No sandboxes were purged.")

            else:
                total_bytes_removed = 0
                for run in runs_purged:
                    bytes_removed_string = "unknown size"
                    if run.sandbox_size is not None:
                        bytes_removed_string = filesizeformat(run.sandbox_size)
                        total_bytes_removed += run.sandbox_size
                    logger.debug("Run %d contained %s.", run.pk, bytes_removed_string)
                logger.info("Removed %d sandboxes containing %s.",
                            len(runs_purged),
                            filesizeformat(total_bytes_removed))
