import logging
from argparse import ArgumentDefaultsHelpFormatter
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db.models.aggregates import Sum

from django.template.defaultfilters import filesizeformat
from django.utils import timezone
from django.utils.dateparse import parse_duration

from container.models import ContainerRun
from portal.models import parse_file_size

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Scan through sandboxes, recording the sandbox size of newly ' \
           'finished runs, and purging sandboxes that are old enough.'

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter

        # # Settings for the purge task. How much storage triggers a purge, and how much
        # # will stop the purge.
        # PURGE_START = os.environ.get('KIVE_PURGE_START', '20 GB')
        # PURGE_STOP = os.environ.get('KIVE_PURGE_STOP', '15 GB')
        # # How fast the different types of storage get purged. Higher aging gets purged faster.
        # PURGE_DATASET_AGING = os.environ.get('KIVE_PURGE_DATASET_AGING', '1.0')
        # PURGE_LOG_AGING = os.environ.get('KIVE_PURGE_LOG_AGING', '10.0')
        # PURGE_CONTAINER_AGING = os.environ.get('KIVE_PURGE_CONTAINER_AGING', '10.0')
        # # How long to wait before purging a file with no entry in the database.
        # # This gets parsed by django.utils.dateparse.parse_duration().
        # PURGE_WAIT = os.environ.get('KIVE_PURGE_WAIT', '0 days, 1:00')

        parser.add_argument('--start',
                            help='How much storage triggers a purge?',
                            default=settings.PURGE_START,
                            type=parse_file_size)
        parser.add_argument('--stop',
                            help='How much storage stops a purge?',
                            default=settings.PURGE_STOP,
                            type=parse_file_size)
        parser.add_argument('--dataset_aging',
                            help='How fast do datasets age, '
                                 'compared to other storage?',
                            default=settings.PURGE_DATASET_AGING,
                            type=float)
        parser.add_argument('--log_aging',
                            help='How fast do log files age, '
                                 'compared to other storage?',
                            default=settings.PURGE_LOG_AGING,
                            type=float)
        parser.add_argument('--container_aging',
                            help='How fast do container sandboxes age, '
                                 'compared to other storage?',
                            default=settings.PURGE_CONTAINER_AGING,
                            type=float)
        parser.add_argument("--synch",
                            help="Synchronize the database and file system by "
                                 "purging any sandboxes, datasets, or log "
                                 "files that don't have a matching entry in "
                                 "the database. Skips the regular purging.",
                            action="store_true")
        parser.add_argument("--wait",
                            help="How long to wait before purging "
                                 "unsynchronized files.",
                            default=settings.PURGE_WAIT,
                            type=parse_duration)

    def handle(self,
               start=2000,
               stop=1000,
               dataset_aging=1.0,
               log_aging=1.0,
               container_aging=1.0,
               synch=False,
               wait=timedelta(seconds=0),
               **kwargs):
        if synch:
            remove_older_than = timezone.now() - wait
            names_removed = sorted(
                ContainerRun.scan_for_unaccounted_sandboxes_and_logs(
                    remove_older_than))
            if names_removed:
                total_size = 0
                for name_removed, size_removed in names_removed:
                    total_size += size_removed
                    logger.warning(
                        'Purged unregistered sandbox file %r containing %s.',
                        name_removed,
                        filesizeformat(size_removed))

                logger.error(
                    'Purged %d unregistered sandbox files containing %s.',
                    len(names_removed),
                    filesizeformat(total_size))
        else:
            need_sizing = ContainerRun.objects.filter(
                end_time__isnull=False,
                sandbox_size__isnull=True,
                sandbox_purged=False).exclude(sandbox_path='')
            for run in need_sizing:
                run.set_sandbox_size()

            total_storage = ContainerRun.objects.aggregate(
                total=Sum('sandbox_size'))['total']
            total_bytes_removed = 0
            purged_count = 0
            if total_storage <= start:
                logger.debug("No sandboxes were purged.")
                return
            run_ids = ContainerRun.objects.order_by('end_time').values_list('id')
            for run_id, in run_ids:
                run = ContainerRun.objects.get(id=run_id)
                total_bytes_removed += run.sandbox_size
                purged_count += 1
                logger.debug("Purged sandbox for run %d containing %s.",
                             run.pk,
                             filesizeformat(run.sandbox_size))
                try:
                    run.delete_sandbox()
                except OSError:
                    logger.error("Failed to purge run %d's sandbox at %r.",
                                 run.id,
                                 run.sandbox_path,
                                 exc_info=True)
                run.sandbox_purged = True  # Don't try to purge it again.
                run.save()
                if total_storage - total_bytes_removed <= stop:
                    break
            logger.info("Purged %d sandboxes containing %s.",
                        purged_count,
                        filesizeformat(total_bytes_removed))
