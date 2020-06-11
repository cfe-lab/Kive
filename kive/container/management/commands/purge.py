import errno
import logging
import os
import shutil
from argparse import ArgumentDefaultsHelpFormatter
from collections import Counter
from datetime import timedelta, datetime
from itertools import chain

from django.contrib.humanize.templatetags.humanize import naturaltime
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import models
from django.db.models.expressions import Value, F
from django.db.models.fields.files import FieldFile
from django.db.models.functions import Now

from django.template.defaultfilters import filesizeformat, pluralize
from django.utils import timezone
from django.utils.dateparse import parse_duration

from container.models import ContainerRun, ContainerLog, Container
from librarian.models import Dataset
from portal.models import parse_file_size

# error - summary of unregistered files, can't meet purge target, or can't purge
# warning - list each unregistered file
# info - summary of regular purge
# debug - list each purged file, summary even when nothing purged
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Scan through storage files, recording the size of new files, ' \
           'and purging old files if needed.'

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter

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
        parser.add_argument('--sandbox_aging',
                            help='How fast do container sandboxes age, '
                                 'compared to other storage?',
                            default=settings.PURGE_SANDBOX_AGING,
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
        parser.add_argument("--batch_size",
                            help="Number of files to check at a time.",
                            default=settings.PURGE_BATCH_SIZE,
                            type=int)

    def handle(self,
               start=2000,
               stop=1000,
               dataset_aging=1.0,
               log_aging=1.0,
               sandbox_aging=1.0,
               synch=False,
               wait=timedelta(seconds=0),
               batch_size=100,
               **kwargs):
        # noinspection PyBroadException
        try:
            if synch:
                logger.debug('Starting purge synchronization.')
                self.synch_model(Container, 'file', wait, batch_size)
                self.synch_model(ContainerRun, 'sandbox_path', wait, batch_size)
                self.synch_model(ContainerLog, 'long_text', wait, batch_size)
                self.synch_model(Dataset, 'dataset_file', wait, batch_size)
                Dataset.external_file_check(batch_size=batch_size)
                logger.debug('Finished purge synchronization.')
            else:
                self.purge(start,
                           stop,
                           dataset_aging,
                           log_aging,
                           sandbox_aging,
                           batch_size)
        except Exception:
            logger.error('Purge failed.', exc_info=True)

    def purge(self,
              start,
              stop,
              dataset_aging,
              log_aging,
              sandbox_aging,
              batch_size):
        logger.debug('Starting purge.')
        container_total = self.set_file_sizes(Container,
                                              'file',
                                              'file_size',
                                              'created')
        sandbox_total = self.set_file_sizes(ContainerRun,
                                            'sandbox_path',
                                            'sandbox_size',
                                            'end_time')
        log_total = self.set_file_sizes(ContainerLog,
                                        'long_text',
                                        'log_size',
                                        'run__end_time')
        dataset_total = self.set_file_sizes(Dataset,
                                            'dataset_file',
                                            'dataset_size',
                                            'date_created')

        total_storage = remaining_storage = (
                container_total + sandbox_total + log_total + dataset_total)
        if total_storage <= start:
            storage_text = self.summarize_storage(container_total,
                                                  dataset_total,
                                                  sandbox_total,
                                                  log_total)
            logger.debug(u"No purge needed for %s: %s.",
                         filesizeformat(total_storage),
                         storage_text)
            return

        sandbox_ages = ContainerRun.find_unneeded().annotate(
            entry_type=Value('r', models.CharField()),
            age=sandbox_aging * (Now() - F('end_time'))).values_list(
            'entry_type',
            'id',
            'age').order_by()

        log_ages = ContainerLog.find_unneeded().annotate(
            entry_type=Value('l', models.CharField()),
            age=log_aging * (Now() - F('run__end_time'))).values_list(
            'entry_type',
            'id',
            'age').order_by()

        dataset_ages = Dataset.find_unneeded().annotate(
            entry_type=Value('d', models.CharField()),
            age=dataset_aging * (Now() - F('date_created'))).values_list(
            'entry_type',
            'id',
            'age').order_by()

        purge_counts = Counter()
        max_purge_dates = {}
        min_purge_dates = {}
        purge_entries = sandbox_ages.union(log_ages,
                                           dataset_ages,
                                           all=True).order_by('-age')
        while remaining_storage > stop:
            entry_count = 0
            for entry_type, entry_id, age in purge_entries[:batch_size]:
                entry_count += 1
                if entry_type == 'r':
                    run = ContainerRun.objects.get(id=entry_id)
                    entry_size = run.sandbox_size
                    entry_date = run.end_time
                    logger.debug("Purged container run %d containing %s.",
                                 run.pk,
                                 filesizeformat(entry_size))
                    try:
                        run.delete_sandbox()
                    except OSError:
                        logger.error(u"Failed to purge container run %d at %r.",
                                     run.id,
                                     run.sandbox_path,
                                     exc_info=True)
                        run.sandbox_path = ''
                    run.save()
                elif entry_type == 'l':
                    log = ContainerLog.objects.get(id=entry_id)
                    entry_size = log.log_size
                    entry_date = log.run.end_time
                    logger.debug("Purged container log %d containing %s.",
                                 log.id,
                                 filesizeformat(entry_size))
                    log.long_text.delete()
                else:
                    assert entry_type == 'd'
                    dataset = Dataset.objects.get(id=entry_id)
                    entry_size = dataset.dataset_size
                    dataset_total -= dataset.dataset_size
                    entry_date = dataset.date_created
                    logger.debug("Purged dataset %d containing %s.",
                                 dataset.pk,
                                 filesizeformat(entry_size))
                    dataset.dataset_file.delete()
                purge_counts[entry_type] += 1
                purge_counts[entry_type + ' bytes'] += entry_size
                # PyCharm false positives...
                # noinspection PyUnresolvedReferences
                min_purge_dates[entry_type] = min(entry_date,
                                                  min_purge_dates.get(entry_type, entry_date))
                # noinspection PyUnresolvedReferences
                max_purge_dates[entry_type] = max(entry_date,
                                                  max_purge_dates.get(entry_type, entry_date))
                remaining_storage -= entry_size
                if remaining_storage <= stop:
                    break
            if entry_count == 0:
                break
        for entry_type, entry_name in (('r', 'container run'),
                                       ('l', 'container log'),
                                       ('d', 'dataset')):
            purged_count = purge_counts[entry_type]
            if not purged_count:
                continue
            min_purge_date = min_purge_dates[entry_type]
            max_purge_date = max_purge_dates[entry_type]
            collective = entry_name + pluralize(purged_count)
            bytes_removed = purge_counts[entry_type + ' bytes']
            start_text = naturaltime(min_purge_date)
            end_text = naturaltime(max_purge_date)
            date_range = (start_text
                          if start_text == end_text
                          else start_text + ' to ' + end_text)
            logger.info("Purged %d %s containing %s from %s.",
                        purged_count,
                        collective,
                        filesizeformat(bytes_removed),
                        date_range)
        if remaining_storage > stop:
            storage_text = self.summarize_storage(container_total,
                                                  dataset_total)
            logger.error('Cannot reduce storage to %s: %s.',
                         filesizeformat(stop),
                         storage_text)

    def set_file_sizes(self, model, file_field, size_field, date_field):
        """
        Scan through all model rows that do not have their sizes set and set them.
        :return: the total storage used by all files referenced by rows in the
            model
        """
        rows_to_set = model.objects.filter(
            **{file_field+'__isnull': False,
               size_field+'__isnull': True}).exclude(
            **{file_field: ''}).exclude(
            **{date_field: None}).annotate(extra__date=F(date_field))
        model_name = getattr(model, '_meta').model_name
        min_missing_date = max_missing_date = None
        missing_count = 0
        for row in rows_to_set:
            f = getattr(row, file_field)
            try:
                if isinstance(f, FieldFile):
                    file_size = f.size
                else:
                    file_size = self.scan_folder_size(f)
            except OSError as ex:
                if ex.errno != errno.ENOENT:
                    raise
                file_size = 0
                row_date = row.extra__date
                file_name = os.path.relpath(ex.filename, settings.MEDIA_ROOT)
                setattr(row, file_field, '')
                logger.warning('Missing %s file %r from %s.',
                               model_name,
                               str(file_name),
                               naturaltime(row_date))
                if min_missing_date is None or row_date < min_missing_date:
                    min_missing_date = row_date
                if max_missing_date is None or max_missing_date < row_date:
                    max_missing_date = row_date
                missing_count += 1
            setattr(row, size_field, file_size)
            row.save()
        if missing_count:
            start_text = naturaltime(min_missing_date)
            end_text = naturaltime(max_missing_date)
            date_range = (start_text
                          if start_text == end_text
                          else start_text + ' to ' + end_text)
            logger.error('Missing %d %s file%s from %s.',
                         missing_count,
                         model_name,
                         pluralize(missing_count),
                         date_range)

        # Get the total amount of active storage recorded.
        return model.objects.exclude(
            **{file_field: ''}).exclude(  # Already purged.
            **{file_field: None}).aggregate(  # Not used.
            models.Sum(size_field))[size_field + "__sum"] or 0

    def scan_folder_size(self, folder_path, newest_allowed=None):
        """ Scan the total size of all the files in and below a folder.

        :param str folder_path: the folder to scan, relative to MEDIA_ROOT.
        :param datetime newest_allowed: if there are any files newer than this,
            return None.
        :return: the total size if all of the files are old enough or
            newest_allowed is None, otherwise return None.
        """
        full_path = os.path.join(settings.MEDIA_ROOT, folder_path)
        size_accumulator = 0
        sandbox_files = (os.path.join(root, file_name)
                         for root, _, files in os.walk(full_path, onerror=raise_error)
                         for file_name in files)
        for file_path in sandbox_files:
            file_size = self.get_file_size(file_path, newest_allowed)
            if file_size is None:
                return  # File was too new, or was deleted (indicating an active run).
            size_accumulator += file_size
        return size_accumulator  # we don't set self.sandbox_size here, we do that explicitly elsewhere.

    @staticmethod
    def get_file_size(file_path, newest_allowed=None):
        """ Get the size of a file, if it exists and isn't too new.

        :param str file_path: the absolute path of the file to check
        :param datetime newest_allowed: if the file is newer than this,
            return None.
        :return: the file size if it's old enough or newest_allowed is None,
            otherwise return None.
        """
        if os.path.islink(file_path):
            file_stat = os.lstat(file_path)
        else:
            try:
                file_stat = os.stat(file_path)
            except FileNotFoundError:
                return
        if newest_allowed is not None:
            modification_time = datetime.fromtimestamp(
                file_stat.st_mtime,
                timezone.get_current_timezone())
            if modification_time > newest_allowed:
                return
        return file_stat.st_size

    def summarize_storage(self,
                          container_total,
                          dataset_total,
                          sandbox_total=0,
                          log_total=0):
        remainders = []
        for size, label in [(container_total, 'containers'),
                            (sandbox_total, 'container runs'),
                            (log_total, 'container logs'),
                            (dataset_total, 'datasets')]:
            if size:
                remainders.append('{} of {}'.format(
                    filesizeformat(size),
                    label))
        storage_text = ', '.join(remainders) if remainders else 'empty storage'
        return storage_text

    def synch_model(self, model, path_field_name, wait, batch_size):
        file_names = set()
        total_files = total_bytes = 0
        for file_name in chain(model.scan_file_names(), [None]):
            if file_name is not None:
                file_names.add(file_name)
            if len(file_names) >= batch_size or file_name is None:
                files_removed, bytes_removed = self.synch_model_files(
                    model,
                    path_field_name,
                    file_names,
                    wait)
                total_files += files_removed
                total_bytes += bytes_removed
                file_names.clear()
        if total_files:
            # noinspection PyProtectedMember
            logger.error(
                'Purged %d unregistered %s file%s containing %s.',
                total_files,
                model._meta.verbose_name,
                pluralize(total_files),
                filesizeformat(total_bytes))

    def synch_model_files(self, model, path_field_name, file_names, wait):
        remove_older_than = timezone.now() - wait
        values_list = model.objects.filter(
            **{path_field_name+'__in': file_names}).values_list(path_field_name)
        found_file_names = {file_name for file_name, in values_list}
        unknown_file_names = file_names - found_file_names
        bytes_removed = files_removed = 0
        for file_name in sorted(unknown_file_names):
            file_path = os.path.join(settings.MEDIA_ROOT, file_name)
            if os.path.isdir(file_path):
                file_size = self.scan_folder_size(file_path, remove_older_than)
                if file_size is None:
                    continue  # Found some new files in there.
                shutil.rmtree(file_path)
            else:
                file_size = self.get_file_size(file_path, remove_older_than)
                if file_size is None:
                    continue  # File was too new or already deleted.
                os.remove(file_path)
            logger.warning(
                'Purged unregistered file %r containing %s.',
                str(file_name),
                filesizeformat(file_size))
            files_removed += 1
            bytes_removed += file_size
        return files_removed, bytes_removed


def raise_error(ex):
    raise ex
