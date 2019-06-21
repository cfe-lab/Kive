from threading import Thread, Event

import errno
from django.core.management.base import BaseCommand

from librarian.models import Dataset


class Command(BaseCommand):
    help = 'Checks MD5 checksums for all datasets.'

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self.dataset_count = self.dataset_failures = self.dataset_skips = 0
        self.dataset_purged = self.dataset_missing = self.dataset_passed = 0

    def add_arguments(self, parser):
        parser.add_argument('--max_size',
                            '-m',
                            type=int,
                            help='Maximum size of file to check (bytes)')
        parser.add_argument('--report_interval',
                            '-r',
                            type=int,
                            default=10,
                            help='Interval between status reports (seconds)')

    def handle(self, *args, **options):
        max_size = options['max_size']
        finish_event = Event()
        report_thread = Thread(target=self.report,
                               args=(options['report_interval'], finish_event))
        report_thread.daemon = True
        report_thread.start()
        for ds in Dataset.objects.iterator():
            self.dataset_count += 1
            try:
                has_data = ds.has_data(raise_errors=True)
            except IOError as ex:
                if ex.errno != errno.ENOENT:
                    raise
                self.dataset_missing += 1
                continue
            if not has_data:
                self.dataset_purged += 1
            elif max_size is not None and ds.get_filesize() > max_size:
                self.dataset_skips += 1
            elif not ds.check_md5():
                self.dataset_failures += 1
            else:
                self.dataset_passed += 1
        finish_event.set()
        report_thread.join()

    def report(self, interval, finish_event):
        """ Loop until finish_event is set, reporting every few seconds.

        :param int interval: number of seconds between each report
        :param finish_event: a threading event that will be set when processing
        is finished.
        """
        is_finished = False
        while not is_finished:
            is_finished = finish_event.wait(interval)
            if self.dataset_count:
                print('Of {} datasets, {} purged, {} missing, {} skipped, '
                      '{} passed, and {} failed.'.format(
                        self.dataset_count,
                        self.dataset_purged,
                        self.dataset_missing,
                        self.dataset_skips,
                        self.dataset_passed,
                        self.dataset_failures))
