from threading import Thread, Event

from django.core.management.base import BaseCommand

from librarian.models import Dataset
from method.models import CodeResourceRevision


class Command(BaseCommand):
    help = 'Checks MD5 checksums for all code resources.'

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
        self.code_count = self.code_failures = self.code_skips = 0
        self.dataset_count = self.dataset_failures = self.dataset_skips = 0
        self.dataset_purged = 0
        finish_event = Event()
        report_thread = Thread(target=self.report,
                               args=(options['report_interval'], finish_event))
        report_thread.daemon = True
        report_thread.start()
        for r in CodeResourceRevision.objects.iterator():
            self.code_count += 1
            if max_size is not None and r.content_file.size > max_size:
                self.code_skips += 1
            elif not r.check_md5():
                self.code_failures += 1
        for ds in Dataset.objects.iterator():
            self.dataset_count += 1
            if not ds.has_data():
                self.dataset_purged += 1
            elif max_size is not None and ds.get_filesize() > max_size:
                self.dataset_skips += 1
            elif not ds.check_md5():
                self.dataset_failures += 1
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
            if self.code_count:
                print('Of {} code resources, {} failures and {} skipped.'.format(
                    self.code_count,
                    self.code_failures,
                    self.code_skips))
            if self.dataset_count:
                print('Of {} datasets, {} purged, {} failures, and {} skipped.'.format(
                    self.dataset_count,
                    self.dataset_purged,
                    self.dataset_failures,
                    self.dataset_skips))
        print('See log for details.')
