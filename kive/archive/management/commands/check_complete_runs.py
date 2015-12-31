from django.core.management.base import BaseCommand
from django.db import connection

from archive.models import Run
import re
from collections import Counter


class Command(BaseCommand):
    help = "Exercise the Run.is_complete() method for performance testing. "

    def handle(self, *args, **options):
        self.count_queries()

    def check_many_runs(self):
        RUN_COUNT = 100
        runs = Run.objects.order_by('-id')[:RUN_COUNT]
        complete_count = sum(1 for run in runs if run.is_complete())
        print('Found {} complete in {} most recent runs.'.format(complete_count,
                                                                 RUN_COUNT))

    def count_queries(self):
        run = Run.objects.filter(end_time__isnull=False).last()
        start_count = len(connection.queries)
        is_complete = run.is_complete()
        end_count = len(connection.queries)
        print('is_complete = {} after {} queries.'.format(
            is_complete,
            end_count - start_count))
        table_counts = Counter()
        for query in connection.queries[start_count:]:
            m = re.match('SELECT +"([^"]*)"', query['sql'])
            if m:
                table_counts[m.group(1)] += 1
        for table, count in table_counts.most_common(20):
            print('{}: {}'.format(table, count))

    def test_prefetch(self):
        run = Run.objects.filter(end_time__isnull=False).last()
        cables = run.runoutputcables.prefetch_related(
            'log__record__execrecord__generator')
        print('---\n\n')
        cable = cables[0]
        print('---\n\n')
        log = cable.log
        record = log.record
        execrecord = record.execrecord
        generator = execrecord.generator
        print generator.id
