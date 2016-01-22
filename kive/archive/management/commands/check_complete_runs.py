from collections import Counter
from datetime import datetime
import re

from django.core.management.base import BaseCommand
from django.core.urlresolvers import reverse, resolve
from django.db import connection
from rest_framework.test import APIRequestFactory, force_authenticate

from archive.models import Run
from metadata.models import kive_user


class Command(BaseCommand):
    help = "Exercise the Run.is_complete() method for performance testing. "

    def handle(self, *args, **options):
        self.count_queries(self.test_ajax)

    def check_many_runs(self):
        RUN_COUNT = 100
        runs = Run.objects.order_by('-id')[:RUN_COUNT]
        complete_count = sum(1 for run in runs if run.is_complete())
        print('Found {} complete in {} most recent runs.'.format(complete_count,
                                                                 RUN_COUNT))

    def count_queries(self, task):
        """ Count the queries triggered by task, and print a summary.

        @param task: a callable that will trigger some database queries.
        """
        start_count = len(connection.queries)
        start_time = datetime.now()
        result = task()
        duration = datetime.now() - start_time
        end_count = len(connection.queries)
        print('{!r} after {} queries and {}.'.format(
            result,
            end_count - start_count,
            duration))
        table_counts = Counter()
        table_times = Counter()
        for query in connection.queries[start_count:]:
            m = re.match('SELECT +"([^"]*)"', query['sql'])
            if m:
                table_counts[m.group(1)] += 1
                table_times[m.group(1)] += float(query['time'])
                if m.group(1) == 'pipeline_pipelinecableXXX':
                    print query['sql']
        print('')
        print('Query counts:')
        for table, count in table_counts.most_common(20):
            print('{}: {}'.format(table, count))
        print('')
        print('Query times:')
        for table, time in table_times.most_common(20):
            print('{}: {}'.format(table, time))

        return result

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

    def test_ajax(self):
        factory = APIRequestFactory()
        run_status_path = reverse('run-status')
        run_status_view, _, _ = resolve(run_status_path)
        request = factory.get(run_status_path + '?page_size=25')
        force_authenticate(request, user=kive_user())
        response = run_status_view(request).render()
        data = response.render().data
        return data['count']
