from collections import Counter
from datetime import datetime
import re
from operator import itemgetter

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.urls import reverse, resolve
from django.db import connection
from rest_framework.test import APIRequestFactory, force_authenticate

from metadata.models import kive_user


class Command(BaseCommand):
    help = "Exercise the Run.is_complete() method for performance testing. "

    def handle(self, *args, **options):
        self.count_queries(self.test_purge_synch)

    def test_purge_synch(self):
        call_command('purge', synch=True)

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
        active_queries = connection.queries[start_count:]
        min_time = duration.total_seconds() * 0.01
        slow_queries = [query
                        for query in active_queries
                        if float(query['time']) > min_time]
        if slow_queries:
            print('')
            total_slow_time = sum(map(float, map(itemgetter('time'), slow_queries)))
            total_time = sum(map(float, map(itemgetter('time'), active_queries)))
            print("Slow queries ({:.2f}s for slow and {:.2f}s for all):".format(
                total_slow_time,
                total_time))
            max_display = 2000
            tail_size = 200
            for query in slow_queries:
                display = str(query)
                if len(display) > max_display:
                    display = (display[:max_display-tail_size] +
                               '...' +
                               display[-tail_size:])
                print(display)

        table_counts = Counter()
        table_times = Counter()
        for query in active_queries:
            m = re.match('SELECT +"([^"]*)"', query['sql'])
            if m:
                table_counts[m.group(1)] += 1
                table_times[m.group(1)] += float(query['time'])
                if m.group(1) == 'transformation_xputstructureXXX':
                    print(query['sql'])
        print('')
        print('Query counts:')
        for table, count in table_counts.most_common(20):
            print('{}: {}'.format(table, count))
        print('')
        print('Query times:')
        for table, time in table_times.most_common(20):
            print('{}: {}'.format(table, time))

        return result

    def test_ajax(self):
        factory = APIRequestFactory()
        path = '/api/datasets/'
        view, _, _ = resolve(path)
        request = factory.get(
            path + '?is_granted=true&filters%5B0%5D%5Bkey%5D=uploaded'
                   '&filters%5B1%5D%5Bkey%5D=cdt&filters%5B1%5D%5Bval%5D=31'
                   '&page_size=8&page=1')
        force_authenticate(request, user=kive_user())
        response = view(request).render()
        data = response.render().data
        return data['count']

    def test_ajax_download(self):
        factory = APIRequestFactory()
        dataset_path = reverse('dataset-download', kwargs={'pk': 283134})
        view, _, _ = resolve(dataset_path)
        request = factory.get(dataset_path)
        force_authenticate(request, user=kive_user())
        response = view(request, pk=283134)
        content = response.content
        return content
