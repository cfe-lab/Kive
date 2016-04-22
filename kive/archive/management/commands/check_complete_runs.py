from collections import Counter
from datetime import datetime
import re

from django.core.management.base import BaseCommand
from django.core.urlresolvers import reverse, resolve
from django.db import connection
from django.db.models import Count
from rest_framework.test import APIRequestFactory, force_authenticate

from archive.models import Run, RunStep
from metadata.models import kive_user, CompoundDatatype
from librarian.models import ExecRecord, Dataset
from librarian.views import dataset_view
from kive.tests import DuckRequest


class Command(BaseCommand):
    help = "Exercise the Run.is_complete() method for performance testing. "

    def handle(self, *args, **options):
        self.count_queries(self.test_dataset_rows)

    def check_many_runs(self):
        RUN_COUNT = 100
        runs = Run.objects.order_by('-id')[:RUN_COUNT]
        complete_count = sum(1 for run in runs if run.is_complete())
        print('Found {} complete in {} most recent runs.'.format(complete_count,
                                                                 RUN_COUNT))

    def test_dataset_view(self):
        dataset_id = self.find_big_dataset().id
        request = DuckRequest()
        return dataset_view(request, dataset_id)

    def test_dataset_rows(self):
        dataset = self.find_big_dataset()
        return len(list(dataset.rows(data_check=True, limit=7)))

    def test_many_runsteps(self):
        COUNT = 100
        run_steps = list(RunStep.objects.filter(
            reused=False).prefetch_related('RSICs').order_by('-id')[:COUNT])
        success_count = sum(1
                            for step in run_steps
                            if step.is_successful())
        print('Found {} successful in {} most recent runs.'.format(success_count,
                                                                   len(run_steps)))

    def test_many_execrecords(self):
        COUNT = 100
        execrecords = list(ExecRecord.objects.order_by('-id')[:COUNT])
        fail_count = sum(1 for r in execrecords if r.has_ever_failed())
        print('Found {} failed in {} most recent runs.'.format(fail_count,
                                                               len(execrecords)))

    def find_big_dataset(self):
        compound_datatypes = CompoundDatatype.objects.annotate(
            Count('members')).order_by('-members__count')
        for compound_datatype in compound_datatypes:
            datasets = Dataset.objects.filter(
                structure__compounddatatype=compound_datatype).exclude(
                    dataset_file='').order_by('-structure__num_rows')
            for dataset in datasets:
                return dataset
        raise RuntimeError('No structured datasets found.')

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

    def test_ajax_download(self):
        factory = APIRequestFactory()
        dataset_path = reverse('dataset-download', kwargs={'pk': 283134})
        dataset_view, _, _ = resolve(dataset_path)
        request = factory.get(dataset_path)
        force_authenticate(request, user=kive_user())
        response = dataset_view(request, pk=283134)
        content = response.content
        return content
