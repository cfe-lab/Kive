from collections import Counter
from datetime import datetime
import re
from operator import itemgetter

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
from pipeline.models import Pipeline
import itertools


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
            for query in slow_queries:
                print(query)

        table_counts = Counter()
        table_times = Counter()
        for query in active_queries:
            m = re.match('SELECT +"([^"]*)"', query['sql'])
            if m:
                table_counts[m.group(1)] += 1
                table_times[m.group(1)] += float(query['time'])
                if m.group(1) == 'transformation_xputstructureXXX':
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

    def test_prefetch_new(self):
        queryset = Pipeline.objects.prefetch_related(
            'steps__transformation__outputs__structure').filter(family_id=1)[:25]
        pipeline = queryset.first()
        # pipelines[0].steps.all()[0].outputs[0].structure
        step = pipeline.steps.all()[0]
        if False:
            transformation = step.transformation
            output = transformation.outputs.all()[0]
        else:
            output = step.outputs[0]
        structure = output.structure
        return len([structure])

    def test_prefetch(self):
        queryset = Pipeline.objects.prefetch_related(
            'steps__transformation__method__family',
            'steps__transformation__pipeline__family',
            'steps__transformation__method__inputs__structure__compounddatatype__members__datatype',
            'steps__transformation__method__outputs__structure__compounddatatype__members__datatype',
            'steps__transformation__outputs__structure',
            'steps__transformation__method__family',
            'steps__cables_in__custom_wires',
            'steps__cables_in__dest__transformationinput',
            'steps__cables_in__dest__transformationoutput',
            'steps__cables_in__source__transformationinput',
            'steps__cables_in__source__transformationoutput',
            'steps__outputs_to_delete',
            'inputs__structure',
            'inputs__transformation',
            'outcables__source__structure',
            'outcables__source__transformationinput',
            'outcables__source__transformationoutput',
            'outcables__custom_wires__source_pin',
            'outcables__custom_wires__dest_pin',
            'outcables__pipeline',
            'outcables__output_cdt',
            'outputs__structure').filter(family_id=1)[:25]
        pipelines = list(queryset)
        # pipelines[0].steps.all()[0].outputs[0].structure
        steps = list(itertools.chain(*(p.steps.all() for p in pipelines)))
        outputs = list(itertools.chain(*(s.outputs for s in steps)))
        structures = [o.structure for o in outputs if hasattr(o, 'structure')]
        return len(structures)

    def test_ajax(self):
        factory = APIRequestFactory()
        path = reverse('run-status')
        view, _, _ = resolve(path)
        request = factory.get(
            path + '?is_granted=true&page_size=25')
        force_authenticate(request, user=kive_user())
        response = view(request).render()
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
