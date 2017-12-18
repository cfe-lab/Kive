import logging
from time import sleep

from collections import defaultdict
from django.core.management.base import BaseCommand

from archive.models import Run, RunBatch
from constants import runstates
from pipeline.models import Pipeline


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Purges output datasets from runs and requests reruns."

    def add_arguments(self, parser):
        parser.add_argument(
            "--run_ids",
            help='comma-separated list of runs to purge and rerun')
        parser.add_argument(
            "--pipeline_ids",
            help='comma-separated list of pipeline ids to search for runs')
        parser.add_argument(
            "--skip_pipeline_ids",
            help='comma-separated list of pipeline ids to avoid')
        parser.add_argument(
            "--runs_per_pipeline",
            type=int,
            default=10,
            help='number of runs to purge for each pipeline, chosen randomly')
        parser.add_argument(
            "--active_count",
            type=int,
            default=50,
            help='number of runs active at one time')
        parser.add_argument(
            "--outputs",
            default="",
            help='comma-separated list of output names to purge')

    def handle(self, *args, **options):
        logger.info('Starting.')
        if options['run_ids']:
            run_ids = map(int, options['run_ids'].split(','))
            runs = Run.objects.filter(id__in=run_ids)
        else:
            if options['pipeline_ids']:
                pipeline_ids = map(int, options['pipeline_ids'].split(','))
            else:
                pipelines = Pipeline.objects.order_by('id')
                if options['skip_pipeline_ids']:
                    skip_ids = map(int, options['skip_pipeline_ids'].split(','))
                    pipelines = pipelines.exclude(id__in=skip_ids)
                pipeline_ids = (row['id'] for row in pipelines.values('id'))
            run_count = options['runs_per_pipeline']
            runs = self.find_runs(pipeline_ids, run_count)
        active_count = options['active_count']
        targets = filter(None, options['outputs'].split(','))
        batch = None
        current_pipeline_id = None
        for run in runs:
            while Run.objects.filter(end_time=None).count() >= active_count:
                sleep(10)

            logger.debug('Rerunning %s', run)
            for step in run.runsteps_in_order:
                execrecord = step.execrecord
                if execrecord is not None:
                    for output in execrecord.execrecordouts_in_order:
                        output_name = output.generic_output.definite.dataset_name
                        dataset = output.dataset
                        if not targets or output_name in targets:
                            dataset.dataset_file.delete()
            if run.pipeline_id != current_pipeline_id:
                batch = RunBatch.objects.create(name=str(run.pipeline),
                                                user_id=run.user_id)
                current_pipeline_id = run.pipeline_id
            rerun = Run.objects.create(pipeline_id=run.pipeline_id,
                                       user_id=run.user_id,
                                       name=run.name,
                                       runbatch=batch)
            for old_input in run.inputs.all():
                rerun.inputs.create(dataset=old_input.dataset,
                                    index=old_input.index)
            for old_group in run.groups_allowed.all():
                rerun.groups_allowed.add(old_group)
            for old_user in run.users_allowed.all():
                rerun.users_allowed.add(old_user)
        logger.info('Done.')

    def find_runs(self, pipeline_ids, run_count):
        launched_input_ids = defaultdict(set)  # {family_id: {input_id}}
        for pipeline_id in pipeline_ids:
            # We run into trouble when two versions of a pipeline run at the
            # same time and try to restore the same dataset at the same time.
            pipeline = Pipeline.objects.get(id=pipeline_id)
            family_input_ids = launched_input_ids[pipeline.family_id]
            pipeline_run_count = 0
            for run in Run.objects.filter(pipeline_id=pipeline_id,
                                          _runstate_id=runstates.SUCCESSFUL_PK).order_by('?'):
                input_ids = {run_input.dataset_id
                             for run_input in run.inputs.order_by('index')}
                if not input_ids & family_input_ids:
                    yield run
                    family_input_ids |= input_ids
                    pipeline_run_count += 1
                if pipeline_run_count >= run_count:
                    break
            if pipeline_run_count < run_count:
                logger.warn('Only found %d runs for pipeline %s.',
                            pipeline_run_count,
                            pipeline)
