from __future__ import print_function
import logging
import os
import re
import shutil
from argparse import ArgumentDefaultsHelpFormatter
from tempfile import NamedTemporaryFile, mkdtemp
from zipfile import ZipFile, ZIP_DEFLATED

from django.core.files.base import File
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import transaction
from six.moves import input

from archive.models import Run
from constants import runstates
from container.models import Container, ContainerFamily, ContainerRun, Batch, ContainerArgument, ContainerLog
from pipeline.models import PipelineFamily, Pipeline

logger = logging.getLogger(__name__)
CONVERTED_STATES = {
    runstates.SUCCESSFUL_PK: ContainerRun.COMPLETE,
    runstates.CANCELLED_PK: ContainerRun.CANCELLED,
    runstates.FAILED_PK: ContainerRun.FAILED,
    runstates.QUARANTINED_PK: ContainerRun.CANCELLED
}


def get_converting_pipeline_marker(container_id):
    return 'Converting to container id {}.'.format(container_id)


def get_converted_pipeline_marker(container_id):
    return 'Converted to container id {}.'.format(container_id)


def get_converted_family_marker(family_id):
    return 'Converted to container family id {}.'.format(family_id)


def get_converted_run_marker(container_run_id):
    return 'Converted to container run id {}.'.format(container_run_id)


def get_converted_batch_marker(container_batch_id):
    return 'Converted to container batch id {}.'.format(container_batch_id)


def find_target_id(description, get_marker):
    zero_marker = get_marker(0)
    pattern = re.escape(zero_marker).replace('0', r'(\d+)')
    match = re.search(pattern, description)
    return match and match.group(1)


class Command(BaseCommand):
    help = "Converts old pipelines into new archive containers."

    container_directory = os.path.normpath(os.path.join(settings.MEDIA_ROOT, Container.UPLOAD_DIR))

    def add_arguments(self, parser):
        parser.formatter_class = ArgumentDefaultsHelpFormatter
        default_container_family = ContainerFamily.objects.filter(name='kive-default').first()
        if default_container_family is None:
            default_container_id = None
        else:
            default_container = default_container_family.containers.first()
            default_container_id = default_container and default_container.id
        parser.add_argument('--parent_container_id',
                            type=int,
                            default=default_container_id,
                            help='parent container to launch pipelines in')
        parser.add_argument('--pipeline_id',
                            type=int,
                            help='pipeline to convert')
        parser.add_argument('--batch_size',
                            type=int,
                            default=10,
                            help='number of runs to load in memory')

    def handle(self,
               pipeline_id=None,
               parent_container_id=None,
               batch_size=None,
               **options):
        if parent_container_id is None:
            raise ValueError('No parent container given.')
        default_parent_container = Container.objects.get(id=parent_container_id)

        if pipeline_id is not None:
            pipeline = Pipeline.objects.get(id=pipeline_id)
        else:
            pipeline = self.choose_pipeline()
            if pipeline is None:
                return
        container_id = find_target_id(pipeline.revision_desc,
                                      get_converted_pipeline_marker)
        if container_id is not None:
            raise ValueError('Pipeline id {} already converted.'.format(
                pipeline.id))
        self.check_incomplete_runs(pipeline)
        container_id = find_target_id(pipeline.revision_desc,
                                      get_converting_pipeline_marker)
        if container_id is None:
            container = self.create_container(pipeline, default_parent_container)
        else:
            container = Container.objects.get(id=container_id)

        self.convert_runs(pipeline, container, batch_size)
        converting_marker = get_converting_pipeline_marker(container.id)
        converted_marker = get_converted_pipeline_marker(container.id)
        pipeline.revision_desc = pipeline.revision_desc.replace(
            converting_marker,
            converted_marker)
        pipeline.save()

    def convert_runs(self, pipeline, container, batch_size):
        app = container.apps.get()
        pipeline_runs = Run.objects.filter(pipeline=pipeline).order_by('id')
        zero_marker = get_converted_run_marker(0)
        sql_marker = zero_marker.split('0')[0]
        unconverted_runs = pipeline_runs.exclude(description__icontains=sql_marker)
        pipeline_run_count = pipeline_runs.count()
        while True:
            unconverted_run_count = unconverted_runs.count()
            if not unconverted_run_count:
                break
            converted_run_count = pipeline_run_count - unconverted_run_count
            print('Converted', converted_run_count, 'of', pipeline_run_count, 'runs.')
            batch_runs = unconverted_runs[:batch_size]
            for run in batch_runs:
                with transaction.atomic():
                    batch = run.runbatch
                    container_batch = self.find_or_create_batch(batch)
                    # noinspection PyProtectedMember
                    state = CONVERTED_STATES[run._runstate_id]
                    return_codes = [
                        step.execrecord.generator.methodoutput.return_code
                        for step in run.runsteps.filter(execrecord__isnull=False)]
                    bad_return_codes = filter(None, return_codes)
                    return_code = bad_return_codes and bad_return_codes[-1] or 0
                    container_run = ContainerRun.objects.create(
                        name=run.name,
                        description=run.description,
                        state=state,
                        app=app,
                        batch=container_batch,
                        user=run.user,
                        start_time=run.start_time,
                        end_time=run.end_time,
                        priority=run.priority,
                        return_code=return_code,
                        stopped_by=run.stopped_by)
                    container_run.copy_permissions(run)

                    for run_input in run.inputs.all():
                        argument = app.arguments.get(position=run_input.index,
                                                     type=ContainerArgument.INPUT)
                        container_run.datasets.create(argument=argument,
                                                      dataset=run_input.dataset)
                    for run_output in run.runoutputcables.all():
                        if run_output.execrecord is None:
                            continue
                        output_index = run_output.pipelineoutputcable.output_idx
                        dataset = run_output.execrecord.execrecordouts.get().dataset
                        argument = app.arguments.get(position=output_index,
                                                     type=ContainerArgument.OUTPUT)
                        container_run.datasets.create(argument=argument,
                                                      dataset=dataset)
                    self.convert_logs(run, container_run)
                    container_run.set_md5()
                    container_run.submit_time = run.time_queued
                    container_run.save()
                    if run.description:
                        run.description += '\n'
                    run.description += get_converted_run_marker(container_run.id)
                    run.save()
        print('Converted all {} runs to container id {}.'.format(
            pipeline_run_count,
            container.id))

    def convert_logs(self, run, container_run):
        work_path = mkdtemp(prefix='convert_pipelines_')
        try:
            stdout_path = os.path.join(work_path, 'stdout.txt')
            stderr_path = os.path.join(work_path, 'stderr.txt')
            for step in run.runsteps_in_order:
                if step.execrecord is None:
                    continue
                method_output = step.execrecord.generator.methodoutput
                for log, log_path in ((method_output.output_log,
                                       stdout_path),
                                      (method_output.error_log,
                                       stderr_path)):
                    try:
                        log_size = log.size
                    except ValueError:
                        # File was deleted.
                        log_size = 0
                    if log_size:
                        header = '========== Step {} ==========\n'.format(
                            step.step_num)
                        self.copy_log(log, log_path, header)
            for log_path, log_type in ((stdout_path, ContainerLog.STDOUT),
                                       (stderr_path, ContainerLog.STDERR)):
                with open(log_path, 'a'):
                    pass  # Make sure the file exists.
                container_run.load_log(log_path, log_type)
        finally:
            shutil.rmtree(work_path)

    def copy_log(self, log, dest_path, header):
        log.open()
        try:
            with open(dest_path, 'a') as dest_log:
                dest_log.write(header)
                shutil.copyfileobj(log, dest_log)
        finally:
            log.close()

    def check_incomplete_runs(self, pipeline):
        pipeline_runs = Run.objects.filter(pipeline=pipeline)
        pipeline_run_count = pipeline_runs.count()
        incomplete_runs = pipeline_runs.exclude(
            _runstate__in=CONVERTED_STATES.keys())
        incomplete_run_count = incomplete_runs.count()
        if incomplete_run_count:
            raise ValueError(
                '{} runs out of {} are incomplete. Cannot convert.'.format(
                    incomplete_run_count,
                    pipeline_run_count))

    def find_or_create_batch(self, run_batch):
        if run_batch is None:
            return None
        container_batch_id = find_target_id(run_batch.description,
                                            get_converted_batch_marker)
        if container_batch_id is not None:
            return Batch.objects.get(container_batch_id)
        with transaction.atomic():
            container_batch = Batch.objects.create(
                name=run_batch.name,
                description=run_batch.description,
                user=run_batch.user)
            container_batch.copy_permissions(run_batch)
            if run_batch.description:
                run_batch.description += '\n'
            run_batch.description += get_converted_batch_marker(
                container_batch.id)
        return container_batch

    def create_container(self, pipeline, default_parent_container):
        container = None
        base_name = 'pipeline{}'.format(pipeline.id)
        pipeline_config = self.build_pipeline_config(pipeline)
        with NamedTemporaryFile(prefix=base_name, suffix='.zip') as f:
            parent_containers = set()
            copied_paths = set()
            with ZipFile(f, 'w', ZIP_DEFLATED) as z:
                for step in pipeline.steps.all():
                    method = step.transformation.definite
                    if method.container is None and method.docker_image is not None:
                        raise ValueError('Convert docker image {}.'.format(
                            method.docker_image))
                    parent_containers.add(method.container)
                    code_resource_revision = method.driver
                    install_path = code_resource_revision.coderesource.filename
                    self.add_script(code_resource_revision,
                                    install_path,
                                    copied_paths,
                                    z)
                    for dependency in method.dependencies.all():
                        code_resource_revision = dependency.requirement
                        install_path = os.path.join(dependency.path,
                                                    dependency.get_filename())
                        self.add_script(code_resource_revision,
                                        install_path,
                                        copied_paths,
                                        z)
            parent_containers.discard(None)
            if len(parent_containers) > 1:
                raise ValueError('Found multiple containers: ' +
                                 ', '.join(str(container)
                                           for container in parent_containers))
            if not parent_containers:
                parent_container = default_parent_container
            else:
                parent_container, = parent_containers

            with transaction.atomic():
                container_family = self.find_or_create_family(pipeline.family)
                container = container_family.containers.create(
                    parent=parent_container,
                    tag=pipeline.revision_name,
                    description=pipeline.revision_desc,
                    user=pipeline.user,
                    file=File(f, name=base_name + '.zip'),
                    file_type=Container.ZIP)
                container.copy_permissions(pipeline)
                container.full_clean()
                container.refresh_from_db()
                container.write_archive_content(dict(pipeline=pipeline_config))
                container.created = pipeline.revision_DateTime
                container.save()

                if pipeline.revision_desc:
                    pipeline.revision_desc += '\n'
                pipeline_converting_marker = get_converting_pipeline_marker(container.id)
                pipeline.revision_desc += pipeline_converting_marker
                pipeline.save()
        return container

    def build_pipeline_config(self, pipeline):
        max_memory = 200
        max_threads = 1
        pipeline_config = dict(inputs=[],
                               steps=[],
                               outputs=[])
        for pipeline_input in pipeline.inputs.all():
            input_config = dict(dataset_name=pipeline_input.dataset_name,
                                x=pipeline_input.x,
                                y=pipeline_input.y)
            pipeline_config['inputs'].append(input_config)
        for step in pipeline.steps.all():
            method = step.transformation.definite
            max_memory = max(max_memory, method.memory)
            max_threads = max(max_threads, method.threads)
            code_resource_revision = method.driver
            install_path = code_resource_revision.coderesource.filename
            inputs = [dict(dataset_name=cable.dest.dataset_name,
                           source_dataset_name=cable.source.definite.dataset_name,
                           source_step=cable.source_step)
                      for cable in step.cables_in.order_by('dest__dataset_idx')]
            output_names = [o.dataset_name for o in method.outputs.all()]
            step_config = dict(inputs=inputs,
                               driver=install_path,
                               outputs=output_names,
                               x=step.x,
                               y=step.y)
            pipeline_config['steps'].append(step_config)
        for pipeline_output in pipeline.outputs.all():
            cable = pipeline.outcables.get(output_idx=pipeline_output.dataset_idx)
            output_config = dict(
                dataset_name=pipeline_output.dataset_name,
                source_dataset_name=cable.source.dataset_name,
                source_step=cable.source_step,
                x=pipeline_output.x,
                y=pipeline_output.y)
            pipeline_config['outputs'].append(output_config)
        pipeline_config['default_config'] = dict(memory=max_memory,
                                                 threads=max_threads)
        return pipeline_config

    def add_script(self,
                   code_resource_revision,
                   install_path,
                   copied_paths,
                   archive_file):
        if install_path not in copied_paths:
            driver_path = code_resource_revision.content_file.path
            archive_file.write(driver_path, install_path)
            copied_paths.add(install_path)

    def choose_pipeline(self):
        # Look for pipeline in progress.
        zero_marker = get_converting_pipeline_marker(0)
        sql_search = zero_marker.split('0')[0]
        pipelines = Pipeline.objects.filter(revision_desc__icontains=sql_search)
        for pipeline in pipelines:
            container_id = find_target_id(pipeline.revision_desc,
                                          get_converting_pipeline_marker)
            if container_id is not None:
                print(pipeline)
                # noinspection PyCompatibility
                if input('In progress, continue? [Y]/N').upper() not in ('Y', ''):
                    return
                return pipeline

        pipeline_families = PipelineFamily.objects.all()
        family_map = {}
        for i, pipeline_family in enumerate(pipeline_families, 1):
            pipelines = pipeline_family.members.all()
            unconverted_pipelines = []
            converted_pipelines = total_pipelines = 0
            for pipeline in pipelines:
                container_id = find_target_id(pipeline.revision_desc,
                                              get_converted_pipeline_marker)
                if container_id is None:
                    unconverted_pipelines.append(pipeline)
                else:
                    converted_pipelines += 1
                total_pipelines += 1
            family_map[pipeline_family.id] = unconverted_pipelines
            print('{}: {} ({} of {} converted)'.format(i,
                                                       pipeline_family.name,
                                                       converted_pipelines,
                                                       total_pipelines))
        # noinspection PyCompatibility
        choice = int(input('Pick a pipeline family: '))
        pipeline_family = pipeline_families[choice - 1]
        unconverted_pipelines = family_map[pipeline_family.id]
        for pipeline in unconverted_pipelines:
            print('{}: {}'.format(pipeline.revision_number,
                                  pipeline.revision_name))
        # noinspection PyCompatibility
        choice = int(input('Pick a pipeline revision: '))
        pipeline = pipeline_family.members.get(revision_number=choice)
        print(pipeline.revision_name)
        return pipeline

    def find_or_create_family(self, pipeline_family):
        container_family_id = find_target_id(pipeline_family.description,
                                             get_converted_family_marker)
        if container_family_id is not None:
            return ContainerFamily.objects.get(id=container_family_id)
        with transaction.atomic():
            container_family = ContainerFamily.objects.create(
                name=pipeline_family.name,
                description=pipeline_family.description,
                user=pipeline_family.user)
            container_family.copy_permissions(pipeline_family)

            if pipeline_family.description:
                pipeline_family.description += '\n'
            family_marker = get_converted_family_marker(container_family.id)
            pipeline_family.description += family_marker
            pipeline_family.save()
            print('Created family id', container_family.id)
        return container_family
