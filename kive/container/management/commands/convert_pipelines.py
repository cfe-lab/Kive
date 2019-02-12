from __future__ import print_function
import logging
import os
from argparse import ArgumentDefaultsHelpFormatter
from tempfile import NamedTemporaryFile
from zipfile import ZipFile, ZIP_DEFLATED

from django.core.files.base import File
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import transaction

from container.models import Container, ContainerFamily
from pipeline.models import PipelineFamily, Pipeline

logger = logging.getLogger(__name__)

if hasattr(__builtins__, 'raw_input'):
    # noinspection PyShadowingBuiltins
    input = raw_input


# This assumes we are using the default FileSystemStorage class.
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

    def handle(self, pipeline_id=None, parent_container_id=None, **options):
        if parent_container_id is None:
            raise ValueError('No parent container given.')
        default_parent_container = Container.objects.get(id=parent_container_id)

        if pipeline_id is not None:
            pipeline = Pipeline.objects.get(id=pipeline_id)
        else:
            pipeline = self.choose_pipeline()

        pipeline_marker = 'Converted from pipeline id {}.'.format(pipeline.id)
        container = Container.objects.filter(
            description__icontains=pipeline_marker).first()
        if container is not None:
            raise ValueError('Pipeline id {} already converted.'.format(
                pipeline.id))
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

            description = pipeline.revision_desc
            if not description:
                description += '\n'
            description += pipeline_marker
            with transaction.atomic():
                container_family = self.find_or_create_family(pipeline.family)
                container = container_family.containers.create(
                    parent=parent_container,
                    tag=pipeline.revision_name,
                    description=description,
                    user=pipeline.user,
                    file=File(f, name=base_name+'.zip'),
                    file_type=Container.ZIP)
                container.copy_permissions(pipeline)
                container.full_clean()
                container.refresh_from_db()
                container.write_content(dict(pipeline=pipeline_config))
                container.created = pipeline.revision_DateTime
                container.save()
                print('Created container id', container.id)

    def build_pipeline_config(self, pipeline):
        max_memory = max_threads = 1
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
        pipeline_families = PipelineFamily.objects.all()
        for i, pipeline_family in enumerate(pipeline_families, 1):
            pipelines = pipeline_family.members.all()
            converted_pipelines = 0
            total_pipelines = pipelines.count()
            print('{}: {} ({} of {} converted)'.format(i,
                                                       pipeline_family.name,
                                                       converted_pipelines,
                                                       total_pipelines))
        choice = int(input('Pick a pipeline family: '))
        pipeline_family = pipeline_families[choice - 1]
        for pipeline in pipeline_family.members.all():
            print('{}: {}'.format(pipeline.revision_number,
                                  pipeline.revision_name))
        choice = int(input('Pick a pipeline revision: '))
        pipeline = pipeline_family.members.get(revision_number=choice)
        print(pipeline.revision_name)
        return pipeline

    def find_or_create_family(self, pipeline_family):
        family_marker = 'Converted from pipeline family id {}.'.format(
            pipeline_family.id)
        try:
            container_family = ContainerFamily.objects.get(
                description__icontains=family_marker)
        except ContainerFamily.DoesNotExist:
            new_description = pipeline_family.description
            if new_description:
                new_description += '\n'
            new_description += family_marker
            container_family = ContainerFamily.objects.create(
                name=pipeline_family.name,
                description=new_description,
                user=pipeline_family.user)
            container_family.copy_permissions(pipeline_family)
            print('Created family id', container_family.id)
        return container_family
