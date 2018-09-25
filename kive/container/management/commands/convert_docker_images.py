from __future__ import print_function
import logging
import sys

from django.core.management.base import BaseCommand

from archive.models import RunStep
from container.models import Container
from method.models import Method, DockerImage


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Converts methods from using docker to singularity."

    def handle(self, *args, **options):
        # See also: utils/request_reruns.py
        print('Containers with descriptions that start with "Broken:":')
        broken_containers = Container.objects.filter(
            description__istartswith='Broken:')
        methods = Method.objects.filter(
            container__in=broken_containers).order_by('container__family__name',
                                                      'container__tag',
                                                      'family__name',
                                                      'revision_number')
        for method in methods:
            print(method,
                  'uses',
                  method.container.family,
                  method.container.tag)
        if not methods:
            print('None in use.')
        else:
            if self.confirm("Set these methods' containers to NULL? [y/N]"):
                for method in methods:
                    method.container = None
                    method.save()
                print('Done.')
        print()

        print('Docker images needing to convert:')
        images_to_convert = DockerImage.objects.filter(
            pk__in=Method.objects.filter(
                docker_image_id__isnull=False,
                container_id__isnull=True).values('docker_image_id'))

        conversions = []  # [(image, container)]
        for image in images_to_convert:
            try:
                container = Container.objects.exclude(
                    pk__in=broken_containers.values('pk')).get(
                    family__git=image.git,
                    tag__startswith=image.tag + '-singularity')
            except Container.DoesNotExist:
                latest_run_step = self.find_run_step(image)
                print(image.name,
                      image.git,
                      image.tag,
                      'run',
                      latest_run_step.run_id)
                continue
            conversions.append((image, container))

        print()
        print('Docker images to convert:')
        for image, container in conversions:
            latest_run_step = self.find_run_step(image)
            print(image.name,
                  image.git,
                  image.tag,
                  '=>',
                  container.tag,
                  'run',
                  latest_run_step and latest_run_step.run_id)

        if not conversions:
            print('None found.')
            return
        elif not self.confirm('Continue? [y/N]'):
            return
        logger.info('Starting.')
        for image, container in conversions:
            for method in image.methods.all():
                if method.container is not None:
                    logger.info('skipping %s.', method)
                else:
                    try:
                        latest_run_step = self.find_method_run_step(method)
                        run_text = 'run {}'.format(latest_run_step.run_id)
                    except RunStep.DoesNotExist:
                        run_text = 'unused'

                    method.container = container
                    method.save()
                    logger.info('converted %s (%s).', method, run_text)
        logger.info('Done.')

    def find_run_step(self, image):
        latest_run_step = RunStep.objects.filter(
            pipelinestep__transformation__method__docker_image_id=image.id).order_by(
            'run_id').last()
        return latest_run_step

    def find_method_run_step(self, method):
        latest_run_step = RunStep.objects.filter(
            pipelinestep__transformation_id=method.id).latest('run_id')
        return latest_run_step

    def confirm(self, prompt):
        print(prompt, end=' ')
        confirmation = sys.stdin.readline().strip()
        is_confirmed = confirmation.lower() == 'y'
        return is_confirmed
