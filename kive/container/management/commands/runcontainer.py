from __future__ import print_function

import errno
import logging
import os
import shutil
from subprocess import call
from tempfile import mkdtemp

from django.conf import settings
from django.core.files import File
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from container.models import ContainerRun, ContainerArgument, ContainerLog
from librarian.models import Dataset

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Executes a container run in singularity."

    def add_arguments(self, parser):
        parser.add_argument(
            "run_id",
            type=int,
            help='ContainerRun to execute')

    def handle(self, run_id, **kwargs):
        run = self.record_start(run_id)
        try:
            self.create_sandbox(run)
            run.save()

            self.run_container(run)
            run.save()

            self.save_outputs(run)
            run.save()
        except Exception:
            run.state = ContainerRun.FAILED
            run.end_time = timezone.now()
            run.save()
            raise

    def record_start(self, run_id):
        old_state = ContainerRun.NEW
        new_state = ContainerRun.LOADING
        slurm_job_id = os.environ.get('SLURM_JOB_ID')
        rows_updated = ContainerRun.objects.filter(
            id=run_id, state=old_state).update(state=new_state,
                                               start_time=timezone.now(),
                                               slurm_job_id=slurm_job_id)

        # Defer the stopped_by field so we don't overwrite it when another
        # process tries to stop this job.
        run = ContainerRun.objects.defer('stopped_by').get(id=run_id)
        if rows_updated == 0:
            raise CommandError(
                'Expected state {} for run id {}, but was {}.'.format(
                    old_state,
                    run_id,
                    run.state))
        return run

    def create_sandbox(self, run):
        sandbox_root = ContainerRun.SANDBOX_ROOT
        try:
            os.mkdir(sandbox_root)
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise
        prefix = 'user{}_run{}_'.format(run.user.username, run.pk)
        full_sandbox_path = mkdtemp(prefix=prefix, dir=sandbox_root)
        run.sandbox_path = os.path.relpath(full_sandbox_path, settings.MEDIA_ROOT)

        input_path = os.path.join(run.full_sandbox_path, 'input')
        os.mkdir(input_path)
        for dataset in run.datasets.all():
            target_path = os.path.join(input_path, dataset.argument.name)
            source_file = dataset.dataset.get_open_file_handle(raise_errors=True)
            with source_file, open(target_path, 'wb') as target_file:
                shutil.copyfileobj(source_file, target_file)
        os.mkdir(os.path.join(run.full_sandbox_path, 'output'))
        os.mkdir(os.path.join(run.full_sandbox_path, 'logs'))

        run.state = ContainerRun.RUNNING

    def run_container(self, run):
        logs_path = os.path.join(run.full_sandbox_path, 'logs')
        stdout_path = os.path.join(logs_path, 'stdout.txt')
        stderr_path = os.path.join(logs_path, 'stderr.txt')
        command = self.build_command(run)
        with open(stdout_path, 'w') as stdout, open(stderr_path, 'w') as stderr:
            run.return_code = call(command, stdout=stdout, stderr=stderr)

        run.state = ContainerRun.SAVING

    def build_command(self, run):
        container_path = run.app.container.file.path
        input_path = os.path.join(run.full_sandbox_path, 'input')
        output_path = os.path.join(run.full_sandbox_path, 'output')
        command = ['singularity',
                   'run',
                   '--contain',
                   '--cleanenv',
                   '-B',
                   '{}:/mnt/input,{}:/mnt/output'.format(input_path,
                                                         output_path)]
        if run.app.name:
            command.append('--app')
            command.append(run.app.name)
        command.append(container_path)
        for argument in run.app.arguments.all():
            if argument.type == ContainerArgument.INPUT:
                folder = '/mnt/input'
            else:
                folder = '/mnt/output'
            command.append(os.path.join(folder, argument.name))
        return command

    def save_outputs(self, run):
        output_path = os.path.join(run.full_sandbox_path, 'output')
        for argument in run.app.arguments.filter(type=ContainerArgument.OUTPUT):
            argument_path = os.path.join(output_path, argument.name)
            try:
                dataset = Dataset.create_dataset(argument_path,
                                                 name=argument.name,
                                                 user=run.user)
                dataset.copy_permissions(run)
                run.datasets.create(dataset=dataset,
                                    argument=argument)
            except IOError as ex:
                if ex.errno != errno.ENOENT:
                    raise
        # noinspection PyUnresolvedReferences,PyProtectedMember
        short_size = ContainerLog._meta.get_field('short_text').max_length
        logs_path = os.path.join(run.full_sandbox_path, 'logs')
        for file_name, log_type in (('stdout.txt', ContainerLog.STDOUT),
                                    ('stderr.txt', ContainerLog.STDERR)):
            file_path = os.path.join(logs_path, file_name)
            file_size = os.lstat(file_path).st_size
            with open(file_path) as f:
                if file_size <= short_size:
                    long_text = None
                    short_text = f.read(short_size)
                else:
                    short_text = ''
                    long_text = File(f)
                log = run.logs.create(type=log_type, short_text=short_text)
                if long_text is not None:
                    upload_name = 'run_{}_{}'.format(run.id, file_name)
                    log.long_text.save(upload_name, long_text)

        run.state = (ContainerRun.COMPLETE
                     if run.return_code == 0
                     else ContainerRun.FAILED)
        run.end_time = timezone.now()