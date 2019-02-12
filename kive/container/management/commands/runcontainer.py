from __future__ import print_function

import errno
import logging
import os
import shutil
from subprocess import call
import json

from django.core.files import File
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from container.models import ContainerRun, ContainerArgument, ContainerLog
from librarian.models import Dataset

KNOWN_EXTENSIONS = ('csv',
                    'doc',
                    'fasta',
                    'fastq',
                    'pdf',
                    'txt',
                    'tar',
                    'gz',
                    'zip')
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
            self.fill_sandbox(run)
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

    def fill_sandbox(self, run):
        if not run.sandbox_path:
            # This should only be needed during tests.
            run.create_sandbox()

        input_path = os.path.join(run.full_sandbox_path, 'input')
        os.mkdir(input_path)
        for dataset in run.datasets.all():
            target_path = os.path.join(input_path, dataset.argument.name)
            source_file = dataset.dataset.get_open_file_handle(raise_errors=True)
            with source_file, open(target_path, 'wb') as target_file:
                shutil.copyfileobj(source_file, target_file)
        os.mkdir(os.path.join(run.full_sandbox_path, 'output'))

        run.state = ContainerRun.RUNNING

    def run_container(self, run):
        logs_path = os.path.join(run.full_sandbox_path, 'logs')
        stdout_path = os.path.join(logs_path, 'stdout.txt')
        stderr_path = os.path.join(logs_path, 'stderr.txt')
        with open(stdout_path, 'w') as stdout, open(stderr_path, 'w') as stderr:
            if run.app.container.can_be_parent():
                # This is a Singularity container.
                command = self.build_command(run)
                run.return_code = call(command, stdout=stdout, stderr=stderr)
            else:
                # This is a child container to be run inside another Singularity container.
                bin_dir = os.path.join(run.full_sandbox_path, "bin")
                run.app.container.extract_archive(os.path.join(run.full_sandbox_path, "bin"))
                pipeline_path = os.path.join(bin_dir, "kive", "pipeline.json")
                with open(pipeline_path, "r") as f:
                    instructions = json.loads(f.read())
                run.return_code = self.run_pipeline(
                    instructions,
                    run,
                    stdout,
                    stderr,
                    bin_dir,
                    os.path.join(run.full_sandbox_path, "input"),
                    os.path.join(run.full_sandbox_path, "output")
                )
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
    
    def build_dataset_name(self, run, argument_name):
        parts = argument_name.split('_')
        extension = parts[-1]
        if extension.lower() in KNOWN_EXTENSIONS:
            parts.pop()
            if parts and parts[-1] == 'tar':
                parts.pop()
                extension = 'tar' + '.' + extension
        else:
            extension = None
        parts.append(str(run.id))
        dataset_name = '_'.join(parts)
        if extension is not None:
            dataset_name += '.' + extension
        return dataset_name

    def save_outputs(self, run):
        output_path = os.path.join(run.full_sandbox_path, 'output')
        upload_path = os.path.join(run.full_sandbox_path, 'upload')
        os.mkdir(upload_path)
        for argument in run.app.arguments.filter(type=ContainerArgument.OUTPUT):
            argument_path = os.path.join(output_path, argument.name)
            dataset_name = self.build_dataset_name(run, argument.name)
            new_argument_path = os.path.join(upload_path, dataset_name)
            try:
                os.rename(argument_path, new_argument_path)
                dataset = Dataset.create_dataset(new_argument_path,
                                                 name=dataset_name,
                                                 user=run.user)
                dataset.copy_permissions(run)
                run.datasets.create(dataset=dataset,
                                    argument=argument)
            except (OSError, IOError) as ex:
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

    def run_pipeline(self,
                     instructions,
                     run,
                     standard_out,
                     standard_err,
                     extracted_archive_dir,
                     external_inputs_dir,
                     external_outputs_dir,
                     internal_binary_dir="/mnt/bin",
                     internal_inputs_dir="/mnt/input",
                     internal_outputs_dir="/mnt/output",
                     internal_working_dir="/mnt/bin"):
        """
        Run the pipeline dictated in the instructions.

        :param instructions:
        :param run: run to execute
        :param standard_out: a writable file-like object to write stdout to
        :param standard_err: similarly for stderr
        :param extracted_archive_dir:
        :param external_inputs_dir:
        :param external_outputs_dir:
        :param internal_binary_dir: as it appears inside the container
        :param internal_inputs_dir: as it appears inside the container
        :param internal_outputs_dir: as it appears inside the container
        :param internal_working_dir: as it appears inside the container
        :return:
        """
        # The instructions take the form of a Python representation of a pipeline JSON file.
        # We keep track of what files were produced by what steps in file_map, which is a list of dictionaries.
        # Each dictionary maps dataset_name -|-> (internal path, external path), and the step index is their
        # position in the list.
        inputs_map = {}
        for input_dict in instructions["inputs"]:
            # This dictionary has a field called "dataset_name".
            inputs_map[input_dict["dataset_name"]] = (
                os.path.join(internal_inputs_dir, input_dict["dataset_name"]),
                os.path.join(external_inputs_dir, input_dict["dataset_name"])
            )  # the second isn't needed but we keep it for consistency
        file_map = [inputs_map]

        final_return_code = 0
        log_path = os.path.dirname(standard_out.name)
        for idx, step in enumerate(instructions["steps"], 1):
            step_header = "========\nProcessing step {}: {}\n========\n".format(
                idx,
                step["driver"])

            # Each step is a dictionary with fields:
            # - driver (the executable)
            # - inputs (a list of (step_num, dataset_name) pairs)
            # - outputs (a list of dataset_names)
            executable = os.path.join(internal_binary_dir, step["driver"])
            driver_external_path = os.path.join(extracted_archive_dir,
                                                step["driver"])
            os.chmod(driver_external_path, 0o777)
            input_paths = []
            for input_dict in step["inputs"]:
                source_step = input_dict["source_step"]
                source_dataset_name = input_dict["source_dataset_name"]
                step_outputs = file_map[source_step]
                internal_path, external_path = step_outputs[source_dataset_name]
                input_paths.append(internal_path)
            outputs_map = {}
            for dataset_name in step["outputs"]:
                file_name = self.build_dataset_name(
                    run,
                    "step{}_{}".format(idx, dataset_name))
                outputs_map[dataset_name] = (
                    os.path.join(internal_outputs_dir, file_name),
                    os.path.join(external_outputs_dir, file_name)
                )
            file_map.append(outputs_map)

            output_paths = [outputs_map[x][0] for x in step["outputs"]]
            execution_args = [
                "singularity",
                "exec",
                "--contain",
                "-B",
                extracted_archive_dir + ':' + internal_binary_dir,
                "-B",
                external_inputs_dir + ':' + internal_inputs_dir,
                "-B",
                external_outputs_dir + ':' + internal_outputs_dir,
                "--pwd",
                internal_working_dir,
                run.app.container.parent.file.path,
                executable
            ]
            all_args = [str(arg)
                        for arg in execution_args + input_paths + output_paths]
            command_path = os.path.join(log_path, 'step_{}_command.txt'.format(idx))
            with open(command_path, 'w') as f:
                f.write(' '.join(all_args))
            step_stdout_path = os.path.join(log_path, 'step_{}_stdout.txt'.format(idx))
            step_stderr_path = os.path.join(log_path, 'step_{}_stderr.txt'.format(idx))
            with open(step_stdout_path, 'w') as step_stdout, \
                    open(step_stderr_path, 'w') as step_stderr:
                step_return_code = call(all_args,
                                        stdout=step_stdout,
                                        stderr=step_stderr)
            for step_path, main_file in ((step_stdout_path, standard_out),
                                         (step_stderr_path, standard_err)):
                log_size = os.stat(step_path).st_size
                if log_size:
                    main_file.write(step_header)
                    with open(step_path) as step_file:
                        shutil.copyfileobj(step_file, main_file)
            if step_return_code != 0:
                final_return_code = step_return_code
                break

        if final_return_code == 0:
            # Now rename the outputs.
            for output in instructions["outputs"]:
                # This dictionary has fields
                # - dataset_name
                # - source (pairs that look like [step_num, output_name])
                final_output_path = os.path.join(external_outputs_dir, output["dataset_name"])
                source_step = output["source_step"]
                source_dataset_name = output["source_dataset_name"]
                os.link(file_map[source_step][source_dataset_name][1], final_output_path)

        return final_return_code
