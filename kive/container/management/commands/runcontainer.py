import errno
import json
import logging
import os
import pathlib
import shutil
from subprocess import call
import sys
from traceback import format_exception_only
import typing

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from container.models import (
    ContainerRun, ContainerArgument, ContainerArgumentType,
    ContainerLog, ContainerDataset,
)
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
        # noinspection PyBroadException
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
            self.save_exception(run)
            logger.error('Running container failed.', exc_info=True)
            exit(1)

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

        reruns_needed = run.create_inputs_from_original_run()
        if reruns_needed:
            raise RuntimeError('Inputs missing from reruns.')
        input_path = os.path.join(run.full_sandbox_path, 'input')
        os.mkdir(input_path)
        for dataset in run.datasets.all():
            if dataset.argument.argtype in (
                    ContainerArgumentType.OPTIONAL_MULTIPLE_INPUT,
                    ContainerArgumentType.OPTIONAL_INPUT):
                unique_filename = dataset.dataset.unique_filename()
                target_path = os.path.join(input_path, unique_filename)
                if os.path.exists(target_path):
                    raise RuntimeError(
                        "Supposedly unique output file already exists: {}".
                        format(unique_filename))
            else:
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

        for input_cd in run.datasets.filter(argument__type=ContainerArgument.INPUT):
            input_dataset = input_cd.dataset
            current_md5 = input_dataset.compute_md5()
            if current_md5 != input_dataset.MD5_checksum:
                raise ValueError(
                    "Dataset with pk={} has an inconsistent checksum (original {}; current {})".format(
                        input_dataset.pk,
                        input_dataset.MD5_checksum,
                        current_md5
                    )
                )

        container_to_run = run.app.container
        container_to_run.validate_md5()
        if not container_to_run.is_singularity():
            container_to_run.parent.validate_md5()

        with open(stdout_path, 'w') as stdout, open(stderr_path, 'w') as stderr:
            if run.app.container.is_singularity():
                # This is a Singularity container.
                command = self.build_command(run)
                command_path = os.path.join(logs_path, 'command.txt')
                with open(command_path, 'w') as f:
                    f.write(' '.join(command) + '\n')
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

    @classmethod
    def build_command(cls, run):
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
        datasets = list(run.datasets.all())
        # Add optional input arguments specified with ContainerDataset
        optional_arguments = [
            d for d in datasets
            if d.argument.argtype in ContainerArgument.KEYWORD_ARG_TYPES
        ]
        if optional_arguments:
            command.extend(cls._format_kw_args(optional_arguments))
            command.append("--")
        # Add fixed arguments as specified by the ContainerApp
        fixed_arguments = [
            arg for arg in run.app.arguments.all()
            if arg.argtype in ContainerArgument.FIXED_ARG_TYPES
        ]
        command.extend(cls._format_fixed_arg(arg) for arg in fixed_arguments)
        return command

    @staticmethod
    def _format_kw_args(
            containerdatasets: typing.List[ContainerDataset]) -> typing.Iterable[str]:
        def sort_by_position(
            cds: typing.Iterable[ContainerDataset]
        ) -> typing.List[ContainerDataset]:
            return sorted(
                cds,
                key=lambda d: d.multi_position or 0)

        grouped = {
            arg: sort_by_position(d for d in containerdatasets if d.argument == arg)
            for arg in set(d.argument for d in containerdatasets)
        }
        for arg, argcontainerdatasets in grouped.items():
            if arg.type == ContainerArgument.INPUT:
                datasetfolder = "/mnt/input"
            else:
                datasetfolder = "/mnt/output"
            yield "--{}".format(arg.name)
            yield from (os.path.join(datasetfolder, containerdataset.dataset.name)
                        for containerdataset in argcontainerdatasets)

    @staticmethod
    def _format_fixed_arg(arg: ContainerArgument) -> str:
        if arg.type == ContainerArgument.INPUT:
            datasetfolder = "/mnt/input"
        else:
            datasetfolder = "/mnt/output"
        return os.path.join(datasetfolder, arg.name)

    @staticmethod
    def build_dataset_name(run, argument_name: str):
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
            if argument.argtype == ContainerArgumentType.FIXED_OUTPUT:
                self._save_output_argument(run, argument, output_path, upload_path)
            elif argument.argtype == ContainerArgumentType.FIXED_DIRECTORY_OUTPUT:
                self._save_output_directory_argument(run, argument, output_path, upload_path)
            else:
                raise RuntimeError(f"Invalid output argument type in {run}: {argument.argtype}")
        logs_path = os.path.join(run.full_sandbox_path, 'logs')
        for file_name, log_type in (('stdout.txt', ContainerLog.STDOUT),
                                    ('stderr.txt', ContainerLog.STDERR)):
            run.load_log(os.path.join(logs_path, file_name), log_type)

        run.set_md5()
        run.state = (ContainerRun.COMPLETE
                     if run.return_code == 0
                     else ContainerRun.FAILED)
        run.end_time = timezone.now()

    def _save_output_argument(self, run: ContainerRun, argument: ContainerArgument, output_path: str, upload_path: str):
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

    @staticmethod
    def _build_directory_dataset_name(runid: int, output_path: pathlib.Path, file_path: pathlib.Path) -> str:
        # Exclude generic directories (e.g. /var/kive/media_root/...)from the dataset name
        directories = file_path.parent.parts[len(output_path.parts):]  # Drop 'output_path'
        directories_part = "_".join(directories)
        suffixes = ''.join(file_path.suffixes)
        filename = file_path.name[:-len(suffixes)] if suffixes else file_path.name
        filename_part = "{name}_{runid}{suffixes}".format(name=filename,
                                                          runid=runid,
                                                          suffixes=suffixes)
        return "_".join([directories_part, filename_part])

    def _save_output_directory_argument(self, run: ContainerRun,
                                        argument: ContainerArgument,
                                        output_path: str,
                                        upload_path: str) -> None:
        output_path = pathlib.Path(output_path)
        dirarg_path = output_path / argument.name
        for dirpath, _, filenames in os.walk(dirarg_path):
            dirpath = pathlib.Path(dirpath)
            for filename in filenames:
                datafile_path: pathlib.Path = dirpath / filename
                dataset_name = self._build_directory_dataset_name(
                    run.id, output_path.absolute(), datafile_path.absolute())
                destination_path = os.path.join(upload_path, dataset_name)
                try:
                    os.rename(datafile_path, destination_path)
                    dataset = Dataset.create_dataset(destination_path,
                                                     name=dataset_name,
                                                     user=run.user)
                    dataset.copy_permissions(run)
                    run.datasets.create(dataset=dataset, argument=argument)
                except (OSError, IOError) as ex:
                    if ex.errno != errno.ENOENT:
                        raise

    def save_exception(self, run):
        log_path = os.path.join(run.full_sandbox_path, 'logs', 'stderr.txt')
        with open(log_path, 'w') as f:
            f.write('========\nInternal Kive Error\n========\n')
            exc_type, exc_value, exc_tb = sys.exc_info()
            f.write(''.join(format_exception_only(exc_type, exc_value)))
        run.load_log(log_path, ContainerLog.STDERR)

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
        # Each dictionary maps dataset_name -|-> external path, and the step index is their
        # position in the list.
        inputs_map = {}
        for input_dict in instructions["inputs"]:
            # This dictionary has a field called "dataset_name".
            inputs_map[input_dict["dataset_name"]] = os.path.join(external_inputs_dir, input_dict["dataset_name"])
        file_map = [inputs_map]

        final_return_code = 0
        log_path = os.path.dirname(standard_out.name)
        for idx, step in enumerate(instructions["steps"], 1):
            step_header = "========\nProcessing step {}: {}\n========\n".format(
                idx,
                step["driver"])

            external_step_input_dir = os.path.join(run.full_sandbox_path, "step{}".format(idx), "input")
            external_step_output_dir = os.path.join(run.full_sandbox_path, "step{}".format(idx), "output")
            os.makedirs(external_step_input_dir)
            os.makedirs(external_step_output_dir)

            external_step_bin_dir = os.path.join(run.full_sandbox_path, "step{}".format(idx), "bin")
            dependency_filter = DependencyFilter(extracted_archive_dir, step)
            shutil.copytree(extracted_archive_dir,
                            external_step_bin_dir,
                            symlinks=True,
                            ignore=dependency_filter.ignore)

            # Each step is a dictionary with fields:
            # - driver (the executable)
            # - inputs (a list of (step_num, dataset_name) pairs)
            # - outputs (a list of dataset_names)
            executable = os.path.join(internal_binary_dir, step["driver"])
            driver_external_path = os.path.join(external_step_bin_dir,
                                                step["driver"])
            os.chmod(driver_external_path, 0o777)
            input_paths = []
            for input_dict in step["inputs"]:
                source_step = input_dict["source_step"]
                source_dataset_name = input_dict["source_dataset_name"]
                step_outputs = file_map[source_step]
                external_path = step_outputs[source_dataset_name]
                os.link(external_path, os.path.join(external_step_input_dir, input_dict["dataset_name"]))
                input_paths.append(os.path.join(internal_inputs_dir, input_dict["dataset_name"]))
            outputs_map = {}
            output_paths = []
            for dataset_name in step["outputs"]:
                file_name = self.build_dataset_name(
                    run,
                    "step{}_{}".format(idx, dataset_name))
                output_paths.append(os.path.join(internal_outputs_dir, file_name))
                outputs_map[dataset_name] = os.path.join(external_step_output_dir, file_name)
            file_map.append(outputs_map)

            execution_args = [
                "singularity",
                "exec",
                "--contain",
                "-B",
                external_step_bin_dir + ':' + internal_binary_dir,
                "-B",
                external_step_input_dir + ':' + internal_inputs_dir,
                "-B",
                external_step_output_dir + ':' + internal_outputs_dir,
                "--pwd",
                internal_working_dir,
                run.app.container.parent.file.path,
                executable
            ]
            all_args = [str(arg)
                        for arg in execution_args + input_paths + output_paths]
            child_environment = {'LANG': 'en_CA.UTF-8',
                                 'PATH': os.environ['PATH']}
            command_path = os.path.join(log_path, 'step_{}_command.txt'.format(idx))
            with open(command_path, 'w') as f:
                f.write(' '.join(all_args))
            step_stdout_path = os.path.join(log_path, 'step_{}_stdout.txt'.format(idx))
            step_stderr_path = os.path.join(log_path, 'step_{}_stderr.txt'.format(idx))
            with open(step_stdout_path, 'w') as step_stdout, \
                    open(step_stderr_path, 'w') as step_stderr:
                step_return_code = call(all_args,
                                        stdout=step_stdout,
                                        stderr=step_stderr,
                                        env=child_environment)
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
                os.link(file_map[source_step][source_dataset_name], final_output_path)

        return final_return_code


class DependencyFilter:
    def __init__(self, archive_directory, step):
        self.archive_directory = archive_directory
        # The dependencies list is only used when converting old pipelines.
        # New archive containers copy all bin files for every step, so the
        # developer needs to fix any incompatibilities between steps.
        dependency_list = step.get('dependencies')
        if dependency_list is None:
            self.to_copy = None
        else:
            self.to_copy = {os.path.normpath(dependency)
                            for dependency in dependency_list}
            self.to_copy.add(os.path.normpath(step['driver']))

    def ignore(self, directory, entries):
        to_ignore = set()
        if self.to_copy is None:
            # Copy everything, ignore nothing.
            return to_ignore
        rel_directory = os.path.relpath(directory, self.archive_directory)
        for entry in entries:
            abs_entry = os.path.join(directory, entry)
            if os.path.isdir(abs_entry):
                continue
            rel_entry = os.path.join(rel_directory, entry)
            rel_entry = os.path.normpath(rel_entry)
            if rel_entry not in self.to_copy:
                to_ignore.add(entry)
        return to_ignore
