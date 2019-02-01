# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import errno
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from subprocess import STDOUT, CalledProcessError, check_output, check_call
from tempfile import mkdtemp, mkstemp
import shutil
import zipfile
import tarfile
import io

from django.conf import settings
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator, MinValueValidator
from django.db import models, transaction
from django.db.models.functions import Now
from django.dispatch import receiver
from django.urls import reverse
from django.utils import timezone

from constants import maxlengths
from metadata.models import AccessControl, empty_removal_plan, remove_helper
from stopwatch.models import Stopwatch
import file_access_utils

logger = logging.getLogger(__name__)


class ContainerFamily(AccessControl):
    name = models.CharField(max_length=maxlengths.MAX_NAME_LENGTH)
    description = models.CharField(max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
                                   blank=True)
    git = models.CharField(
        'Git URL',
        help_text='URL of Git repository that containers were built from',
        max_length=2000,
        blank=True)
    containers = None  # Filled in later from child table.

    class Meta(object):
        ordering = ['name']

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        return reverse('container_family_update', kwargs=dict(pk=self.pk))

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """ Make a manifest of objects to remove when removing this. """
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["ContainerFamilies"]
        removal_plan["ContainerFamilies"].add(self)

        for container in self.containers.all():
            container.build_removal_plan(removal_plan)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)


class ContainerNotChild(Exception):
    pass


class ChildNotConfigured(Exception):
    pass


class Container(AccessControl):
    UPLOAD_DIR = "Containers"

    SIMG = "SIMG"
    ZIP = "ZIP"
    TAR = "TAR"
    TGZ = "TGZ"
    SUPPORTED_FILE_TYPES = (
        (SIMG, "Singularity"),
        (ZIP, "Zip"),
        (TAR, "Tar"),
        (TGZ, "Gzipped tar")
    )

    DEFAULT_ERROR_MESSAGES = {
        'invalid_singularity_container': "Upload a valid Singularity container file.",
        'invalid_archive': "Upload a valid archive file.",
        'singularity_cannot_have_parent': "Singularity containers cannot have parents",
        'archive_must_have_parent': "Archive containers must have a valid Singularity container parent",
        'parent_container_not_singularity': "Parent container must be a Singularity container"
    }

    family = models.ForeignKey(ContainerFamily, related_name="containers")

    file = models.FileField(
        "Container file",
        upload_to=UPLOAD_DIR,
        help_text="Singularity or archive container file")

    file_type = models.CharField(
        choices=SUPPORTED_FILE_TYPES,
        default=SIMG,
        max_length=20)

    parent = models.ForeignKey("Container",
                               related_name="children",
                               null=True,
                               blank=True)

    tag = models.CharField('Tag',
                           help_text='Git tag or revision name',
                           max_length=128)
    description = models.CharField('Description',
                                   blank=True,
                                   max_length=maxlengths.MAX_DESCRIPTION_LENGTH)
    md5 = models.CharField(
        max_length=64,
        validators=[RegexValidator(
            regex=re.compile("(^[0-9A-Fa-f]{32}$)|(^$)"),
            message="MD5 checksum is not either 32 hex characters or blank")],
        blank=True,
        help_text="Validates file integrity")
    created = models.DateTimeField(
        auto_now_add=True,
        help_text="When this was added to Kive.")
    file_size = models.BigIntegerField(
        blank=True,
        null=True,
        help_text="Size of the container file in bytes.  If null, this has "
                  "not been computed yet.")

    # Related models get set later.
    methods = None
    apps = None

    @property
    def display_name(self):
        return '{}:{}'.format(self.family.name, self.tag)

    class Meta:
        ordering = ['family__name', '-created']

    @classmethod
    def validate_singularity_container(cls, file_path):
        """
        Confirm that the given file is a Singularity container.
        :param file_path:
        :return:
        """
        try:
            check_output(['singularity', 'check', file_path], stderr=STDOUT)
        except CalledProcessError as ex:
            logger.warning('Invalid container file:\n%s', ex.output)
            raise ValidationError(cls.DEFAULT_ERROR_MESSAGES['invalid_singularity_container'],
                                  code='invalid_singularity_container')

    def clean(self):
        """
        Confirm that the file is of the correct type.
        :return:
        """
        if self.file_type == Container.SIMG:
            if self.parent is not None:
                raise ValidationError(self.DEFAULT_ERROR_MESSAGES["singularity_cannot_have_parent"],
                                      code="singularity_cannot_have_parent")

            # Because it's potentially more efficient to validate a Singularity container before
            # this step, we check for an "already validated" flag.
            if not getattr(self, "singularity_validated", False):
                fd, file_path = mkstemp()
                with io.open(fd, mode="w+b") as f:
                    with self.file:
                        for chunk in self.file.chunks():
                            f.write(chunk)

                Container.validate_singularity_container(file_path)
                os.remove(file_path)

        else:
            if self.parent is None:
                raise ValidationError(self.DEFAULT_ERROR_MESSAGES["archive_must_have_parent"],
                                      code="archive_must_have_parent")
            elif not self.parent.can_be_parent():
                raise ValidationError(self.DEFAULT_ERROR_MESSAGES["parent_container_not_singularity"],
                                      code="parent_container_not_singularity")

            if self.file_type == Container.ZIP:
                try:
                    with self.file:
                        with zipfile.ZipFile(self.file):
                            pass
                except zipfile.BadZipfile:
                    raise ValidationError(self.DEFAULT_ERROR_MESSAGES["invalid_archive"],
                                          code="invalid_archive")

            else:  # this is either a tarfile or a gzipped tar file
                try:
                    with self.file:
                        with tarfile.open(fileobj=self.file, mode="r"):
                            pass
                except tarfile.ReadError:
                    raise ValidationError(self.DEFAULT_ERROR_MESSAGES["invalid_archive"],
                                          code="invalid_archive")

    def __str__(self):
        return self.display_name

    def __repr__(self):
        return 'Container(id={})'.format(self.pk)

    def can_be_parent(self):
        return self.file_type == self.SIMG

    def extract_archive(self, extraction_path):
        """
        Extract this child container to the specified extraction path.

        Raises ContainerNotChild if this is not a child container.

        :param extraction_path:
        :return:
        """
        if self.can_be_parent():
            raise ContainerNotChild()

        if self.file_type == self.ZIP:
            with zipfile.ZipFile(self.file) as z:
                z.extractall(path=extraction_path)

        elif self.file_type in (self.TAR, self.TGZ):
            with tarfile.open(self.file.path, mode="r") as t:
                t.extractall(path=extraction_path)

    def get_file_list(self):
        """
        Retrieve the file list for this child container.

        Raises ContainerNotChild if this is not a child container.
        :return:
        """
        if self.can_be_parent():
            raise ContainerNotChild()

        file_list = []
        if self.file_type == self.ZIP:
            with zipfile.ZipFile(self.file) as z:
                file_info_list = z.infolist()
                for file_info in file_info_list:
                    if not file_info.filename.endswith("/"):  # file_info.is_dir() doesn't exist until Python 3.6
                        file_list.append(file_info.filename)

        elif self.file_type in (self.TAR, self.TGZ):
            with tarfile.open(self.file.path, mode="r") as t:
                file_info_list = t.getmembers()
                for file_info in file_info_list:
                    if file_info.isfile():
                        file_list.append(file_info.name)

        return file_list

    def get_pipeline_json(self):
        """
        Retrieve the pipeline JSON file for this child container.

        Raises ContainerNotChild if this is not a child container.
        Raises ChildNotConfigured if no such JSON is found.
        :return:
        """
        if self.can_be_parent():
            raise ContainerNotChild()

        def check_and_extract(file_list, single_file_extractor):
            base_dir = os.path.commonprefix(file_list)
            # Look for a `pipeline.json` file in that base directory.
            pipeline_json_path = os.path.join(base_dir, "pipeline.json")
            if pipeline_json_path not in file_list:
                raise ChildNotConfigured()
            # The `extractfile` method on a TarFile does not appear to be usable as a context manager,
            # so we do this the old-fashioned way.
            try:
                json = single_file_extractor(pipeline_json_path)
                return json.read()
            finally:
                json.close()

        if self.file_type == self.ZIP:
            with zipfile.ZipFile(self.file) as z:
                return check_and_extract(z.namelist(), lambda x: z.open(x, mode="r"))

        elif self.file_type in (self.TAR, self.TGZ):
            with tarfile.open(self.file.path, mode="r") as t:
                return check_and_extract(t.getnames(), t.extractfile)

    def get_absolute_url(self):
        return reverse('container_update', kwargs=dict(pk=self.pk))

    def get_absolute_path(self):
        return os.path.join(settings.MEDIA_ROOT, self.file.name)

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """ Make a manifest of objects to remove when removing this. """
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["Containers"]
        removal_plan["Containers"].add(self)

        for method in self.methods.all():
            method.build_removal_plan(removal_plan)

        for app in self.apps.all():
            app.build_removal_plan(removal_plan)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    @classmethod
    def scan_file_names(cls):
        """ Yield all file names, relative to MEDIA_ROOT. """
        relative_root = Container.UPLOAD_DIR
        absolute_root = os.path.join(settings.MEDIA_ROOT, relative_root)
        if not os.path.exists(absolute_root):
            return

        for file_name in os.listdir(absolute_root):
            yield os.path.join(relative_root, file_name)

    @classmethod
    def set_file_sizes(cls):
        """ Set all missing file sizes. """
        to_set = cls.objects.filter(
            file__isnull=False,
            file_size__isnull=True).exclude(file='')
        for x in to_set:
            try:
                x.file_size = x.file.size
                x.save()
            except OSError:
                logger.error('Failed to set file size for container id %d.',
                             x.id,
                             exc_info=True)

    @classmethod
    def known_storage_used(cls):
        """ Get the total amount of active storage recorded. """

        return cls.objects.exclude(
            file='').exclude(  # Purged for some reason
            file=None).aggregate(  # No file?
            models.Sum('file_size'))['file_size__sum'] or 0


class ContainerApp(models.Model):
    container = models.ForeignKey(Container, related_name="apps")
    name = models.CharField(max_length=maxlengths.MAX_NAME_LENGTH,
                            help_text="Leave blank for default",
                            blank=True)
    description = models.CharField('Description',
                                   blank=True,
                                   max_length=maxlengths.MAX_DESCRIPTION_LENGTH)
    threads = models.PositiveIntegerField(
        "Number of threads",
        help_text="How many threads does this app use during execution?",
        default=1,
        validators=[MinValueValidator(1)])
    memory = models.PositiveIntegerField(
        "Memory required (MB)",
        help_text="Megabytes of memory Slurm will allocate for this app "
                  "(0 allocates all memory)",
        default=6000)
    arguments = None  # Filled in later from child table.
    runs = None  # Filled in later from child table.
    objects = None  # Filled in later by Django.

    class Meta:
        ordering = ('-container_id', 'name',)

    @property
    def display_name(self):
        name = self.container.display_name
        if self.name:
            # noinspection PyTypeChecker
            name += ' / ' + self.name
        return name

    def __str__(self):
        return self.display_name

    def __repr__(self):
        return 'ContainerApp(id={})'.format(self.pk)

    @property
    def inputs(self):
        return self._format_arguments(ContainerArgument.INPUT)

    @property
    def outputs(self):
        return self._format_arguments(ContainerArgument.OUTPUT)

    def _format_arguments(self, argument_type):
        arguments = self.arguments.filter(type=argument_type)
        optionals = [argument
                     for argument in arguments
                     if argument.position is None]
        positionals = [argument
                       for argument in arguments
                       if argument.position is not None]
        terms = [argument.formatted for argument in optionals]
        if (argument_type == ContainerArgument.INPUT and
                any(argument.allow_multiple for argument in optionals)):
            terms.append('--')
        terms.extend(argument.formatted for argument in positionals)
        return ' '.join(terms)

    def write_inputs(self, formatted):
        self._write_arguments(ContainerArgument.INPUT, formatted)

    def write_outputs(self, formatted):
        self._write_arguments(ContainerArgument.OUTPUT, formatted)

    def _write_arguments(self, argument_type, formatted):
        self.arguments.filter(type=argument_type).delete()
        expected_multiples = {ContainerArgument.INPUT: '*',
                              ContainerArgument.OUTPUT: '/'}
        for position, term in enumerate(formatted.split(), 1):
            if term == '--':
                continue
            match = re.match(r'(--)?(\w+)([*/])?$', term)
            if match is None:
                raise ValueError('Invalid argument name: {}'.format(term))
            if match.group(1):
                position = None
            if not match.group(3):
                allow_multiple = False
            elif match.group(3) == expected_multiples[argument_type]:
                allow_multiple = True
            else:
                raise ValueError('Invalid argument name: {}'.format(term))
            self.arguments.create(name=match.group(2),
                                  position=position,
                                  allow_multiple=allow_multiple,
                                  type=argument_type)

    def can_be_accessed(self, user):
        return self.container.can_be_accessed(user)

    def get_absolute_url(self):
        return reverse('container_app_update', kwargs=dict(pk=self.pk))

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """ Make a manifest of objects to remove when removing this. """
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["ContainerApps"]
        removal_plan["ContainerApps"].add(self)

        for run in self.runs.all():
            if run not in removal_plan['ContainerRuns']:
                run.build_removal_plan(removal_plan)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)


class ContainerArgument(models.Model):
    INPUT = 'I'
    OUTPUT = 'O'
    TYPES = ((INPUT, 'Input'),
             (OUTPUT, 'Output'))

    app = models.ForeignKey(ContainerApp, related_name="arguments")
    name = models.CharField(max_length=maxlengths.MAX_NAME_LENGTH)
    position = models.IntegerField(
        null=True,
        blank=True,
        help_text="Position in the arguments (gaps and duplicates are allowed). "
                  "Leave position blank to pass as an option with --name.")
    type = models.CharField(max_length=1, choices=TYPES)
    allow_multiple = models.BooleanField(
        default=False,
        help_text="True for optional inputs that accept multiple datasets and "
                  "outputs that just collect all files written to a directory")

    objects = None  # Filled in later by Django.

    class Meta(object):
        ordering = ('app_id', 'type', 'position', 'name')

    def __repr__(self):
        return 'ContainerArgument(name={!r})'.format(self.name)

    @property
    def formatted(self):
        text = self.name
        if self.position is None:
            # noinspection PyTypeChecker
            text = '--' + text
        if self.allow_multiple:
            text += '*' if self.type == ContainerArgument.INPUT else '/'
        return text


@receiver(models.signals.post_delete, sender=Container)
def delete_container_file(instance, **_kwargs):
    if instance.file:
        try:
            os.remove(instance.file.path)
        except OSError as ex:
            if ex.errno != errno.ENOENT:
                raise


class Batch(AccessControl):
    name = models.CharField(
        "Batch Name",
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text='Name of this batch of container runs',
        blank=True)
    description = models.TextField(
        max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
        blank=True)

    runs = None  # Filled in later by Django.

    class Meta(object):
        ordering = ('-id',)

    @property
    def absolute_url(self):
        return reverse('batch_update', kwargs=dict(pk=self.pk))

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """ Make a manifest of objects to remove when removing this. """
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["Batches"]
        removal_plan["Batches"].add(self)

        for run in self.runs.all():
            run.build_removal_plan(removal_plan)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)


class SandboxMissingException(Exception):
    pass


class ContainerRun(Stopwatch, AccessControl):
    NEW = 'N'
    LOADING = 'L'
    RUNNING = 'R'
    SAVING = 'S'
    COMPLETE = 'C'
    FAILED = 'F'
    CANCELLED = 'X'
    STATES = ((NEW, 'New'),
              (LOADING, 'Loading'),
              (RUNNING, 'Running'),
              (SAVING, 'Saving'),
              (COMPLETE, 'Complete'),
              (FAILED, 'Failed'),
              (CANCELLED, 'Cancelled'))

    ACTIVE_STATES = [
        NEW,
        LOADING,
        RUNNING,
        SAVING
    ]
    SANDBOX_ROOT = os.path.join(settings.MEDIA_ROOT, 'ContainerRuns')

    app = models.ForeignKey(ContainerApp, related_name="runs")
    batch = models.ForeignKey(Batch, related_name="runs", blank=True, null=True)
    name = models.CharField(max_length=maxlengths.MAX_NAME_LENGTH, blank=True)
    description = models.CharField(max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
                                   blank=True)
    state = models.CharField(max_length=1, choices=STATES, default=NEW)
    submit_time = models.DateTimeField(
        auto_now_add=True,
        help_text='When this job was put in the queue.')
    priority = models.IntegerField(default=0,
                                   help_text='Chooses which slurm queue to use.')
    sandbox_path = models.CharField(
        max_length=maxlengths.MAX_EXTERNAL_PATH_LENGTH,
        blank=True)  # type: str
    slurm_job_id = models.IntegerField(blank=True, null=True)
    return_code = models.IntegerField(blank=True, null=True)
    stopped_by = models.ForeignKey(User,
                                   help_text="User that stopped this run",
                                   null=True,
                                   blank=True,
                                   related_name="container_runs_stopped")
    is_redacted = models.BooleanField(
        default=False,
        help_text="True if the outputs or logs were redacted for sensitive data")

    datasets = None  # Filled in later by Django.

    sandbox_size = models.BigIntegerField(
        blank=True,
        null=True,
        help_text="Size of the sandbox in bytes.  If null, this has not been computed yet."
    )

    sandbox_purged = models.BooleanField(
        default=False,
        help_text="True if the sandbox has already been purged, False otherwise."
    )

    class Meta(object):
        ordering = ('-submit_time',)

    def __repr__(self):
        return 'ContainerRun(id={!r})'.format(self.pk)

    def get_absolute_url(self):
        return reverse('container_run_detail', kwargs=dict(pk=self.pk))

    def get_access_limits(self, access_limits=None):
        if access_limits is None:
            access_limits = []
        access_limits.append(self.app.container)
        input_entries = self.datasets.filter(
            argument__type=ContainerArgument.INPUT).prefetch_related('dataset')
        access_limits.extend(entry.dataset for entry in input_entries)
        return access_limits

    def save(self,
             force_insert=False,
             force_update=False,
             using=None,
             update_fields=None):
        super(ContainerRun, self).save(force_insert,
                                       force_update,
                                       using,
                                       update_fields)
        if self.state == self.NEW and not self.sandbox_path:
            transaction.on_commit(self.schedule)

    @property
    def full_sandbox_path(self):
        if not self.sandbox_path:
            return ''
        return os.path.join(settings.MEDIA_ROOT, self.sandbox_path)

    def create_sandbox(self):
        sandbox_root = self.SANDBOX_ROOT
        try:
            os.mkdir(sandbox_root)
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise
        prefix = 'user{}_run{}_'.format(self.user.username, self.pk)
        full_sandbox_path = mkdtemp(prefix=prefix, dir=sandbox_root)
        os.mkdir(os.path.join(full_sandbox_path, 'logs'))
        self.sandbox_path = os.path.relpath(full_sandbox_path, settings.MEDIA_ROOT)

    def schedule(self):
        self.create_sandbox()
        self.save()

        child_env = dict(os.environ)
        child_env['PYTHONPATH'] = os.pathsep.join(sys.path)
        child_env.pop('KIVE_LOG', None)
        output = check_output(self.build_slurm_command(settings.SLURM_QUEUES),
                              env=child_env)

        self.slurm_job_id = int(output)
        # It's just possible the slurm job has already started modifying the
        # run, so only update one field.
        self.save(update_fields=['slurm_job_id'])

    def build_slurm_command(self, slurm_queues=None):
        if not self.sandbox_path:
            raise RuntimeError(
                'Container run needs a sandbox before calling Slurm.')
        slurm_prefix = os.path.join(settings.MEDIA_ROOT,
                                    self.sandbox_path,
                                    'logs',
                                    'job%J_node%N_')
        job_name = 'r{} {}'.format(self.pk,
                                   self.app.name or
                                   self.app.container.family.name)
        command = ['sbatch',
                   '-J', job_name,
                   '--parsable',
                   '--output', slurm_prefix + 'stdout.txt',
                   '--error', slurm_prefix + 'stderr.txt',
                   '-c', str(self.app.threads),
                   '--mem', str(self.app.memory)]
        if slurm_queues is not None:
            kive_name, slurm_name = slurm_queues[self.priority]
            command.extend(['-p', slurm_name])
        manage_path = os.path.abspath(os.path.join(__file__,
                                                   '../../manage.py'))
        command.extend([manage_path, 'runcontainer', str(self.pk)])
        return command

    def get_sandbox_prefix(self):
        return 'user{}_run{}_'.format(self.user.username, self.pk)

    def request_stop(self, user):
        end_time = timezone.now()
        rows_updated = ContainerRun.objects.filter(
            pk=self.pk,
            state=ContainerRun.NEW).update(state=ContainerRun.CANCELLED,
                                           stopped_by=user,
                                           end_time=end_time)
        if rows_updated == 0:
            # Run has already started. Must call scancel.
            check_call(['scancel', '-f', str(self.slurm_job_id)])
            self.state = ContainerRun.CANCELLED
            self.stopped_by = user
            self.end_time = end_time
            self.save()

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """ Make a manifest of objects to remove when removing this. """
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["ContainerRuns"]
        if self.state not in (ContainerRun.COMPLETE,
                              ContainerRun.FAILED,
                              ContainerRun.CANCELLED):
            raise ValueError(
                'ContainerRun id {} is still active.'.format(self.pk))
        removal_plan["ContainerRuns"].add(self)

        for run_dataset in self.datasets.all():
            if run_dataset.argument.type == ContainerArgument.OUTPUT:
                run_dataset.dataset.build_removal_plan(removal_plan)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    def calculate_sandbox_size(self):
        """
        Compute the size of this ContainerRun's sandbox and Slurm logs.
        :return int: size
        """
        assert not self.sandbox_purged
        size_accumulator = 0
        sandbox_files = (os.path.join(root, file_name)
                         for root, _, files in os.walk(self.full_sandbox_path)
                         for file_name in files)
        for file_path in sandbox_files:
            size_accumulator += os.path.getsize(file_path)
        return size_accumulator  # we don't set self.sandbox_size here, we do that explicitly elsewhere.

    @classmethod
    def set_sandbox_sizes(cls):
        runs_to_size = cls.objects.filter(
            end_time__isnull=False,
            sandbox_size__isnull=True,
            sandbox_purged=False).exclude(sandbox_path='')
        for run in runs_to_size:
            run.sandbox_size = run.calculate_sandbox_size()
            run.save()

    @classmethod
    def known_storage_used(cls):
        """ Get the total amount of active storage recorded. """

        return cls.objects.filter(
            sandbox_purged=False).aggregate(
            models.Sum('sandbox_size'))['sandbox_size__sum'] or 0

    def delete_sandbox(self):
        """
        Delete the sandbox.

        Note that this does *not* set self.purged to True.
        :return:
        """
        assert not self.sandbox_purged
        shutil.rmtree(self.full_sandbox_path)

    @classmethod
    def find_unneeded(cls):
        """ A queryset of records that could be purged. """
        return cls.objects.filter(sandbox_purged=False,
                                  sandbox_size__isnull=False).exclude(
            sandbox_path='')

    @classmethod
    def scan_file_names(cls):
        """ Yield all file names, relative to MEDIA_ROOT. """
        relative_root = os.path.relpath(ContainerRun.SANDBOX_ROOT,
                                        settings.MEDIA_ROOT)
        if not os.path.exists(ContainerRun.SANDBOX_ROOT):
            return

        for file_name in os.listdir(ContainerRun.SANDBOX_ROOT):
            yield os.path.join(relative_root, file_name)

    @classmethod
    def check_slurm_state(cls, pk=None):
        """ Check active runs to make sure their Slurm jobs haven't died.

        :param pk: a run id to check, or None if all active runs should be
            checked.
        """
        runs = cls.objects.filter(state__in=cls.ACTIVE_STATES).only(
            'state',
            'end_time',
            'slurm_job_id')
        if pk is not None:
            runs = runs.filter(pk=pk)
        job_runs = {str(run.slurm_job_id): run
                    for run in runs
                    if run.slurm_job_id is not None}
        if not job_runs:
            # No jobs to check.
            return
        job_id_text = ','.join(job_runs)
        output = check_output(['sacct',
                               '-j', job_id_text,
                               '-o', 'jobid,end',
                               '--noheader',
                               '--parsable2'])
        max_end_time = datetime.now() - timedelta(minutes=1)
        max_end_time_text = max_end_time.strftime('%Y-%m-%dT%H:%M:%S')
        for line in output.splitlines():
            job_id, end_time = line.split('|')
            if end_time > max_end_time_text:
                continue
            run = job_runs.get(job_id)
            if run is not None:
                run.state = cls.FAILED
                run.end_time = Now()
                run.save()


class ContainerDataset(models.Model):
    run = models.ForeignKey(ContainerRun, related_name="datasets")
    argument = models.ForeignKey(ContainerArgument, related_name="datasets")
    dataset = models.ForeignKey("librarian.Dataset", related_name="containers")
    name = models.CharField(
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="Local file name, also used to sort multiple inputs for a "
                  "single argument.",
        blank=True)
    created = models.DateTimeField(
        auto_now_add=True,
        help_text="When this was added to Kive.")

    objects = None  # Filled in later by Django.

    class Meta(object):
        ordering = ('run',
                    'argument__type',
                    'argument__position',
                    'argument__name')


class ContainerLog(models.Model):
    UPLOAD_DIR = 'ContainerLogs'
    STDOUT = 'O'
    STDERR = 'E'
    TYPES = ((STDOUT, 'stdout'),
             (STDERR, 'stderr'))
    type = models.CharField(max_length=1, choices=TYPES)
    run = models.ForeignKey(ContainerRun, related_name="logs")
    short_text = models.CharField(
        max_length=2000,
        blank=True,
        help_text="Holds the log text if it's shorter than the max length.")
    long_text = models.FileField(
        upload_to=UPLOAD_DIR,
        help_text="Holds the log text if it's longer than the max length.")
    log_size = models.BigIntegerField(
        blank=True,
        null=True,
        help_text="Size of the log file in bytes.  If null, this has not been computed yet, or the log is short"
                  "and not stored in a file.")

    objects = None  # Filled in later by Django.

    def get_absolute_url(self):
        return reverse('container_log_detail', kwargs=dict(pk=self.pk))

    def read(self, size=None):
        if self.long_text:
            self.long_text.open('r')
            try:
                return self.long_text.read(size or -1)
            finally:
                self.long_text.close()

        return self.short_text[:size]

    @classmethod
    def total_storage_used(cls):
        """
        Return the number of bytes used by all ContainerLogs.
        :return:
        """
        return file_access_utils.total_storage_used(os.path.join(settings.MEDIA_ROOT, cls.UPLOAD_DIR))

    @classmethod
    def set_log_sizes(cls):
        """
        Scan through all logs that do not have their log sizes set and set them.
        :return:
        """
        logs_to_set = cls.objects.filter(
            long_text__isnull=False,
            log_size__isnull=True).exclude(long_text='')
        for log in logs_to_set:
            with transaction.atomic():
                try:
                    log.log_size = log.long_text.size
                    log.save()
                except ValueError:
                    # This has somehow disappeared in the interim, so pass.
                    pass

    @classmethod
    def known_storage_used(cls):
        """ Get the total amount of active storage recorded. """
        return cls.objects.exclude(
            long_text='').exclude(  # Already purged.
            long_text=None).aggregate(  # Short text.
            models.Sum('log_size'))['log_size__sum'] or 0

    @staticmethod
    def _currently_used_by_container():
        """
        A generator that produces the ContainerLogs being used by all active ContainerRuns.
        :return:
        """
        active_runs = ContainerRun.objects.filter(state__in=ContainerRun.ACTIVE_STATES)
        for run in active_runs:
            for cr in run.logs.all():
                yield cr

    def currently_being_used(self):
        """
        Returns True if this is currently in use by an active ContainerRun; False otherwise.
        :return:
        """
        return self.run.state in ContainerRun.ACTIVE_STATES

    @classmethod
    def find_unneeded(cls):
        """ A queryset of records that could be purged. """
        return cls.objects.exclude(
            long_text=None).exclude(  # short log
            long_text='').exclude(  # purged log
            log_size=None)  # new log

    @classmethod
    def scan_file_names(cls):
        """ Yield all file names, relative to MEDIA_ROOT. """
        relative_root = ContainerLog.UPLOAD_DIR
        absolute_root = os.path.join(settings.MEDIA_ROOT, relative_root)
        if not os.path.exists(absolute_root):
            return

        for file_name in os.listdir(absolute_root):
            yield os.path.join(relative_root, file_name)

    @classmethod
    def purge_registered_logs(cls, bytes_to_purge, date_cutoff=None):
        """
        Purge ContainerLogs in chronological order until the target number of bytes is achieved.

        If date_cutoff is specified, retain files newer than this.

        :param bytes_to_purge:
        :param date_cutoff: a datetime object
        :return:
        """
        # Exclude Datasets that are currently in use or have no file (i.e. had short logs or their log was already
        # purged).
        expendable_logs = cls.objects.exclude(
            run__state__in=ContainerRun.ACTIVE_STATES,
            long_text__isnull=True
        )
        if date_cutoff is not None:
            expendable_logs = expendable_logs.exclude(run__end_time__gte=date_cutoff)

        return file_access_utils.purge_registered_files(expendable_logs, "long_text", bytes_to_purge)

    @classmethod
    def purge_unregistered_logs(cls, bytes_to_purge=None, date_cutoff=None):
        """
        Clean up files in the ContainerLog folder that do not belong to any known ContainerLogs.

        Files are removed in order from oldest to newest.  If date_cutoff is specified then
        anything newer than it is not deleted.

        :param bytes_to_purge: a target number of bytes to remove.  If this is None, just remove everything.
        :param date_cutoff: a datetime object.
        :return:
        """
        return file_access_utils.purge_unregistered_files(
            os.path.join(settings.MEDIA_ROOT, cls.UPLOAD_DIR),
            cls,
            "long_text",
            bytes_to_purge=bytes_to_purge,
            date_cutoff=date_cutoff
        )
