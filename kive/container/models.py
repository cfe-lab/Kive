# -*- coding: utf-8 -*-
import errno
import logging
import os
import re
from subprocess import STDOUT, CalledProcessError, check_output, check_call
from tempfile import NamedTemporaryFile

from django.conf import settings
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator, MinValueValidator
from django.db import models, transaction
from django.dispatch import receiver
from django.forms.fields import FileField as FileFormField
from django.urls import reverse
from django.utils import timezone

from constants import maxlengths
from librarian.models import Dataset
from metadata.models import AccessControl, empty_removal_plan, remove_helper
from stopwatch.models import Stopwatch

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


class ContainerFileFormField(FileFormField):
    default_error_messages = {
        'invalid_container': "Upload a valid container file."}

    def to_python(self, data):
        """ Checks that the file-upload data contains a valid container. """
        f = super(ContainerFileFormField, self).to_python(data)
        if f is None:
            return None

        # We need to get a file object to validate. We might have a path or we might
        # have to read the data out of memory.
        if hasattr(data, 'temporary_file_path'):
            self.validate_container(data.temporary_file_path())
        else:
            upload_name = getattr(data, 'name', 'container')
            upload_base, upload_ext = os.path.splitext(upload_name)
            with NamedTemporaryFile(prefix=upload_base,
                                    suffix=upload_ext) as f_temp:
                if hasattr(data, 'read'):
                    f_temp.write(data.read())
                else:
                    f_temp.write(data['content'])
                f_temp.flush()
                self.validate_container(f_temp.name)

        if hasattr(f, 'seek') and callable(f.seek):
            f.seek(0)
        return f

    def validate_container(self, filename):
        try:
            check_output(['singularity', 'check', filename], stderr=STDOUT)
        except CalledProcessError as ex:
            logger.warning('Invalid container file:\n%s', ex.output)
            raise ValidationError(self.error_messages['invalid_container'],
                                  code='invalid_container')


class ContainerFileField(models.FileField):
    def formfield(self, **kwargs):
        kwargs.setdefault('form_class', ContainerFileFormField)
        return super(ContainerFileField, self).formfield(**kwargs)


class Container(AccessControl):
    UPLOAD_DIR = "CodeResources"

    family = models.ForeignKey(ContainerFamily, related_name="containers")
    file = ContainerFileField(
        "Container file",
        upload_to=UPLOAD_DIR,
        help_text="Singularity container file")
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

    # Related model gets set later.
    methods = None

    class Meta:
        ordering = ['family__name', '-tag']

    def __str__(self):
        return '{}:{}'.format(self.family.name, self.tag)

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

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)


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
    objects = None  # Filled in later by Django.

    class Meta:
        ordering = ('name',)

    @property
    def display_name(self):
        name = str(self.container)
        if self.name:
            # noinspection PyTypeChecker
            name += ' / ' + self.name
        return name

    def __str__(self):
        return self.display_name

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

    def get_absolute_url(self):
        return reverse('container_app_update', kwargs=dict(pk=self.pk))

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """ Make a manifest of objects to remove when removing this. """
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["ContainerApps"]
        removal_plan["ContainerApps"].add(self)

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
    app = models.ForeignKey(ContainerApp, related_name="runs")
    batch = models.ForeignKey(Batch, related_name="runs", blank=True, null=True)
    name = models.CharField(max_length=maxlengths.MAX_NAME_LENGTH, blank=True)
    description = models.CharField(max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
                                   blank=True)
    state = models.CharField(max_length=1, choices=STATES, default=NEW)
    priority = models.IntegerField(default=0,
                                   help_text='Chooses which slurm queue to use.')
    sandbox_path = models.CharField(
        max_length=maxlengths.MAX_EXTERNAL_PATH_LENGTH,
        blank=True)
    return_code = models.IntegerField(blank=True, null=True)
    stopped_by = models.ForeignKey(User,
                                   help_text="User that stopped this run",
                                   null=True,
                                   blank=True,
                                   related_name="container_runs_stopped")
    is_redacted = models.BooleanField(
        default=False,
        help_text="True if the outputs or logs were redacted for sensitive data")

    class Meta(object):
        ordering = ('-start_time',)

    def __repr__(self):
        return 'ContainerRun(id={!r})'.format(self.pk)

    def get_absolute_url(self):
        return reverse('container_run_detail', kwargs=dict(pk=self.pk))

    def save(self,
             force_insert=False,
             force_update=False,
             using=None,
             update_fields=None):
        if self.pk is None:
            self.start_time = timezone.now()
        super(ContainerRun, self).save(force_insert,
                                       force_update,
                                       using,
                                       update_fields)
        if self.state == self.NEW:
            transaction.on_commit(self.schedule)

    def schedule(self):
        sandbox_root = os.path.join(settings.MEDIA_ROOT, settings.SANDBOX_PATH)
        try:
            os.mkdir(sandbox_root)
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise

        sandbox_prefix = os.path.join(sandbox_root,
                                      self.get_sandbox_prefix())
        slurm_prefix = sandbox_prefix + '_job%J_node%N_'

        check_call(['sbatch',
                    '--output', slurm_prefix + 'stdout.txt',
                    '--error', slurm_prefix + 'stderr.txt',
                    'manage.py', 'runcontainer', str(self.pk)])

    def get_sandbox_prefix(self):
        return 'user{}_run{}_'.format(self.user.username, self.pk)

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """ Make a manifest of objects to remove when removing this. """
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["ContainerRuns"]
        removal_plan["ContainerRuns"].add(self)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)


class ContainerDataset(models.Model):
    run = models.ForeignKey(ContainerRun, related_name="datasets")
    argument = models.ForeignKey(ContainerArgument, related_name="datasets")
    dataset = models.ForeignKey(Dataset, related_name="containers")
    name = models.CharField(
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="Local file name, also used to sort multiple inputs for a "
                  "single argument.",
        blank=True)
    created = models.DateTimeField(
        auto_now_add=True,
        help_text="When this was added to Kive.")


class ContainerLog(models.Model):
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
        help_text="Holds the log text if it's longer than the max length.")

    def get_absolute_url(self):
        return reverse('container_log_detail', kwargs=dict(pk=self.pk))
