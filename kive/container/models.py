# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import errno
import hashlib
import json
import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from itertools import count
from pathlib import Path
from subprocess import STDOUT, CalledProcessError, check_output, check_call
import tarfile
from tarfile import TarFile, TarInfo
from tempfile import mkdtemp, mkstemp
import shutil
import io
from io import BytesIO
from zipfile import ZipFile, BadZipfile
from collections import OrderedDict, namedtuple
from operator import itemgetter

from django.conf import settings
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.files import File
from django.core.validators import RegexValidator, MinValueValidator
from django.db import models, transaction
from django.db.models.functions import Now
from django.dispatch import receiver
from django.template.defaultfilters import filesizeformat
from django.urls import reverse
from django.utils import timezone
import django.utils.six as dsix

from constants import maxlengths
from file_access_utils import compute_md5, use_field_file
from metadata.models import AccessControl, empty_removal_plan, remove_helper
from stopwatch.models import Stopwatch
import container.deffile as deffile


logger = logging.getLogger(__name__)

SINGULARITY_COMMAND = 'singularity'

# MANAGE_PY = "manage.py"
# MANAGE_PY_FULLPATH = os.path.join(settings.KIVE_HOME, MANAGE_PY)
MANAGE_PY_FULLPATH = os.path.abspath(os.path.join(__file__, '../../manage.py'))

NUM_RETRY = settings.SLURM_COMMAND_RETRY_NUM
SLEEP_SECS = settings.SLURM_COMMAND_RETRY_SLEEP_SECS


def multi_check_output(cmd_lst, stderr=None, env=None, num_retry=NUM_RETRY):
    """ Perform a check_output command multiples times.
    We use this routine when calling slurm commands to counter time-outs under
    heavy load. For calls to other commands, we use check_output directly.

    This routine should always return a (unicode) string.
    NOTE: Under python3, subprocess.check_output() returns bytes by default, so we
    set universal_newlines=True to guarantee strings.
    NOTE: this routine was taken from the now defunct slurmlib module.
    """
    itry, cmd_retry = 1, True
    out_str = None
    while cmd_retry:
        cmd_retry = False
        try:
            out_str = check_output(cmd_lst,
                                   stderr=stderr,
                                   env=env,
                                   universal_newlines=True)
        except OSError as e:
            # typically happens if the executable cannot execute at
            # all (e.g. not installed)
            # ==> we just pass this error up with extra context
            e.strerror += ': ' + ' '.join(cmd_lst)
            raise
        except CalledProcessError as e:
            # typically happens if the executable did run, but returned an error
            # ==> assume the slurm command timed out, so we retry
            cmd_retry = True
            logger.warning("timeout #%d/%d on command %s (retcode %s)",
                           itry,
                           num_retry,
                           cmd_lst[0],
                           e.returncode)
            if itry < num_retry:
                itry += 1
                time.sleep(SLEEP_SECS)
            else:
                raise
    return out_str


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


class PipelineCompletionStatus(object):
    def __init__(self, pipeline):
        self.has_inputs = False
        self.has_steps = False
        self.has_outputs = False
        self.inputs_not_connected = []
        self.dangling_outputs = []
        self.assess_pipeline_completion(pipeline)

    def add_unfed_input(self, step_num, dataset_name):
        self.inputs_not_connected.append((step_num, dataset_name))

    def add_dangling_output(self, dataset_name):
        self.dangling_outputs.append(dataset_name)

    def is_complete(self):
        return (self.has_inputs
                and self.has_steps
                and self.has_outputs
                and len(self.inputs_not_connected) == 0
                and len(self.dangling_outputs) == 0)

    def assess_pipeline_completion(self, pipeline):
        """
        Check that the specified pipeline is complete, returning a list of things that must still be satisfied.
        :param pipeline:
        :return:
        """
        if len(pipeline["inputs"]) > 0:
            self.has_inputs = True
        if len(pipeline["steps"]) > 0:
            self.has_steps = True
        if len(pipeline["outputs"]) > 0:
            self.has_outputs = True

        # Construct a dataset mapping to check for unfed inputs and dangling outputs.
        usable_inputs = []  # list of dicts
        pipeline_inputs = [x["dataset_name"] for x in pipeline["inputs"]]
        usable_inputs.append(pipeline_inputs)

        for i, step_dict in enumerate(pipeline["steps"], start=1):
            # Check for unfed inputs.
            for input_dict in step_dict["inputs"]:
                if input_dict["source_step"] is None:
                    self.add_unfed_input(i, input_dict["dataset_name"])
            # Add the step outputs to the list of usable inputs.
            usable_inputs.append(step_dict["outputs"])

        # Check for dangling outputs.
        for output_dict in pipeline["outputs"]:
            if output_dict["source_step"] is None:
                self.add_dangling_output(output_dict["dataset_name"])


class ExistingRunsError(Exception):
    def __init__(self, message=None):
        if message is None:
            message = 'Container has runs. Save changes as a new container.'
        super(ExistingRunsError, self).__init__(message)


def get_drivers(archive):
    """
    Return a list of files that can be a driver.

    :param archive: an archive as returned by open_content.
    """
    drivers = []
    for info in archive.infolist():
        if is_driver(archive, info):
            drivers.append(info)
    return drivers


def is_driver(archive, info):
    """
    True if the file in the archive that is specified by info is an admissible driver.
    :param archive:
    :param info:
    :return:
    """
    return archive.read(info).startswith(b"#!")


class Container(AccessControl):
    UPLOAD_DIR = "Containers"

    SIMG = "SIMG"
    ZIP = "ZIP"
    TAR = "TAR"
    SUPPORTED_FILE_TYPES = (
        (SIMG, "Singularity"),
        (ZIP, "Zip"),
        (TAR, "Tar")
    )

    ACCEPTED_FILE_EXTENSIONS = OrderedDict(
        [
            (".simg", SIMG),
            (".zip", ZIP),
            (".tar", TAR)
        ]
    )
    DEFAULT_APP_CONFIG = dict(memory=5000, threads=1)

    EMPTY = "empty"
    INCOMPLETE = "incomplete"
    VALID = "valid"

    accepted_extensions = ACCEPTED_FILE_EXTENSIONS.keys()
    if dsix.PY3:
        accepted_extensions = list(accepted_extensions)
    accepted_extension_str = ", ".join(accepted_extensions[:-1])
    accepted_extension_str += ", or {}".format(accepted_extensions[-1])

    DEFAULT_ERROR_MESSAGES = {
        'invalid_singularity_container': "Upload a valid Singularity container file.",
        'invalid_singularity_deffile': "Upload a valid Singularity container file (problem with deffile).",
        'invalid_archive': "Upload a valid archive file.",
        'singularity_cannot_have_parent': "Singularity containers cannot have parents",
        'archive_must_have_parent': "Archive containers must have a valid Singularity container parent",
        'parent_container_not_singularity': "Parent container must be a Singularity container",
        'bad_extension': "File must have one of the following: {}".format(accepted_extension_str),
        'archive_has_no_drivers': "Archive containers must contain at least one driver file",
        'driver_not_in_archive': 'Step drivers must all be in the archive',
        'inadmissible_driver': 'Step drivers must start with "#!"'
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

    parent = models.ForeignKey(
        "Container",
        related_name="children",
        null=True,
        blank=True,
        help_text='Singularity container that an archive container runs in')

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
    children = None

    @property
    def display_name(self):
        return '{}:{}'.format(self.family.name, self.tag)

    @property
    def file_path(self):
        return os.path.join(settings.MEDIA_ROOT, self.file.name)

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
            check_output([SINGULARITY_COMMAND, 'check', file_path], stderr=STDOUT)
        except CalledProcessError as ex:
            logger.warning('Invalid container file:\n%s', ex.output)
            raise ValidationError(cls.DEFAULT_ERROR_MESSAGES['invalid_singularity_container'],
                                  code='invalid_singularity_container')

    def save(self, *args, **kwargs):
        if not self.md5:
            self.set_md5()
        super(Container, self).save(*args, **kwargs)

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
                with use_field_file(self.file), io.open(fd, mode="w+b") as f:
                    for chunk in self.file.chunks():
                        f.write(chunk)

                Container.validate_singularity_container(file_path)
                os.remove(file_path)

        else:
            if self.parent is None:
                raise ValidationError(self.DEFAULT_ERROR_MESSAGES["archive_must_have_parent"],
                                      code="archive_must_have_parent")
            elif not self.parent.is_singularity():
                raise ValidationError(self.DEFAULT_ERROR_MESSAGES["parent_container_not_singularity"],
                                      code="parent_container_not_singularity")

            try:
                with self.open_content() as a:
                    drivers = get_drivers(a)
                    if len(drivers) == 0:
                        raise ValidationError(self.DEFAULT_ERROR_MESSAGES["archive_has_no_drivers"],
                                              code="archive_has_no_drivers")

                # Check that all of the step drivers are admissible drivers.
                archive_content = self.get_archive_content(False)
                if archive_content is None:
                    return

                with self.open_content() as archive:
                    all_members = archive.infolist()
                    members = [member
                               for member in all_members
                               if not member.name.startswith('kive/pipeline')]
                    members_by_name = {}
                    for member in members:
                        members_by_name[member.name] = member

                    pipeline = archive_content["pipeline"]
                    if pipeline is None:
                        return
                    for step_dict in pipeline["steps"]:
                        driver = step_dict["driver"]
                        if driver not in members_by_name:
                            raise ValidationError(self.DEFAULT_ERROR_MESSAGES["driver_not_in_archive"],
                                                  code="driver_not_in_archive")
                        if not is_driver(archive, members_by_name[driver]):
                            raise ValidationError(self.DEFAULT_ERROR_MESSAGES["inadmissible_driver"],
                                                  code="inadmissible_driver")

            except (BadZipfile, tarfile.ReadError):
                raise ValidationError(self.DEFAULT_ERROR_MESSAGES["invalid_archive"],
                                      code="invalid_archive")

    def set_md5(self):
        """
        Set this instance's md5 attribute.  Note that this does not save the instance.

        This leaves self.file open and seek'd to the 0 position.
        :return:
        """
        if not self.file:
            return
        with use_field_file(self.file):
            self.md5 = compute_md5(self.file)

    def validate_md5(self):
        """
        Compute the MD5 and check that it is as expected.
        :return:
        """
        with self.file:
            current_md5 = compute_md5(self.file)
        if current_md5 != self.md5:
            raise ValueError(
                "Container {} file MD5 has changed (original {}, current {})".format(self, self.md5, current_md5)
            )

    def __str__(self):
        return self.display_name

    def __repr__(self):
        return 'Container(id={})'.format(self.pk)

    def is_singularity(self):
        return self.file_type == self.SIMG

    def extract_archive(self, extraction_path):
        """ Extract this child container to the specified extraction path.

        Raises ContainerNotChild if this is not a child container.

        :param extraction_path: where to extract the contents
        """
        if self.is_singularity():
            raise ContainerNotChild()

        with self.open_content() as archive:
            all_members = archive.infolist()
            members = [member
                       for member in all_members
                       if not member.name.startswith('kive/pipeline')]
            last_member = all_members[-1]
            if last_member.name.startswith('kive/pipeline'):
                members.append(last_member)
            else:
                last_member = None
            archive.extractall(extraction_path, members)
            if last_member is not None:
                old_name = os.path.join(extraction_path, last_member.name)
                new_name = os.path.join(extraction_path, 'kive', 'pipeline.json')
                os.rename(old_name, new_name)

    @contextmanager
    def open_content(self, mode='r'):
        if mode == 'r':
            file_mode = 'rb'
        elif mode == 'a':
            file_mode = 'rb+'
        else:
            raise ValueError('Unsupported mode for archive content: {!r}.'.format(mode))
        with use_field_file(self.file, file_mode):
            if self.file_type == Container.ZIP:
                archive = ZipHandler(self.file, mode)
            elif self.file_type == Container.TAR:
                archive = TarHandler(self.file, mode)
            else:
                raise ValueError(
                    'Cannot open content for a {} container.'.format(
                        self.file_type))
            yield archive
            archive.close()

    def get_content(self, add_default=True):
        """Read the pipeline definitions, aka content, from an archive file (tar or zip)
        or a singularity image file.
        """
        if self.is_singularity():
            return self.get_singularity_content()
        return self.get_archive_content(add_default)

    def get_singularity_content(self):
        """Determine pipeline definitions from a singularity file.
        We need to extract and parse a deffile from the image for this to work.
        If its not a singularity file: raise a ValidationError
        If there is no deffile: do not complain (there are no apps defined)
        If the deffile cannot be parsed: raise a ValidationError
        """
        file_path = self.file_path
        try:
            json_data = check_output([SINGULARITY_COMMAND, 'inspect',
                                      '-d', '-j', file_path], stderr=STDOUT)
        except CalledProcessError:
            logger.warning('Invalid container file', exc_info=True)
            raise ValidationError(self.DEFAULT_ERROR_MESSAGES['invalid_singularity_container'],
                                  code='invalid_singularity_container')
        sing_data = json.loads(json_data.decode('utf-8'))
        def_file_str = sing_data['data']['attributes']['deffile']
        # if the container was not made using a deffile, this will be None.
        # In this case, return an empty applist.
        if def_file_str is None:
            appinfo_lst = []
        else:
            appinfo_lst = deffile.parse_string(def_file_str)
        return dict(applist=appinfo_lst)

    def get_archive_content(self, add_default):
        """Determine the pipeline content from an archive container."""
        with self.open_content() as archive:
            last_entry = archive.infolist()[-1]
            if re.match(r'kive/pipeline\d+\.json', last_entry.name):
                pipeline_json = archive.read(last_entry)
                pipeline = json.loads(pipeline_json.decode('utf-8'))
            elif add_default:
                pipeline = dict(default_config=self.DEFAULT_APP_CONFIG,
                                inputs=[],
                                steps=[],
                                outputs=[])
            else:
                pipeline = None

            file_and_driver_status = [
                (entry.name, is_driver(archive, entry))
                for entry in archive.infolist()
                if not entry.name.startswith('kive/')
            ]
            file_and_driver_status = sorted(file_and_driver_status, key=itemgetter(0))
            content = dict(files=file_and_driver_status,
                           pipeline=pipeline,
                           id=self.pk)
            return content

    def write_archive_content(self, content):
        """Write the contents of an archive (i.e. non singularity) container.
        This method is typically called with a content dict taken from an ajax request.
        Singularity containers are not made this way.
        """
        related_runs = ContainerRun.objects.filter(app__in=self.apps.all())
        if related_runs.exists():
            raise ExistingRunsError()
        pipeline = content['pipeline']
        pipeline_json = json.dumps(pipeline)
        with self.open_content('a') as archive:
            file_names = set(entry.name
                             for entry in archive.infolist()
                             if entry.name.startswith('kive/pipeline'))
            for i in count(1):
                file_name = 'kive/pipeline{}.json'.format(i)
                if file_name not in file_names:
                    archive.write(file_name, pipeline_json)
                    break
        self.set_md5()
        self.create_app_from_content(content)

    def get_pipeline_state(self):
        content = self.get_content(add_default=False)
        if content is None:
            return self.EMPTY
        pipeline = content['pipeline']
        if pipeline is None:
            return self.EMPTY
        if self.pipeline_valid(pipeline):
            return self.VALID
        return self.INCOMPLETE

    def create_app_from_content(self, content=None):
        """Create apps based on the content configuration.
        This method handles archive as well as singularity images.

        In the case of singularity images:
         if applist is None: no changes are made to current apps of a container.
        if applist is []: all apps are deleted.
        """
        content = content or self.get_content()
        error_messages = []
        if content is None:
            logger.warning("failed to obtain content from container")
            return
        if self.is_singularity():
            app_lst = content.get('applist', None)
            if not app_lst:
                error_messages.append(
                    'No definition file found in singularity file.')
            else:
                default_config = self.DEFAULT_APP_CONFIG
                self.apps.all().delete()
                for app_dct in app_lst:
                    appname = app_dct[deffile.AppInfo.KW_APP_NAME]
                    app_errors = app_dct[deffile.AppInfo.KW_ERROR_MESSAGES]
                    if app_errors:
                        summary = 'The {} app was not created: {}'.format(
                            repr(appname) if appname else 'default',
                            ', '.join(app_errors))
                        error_messages.append(summary)
                        continue
                    num_threads = app_dct[deffile.AppInfo.KW_NUM_THREADS] or default_config['threads']
                    memory = app_dct[deffile.AppInfo.KW_MEMORY] or default_config['memory']
                    inpargs, outargs = app_dct[deffile.AppInfo.KW_IO_ARGS]
                    inpargs = inpargs or "input_txt"
                    outargs = outargs or "output_txt"
                    help_str = app_dct[deffile.AppInfo.KW_HELP_STRING] or ""
                    # attach the help string of the default app to the container's description
                    if appname == "" and help_str != "":
                        self.description = help_str if self.description == "" else self.description + "\n" + help_str
                        self.save()
                    newdb_app = self.apps.create(name=appname,
                                                 description=help_str,
                                                 threads=num_threads,
                                                 memory=memory)
                    newdb_app.write_inputs(inpargs)
                    newdb_app.write_outputs(outargs)
        else:
            # archive container
            pipeline = content['pipeline']
            if self.pipeline_valid(pipeline):
                default_config = pipeline.get('default_config',
                                              self.DEFAULT_APP_CONFIG)
                self.apps.all().delete()
                app = self.apps.create(memory=default_config['memory'],
                                       threads=default_config['threads'])
                # noinspection PyTypeChecker
                input_names = ' '.join(entry['dataset_name']
                                       for entry in pipeline['inputs'])
                # noinspection PyTypeChecker
                output_names = ' '.join(entry['dataset_name']
                                        for entry in pipeline['outputs'])
                app.write_inputs(input_names)
                app.write_outputs(output_names)
        return error_messages

    @staticmethod
    def pipeline_valid(pipeline):
        """
        True if the specified pipeline is valid; False otherwise.
        :param pipeline:
        :return:
        """
        # noinspection PyBroadException
        try:
            return PipelineCompletionStatus(pipeline).is_complete()
        except Exception:
            return False

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

        for app in self.apps.all():
            app.build_removal_plan(removal_plan)

        for child in self.children.all():
            child.build_removal_plan(removal_plan)

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


class ZipHandler(object):
    MemberInfo = namedtuple('MemberInfo', 'name original')

    def __init__(self, fileobj=None, mode='r', archive=None):
        if archive is None:
            archive = ZipFile(fileobj, mode, allowZip64=True)
        self.archive = archive

    def close(self):
        self.archive.close()

    def read(self, info):
        with self.archive.open(info.original) as f:
            return f.read()

    def write(self, file_name, content):
        self.archive.writestr(file_name, content)

    def extractall(self, path, members=None):
        if members is None:
            original_members = None
        else:
            original_members = [member.original for member in members]
        self.archive.extractall(path, original_members)

    def infolist(self):
        return [ZipHandler.MemberInfo(info.filename, info)
                for info in self.archive.infolist()]


class TarHandler(ZipHandler):
    def __init__(self, fileobj=None, mode='r', archive=None):
        if archive is None:
            archive = TarFile(fileobj=fileobj, mode=mode)
        super(TarHandler, self).__init__(fileobj, mode, archive)

    def read(self, info):
        f = self.archive.extractfile(info.original)
        try:
            return f.read()
        finally:
            f.close()

    def write(self, file_name, content):
        tarinfo = TarInfo(file_name)
        tarinfo.size = len(content)
        self.archive.addfile(tarinfo, BytesIO(content.encode('utf8')))

    def infolist(self):
        return [ZipHandler.MemberInfo(info.name, info)
                for info in self.archive.getmembers()]


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

    def __str__(self):
        return self.name or 'Batch {}'.format(self.pk)

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
    logs = None  # Filled in later by Django.

    sandbox_size = models.BigIntegerField(
        blank=True,
        null=True,
        help_text="Size of the sandbox in bytes.  If null, this has not been computed yet."
    )

    original_run = models.ForeignKey(
        'ContainerRun',
        help_text="This run is a rerun of the original.",
        null=True,
        blank=True,
        related_name="reruns")
    md5 = models.CharField(
        max_length=64,
        validators=[RegexValidator(
            regex=re.compile("(^[0-9A-Fa-f]{32}$)|(^$)"),
            message="MD5 checksum is not either 32 hex characters or blank")],
        blank=True,
        help_text="Summary of MD5's for inputs, outputs, and containers.")
    is_warned = models.BooleanField(
        default=False,
        help_text="True if a warning was logged because the Slurm job failed.")

    class Meta(object):
        ordering = ('-submit_time',)

    def __str__(self):
        return self.name or 'Container run {}'.format(self.pk)

    def __repr__(self):
        return 'ContainerRun(id={!r})'.format(self.pk)

    def get_absolute_url(self):
        return reverse('container_run_detail', kwargs=dict(pk=self.pk))

    def get_rerun_name(self):
        """ Create a name to use when rerunning this run.

        Appends a (rerun) suffix, if needed.
        """
        rerun_suffix = '(rerun)'
        name = self.name.rstrip()
        if name.endswith(rerun_suffix):
            return name
        if name:
            name += ' '
        name += rerun_suffix
        return name

    @property
    def has_changed(self):
        if self.state != self.COMPLETE or self.original_run is None:
            return
        return self.md5 != self.original_run.md5

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
             update_fields=None,
             schedule=True):
        super(ContainerRun, self).save(force_insert,
                                       force_update,
                                       using,
                                       update_fields)
        if (schedule and
                self.state == self.NEW and
                not self.sandbox_path and
                not self.original_run):
            transaction.on_commit(self.schedule)

    @property
    def full_sandbox_path(self):
        if not self.sandbox_path:
            return ''
        return os.path.join(settings.MEDIA_ROOT, self.sandbox_path)

    def create_sandbox(self, prefix=None):
        sandbox_root = self.SANDBOX_ROOT
        try:
            os.mkdir(sandbox_root)
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise
        if prefix is None:
            prefix = 'user{}_run{}_'.format(self.user.username, self.pk)
        full_sandbox_path = mkdtemp(prefix=prefix, dir=sandbox_root)
        os.mkdir(os.path.join(full_sandbox_path, 'logs'))
        self.sandbox_path = os.path.relpath(full_sandbox_path, settings.MEDIA_ROOT)

    def schedule(self, dependencies=None):
        try:
            dependency_job_ids = []
            if dependencies:
                for source_run_id, source_dependencies in dependencies.items():
                    source_run = ContainerRun.objects.get(id=source_run_id)
                    source_run.schedule(source_dependencies)
                    dependency_job_ids.append(source_run.slurm_job_id)
            self.create_sandbox()
            self.save()

            child_env = dict(os.environ)
            extra_path = settings.SLURM_PATH
            if extra_path is not None:
                old_system_path = child_env['PATH']
                system_path = extra_path + os.pathsep + old_system_path
                child_env['PATH'] = system_path
            child_env['PYTHONPATH'] = os.pathsep.join(sys.path)
            child_env.pop('KIVE_LOG', None)
            output = multi_check_output(self.build_slurm_command(settings.SLURM_QUEUES,
                                                                 dependency_job_ids),
                                        env=child_env)

            self.slurm_job_id = int(output)
            # It's just possible the slurm job has already started modifying the
            # run, so only update one field.
            self.save(update_fields=['slurm_job_id'])
        except Exception:
            self.state = self.FAILED
            self.save(update_fields=['state'])
            raise

    def build_slurm_command(self, slurm_queues=None, dependency_job_ids=None):
        """Build a list of strings representing a slurm command"""
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
        if dependency_job_ids:
            command.append('--dependency=afterok:' + ':'.join(
                str(job_id)
                for job_id in dependency_job_ids))
        command.extend([MANAGE_PY_FULLPATH, 'runcontainer', str(self.pk)])
        return command

    def create_inputs_from_original_run(self):
        """ Create input datasets by copying original run.

        Checks for reruns of the source runs.
        :return: a set of source runs that need to be rerun to recreate the
        inputs. Calling this again after those reruns will finish creating the
        inputs.
        """
        reruns_needed = set()
        if self.original_run:
            filled_argument_ids = self.datasets.values('argument_id')
            unfilled_input_arguments = self.app.arguments.filter(
                type=ContainerArgument.INPUT).exclude(id__in=filled_argument_ids)
            for container_dataset in self.original_run.datasets.filter(
                    argument__in=unfilled_input_arguments):
                rerun_dataset, source_run = container_dataset.find_rerun_dataset()
                if rerun_dataset is None:
                    reruns_needed.add(source_run)
                    continue
                container_dataset.id = None  # Make a copy.
                container_dataset.dataset = rerun_dataset
                container_dataset.run = self
                container_dataset.save()
        return reruns_needed

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
                if getattr(run_dataset.dataset, 'file_source', None) is not None:
                    # Dataset was converted from an old run. Don't remove it.
                    continue
                run_dataset.dataset.build_removal_plan(removal_plan)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    def load_log(self, file_path, log_type):
        # noinspection PyUnresolvedReferences,PyProtectedMember
        short_size = ContainerLog._meta.get_field('short_text').max_length
        file_size = os.lstat(file_path).st_size
        with open(file_path) as f:
            if file_size <= short_size:
                long_text = None
                short_text = f.read(short_size)
            else:
                short_text = ''
                long_text = File(f)
            # We use update_or_create(), because it's possible that a log could
            # be successfully created, then an error occurs, and we need to
            # update it.
            log, _ = self.logs.update_or_create(
                type=log_type,
                defaults=dict(short_text=short_text))
            if long_text is not None:
                upload_name = 'run_{}_{}'.format(
                    self.pk,
                    os.path.basename(file_path))
                log.long_text.save(upload_name, long_text)

    def delete_sandbox(self):
        assert self.sandbox_path
        shutil.rmtree(self.full_sandbox_path)
        self.sandbox_path = ''

    @classmethod
    def find_unneeded(cls):
        """ A queryset of records that could be purged. """
        return cls.objects.filter(sandbox_size__isnull=False).exclude(
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
        output = multi_check_output(['sacct',
                                     '-j', job_id_text,
                                     '-o', 'jobid,end',
                                     '--noheader',
                                     '--parsable2'])
        slurm_date_format = '%Y-%m-%dT%H:%M:%S'
        warn_end_time = datetime.now() - timedelta(minutes=1)
        max_end_time = warn_end_time - timedelta(minutes=14)
        warn_end_time_text = warn_end_time.strftime(slurm_date_format)
        max_end_time_text = max_end_time.strftime(slurm_date_format)
        for line in output.splitlines():
            job_id, end_time = line.split('|')
            if end_time > warn_end_time_text:
                continue
            run = job_runs.get(job_id)
            if run is not None:
                if end_time > max_end_time_text:
                    if not run.is_warned:
                        logger.warning(
                            'Slurm reports that run id %d ended at %s without '
                            'updating Kive. Waiting 15 minutes to allow '
                            'rescheduling.',
                            run.id,
                            end_time)
                        run.is_warned = True
                        run.save(update_fields=['is_warned'])
                else:
                    logger.error(
                        'Slurm reports that run id %d ended at %s without '
                        'updating Kive. Marked as failed.',
                        run.id,
                        end_time)
                    run.state = cls.FAILED
                    run.end_time = Now()
                    run.save()
                    logs_path = Path(run.full_sandbox_path) / 'logs'
                    log_matches = list(logs_path.glob('job*_node*_stderr.txt'))
                    if log_matches:
                        run.load_log(log_matches[0], ContainerLog.STDERR)

    def set_md5(self):
        """ Set this run's md5.  Note that this does not save the run. """
        encoding = 'utf8'
        md5gen = hashlib.md5()
        container = self.app.container
        container_md5 = container.md5.encode(encoding)
        md5gen.update(container_md5)
        parent_container = container.parent
        if parent_container is not None:
            parent_md5 = parent_container.md5.encode(encoding)
            md5gen.update(parent_md5)

        # Use explict sort order, so changes to default don't invalidate MD5's.
        for container_dataset in self.datasets.order_by('argument__type',
                                                        'argument__position',
                                                        'argument__name'):
            dataset = container_dataset.dataset
            dataset_md5 = dataset.MD5_checksum.encode(encoding)
            md5gen.update(dataset_md5)
        self.md5 = md5gen.hexdigest()


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

    def find_rerun_dataset(self):
        """ Find the dataset, or the matching dataset from a rerun.

        :return: (dataset, source_run) If all of the matching datasets have
            been purged, then dataset is None and source_run is a run that
            produced the dataset as an output. Otherwise, source_run is None.
        """
        if self.dataset.has_data():
            return self.dataset, None

        output_container_dataset = self.dataset.containers.get(
            argument__type=ContainerArgument.OUTPUT)
        output_argument = output_container_dataset.argument
        for rerun in output_container_dataset.run.reruns.all():
            rerun_container_dataset = rerun.datasets.get(argument=output_argument)
            dataset, source_run = rerun_container_dataset.find_rerun_dataset()
            if dataset is not None:
                return dataset, None
        return None, output_container_dataset.run


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

    @property
    def size(self):
        """ Check the size of the log, either short or long.

        :return: the size from whichever log is used, or None if the log was
            purged.
        """
        if self.long_text:
            return self.long_text.size
        if self.log_size:
            return None
        # noinspection PyTypeChecker
        return len(self.short_text)

    @property
    def size_display(self):
        log_size = self.size
        if log_size is None:
            return 'missing'
        return filesizeformat(log_size)

    @property
    def preview(self):
        display_limit = 1000
        log_size = self.size
        if log_size is None:
            return '[purged]'
        display = self.read(display_limit)
        if log_size > display_limit:
            display += '[...download to see the remaining {}.]'.format(
                filesizeformat(log_size - display_limit))
        return display

    def read(self, size=None):
        if self.long_text:
            self.long_text.open('r')
            try:
                return self.long_text.read(size or -1)
            finally:
                self.long_text.close()

        return self.short_text[:size]

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
