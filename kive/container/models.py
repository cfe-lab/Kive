# -*- coding: utf-8 -*-
import errno
import logging
import os
import re
from subprocess import STDOUT, CalledProcessError, check_output
from tempfile import NamedTemporaryFile

from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.db import models, transaction
from django.dispatch import receiver
from django.forms.fields import FileField as FileFormField
from django.urls import reverse

from constants import maxlengths
from metadata.models import AccessControl, empty_removal_plan, remove_helper

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

    class Meta:
        ordering = ['family__name', 'tag']

    def get_absolute_url(self):
        return reverse('container_update', kwargs=dict(pk=self.pk))

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """ Make a manifest of objects to remove when removing this. """
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["Containers"]
        removal_plan["Containers"].add(self)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)


@receiver(models.signals.post_delete, sender=Container)
def delete_container_file(instance, **_kwargs):
    if instance.file:
        try:
            os.remove(instance.file.path)
        except OSError as ex:
            if ex.errno != errno.ENOENT:
                raise
