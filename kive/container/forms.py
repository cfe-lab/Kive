from django import forms
from django.forms.widgets import TextInput
from django.core.exceptions import ValidationError

from subprocess import check_output, CalledProcessError
import logging
import os
import zipfile
import tarfile
from tempfile import NamedTemporaryFile

from container.models import ContainerFamily, Container, ContainerApp, ContainerRun, Batch
from metadata.forms import PermissionsForm

logger = logging.getLogger(__name__)


class BatchForm(PermissionsForm):
    class Meta(object):
        model = Batch
        fields = ['name', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


class ContainerFamilyForm(PermissionsForm):
    class Meta(object):
        model = ContainerFamily
        fields = ['name', 'git', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


class ContainerForm(PermissionsForm):
    default_error_messages = {
        'invalid_singularity_container': "Upload a valid Singularity container file.",
        'invalid_archive': "Upload a valid archive file.",
        'singularity_cannot_have_parent': "Singularity containers cannot have parents",
        'archive_must_have_parent': "Archive containers must have a valid Singularity container parent"
    }

    class Meta(object):
        model = Container
        fields = ['file', 'file_type', 'parent', 'tag', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))  # FIXME figure out a widget for parent

    def validate_file_proper_type(self, uploaded_file, validator):
        """
        Helper that is used for validating uploaded files are of the correct type.
        :param uploaded_file:
        :param validator:
        :return:
        """
        # We need to get a file object to validate. We might have a path or we might
        # have to read the data out of memory.
        if hasattr(uploaded_file, 'temporary_file_path'):
            validator(uploaded_file.temporary_file_path())
        else:
            upload_name = getattr(uploaded_file, 'name', 'container')  # FIXME what does this do?
            upload_base, upload_ext = os.path.splitext(upload_name)
            with NamedTemporaryFile(prefix=upload_base,
                                    suffix=upload_ext) as f_temp:
                if hasattr(uploaded_file, 'read'):
                    f_temp.write(uploaded_file.read())
                else:
                    f_temp.write(uploaded_file['content'])
                f_temp.flush()
                validator(f_temp.name)

    def validate_container_from_filename(self, filename):
        """
        Check that the given file is a valid Singularity container.
        :param filename:
        :return:
        """
        try:
            check_output(['singularity', 'check', filename], stderr=STDOUT)
        except CalledProcessError as ex:
            logger.warning('Invalid container file:\n%s', ex.output)
            raise ValidationError(self.error_messages['invalid_singularity_container'],
                                  code='invalid_singularity_container')

    def validate_tarfile_from_filename(self, filename):
        """
        Check that the given file is a valid tarfile.
        :param filename:
        :return:
        """
        try:
            with tarfile.open(filename):
                pass
        except tarfile.ReadError:
            raise ValidationError(self.default_error_messages["invalid_archive"],
                                  code="invalid_archive")

    def validate_tarfile(self, uploaded_file):
        """
        Check that the uploaded file is a valid tar file.
        :param uploaded_file:
        :return:
        """
        self.validate_file_proper_type(uploaded_file, self.validate_tarfile_from_filename)

    def clean(self):
        """
        Confirm that the file is of the right type.
        :return:
        """
        clean_data = super(ContainerForm, self).clean()
        if clean_data["file_type"] == Container.SIMG:
            if clean_data["parent"] is not None:
                raise ValidationError(self.default_error_messages["singularity_cannot_have_parent"],
                                      code="singularity_cannot_have_parent")
            self.validate_file_proper_type(clean_data["file"], self.validate_container_from_filename())

        else:
            if clean_data["parent"] is None or not clean_data["parent"].can_be_parent():
                raise ValidationError(self.default_error_messages["archive_must_have_parent"],
                                      code="archive_must_have_parent")

            if clean_data["file_type"] == Container.ZIP:
                try:
                    with zipfile.ZipFile(clean_data["file"]):
                        pass
                except zipfile.BadZipfile:
                    raise ValidationError(self.default_error_messages["invalid_archive"],
                                          code="invalid_archive")

            else:  # this is either a tarfile or a gzipped tar file
                self.validate_file_proper_type(clean_data["file"], self.validate_tarfile_from_filename)


class ContainerUpdateForm(ContainerForm):
    def __init__(self, *args, **kwargs):
        super(ContainerUpdateForm, self).__init__(*args, **kwargs)
        self.fields.pop('file')


class ContainerAppForm(forms.ModelForm):
    inputs = forms.CharField(
        widget=TextInput(attrs=dict(size=50)),
        required=False,
        help_text='A space-separated list of argument names. You can also use '
                  'prefixes and suffixes for different kinds of arguments: '
                  '--optional, multiple*, and --optional_multiple*.')
    outputs = forms.CharField(
        widget=TextInput(attrs=dict(size=50)),
        required=False,
        help_text='A space-separated list of argument names. You can also use '
                  'prefixes and suffixes for different kinds of arguments: '
                  '--optional, folder/, and --optional_folder/.')

    class Meta(object):
        model = ContainerApp
        exclude = ['container']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


class ContainerRunForm(PermissionsForm):
    class Meta(object):
        model = ContainerRun
        fields = ['name', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))
