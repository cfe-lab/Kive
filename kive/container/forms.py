from django import forms
from django.forms.widgets import TextInput
from django.core.exceptions import ValidationError

import os
import logging
from tempfile import NamedTemporaryFile

from container.models import ContainerFamily, Container, ContainerApp, ContainerRun, Batch
from metadata.forms import PermissionsForm

logger = logging.getLogger(__name__)


class BatchForm(PermissionsForm):
    class Meta:
        model = Batch
        fields = ['name', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


class ContainerFamilyForm(PermissionsForm):
    class Meta:
        model = ContainerFamily
        fields = ['name', 'git', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


class ContainerUpdateForm(PermissionsForm):
    parent = forms.ModelChoiceField(
        help_text=Container.parent.field.help_text,
        queryset=Container.objects.filter(file_type=Container.SIMG),
        required=False)

    class Meta:
        model = Container
        fields = ['parent', 'tag', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


class ContainerForm(ContainerUpdateForm):
    """
    Form for creation of a Container.
    """
    class Meta(ContainerUpdateForm.Meta):
        fields = ['file'] + ContainerUpdateForm.Meta.fields

    def __init__(self, *args, **kwargs):
        super(ContainerForm, self).__init__(*args, **kwargs)
        self.file_type_detected = None

    def clean(self):
        """
        Perform Singularity container file validation (it's more efficient to do here than at the model level).

        Fill in the values for file and file_type.
        :return:
        """
        self.cleaned_data = super(ContainerForm, self).clean()

        # Check the file extension of the file.
        the_file = self.cleaned_data.get("file")
        if the_file is None:
            raise ValidationError(
                Container.DEFAULT_ERROR_MESSAGES["invalid_archive"],
                code="invalid_archive",
            )
        upload_name = getattr(the_file, 'name', 'container.simg')
        upload_base, upload_ext = os.path.splitext(upload_name)
        upload_lower = upload_name.lower()
        file_type = None
        for ext in Container.ACCEPTED_FILE_EXTENSIONS:
            if upload_lower.endswith(ext):
                file_type = Container.ACCEPTED_FILE_EXTENSIONS[ext]
                break
        if file_type is None:
            raise ValidationError(
                Container.DEFAULT_ERROR_MESSAGES["bad_extension"],
                code="bad_extension"
            )

        if file_type == Container.SIMG:
            # We need to get a file object to validate. We might have a path or we might
            # have to read the data out of memory.
            if hasattr(the_file, 'temporary_file_path'):
                Container.validate_singularity_container(the_file.temporary_file_path())
            else:
                with NamedTemporaryFile(prefix=upload_base,
                                        suffix=upload_ext) as f_temp:
                    if hasattr(the_file, 'read'):
                        f_temp.write(the_file.read())
                    else:
                        f_temp.write(the_file['content'])
                    f_temp.flush()
                    Container.validate_singularity_container(f_temp.name)

                if hasattr(the_file, 'seek') and callable(the_file.seek):
                    the_file.seek(0)

            # Annotate self.instance with a marker that we already validated the container.
            self.instance.singularity_validated = True

        # Having figured out the file type, add it to self.instance manually.
        self.instance.file_type = file_type
        return self.cleaned_data


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

    class Meta:
        model = ContainerApp
        exclude = ['container']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


class ContainerRunForm(PermissionsForm):
    class Meta:
        model = ContainerRun
        fields = ['name', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))
