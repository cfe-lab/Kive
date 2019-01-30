from django import forms
from django.forms.widgets import TextInput

import logging

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
    class Meta(object):
        model = Container
        fields = ['file', 'file_type', 'parent', 'tag', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))  # FIXME figure out a widget for parent


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
