from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.forms.widgets import TextInput

import metadata
from container.models import ContainerFamily, Container, ContainerApp, ContainerRun, Batch
from metadata.forms import PermissionsForm


class ContainerFamilyForm(PermissionsForm):
    class Meta(object):
        model = ContainerFamily
        fields = ['name', 'git', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


class ContainerForm(PermissionsForm):
    class Meta(object):
        model = Container
        fields = ['file', 'tag', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))


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


class BatchForm(forms.ModelForm):
    permissions = metadata.forms.PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this batch of runs?",
        required=False)

    class Meta(object):
        model = Batch
        exclude = ['user']

    def __init__(self, data=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(BatchForm, self).__init__(data, *args, **kwargs)

        # We can't simply use "users_allowed or User.objects.all()" because we may specify
        # an empty QuerySet, and that's falsy.
        users_allowed = users_allowed if users_allowed is not None else get_user_model().objects.all()
        groups_allowed = groups_allowed if groups_allowed is not None else Group.objects.all()
        self.fields["permissions"].set_users_groups_allowed(users_allowed, groups_allowed)


class ContainerRunForm(forms.ModelForm):
    class Meta(object):
        model = ContainerRun
        exclude = ['user', 'state']
