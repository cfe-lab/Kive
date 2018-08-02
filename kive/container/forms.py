from django import forms
from django.contrib.auth.models import User, Group

from container.models import ContainerFamily, Container
from metadata.forms import PermissionsField


class ContainerFamilyForm(forms.ModelForm):
    permissions = PermissionsField(
        label="Users and groups allowed",
        required=False)

    class Meta(object):
        model = ContainerFamily
        fields = ['name', 'git', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))

    def __init__(self, data=None, instance=None, **kwargs):
        super(ContainerFamilyForm, self).__init__(data,
                                                  instance=instance,
                                                  **kwargs)
        if instance is None:
            addable_users = User.objects.all()
            addable_groups = Group.objects.all()
        else:
            addable_users, addable_groups = instance.other_users_groups()
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


class ContainerForm(forms.ModelForm):
    permissions = PermissionsField(
        label="Users and groups allowed",
        required=False)

    class Meta(object):
        model = Container
        fields = ['file', 'tag', 'description', 'permissions']
        widgets = dict(description=forms.Textarea(attrs=dict(cols=50, rows=10)))

    def __init__(self, data=None, instance=None, **kwargs):
        super(ContainerForm, self).__init__(data, instance=instance, **kwargs)
        if instance is None:
            addable_users = User.objects.all()
            addable_groups = Group.objects.all()
        else:
            addable_users, addable_groups = instance.other_users_groups()
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


class ContainerUpdateForm(ContainerForm):
    def __init__(self, *args, **kwargs):
        super(ContainerUpdateForm, self).__init__(*args, **kwargs)
        self.fields.pop('file')
