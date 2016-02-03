"""
Forms for creating Pipeline objects.
"""

from django import forms
from django.contrib.auth.models import User, Group
from django.contrib.auth import get_user_model

from pipeline.models import PipelineStep, PipelineFamily, Pipeline
from metadata.forms import PermissionsField


class PipelineStepForm (forms.ModelForm):
    """
    content_type - either 'method' or 'pipeline'
    object_id - ???
    transformation - ForeignKey to Method or Pipeline
    step_num - the step this transformation occupies in Pipeline
    outputs_to_delete - keys to TransformationOutput objects
    """
    class Meta:
        model = PipelineStep
        exclude = ("pipeline", )


class PipelineStepInputCableForm (forms.ModelForm):
    pass


class PipelineFamilyDetailsForm(forms.ModelForm):
    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this PipelineFamily?",
        required=False
    )

    class Meta:
        model = PipelineFamily
        fields = ("name", "description", "permissions")

    def __init__(self, data=None, addable_users=None, addable_groups=None, *args, **kwargs):
        addable_users = addable_users if addable_users is not None else get_user_model().objects.all()
        addable_groups = addable_groups if addable_groups is not None else Group.objects.all()
        super(PipelineFamilyDetailsForm, self).__init__(data, *args, **kwargs)
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


class PipelineDetailsForm(forms.ModelForm):
    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this Pipeline?",
        required=False
    )

    class Meta:
        model = Pipeline
        fields = ("revision_name", "revision_desc", "permissions")

    def __init__(self, data=None, addable_users=None, addable_groups=None, *args, **kwargs):
        addable_users = addable_users if addable_users is not None else get_user_model().objects.all()
        addable_groups = addable_groups if addable_groups is not None else Group.objects.all()
        super(PipelineDetailsForm, self).__init__(data, *args, **kwargs)
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)