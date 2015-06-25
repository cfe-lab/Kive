"""
Forms for running a Pipeline.
"""

from django import forms
from django.contrib.auth.models import User, Group

from pipeline.models import PipelineFamily, Pipeline
import fleet.models

import metadata.forms


class PipelineSelectionForm(forms.Form):
    """Form for selecting a Pipeline to run."""
    pipeline = forms.ChoiceField()

    def __init__(self, pipeline_family_pk, *args, **kwargs):
        super(PipelineSelectionForm, self).__init__(*args, **kwargs)

        family = PipelineFamily.objects.get(pk=pipeline_family_pk)
        self.family_name = family.name
        self.family_pk = pipeline_family_pk
        choices = [(p.pk, str(p)) for p in family.members.all()]
        self.fields["pipeline"].choices = choices
        self.fields["pipeline"].initial = family.published_version or choices[-1][0]


class InputSubmissionForm(forms.Form):
    """Form for selecting an input for a Pipeline to run."""
    input_pk = forms.IntegerField()


class RunSubmissionForm(forms.ModelForm):
    """
    Form used for validating the request values when submitting a Pipeline to run.
    """
    # Use Pipeline.objects.all() as the default queryset so that when creating a form without pipeline_qs
    # specified, any pipeline will be OK.
    pipeline = forms.ModelChoiceField(
        widget=forms.HiddenInput,
        queryset=Pipeline.objects.all())

    permissions = metadata.forms.PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this run?",
        user_queryset=User.objects.all(),
        group_queryset=Group.objects.all(),
        required=False
    )

    class Meta:
        model = fleet.models.RunToProcess
        fields = ("pipeline", "permissions")

    def __init__(self, data=None, pipeline_qs=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(RunSubmissionForm, self).__init__(data, *args, **kwargs)
        if pipeline_qs is not None:
            self.fields["pipeline"].queryset = pipeline_qs

        users_allowed = users_allowed or User.objects.all()
        groups_allowed = groups_allowed or Group.objects.all()
        self.fields["permissions"].set_users_groups_allowed(users_allowed, groups_allowed)
