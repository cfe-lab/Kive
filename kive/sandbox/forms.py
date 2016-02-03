"""
Forms for running a Pipeline.
"""

from django import forms
from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model

from pipeline.models import PipelineFamily, Pipeline
from archive.models import Run

import metadata.forms

from constants import maxlengths


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


class RunDetailsForm(forms.ModelForm):
    """
    Form used for validating details when updating a Run.
    """
    permissions = metadata.forms.PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this run?",
        required=False
    )

    name = forms.CharField(
        max_length=maxlengths.MAX_NAME_LENGTH,
        label='Name',
        help_text='A name to identify this run',
        required=False
    )

    description = forms.CharField(
        widget=forms.Textarea(attrs={'rows':5}),
        label='Description',
        help_text='A brief description of this run',
        required=False
    )

    class Meta:
        model = Run
        fields = ("permissions", "name", "description")

    def __init__(self, data=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(RunDetailsForm, self).__init__(data, *args, **kwargs)

        # We can't simply use "users_allowed or User.objects.all()" because we may specify
        # an empty QuerySet, and that's falsy.
        users_allowed = users_allowed if users_allowed is not None else get_user_model().objects.all()
        groups_allowed = groups_allowed if groups_allowed is not None else Group.objects.all()
        self.fields["permissions"].set_users_groups_allowed(users_allowed, groups_allowed)


class RunSubmissionForm(RunDetailsForm):
    """
    Form used for validating the request values when submitting a Pipeline to run.
    """
    # Use Pipeline.objects.all() as the default queryset so that when creating a form without pipeline_qs
    # specified, any pipeline will be OK.
    pipeline = forms.ModelChoiceField(
        widget=forms.HiddenInput,
        queryset=Pipeline.objects.all())

    class Meta:
        model = Run
        fields = ("pipeline", "permissions", "name", "description")

    def __init__(self, data=None, pipeline_qs=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(RunSubmissionForm, self).__init__(data, users_allowed=users_allowed,
                                                groups_allowed=groups_allowed, *args, **kwargs)
        if pipeline_qs is not None:
            self.fields["pipeline"].queryset = pipeline_qs
