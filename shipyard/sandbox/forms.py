"""
Forms for running a Pipeline.
"""

from django import forms

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
        choices = []
        for pipeline in family.complete_members:
            choices.append((pipeline.pk, str(pipeline)))
        self.fields["pipeline"].choices = choices
        self.fields["pipeline"].initial = choices[-1][0]


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

    class Meta:
        model = fleet.models.RunToProcess
        fields = ("pipeline", "users_allowed", "groups_allowed")

    def __init__(self, data=None, pipeline_qs=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(RunSubmissionForm, self).__init__(data, *args, **kwargs)
        if pipeline_qs is not None:
            self.fields["pipeline"].queryset = pipeline_qs
        metadata.forms.setup_form_users_allowed(self, users_allowed)
        metadata.forms.setup_form_groups_allowed(self, groups_allowed)
