"""
Forms for running a Pipeline.
"""

from django import forms

from pipeline.models import PipelineFamily, Pipeline
import metadata.forms


class PipelineSelectionForm(forms.Form):
    """Form for selecting a Pipeline to run."""
    pipeline = forms.ChoiceField()

    def __init__(self, pipeline_family_pk, *args, **kwargs):
        self.family_pk = pipeline_family_pk
        super(PipelineSelectionForm, self).__init__(*args, **kwargs)

        family = PipelineFamily.objects.get(pk=self.family_pk)
        self.family_name = family.name
        choices = []
        for pipeline in family.complete_members:
            choices.append((pipeline.pk, str(pipeline)))
        self.fields["pipeline"].choices = choices
        self.fields["pipeline"].initial = choices[-1][0]


class InputSubmissionForm(forms.Form):
    """Form for selecting an input for a Pipeline to run."""
    input_pk = forms.IntegerField()


class PipelineSubmissionForm(metadata.forms.AccessControlForm):
    """
    Form used for validating the request values when submitting a Pipeline to run.
    """
    pipeline_pk = forms.IntegerField()