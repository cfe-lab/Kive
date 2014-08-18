"""
Forms for running a Pipeline.
"""

from django import forms
from pipeline.models import Pipeline, PipelineFamily
from librarian.models import SymbolicDataset

class PipelineSelectionForm(forms.Form):
    """Form for selecting a Pipeline to run."""
    pipeline = forms.ChoiceField()

    def __init__(self, *args, **kwargs):
        self.family_pk = kwargs.pop("pipeline_family_pk")
        super(PipelineSelectionForm, self).__init__(*args, **kwargs)

        family = PipelineFamily.objects.get(pk=self.family_pk)
        self.family_name = family.name
        choices = []
        for pipeline in family.complete_members:
            choices.append((pipeline.pk, str(pipeline)))
        self.fields["pipeline"].choices = choices
        self.fields["pipeline"].initial = choices[-1][0]
