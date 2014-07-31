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

        pipeline = PipelineFamily.objects.get(pk=self.family_pk)
        self.family_name = pipeline.name
        choices = [(rev.pk, str(rev)) for rev in pipeline.members.order_by("revision_number")]
        self.fields["pipeline"].choices = choices
        self.fields["pipeline"].initial = choices[-1][0]
