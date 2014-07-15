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

class InputSelectionForm(forms.Form):
    """Form for selecting input Datasets for a Pipeline."""

    def __init__(self, *args, **kwargs):
        self.pipeline = kwargs.pop("pipeline")
        super(InputSelectionForm, self).__init__(*args, **kwargs)

        my_pipeline = Pipeline.objects.get(pk=self.pipeline)
        symbolic_datasets = SymbolicDataset.objects.filter(dataset__isnull=False)
        for my_input in my_pipeline.inputs.order_by("dataset_idx"):
            if my_input.is_raw():
                symbolic_datasets = symbolic_datasets.filter(structure__isnull=True)
            else:
                compound_datatype = my_input.get_cdt()
                symbolic_datasets = symbolic_datasets.filter(structure__compounddatatype=compound_datatype)
            symbolic_datasets = symbolic_datasets.order_by("dataset__created_by", "dataset__date_created")
            choices = [(s.pk, s.dataset) for s in symbolic_datasets]
            self.fields[my_input.dataset_idx] = forms.ChoiceField(label=str(my_input), choices=choices)
