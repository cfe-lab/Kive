from django import forms
from metadata.models import CompoundDatatype
from archive.models import Dataset
"""
Generate an HTML form to create a new DataSet object
"""



class DatasetForm (forms.ModelForm):
    """
    use for validating only two entries
    """

    class Meta:
        model = Dataset
        fields = ['dataset_file', 'name', 'description']

    # TODO:  user?


    compound_datatypes = CompoundDatatype.objects.all()
    compound_datatype_choices = [(CompoundDatatype.RAW_ID, CompoundDatatype.RAW_VERBOSE_NAME)]
    for compound_datatype in compound_datatypes:
        compound_datatype_choices.extend([compound_datatype.pk, compound_datatype])
    datatype = forms.ChoiceField(choices=compound_datatype_choices)


class BulkDatasetForm(forms.Form):
    bulk_csv = forms.FileField()