from django import forms
from django.contrib.auth.models import User

from metadata.models import CompoundDatatype
from archive.models import Dataset
from librarian.models import SymbolicDataset

import logging

from constants import maxlengths
"""
Generate an HTML form to create a new DataSet object
"""


LOGGER = logging.getLogger(__name__)




class DatasetForm (forms.Form):
    """
    User-entered single dataset.  We avoid using ModelForm since we can't set Dataset.user and Dataset.symbolicdataset
    before checking if the ModelForm.is_valid.  As a result, the internal calls to Model.clean() fail.
    """

    name = forms.CharField(max_length=maxlengths.MAX_NAME_LENGTH)
    description = forms.CharField(widget=forms.Textarea, required=False)
    dataset_file = forms.FileField(allow_empty_file="False",  max_length=maxlengths.MAX_FILENAME_LENGTH)

    compound_datatypes = CompoundDatatype.objects.all()
    compound_datatype_choices = [(CompoundDatatype.RAW_ID, CompoundDatatype.RAW_VERBOSE_NAME)]
    for compound_datatype in compound_datatypes:
        compound_datatype_choices.append([compound_datatype.pk, str(compound_datatype)])
    compound_datatype = forms.ChoiceField(choices=compound_datatype_choices)

    def create_dataset(self):
        """
        Creates and commits the Dataset and its associated SymbolicDataset to db.
        Expects that DatasetForm.is_valid() has been called so that DatasetForm.cleaned_data dict has been populated
        with validated data.
        """

        username = 'shipyard'   # TODO:  do not hardcode this
        user = User.objects.get(username=username)

        compound_datatype_obj = None
        if self.cleaned_data['compound_datatype'] != CompoundDatatype.RAW_ID:
            compound_datatype_obj = CompoundDatatype.objects.get(pk=self.cleaned_data['compound_datatype'])

        symbolicdataset = SymbolicDataset.create_SD(file_path=None, file_handle=self.cleaned_data['dataset_file'],
                                                    cdt=compound_datatype_obj,
                                                    make_dataset=True, user=user, name=self.cleaned_data['name'],
                                                    description=self.cleaned_data['description'],
                                                    created_by=None, check=True)



class BulkDatasetForm (forms.Form):
    """
    Creates multiple datasets from a CSV.
    Expects that BulkDatasetForm.is_valid() has been called so that BulkDatasetForm.cleaned_data dict has been populated
        with validated data.
    """

    datasets_csv = forms.FileField(allow_empty_file="False",  max_length=4096)

    compound_datatypes = CompoundDatatype.objects.all()
    compound_datatype_choices = [(CompoundDatatype.RAW_ID, CompoundDatatype.RAW_VERBOSE_NAME)]
    for compound_datatype in compound_datatypes:
        compound_datatype_choices.append([compound_datatype.pk, str(compound_datatype)])
    compound_datatype = forms.ChoiceField(choices=compound_datatype_choices)

    def create_datasets(self):

        username = 'shipyard'   # TODO:  do not hardcode this
        user = User.objects.get(username=username)

        compound_datatype_obj = None
        if self.cleaned_data['compound_datatype'] != CompoundDatatype.RAW_ID:
            compound_datatype_obj = CompoundDatatype.objects.get(pk=self.cleaned_data['compound_datatype'])

        SymbolicDataset.create_SD_bulk(csv_file_path=None, csv_file_handle=self.cleaned_data['datasets_csv'],
                                       cdt=compound_datatype_obj, make_dataset=True,
                                       user=user, created_by=None, check=True)
