from django import forms
from django.forms.widgets import ClearableFileInput
from metadata.models import CompoundDatatype
from archive.models import Dataset
from django.contrib.auth.models import User
from librarian.models import SymbolicDataset

import logging

from constants import maxlengths
from datetime import datetime
from django.utils.translation import ugettext_lazy as _, ungettext_lazy
"""
Generate an HTML form to create a new DataSet object
"""




LOGGER = logging.getLogger(__name__)




class DatasetForm (forms.Form):
    """
    User-entered single dataset.  We avoid using ModelForm since we can't set Dataset.user and Dataset.symbolicdataset
    before checking if the ModelForm.is_valid().  As a result, the internal calls to Model.clean() fail.
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


class BulkUpdateDatasetModelForm(forms.ModelForm):
    # Holds the original file name as uploaded by client
    orig_filename = forms.CharField(max_length=maxlengths.MAX_FILENAME_LENGTH,
                                    widget=forms.TextInput(attrs={'readonly': 'readonly'}))
    status = forms.IntegerField(widget=forms.NumberInput(attrs={'readonly': 'readonly'}))

    class Meta:
        model = Dataset
        fields = ['name', 'description']


class BulkUpdateDatasetForm(forms.ModelForm):
    # Holds the original file name as uploaded by client
    orig_filename = forms.CharField(max_length=maxlengths.MAX_FILENAME_LENGTH,
                                    widget=forms.TextInput(attrs={'readonly': 'readonly'}))
    name = forms.CharField(max_length=maxlengths.MAX_NAME_LENGTH,)
    description = forms.CharField(widget=forms.Textarea)
    status = forms.IntegerField(widget=forms.NumberInput(attrs={'readonly': 'readonly'}))



class BulkDatasetForm (forms.Form):
    """
    Creates multiple datasets from a CSV.
    Expects that BulkDatasetForm.is_valid() has been called so that BulkDatasetForm.cleaned_data dict has been populated
        with validated data.
    """

    datasets_csv = forms.FileField(allow_empty_file="False",  max_length=4096,
                                   widget=ClearableFileInput(attrs={"multiple": "true"}))  # multiselect files

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



class MultiFileField(forms.Field):
    """
    Django does not have a FileField that support selection of multiple files.
    This extends the FileField to allow multiple files.

    Make sure you assign this request.FILES.getlist[<name of MultiFileField>]
    instead of request.FILES[<name of MultiFileField>]
    """
    widget = ClearableFileInput(attrs={"multiple": "true"})
    default_error_messages = {
        'invalid': _("No file was submitted. Check the encoding type on the form."),
        'missing': _("No file was submitted."),
        'empty': _("The submitted file is empty."),
        'max_length': ungettext_lazy(
            'Ensure this filename has at most %(max)d character (it has %(length)d).',
            'Ensure this filename has at most %(max)d characters (it has %(length)d).',
            'max'),
        'contradiction': _('Please either submit a file or check the clear checkbox, not both.')
    }

    def __init__(self, *args, **kwargs):
        self.max_length = kwargs.pop('max_length', None)
        self.allow_empty_file = kwargs.pop('allow_empty_file', False)
        super(MultiFileField, self).__init__(*args, **kwargs)

    def clean(self, uploaded_file_list, initial=None):
        clean_data = []
        for i, upload_file in enumerate(uploaded_file_list):
            filefield = forms.FileField(max_length=self.max_length, allow_empty_file=self.allow_empty_file)
            clean_data.extend([filefield.clean(data=upload_file, initial=initial)])
        return clean_data


class BulkAddDatasetForm (forms.Form):
    """
    Uploads multiple datasets at once.
    Appends the date and time to the name_prefix to make the dataset name unique.
    """

    name_prefix = forms.CharField(max_length=maxlengths.MAX_NAME_LENGTH, required=False,
                                  help_text="Prefix will be appended with date and time to create unique dataset name.  " +
                                            "If not supplied, the filename is used as the prefix.")

    description = forms.CharField(widget=forms.Textarea, required=False,
                                  help_text="Description text that will be applied to all bulk added datasets " +
                                            "If not supplied, a description will be autogenerated containing the filename.")

    dataset_files = MultiFileField(allow_empty_file="False",  max_length=4096)  # multiselect files

    compound_datatypes = CompoundDatatype.objects.all()
    compound_datatype_choices = [(CompoundDatatype.RAW_ID, CompoundDatatype.RAW_VERBOSE_NAME)]
    for compound_datatype in compound_datatypes:
        compound_datatype_choices.append([compound_datatype.pk, compound_datatype.__unicode__()])
    compound_datatype = forms.ChoiceField(choices=compound_datatype_choices, required=True)

    def __init__(self, *args, **kwargs):
        super(BulkAddDatasetForm, self).__init__(*args, **kwargs)

        files = None
        if kwargs and "files" in kwargs:
            files = kwargs["files"]
        elif len(args) >= 2:
            files = args[1]

        if files:
            # Form validation expects that items are in dict form.
            # Create a dict where the value is the list of files uploaded by the user.
            # If we don't do this, then only the first file in the list is assigned to dataset_files
            self.files = {"dataset_files": files.getlist("dataset_files")}

    def create_datasets(self):
        """
        Creates the Datasets and the corresponding SymbolicDatasets in same order as cleaned_data["dataset_files"].
        Will still save successful Datasets to database even if some of the Datasets fail to create.

        :return:  a list of the created Dataset objects in the same order as cleaned_data["dataset_files"].
            If the Dataset failed to create, then the list element contains error message.
        """
        username = 'shipyard'   # TODO:  do not hardcode this
        user = User.objects.get(username=username)

        compound_datatype_obj = None
        if self.cleaned_data['compound_datatype'] != CompoundDatatype.RAW_ID:
            compound_datatype_obj = CompoundDatatype.objects.get(pk=self.cleaned_data['compound_datatype'])

        results = []
        for uploaded_file in self.cleaned_data['dataset_files']:
            dataset = None
            error_str = None
            try:
                # TODO:  use correct unique constraints
                if self.cleaned_data["name_prefix"]:
                    auto_name = self.cleaned_data["name_prefix"]
                else:
                    auto_name = uploaded_file.name
                auto_name += "_" + datetime.now().strftime('%Y%m%d%H%M%S%f')

                if self.cleaned_data["description"]:
                    auto_description = self.cleaned_data["description"]
                else:
                    auto_description = "Bulk Uploaded File " + uploaded_file.name

                symbolicdataset = SymbolicDataset.create_SD(file_path=None, file_handle=uploaded_file,
                                                            cdt=compound_datatype_obj,
                                                            make_dataset=True, user=user,
                                                            name=auto_name, description=auto_description,
                                                            created_by=None, check=True)
                dataset = Dataset.objects.filter(symbolicdataset=symbolicdataset).get()
            except Exception, e:
                error_str = str(e)
                LOGGER.exception("Error while creating Dataset for file with original file name=" + str(uploaded_file.name) +
                                 " and autogenerated Dataset name = " + str(auto_name))

            if dataset and error_str is None:
                results.extend([dataset])
            elif error_str and dataset is None:
                results.extend([error_str])
            else:
                raise ValueError("Invalid situation.  Must either have a dataset or error.  Can not have both or none.")

        return results


