from django import forms
from metadata.models import CompoundDatatype
from archive.models import Dataset
from django.contrib.auth.models import User
from librarian.models import SymbolicDataset
import os
import tempfile
import logging
from django.core.files.base import ContentFile
"""
Generate an HTML form to create a new DataSet object
"""


LOGGER = logging.getLogger(__name__)

class DatasetForm (forms.ModelForm):
    """
    use for validating only two entries
    """
    compound_datatypes = CompoundDatatype.objects.all()
    compound_datatype_choices = [(CompoundDatatype.RAW_ID, CompoundDatatype.RAW_VERBOSE_NAME)]
    for compound_datatype in compound_datatypes:
        compound_datatype_choices.append([compound_datatype.pk, compound_datatype.__unicode__()])
    datatype = forms.ChoiceField(choices=compound_datatype_choices)



    def save(self, force_insert=False, force_update=False, commit=True):
        """
        Override ModelForm.save()
        :rtype : object
        :param force_insert:
        :param force_update:
        :param commit:
        """

        # Create a new DatasetsetForm object but do not commit to database
        dataset = super(DatasetForm, self).save(commit=False)

        username = 'shipyard'
        dataset.user = User.objects.get(username=username)


        compound_datatype_obj = None
        if self.cleaned_data['datatype'] != CompoundDatatype.RAW_ID:
            compound_datatype_obj = CompoundDatatype.objects.get(pk=self.cleaned_data['datatype'])

        dataset.symbolicdataset = SymbolicDataset.create_empty(compound_datatype=compound_datatype_obj)

        # Upload InMemoryUploadedFile to disk
        uploaded_filepath = os.path.abspath(Dataset.UPLOAD_DIR + "/" + dataset.dataset_file.name)
        dataset.dataset_file.save(uploaded_filepath, dataset.dataset_file)

        # Set MD5 and validate headers
        if compound_datatype_obj is None:
            dataset.symbolicdataset.set_MD5(uploaded_filepath)
        else:
            dataset.symbolicdataset.set_MD5_and_count_rows(uploaded_filepath)
            run_dir = tempfile.mkdtemp(prefix="SD{}".format(dataset.symbolicdataset.pk))
            content_check = dataset.symbolicdataset.check_file_contents(uploaded_filepath, run_dir, None, None, None)
            if content_check.is_fail():
                if content_check.baddata.bad_header:
                    raise ValueError('The header of file "{}" does not match the CompoundDatatype "{}"'
                                     .format(uploaded_filepath, compound_datatype_obj))
                elif content_check.baddata.cell_errors.exists():
                    error = content_check.baddata.cell_errors.first()
                    compound_datatype_objm = error.column
                    raise ValueError('The entry at row {}, column {} of file "{}" did not pass the constraints of '
                                     'Datatype "{}"'.format(error.row_num, compound_datatype_objm.column_idx, uploaded_filepath, compound_datatype_objm.datatype))
                else:
                    # Shouldn't reach here.
                    raise ValueError('The file "{}" was malformed'.format(uploaded_filepath))
            LOGGER.debug("Read {} rows from file {}".format(dataset.symbolicdataset.structure.num_rows, uploaded_filepath))


        dataset.symbolicdataset.clean()
        dataset.save()  # save both the Dataset and SymbolicDataset

        return dataset

    class Meta:
        model = Dataset
        fields = ['dataset_file', 'name', 'description']
        exclude = ['user', 'symbolicdataset']


class BulkDatasetForm(forms.Form):
    bulk_csv = forms.FileField()