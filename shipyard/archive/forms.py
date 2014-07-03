from django import forms
from metadata.models import CompoundDatatype
from archive.models import Dataset
from django.contrib.auth.models import User
from librarian.models import SymbolicDataset
import os
import logging
from django.db import transaction
from time import gmtime, strftime, time
import tempfile
"""
Generate an HTML form to create a new DataSet object
"""


LOGGER = logging.getLogger(__name__)


class Uploader:

    @staticmethod
    def handle_uploaded_file(filestream, filebasename, dest_parent_dir=None):
        """
        Uploads the file in memory to temporary folder.
        :param InMemoryUploadedFile filestream :  InMemoryUploadedFile stream for file stored in memory
        :param str filebasename:  basename of the file to store in temp dir  (i.e.  just the file name, no directory path)
        :param str dest_parent_dir:  if None, then saves to the temporary directory.  Otherwise saves to specifed dir.
        :return str: the full path to the temporary file written to
        """
        if dest_parent_dir:
            destination_filename = dest_parent_dir + os.sep + filebasename
        else:
            destination_filename = tempfile.gettempdir() + os.sep + filebasename

        # TODO: handle possible race conditions with multiple people uploading same file at same time
        if os.path.exists(destination_filename):
            destination_filename += Uploader.timestamp()

        with open(destination_filename, 'wb+') as fh_destination:
            for chunk in filestream.chunks():
                fh_destination.write(chunk)

        return destination_filename

    @staticmethod
    def timestamp():
       now = time.time()
       milliseconds = '%03d' % int((now - int(now)) * 1000)
       return time.strftime('%Y%m%d%H%M%S', gmtime()) + milliseconds

    @staticmethod
    def remove_uploaded_file(filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

class DatasetForm (forms.Form):
    """
    User-entered single dataset.  We avoid using ModelForm since we can't set Dataset.user and Dataset.symbolicdataset
    before checking if the ModelForm.is_valid.  As a result, the internal calls to Model.clean() fail.
    """

    name = forms.CharField(max_length=Dataset.MAX_NAME_LEN)
    description = forms.CharField(widget=forms.Textarea)
    dataset_file = forms.FileField(allow_empty_file="False",  max_length=Dataset.MAX_FILE_LEN)

    compound_datatypes = CompoundDatatype.objects.all()
    compound_datatype_choices = [(CompoundDatatype.RAW_ID, CompoundDatatype.RAW_VERBOSE_NAME)]
    for compound_datatype in compound_datatypes:
        compound_datatype_choices.append([compound_datatype.pk, compound_datatype.__unicode__()])
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

        # Upload InMemoryUploadedFile to disk to calc MD5 on file
        uploaded_filepath = Uploader.handle_uploaded_file(filestream=self.cleaned_data['dataset_file'],
                                      filebasename=self.cleaned_data['dataset_file'].name,
                                      dest_parent_dir=Dataset.UPLOAD_DIR)

        try:
            symbolicdataset = SymbolicDataset.create_SD(uploaded_filepath, cdt=compound_datatype_obj,
                                                        make_dataset=True, user=user, name=self.cleaned_data['name'],
                                                        description=self.cleaned_data['description'],
                                                        created_by=None, check=True)
        except Exception, e:
            # Delete uploaded file from disk
            LOGGER.debug("Removing uploaded file " + uploaded_filepath)
            Uploader.remove_uploaded_file(uploaded_filepath)
            raise e


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
        compound_datatype_choices.append([compound_datatype.pk, compound_datatype.__unicode__()])
    compound_datatype = forms.ChoiceField(choices=compound_datatype_choices)

    def create_datasets(self):

        username = 'shipyard'   # TODO:  do not hardcode this
        user = User.objects.get(username=username)

        compound_datatype_obj = None
        if self.cleaned_data['compound_datatype'] != CompoundDatatype.RAW_ID:
            compound_datatype_obj = CompoundDatatype.objects.get(pk=self.cleaned_data['compound_datatype'])

        uploaded_filepath = Uploader.handle_uploaded_file(filestream=self.cleaned_data['datasets_csv'],
                                                          filebasename=self.cleaned_data['datasets_csv'].name,
                                                          dest_parent_dir=None)
        SymbolicDataset.create_SD_bulk(csv_file_path=uploaded_filepath, cdt=compound_datatype_obj, make_dataset=True,
                                       user=user, created_by=None, check=True)
        Uploader.remove_uploaded_file(uploaded_filepath)