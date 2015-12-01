"""
Generate an HTML form to create a new DataSet object
"""
from django import forms
from django.forms.widgets import ClearableFileInput
from django.utils.translation import ugettext_lazy as _, ungettext_lazy
from django.contrib.auth.models import User, Group

import logging
import itertools
from datetime import datetime

from metadata.models import CompoundDatatype
from librarian.models import Dataset
import metadata.forms

import zipfile
import tarfile
import StringIO
from zipfile import ZipFile

from constants import maxlengths

LOGGER = logging.getLogger(__name__)


class DatasetMetadataForm(forms.ModelForm):
    """
    Handles just the Dataset metadata.
    """
    permissions = metadata.forms.PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this Datatype?",
        user_queryset=User.objects.all(),
        group_queryset=Group.objects.all(),
        required=False
    )

    class Meta:
        model = Dataset
        fields = ("name", "description", "permissions")

    def _post_clean(self):
        pass

    def __init__(self, data=None, files=None, owner=None,
                 users_already_allowed=None, groups_already_allowed=None,
                 *args, **kwargs):
        super(DatasetMetadataForm, self).__init__(data, files, *args, **kwargs)

        users_already_allowed = users_already_allowed or []
        groups_already_allowed = groups_already_allowed or []

        addable_users = User.objects.exclude(pk__in=[x.pk for x in itertools.chain([owner], users_already_allowed)])
        addable_groups = Group.objects.exclude(pk__in=[x.pk for x in groups_already_allowed])

        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


class DatasetForm(forms.ModelForm):
    """
    User-entered single dataset.
    """
    permissions = metadata.forms.PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this Datatype?",
        user_queryset=User.objects.all(),
        group_queryset=Group.objects.all(),
        required=False
    )

    dataset_file = forms.FileField(allow_empty_file="False",  max_length=maxlengths.MAX_FILENAME_LENGTH)

    RAW_CDT_CHOICE = (CompoundDatatype.RAW_ID, CompoundDatatype.RAW_VERBOSE_NAME)
    compound_datatype_choices = [RAW_CDT_CHOICE]
    compound_datatype = forms.ChoiceField(choices=compound_datatype_choices)

    class Meta:
        model = Dataset
        fields = ('name', 'description', 'dataset_file', "permissions", "compound_datatype")

    def __init__(self, data=None, files=None, users_allowed=None, groups_allowed=None, user=None, *args, **kwargs):
        super(DatasetForm, self).__init__(data, files, *args, **kwargs)
        users_allowed = users_allowed or User.objects.all()
        groups_allowed = groups_allowed or Group.objects.all()
        self.fields["permissions"].set_users_groups_allowed(users_allowed, groups_allowed)

        user_specific_choices = ([DatasetForm.RAW_CDT_CHOICE] +
                                 CompoundDatatype.choices(user))
        self.fields["compound_datatype"].choices = user_specific_choices

    def _post_clean(self):
        """
        Special override for DatasetForm that doesn't validate the Dataset.
        """
        pass


class BulkDatasetUpdateForm(forms.Form):
    # dataset primary key
    id = forms.IntegerField(widget=forms.TextInput(attrs={'readonly': 'readonly'}), required=False)

    # dataset name
    name = forms.CharField(max_length=maxlengths.MAX_FILENAME_LENGTH, required=False)
    description = forms.CharField(widget=forms.Textarea, required=False)

    filesize = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'readonly': 'readonly',
        'class': 'display_only_input'
    }))

    md5 = forms.CharField(required=False, widget=forms.TextInput(attrs={
        'readonly': 'readonly',
        'class': 'display_only_input'
    }))

    # The original name of the file uploaded by the user
    # Do not bother exposing the actual filename as it exists in the fileserver
    orig_filename = forms.CharField(widget=forms.TextInput(attrs={
        'readonly': 'readonly',
        'class': 'display_only_input'
    }), required=False)

    # Dataset instance
    # We don't use ModelForm because the formset.form.instance template property doesn't seem to work in django 1.6
    def __init__(self, *args, **kwargs):
        super(BulkDatasetUpdateForm, self).__init__(*args, **kwargs)
        self.dataset = Dataset()
        self.status = 0

    def update(self):
        if self.cleaned_data['id']:
            dataset = Dataset.objects.get(id=self.cleaned_data['id'])
            dataset.name = self.cleaned_data['name']
            dataset.description = self.cleaned_data['description']
            dataset.save()
            return dataset
        return None


# FIXME: This was modified to support users and groups, but is not called by any view.
# If you get to implementing a view using this, beware that it was not tested!
class BulkCSVDatasetForm (metadata.forms.AccessControlForm):
    """
    Creates multiple datasets from a CSV.
    Expects that BulkDatasetForm.is_valid() has been called so that BulkDatasetForm.cleaned_data dict has been populated
        with validated data.
    """

    datasets_csv = forms.FileField(allow_empty_file="False",  max_length=4096,
                                   widget=ClearableFileInput(attrs={"multiple": "true"}))  # multiselect files

    compound_datatype_choices = [DatasetForm.RAW_CDT_CHOICE]
    compound_datatype = forms.ChoiceField(choices=compound_datatype_choices)

    def create_datasets(self, user):

        compound_datatype_obj = None
        if self.cleaned_data['compound_datatype'] != CompoundDatatype.RAW_ID:
            compound_datatype_obj = CompoundDatatype.objects.get(pk=self.cleaned_data['compound_datatype'])

        # FIXME this doesn't support PermissionsWidget.
        Dataset.create_dataset_bulk(csv_file_path=None, user=user,
                                    users_allowed=self.cleaned_data["users_allowed"],
                                    groups_allowed=self.cleaned_data["groups_allowed"],
                                    csv_file_handle=self.cleaned_data['datasets_csv'], cdt=compound_datatype_obj,
                                    keep_files=True, file_source=None, check=True)


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
        for upload_file in uploaded_file_list:
            filefield = forms.FileField(max_length=self.max_length, allow_empty_file=self.allow_empty_file)
            clean_data.extend([filefield.clean(data=upload_file, initial=initial)])
        return clean_data


class BulkAddDatasetForm (metadata.forms.AccessControlForm):
    """
    Uploads multiple datasets at once.
    Appends the date and time to the name_prefix to make the dataset name unique.
    """

    name_prefix = forms.CharField(max_length=maxlengths.MAX_NAME_LENGTH, required=False,
                                  help_text="Prefix will be prepended with date and time to create unique " +
                                            "dataset name.")

    description = forms.CharField(widget=forms.Textarea, required=False,
                                  help_text="Description text that will be applied to all bulk added datasets " +
                                            "If not supplied, a description will be autogenerated containing " +
                                            "the filename.")

    dataset_files = MultiFileField(allow_empty_file="False",  max_length=4096)  # multiselect files

    compound_datatype_choices = [DatasetForm.RAW_CDT_CHOICE]
    compound_datatype = forms.ChoiceField(choices=compound_datatype_choices)

    def __init__(self, data=None, files=None, user=None, *args, **kwargs):
        super(BulkAddDatasetForm, self).__init__(data, files, *args, **kwargs)

        if files:
            # Form validation expects that items are in dict form.
            # Create a dict where the value is the list of files uploaded by the user.
            # If we don't do this, then only the first file in the list is assigned to dataset_files
            self.files = {"dataset_files": files.getlist("dataset_files")}

        user_specific_choices = ([DatasetForm.RAW_CDT_CHOICE] +
                                 CompoundDatatype.choices(user))
        self.fields["compound_datatype"].choices = user_specific_choices

    def create_datasets(self, user):
        """
        Creates the Datasets and the corresponding SymbolicDatasets in same order as cleaned_data["dataset_files"].
        Will still save successful Datasets to database even if some of the Datasets fail to create.

        :return:  a list of the created Dataset objects in the same order as cleaned_data["dataset_files"].
            If the Dataset failed to create, then the list element contains error message.
        """
        compound_datatype_obj = None
        if self.cleaned_data['compound_datatype'] != CompoundDatatype.RAW_ID:
            compound_datatype_obj = CompoundDatatype.objects.get(pk=self.cleaned_data['compound_datatype'])

        results = []
        for uploaded_file in self.cleaned_data['dataset_files']:
            dataset = None
            error_str = None
            try:
                # TODO:  use correct unique constraints
                name_prefix = ""
                if self.cleaned_data["name_prefix"]:
                    name_prefix = self.cleaned_data["name_prefix"] + "_"
                auto_name = name_prefix + uploaded_file.name + "_" + datetime.now().strftime('%Y%m%d%H%M%S%f')

                if self.cleaned_data["description"]:
                    auto_description = self.cleaned_data["description"]
                else:
                    auto_description = "Bulk Uploaded File " + uploaded_file.name

                dataset = Dataset.create_dataset(file_path=None, user=user,
                                                 cdt=compound_datatype_obj, keep_file=True, name=auto_name,
                                                 description=auto_description, file_source=None, check=True,
                                                 file_handle=uploaded_file)
                dataset.grant_from_json(self.cleaned_data["permissions"])

            except Exception, e:
                error_str = str(e)
                LOGGER.exception("Error while creating Dataset for file with original file name=" +
                                 str(uploaded_file.name) +
                                 " and autogenerated Dataset name = " +
                                 str(auto_name))

            if dataset and error_str is None:
                results.extend([dataset])
            elif error_str and dataset is None:
                results.extend([error_str])
            else:
                raise ValueError("Invalid situation.  Must either have a dataset or error.  Can not have both or none.")

        return results


class ArchiveAddDatasetForm(metadata.forms.AccessControlForm):
    """
    Uploads multiple datasets at once.
    Appends the date and time to the name_prefix to make the dataset name unique.
    """
    # TODO: There's duplicated code between this class and the BulkAddDatasetForm. Refactor: Pull out common code to a
    # new class
    name_prefix = forms.CharField(max_length=maxlengths.MAX_NAME_LENGTH, required=False,
                                  help_text="Prefix will be prepended with date and time to create unique dataset " +
                                            "name.")

    description = forms.CharField(widget=forms.Textarea, required=False,
                                  help_text="Description text that will be applied to all added datasets " +
                                            "If not supplied, a description will be autogenerated containing the " +
                                            "filename.")

    dataset_file = forms.FileField(allow_empty_file="False",  max_length=maxlengths.MAX_FILENAME_LENGTH,
                                   label='Archive file')

    compound_datatype_choices = [DatasetForm.RAW_CDT_CHOICE]
    compound_datatype = forms.ChoiceField(choices=compound_datatype_choices)

    def __init__(self, data=None, files=None, user=None, *args, **kwargs):
        super(ArchiveAddDatasetForm, self).__init__(data, files, *args, **kwargs)

        user_specific_choices = ([DatasetForm.RAW_CDT_CHOICE] +
                                 CompoundDatatype.choices(user))
        self.fields["compound_datatype"].choices = user_specific_choices

    def clean_dataset_file(self):
        files = []

        # First try to unzip the archive
        try:
            archive = ZipFile(self.cleaned_data["dataset_file"])

            def get_filestream(filename):
                f = archive.open(filename)
                streamable = StringIO.StringIO(f.read())
                streamable.name = f.name.replace('/', '_')
#                streamable.name = f.name.split('/')[-1]
                f.close()
                return streamable

            def should_include(filename):
                # Bail on directories
                if filename.endswith("/"):
                    return False

                # And on hidden files
                if filename.split("/")[-1].startswith("."):
                    return False

                return True

            files = [get_filestream(file_name) for file_name in archive.namelist() if should_include(file_name)]

        except zipfile.BadZipfile:
            # Bad zip? Try tar why not
            try:
                self.cleaned_data["dataset_file"].seek(0)  # Reset the file so we can read it again
                archive = tarfile.open(name=None, mode='r', fileobj=self.cleaned_data["dataset_file"])

                def get_filestream(archive_member):
                    xfile = archive.extractfile(archive_member)
                    if xfile is not None:
                        xfile.name = xfile.name[2:].replace('/', '_')
                    return xfile

                def should_include(name):
                    name = name[2:]
                    if name.endswith("/"):
                        return False

                    # And on hidden files
                    if name.split("/")[-1].startswith("."):
                        return False
                    return True

                files = [get_filestream(member) for member in archive.getmembers() if should_include(member.name)]
                files = filter(lambda x: x is not None, files)

            except tarfile.TarError:
                raise forms.ValidationError(_('Not a valid archive file. We currently accept Zip and Tar files.'),
                                            code='invalid')
        return files

    def create_datasets(self, user):
        """
        Creates the Datasets and the corresponding SymbolicDatasets in same order as cleaned_data["dataset_files"].
        Will still save successful Datasets to database even if some of the Datasets fail to create.

        :return:  a list of the created Dataset objects in the same order as cleaned_data["dataset_files"].
            If the Dataset failed to create, then the list element contains error message.
        """
        compound_datatype_obj = None
        if self.cleaned_data['compound_datatype'] != CompoundDatatype.RAW_ID:
            compound_datatype_obj = CompoundDatatype.objects.get(pk=self.cleaned_data['compound_datatype'])

        results = []
        for uploaded_file in self.cleaned_data['dataset_file']:
            dataset = None
            error_str = None
            try:
                # TODO:  use correct unique constraints
                name_prefix = ""
                if self.cleaned_data["name_prefix"]:
                    name_prefix = self.cleaned_data["name_prefix"] + "_"
                auto_name = name_prefix + uploaded_file.name + "_" + datetime.now().strftime('%Y%m%d%H%M%S%f')

                if self.cleaned_data["description"]:
                    auto_description = self.cleaned_data["description"]
                else:
                    auto_description = "Bulk Uploaded File " + uploaded_file.name

                dataset = Dataset.create_dataset(file_path=None, user=user,
                                                 cdt=compound_datatype_obj, keep_file=True, name=auto_name,
                                                 description=auto_description, file_source=None, check=True,
                                                 file_handle=uploaded_file)
                dataset.grant_from_json(self.cleaned_data["permissions"])

            except Exception, e:
                error_str = str(e)
                LOGGER.exception("Error while creating Dataset for file with original file name=" +
                                 str(uploaded_file.name) +
                                 " and autogenerated Dataset name = " +
                                 str(auto_name))

            if dataset and error_str is None:
                results.extend([dataset])
            elif error_str and dataset is None:
                results.extend([error_str])
            else:
                raise ValueError("Invalid situation.  Must either have a dataset or error.  Can not have both or none.")

        return results
