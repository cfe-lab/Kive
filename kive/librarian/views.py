"""
librarian views
"""
import urllib
import logging
import mimetypes
import os

from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.core.urlresolvers import reverse
from django.db import transaction
from django.forms.formsets import formset_factory
from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.template import loader
from wsgiref.util import FileWrapper
from django.conf import settings
from django.utils.encoding import DjangoUnicodeDecodeError

from librarian.forms import DatasetForm, DatasetDetailsForm, BulkAddDatasetForm, BulkDatasetUpdateForm,\
    ArchiveAddDatasetForm
from archive.models import RunInput
from librarian.models import Dataset
from portal.views import admin_check
from metadata.models import CompoundDatatype
import librarian.models

LOGGER = logging.getLogger(__name__)


def _build_download_response(source_file):
    file_chunker = FileWrapper(source_file)  # Stream file in chunks to avoid overloading memory.
    mimetype = mimetypes.guess_type(source_file.name)[0]
    response = HttpResponse(file_chunker, content_type=mimetype)
    response['Content-Length'] = source_file.size
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(os.path.basename(source_file.name))
    return response


def _build_raw_viewer(request, file, name, download=None, return_to_url=None):
    t = loader.get_template("librarian/raw_view.html")
    c = {"file": file, "name": name, 'download': download, 'return': return_to_url}
    return HttpResponse(t.render(c, request))


@login_required
def datasets(request):
    """
    Display a list of all Datasets in database
    """
    t = loader.get_template('librarian/datasets.html')
    c = {'is_user_admin': admin_check(request.user)}

    return HttpResponse(t.render(c, request))


@login_required
def dataset_download(request, dataset_id):
    """
    Retrieve the file associated with the dataset for client download.
    """
    try:
        dataset = librarian.models.Dataset.filter_by_user(request.user).get(pk=dataset_id)
    except ObjectDoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id))

    with dataset.get_open_file_handle() as data_handle:
        return _build_download_response(data_handle)


@login_required
def dataset_view(request, dataset_id):
    """
    Display the file associated with the dataset in the browser, or update its name/description.
    """
    return_to_run = request.GET.get('run_id', None)
    is_view_results = "view_results" in request.GET
    is_view_run = "view_run" in request.GET
    return_url = reverse("datasets")
    if return_to_run is not None:
        if is_view_run:
            return_url = reverse('view_run', kwargs={'run_id': return_to_run})
        elif is_view_results:
            return_url = reverse('view_results', kwargs={'run_id': return_to_run})

    try:
        if admin_check(request.user):
            accessible_datasets = Dataset.objects
        else:
            accessible_datasets = Dataset.filter_by_user(request.user)
        dataset = accessible_datasets.prefetch_related(
            'structure',
            'structure__compounddatatype',
            'structure__compounddatatype__members',
            'structure__compounddatatype__members__datatype',
            'structure__compounddatatype__members__datatype__basic_constraints'
        ).get(pk=dataset_id)

    except ObjectDoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id))

    # Figure out which users and groups could be given access to this Dataset.
    # If the Dataset is uploaded, it's anyone who doesn't already have access;
    # if it was generated, it's anyone who had access to the generating run.
    addable_users, addable_groups = dataset.other_users_groups()

    if dataset.file_source is not None:
        generating_run = dataset.file_source.top_level_run
        addable_users.exclude(pk=generating_run.user_id)
        addable_users.exclude(pk__in=generating_run.users_allowed.values_list("pk", flat=True))
        addable_groups.exclude(pk__in=generating_run.groups_allowed.values_list("pk", flat=True))

    if request.method == "POST":
        # We are going to try and update this Dataset.
        dataset_form = DatasetDetailsForm(
            request.POST,
            addable_users=addable_users,
            addable_groups=addable_groups,
            instance=dataset
        )
        try:
            if dataset_form.is_valid():
                dataset.name = dataset_form.cleaned_data["name"]
                dataset.description = dataset_form.cleaned_data["description"]
                dataset.clean()
                dataset.save()
                dataset.grant_from_json(dataset_form.cleaned_data["permissions"])

                return HttpResponseRedirect(return_url)
        except (AttributeError, ValidationError, ValueError) as e:
            LOGGER.exception(e.message)
            dataset_form.add_error(None, e)

    else:
        # A DatasetForm which we can use to make submission and editing easier.
        dataset_form = DatasetDetailsForm(
            addable_users=addable_users,
            addable_groups=addable_groups,
            initial={"name": dataset.name, "description": dataset.description}
        )

    c = {
        "is_admin": admin_check(request.user),
        "is_owner": dataset.user == request.user,
        "dataset": dataset,
        "return": return_url,
        "dataset_form": dataset_form
    }

    rendered_response = None
    if not dataset.has_data():
        t = loader.get_template("librarian/missing_dataset_view.html")
        if dataset.external_path:
            c["missing_data_message"] = "This dataset's external file is missing.  " \
                                        "Consult your system administrator if this was unexpected."
        elif dataset.is_redacted():
            c["missing_data_message"] = "Data has been redacted."
        else:
            c["missing_data_message"] = "Data was not retained or has been purged."
        rendered_response = t.render(c, request)

    elif dataset.is_raw():
        t = loader.get_template("librarian/raw_dataset_view.html")

        # Test whether this is a binary file or not.
        # Read 1000 characters.
        with dataset.get_open_file_handle() as data_handle:
            sample_content = data_handle.read(1000)
        c.update(
            {
                "sample_content": sample_content
            }
        )
        c["is_binary"] = False
        try:
            rendered_response = t.render(c, request)
        except DjangoUnicodeDecodeError as e:
            c["is_binary"] = True
            del c["sample_content"]
            rendered_response = t.render(c, request)
    else:
        extra_errors = []
        # If we have a mismatched output, we do an alignment
        # over the columns.
        if dataset.content_matches_header:
            col_matching, processed_rows = None, dataset.rows(
                True,
                limit=settings.DATASET_DISPLAY_MAX,
                extra_errors=extra_errors)
        else:
            col_matching, insert = dataset.column_alignment()
            processed_rows = dataset.rows(data_check=True,
                                          insert_at=insert,
                                          limit=settings.DATASET_DISPLAY_MAX,
                                          extra_errors=extra_errors)
        t = loader.get_template("librarian/csv_dataset_view.html")
        processed_rows = list(processed_rows)
        c.update(
            {
                'column_matching': col_matching,
                'processed_rows': processed_rows,
                'extra_errors': extra_errors,
                "are_rows_truncated": len(processed_rows) >= settings.DATASET_DISPLAY_MAX
            }
        )
        rendered_response = t.render(c, request)
    return HttpResponse(rendered_response)


class BulkDatasetDisplay:
    """
    Helper class for displaying
    """
    # NOTE: these must be strings because they are read within a template file
    # datasets_bulk.html
    STATUS_FAIL = "1"
    STATUS_SUCCESS = "0"


@login_required
def datasets_add_archive(request):
    """
    Add datasets in bulk to db from an archive file (zip or tarfile).
    Redirect to /datasets_bulk view so user can examine upload status of each dataset.
    """
    c = {}
    # If we got posted to, try to create DB entries
    if request.method == 'POST':
        try:
            archive_add_dataset_form = ArchiveAddDatasetForm(
                data=request.POST,
                files=request.FILES,
                user=request.user,
            )
            # Try to retrieve new datasets. If this fails, we return to our current page
            is_ok = archive_add_dataset_form.is_valid()
            if is_ok:
                CDT_obj, add_results = archive_add_dataset_form.create_datasets(request.user)
                is_ok = len(add_results) > 0
            if not is_ok:
                # give up and let user try again
                t = loader.get_template('librarian/datasets_add_archive.html')
                c = {'archiveAddDatasetForm': archive_add_dataset_form}
                return HttpResponse(t.render(c, request))
            # have some files in the archive, lets display them
            # NOTE: at this point, we have a list of files in the archive.
            # some files might be legit, others not.
            # we have to cobble together information from add_results and the form cleaned data
            # for display.
            uploaded_files = archive_add_dataset_form.cleaned_data["dataset_file"]

            if len(uploaded_files) != len(add_results):
                raise RuntimeError("List length mismatch")
            t = loader.get_template('librarian/datasets_bulk.html')
            # Now have add_results, a list of elements e, where e is either
            # a dataset if the dataset was successfully created
            # or
            # a dict if a dataset was not successfully created
            # Generate a response
            archive_display_results = []
            # Fill in default values for the form fields
            for add_result, upload_info in zip(add_results, uploaded_files):
                display_result = {}
                if isinstance(add_result, dict):
                    # the dataset is invalid
                    display_result["name"] = add_result["name"]
                    display_result["description"] = ""
                    display_result["orig_filename"] = add_result["name"]
                    display_result["filesize"] = add_result["size"]
                    display_result["md5"] = ""
                    display_result["id"] = ""
                    display_result["is_valid"] = False
                else:
                    display_result["name"] = add_result.name
                    display_result["description"] = add_result.description
                    # This is the original filename as uploaded by the client, not the filename as stored
                    # on the file server.
                    display_result["orig_filename"] = upload_info[1].name
                    display_result["filesize"] = add_result.get_formatted_filesize()
                    display_result["md5"] = add_result.compute_md5()
                    display_result["id"] = add_result.id
                    display_result["is_valid"] = True
                archive_display_results.append(display_result)

            # now create forms from the display results.
            BulkDatasetUpdateFormSet = formset_factory(form=BulkDatasetUpdateForm,
                                                       max_num=len(archive_display_results))
            bulk_dataset_update_formset = BulkDatasetUpdateFormSet(initial=archive_display_results)

            # Fill in the attributes that are not fields in the form
            # These are not set by the BulkDatasetUpdateFormSet(initial=...) parameter,
            # so we have to tweak the forms after they have been created
            for dataset_form, display_result, add_result in zip(bulk_dataset_update_formset,
                                                                archive_display_results,
                                                                add_results):
                if display_result["is_valid"]:
                    dataset_form.dataset = add_result
                    dataset_form.status = BulkDatasetDisplay.STATUS_SUCCESS
                else:
                    dataset_form.dataset = Dataset()
                    dataset_form.non_field_errors = add_result["errstr"]
                    dataset_form.status = BulkDatasetDisplay.STATUS_FAIL

            # finally, add some other pertinent information which the template will display
            num_files_added = sum([a["is_valid"] for a in archive_display_results])
            c["bulk_dataset_formset"] = bulk_dataset_update_formset
            c["num_files_selected"] = len(add_results)
            c["num_files_added"] = num_files_added
            c["cdt_typestr"] = "Unstructured" if CDT_obj is None else CDT_obj
        except ValidationError as e:
            LOGGER.exception(e.message)
            archive_add_dataset_form.add_error(None, e)
            t = loader.get_template('librarian/datasets_add_archive.html')
            c.update({'archiveAddDatasetForm': archive_add_dataset_form})

    else:  # return an empty form for the user to fill in
        t = loader.get_template('librarian/datasets_add_archive.html')
        c['archiveAddDatasetForm'] = ArchiveAddDatasetForm(user=request.user)

    return HttpResponse(t.render(c, request))


@login_required
def datasets_add_bulk(request):
    """
    Add datasets in bulk to db.  Redirect to /datasets_bulk view so user can examine upload
    status of each dataset.
    """
    # Redirect to page to allow user to view status of added datasets.
    c = {}
    if request.method == 'POST':
        try:
            # Add new datasets.
            bulk_add_dataset_form = BulkAddDatasetForm(data=request.POST,
                                                       files=request.FILES,
                                                       user=request.user)
            isok = bulk_add_dataset_form.is_valid()
            if isok:
                CDT_obj, add_results = bulk_add_dataset_form.create_datasets(request.user)
                isok = len(add_results) > 0
            if not isok:
                # give up and let user try again
                t = loader.get_template('librarian/datasets_add_bulk.html')
                c = {'bulkAddDatasetForm': bulk_add_dataset_form}
                return HttpResponse(t.render(c, request))

            # Generate response.
            uploaded_files = bulk_add_dataset_form.cleaned_data["dataset_files"]
            if len(uploaded_files) != len(add_results):
                raise RuntimeError("List length mismatch")

            t = loader.get_template('librarian/datasets_bulk.html')
            bulk_display_results = []
            # Fill in default values for the form fields
            for add_result, upload_info in zip(add_results, uploaded_files):
                display_result = {}
                if isinstance(add_result, dict):
                    # dataset is invalid
                    display_result["name"] = add_result["name"]
                    display_result["description"] = ""
                    display_result["orig_filename"] = add_result["name"]
                    display_result["filesize"] = add_result["size"]
                    display_result["md5"] = ""
                    display_result["id"] = ""
                    display_result["is_valid"] = False
                else:
                    display_result["name"] = add_result.name
                    display_result["description"] = add_result.description
                    # This is the original filename as uploaded by the client, not the filename as stored
                    # on the file server.
                    display_result["orig_filename"] = upload_info[1].name
                    display_result["filesize"] = add_result.get_formatted_filesize()
                    display_result["md5"] = add_result.compute_md5()
                    display_result["id"] = add_result.id
                    display_result["is_valid"] = True
                bulk_display_results.append(display_result)

            BulkDatasetUpdateFormSet = formset_factory(form=BulkDatasetUpdateForm, max_num=len(bulk_display_results))
            bulk_dataset_update_formset = BulkDatasetUpdateFormSet(initial=bulk_display_results)

            # Fill in the attributes that are not fields in the form
            # These are not set by the BulkDatasetUpdateFormSet(initial=...) parameter
            for dataset_form, display_result, add_result in zip(bulk_dataset_update_formset,
                                                                bulk_display_results,
                                                                add_results):
                if display_result["is_valid"]:
                    dataset_form.dataset = add_result
                    dataset_form.status = BulkDatasetDisplay.STATUS_SUCCESS
                else:
                    dataset_form.dataset = Dataset()
                    dataset_form.non_field_errors = add_result["errstr"]
                    dataset_form.status = BulkDatasetDisplay.STATUS_FAIL

            # finally, add some other pertinent information which the template will display
            num_files_added = sum([a["is_valid"] for a in bulk_display_results])
            c["bulk_dataset_formset"] = bulk_dataset_update_formset
            c["num_files_selected"] = len(add_results)
            c["num_files_added"] = num_files_added
            c["cdt_typestr"] = "Unstructured" if CDT_obj is None else CDT_obj

        except ValidationError as e:
            LOGGER.exception(e.message)
            bulk_add_dataset_form.add_error(None, e)
            c.update({'bulkAddDatasetForm': bulk_add_dataset_form})

    else:  # return an empty form for the user to fill in
        t = loader.get_template('librarian/datasets_add_bulk.html')
        c.update({'bulkAddDatasetForm': BulkAddDatasetForm(user=request.user)})

    return HttpResponse(t.render(c, request))


@login_required
def datasets_bulk(request):
    """
    View recently added bulk datasets in /datasets_bulk.html view so that user can keep editing the
    recently bulk-added datasets without having to filter through all datasets in the /datasets.html page.
    Now the user wants to edit those bulk datasets.  Redirect to /datasets_update_bulk.html
    :param request:
    :return:
    """
    c = {}
    t = loader.get_template('librarian/datasets_bulk.html')
    if request.method == 'POST':
        BulkDatasetUpdateFormSet = formset_factory(form=BulkDatasetUpdateForm)
        bulk_dataset_update_formset = BulkDatasetUpdateFormSet(request.POST)
        # TODO: skip the datasets that are invalid

        all_good = True
        try:
            if bulk_dataset_update_formset.is_valid():
                with transaction.atomic():
                    for bulk_dataset_form in bulk_dataset_update_formset:
                        try:
                            bulk_dataset_form.update()
                        except (Dataset.DoesNotExist, KeyError) as e:
                            bulk_dataset_form.add_error(None, e)
                            # Re-raise this exception so we roll back the transaction.
                            raise e

            else:
                # All the forms have now been annotated with errors.
                all_good = False
        except (Dataset.DoesNotExist, KeyError) as e:
            all_good = False

        if all_good:
            # Success!
            return HttpResponseRedirect("datasets")
        else:
            # Failure!
            c.update({'bulk_dataset_formset': bulk_dataset_update_formset})
            t = loader.get_template("librarian/datasets_bulk.html")

    else:
        # You must access the /datasets_bulk.html page by adding datasets in bulk form /datasets_add_bulk.html
        # A GET to /datasets_bulk.html will only redirect to you the /dataset_add_bulk.html page
        t = loader.get_template('librarian/datasets_add_bulk.html')
        bulk_dataset_form = BulkAddDatasetForm(user=request.user)
        c.update({'bulkAddDatasetForm': bulk_dataset_form})

    return HttpResponse(t.render(c, request))


@login_required
def dataset_lookup(request, filename=None, filesize=None, md5_checksum=None):
    """Perform a lookup for runs involving a particular file.
    The search is performed on the basis of the md5_checksum, NOT the file name.
    The file name is only displayed in the HTML template.

    filename: a string that has been encoded using encodeURIComponent in javascript.
    See lookup.js for details.

    filesize: the size of the file in bytes. Note that this is a string.
    This is used in the following way:
     a) the user is warned if the size is zero
     b) the user is warned if the size is smaller than 1K

    The number of datasets displayed is limited to DISPLAY_LIMIT .
    If more matches are found, a warning is displayed.

    """
    # previous versions of this code seemed to expect a POST request.
    # However, in actual fact, we are sent a GET request, where the three parameters
    # have been calculated in the browser in javascript
    if request.method == 'POST':
        raise RuntimeError("NOT expecting a POST ")
    DISPLAY_LIMIT = 50
    ONE_KB = 1024
    if filename is None or filesize is None or md5_checksum is None:
        raise RuntimeError("filename or md5_sum is missing")
    try:
        filesize_int = int(filesize)
    except:
        raise RuntimeError("integer conversion error")
    filename = urllib.unquote(filename)
    dataset_query = librarian.models.Dataset.filter_by_user(request.user).filter(
        MD5_checksum=md5_checksum).exclude(file_source=None).order_by('-date_created')
    num_datasets = dataset_query.count()
    datasets = list(dataset_query[:DISPLAY_LIMIT])

    q_set_a = RunInput.objects.filter(dataset__user=request.user)
    runinput_query = q_set_a.filter(dataset__MD5_checksum=md5_checksum).order_by('-dataset__date_created')

    num_runinputs = len(runinput_query)
    runinputs = list(runinput_query[:DISPLAY_LIMIT])
    t = loader.get_template('librarian/dataset_lookup.html')
    c = {'datasets': datasets,
         'num_datasets': num_datasets,
         'toomany_datasets': num_datasets > DISPLAY_LIMIT,
         'runinputs': runinputs,
         'num_runinputs': num_runinputs,
         'toomany_runinputs': num_runinputs > DISPLAY_LIMIT,
         'md5': md5_checksum,
         'search_term': filename,
         'display_limit_num': DISPLAY_LIMIT,
         'file_size': filesize,
         'file_is_empty': filesize_int == 0,
         'file_is_small': 0 < filesize_int < ONE_KB}
    return HttpResponse(t.render(c, request))


@login_required
def lookup(request):
    t = loader.get_template("librarian/lookup.html")
    return HttpResponse(t.render({}, request))
