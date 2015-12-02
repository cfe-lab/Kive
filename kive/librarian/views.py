"""
librarian views
"""
import hashlib
import logging
import mimetypes
import os
import itertools

from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.core.urlresolvers import reverse
from django.db import transaction
from django.forms.formsets import formset_factory
from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.template import loader, RequestContext
from django.core.servers.basehttp import FileWrapper
from django.conf import settings
from django.contrib.auth.models import User, Group

from librarian.forms import DatasetForm, DatasetDetailsForm, BulkAddDatasetForm, BulkDatasetUpdateForm,\
    ArchiveAddDatasetForm
from archive.models import Run
from librarian.models import Dataset
from portal.views import admin_check
from metadata.models import CompoundDatatype
import librarian.models

LOGGER = logging.getLogger(__name__)


def _build_download_response(source_file):
    file_chunker = FileWrapper(source_file)  # Stream file in chunks to avoid overloading memory.
    mimetype = mimetypes.guess_type(source_file.url)[0]
    response = HttpResponse(file_chunker, content_type=mimetype)
    response['Content-Length'] = source_file.size
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(os.path.basename(source_file.name))
    return response


def _build_raw_viewer(request, file, name, download=None, return_to_url=None):
    t = loader.get_template("librarian/raw_view.html")
    c = RequestContext(request, {"file": file, "name": name, 'download': download, 'return': return_to_url})
    return HttpResponse(t.render(c))


@login_required
def datasets(request):
    """
    Display a list of all Datasets in database
    """
    t = loader.get_template('librarian/datasets.html')
    c = RequestContext(request)
    c['is_user_admin'] = admin_check(request.user)

    return HttpResponse(t.render(c))


@login_required
def dataset_download(request, dataset_id):
    """
    Retrieve the file associated with the dataset for client download.
    """
    try:
        dataset = librarian.models.Dataset.filter_by_user(request.user).get(pk=dataset_id)
    except Dataset.DoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id))

    return _build_download_response(dataset.dataset_file)


@login_required
def dataset_view(request, dataset_id):
    """
    Display the file associated with the dataset in the browser, or update its name/description.
    """
    return_to_run = request.GET.get('run_id', None)
    is_view_results = "view_results" in request.GET
    is_view_run = "view_run" in request.GET
    return_url = None
    if return_to_run is not None:
        if is_view_run:
            return_url = reverse('view_run', kwargs={'run_id': return_to_run})
        elif is_view_results:
            return_url = reverse('view_results', kwargs={'run_id': return_to_run})

    try:
        if admin_check(request.user):
            dataset = Dataset.objects.prefetch_related(
                'structure',
                'structure__compounddatatype',
                'structure__compounddatatype__members',
                'structure__compounddatatype__members__datatype',
                'structure__compounddatatype__members__datatype__basic_constraints'
            ).get(pk=dataset_id)

        else:
            accessible_datasets = librarian.models.Dataset.filter_by_user(request.user)
            dataset = Dataset.objects.prefetch_related(
                'structure',
                'structure__compounddatatype',
                'structure__compounddatatype__members',
                'structure__compounddatatype__members__datatype',
                'structure__compounddatatype__members__datatype__basic_constraints'
            ).get(pk__in=accessible_datasets, pk=dataset_id)

    except Dataset.DoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id))

    # Figure out which users and groups could be given access to this Dataset.
    # If the Dataset is uploaded, it's anyone who doesn't already have access;
    # if it was generated, it's anyone who had access to the generating run.
    user_pks_already_allowed = dataset.users_allowed.values_list("pk", flat=True)
    group_pks_already_allowed = dataset.groups_allowed.values_list("pk", flat=True)

    all_potential_users = (User.objects.all() if dataset.file_source is None
                           else dataset.file_source.top_level_run.users_allowed.all())
    addable_users = all_potential_users.exclude(
        pk__in=itertools.chain([dataset.user.pk], user_pks_already_allowed)
    )

    all_potential_groups = (Group.objects.all() if dataset.file_source is None
                            else dataset.file_source.top_level_run.groups_allowed.all())
    addable_groups = all_potential_groups.exclude(pk__in=group_pks_already_allowed)

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

                return HttpResponseRedirect("/datasets")
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

    c = RequestContext(
        request,
        {
            "is_admin": admin_check(request.user),
            "is_owner": dataset.user == request.user,
            "dataset": dataset,
            "return": return_url,
            "dataset_form": dataset_form
        }
    )
    if dataset.is_raw():
        t = loader.get_template("librarian/raw_dataset_view.html")
    else:
        # If we have a mismatched output, we do an alignment
        # over the columns.
        if dataset.content_matches_header:
            col_matching, processed_rows = None, dataset.rows(True, limit=settings.DATASET_DISPLAY_MAX)
        else:
            col_matching, insert = dataset.column_alignment()
            processed_rows = dataset.rows(data_check=True,
                                          insert_at=insert,
                                          limit=settings.DATASET_DISPLAY_MAX)

        t = loader.get_template("librarian/csv_dataset_view.html")
        c.update(
            {
                'column_matching': col_matching,
                'processed_rows': processed_rows,
                "DATASET_DISPLAY_MAX": settings.DATASET_DISPLAY_MAX
            }
        )
    return HttpResponse(t.render(c))


@login_required
def datasets_add(request):
    """
    Add datasets to db.
    """
    t = loader.get_template('librarian/datasets_add.html')
    c = RequestContext(request)
    if request.method == 'POST':
        # The new Dataset; we need it here for validation purposes.

        ds = Dataset(user=request.user)
        df = DatasetForm(request.POST, request.FILES, instance=ds, user=request.user,
                         prefix="single")

        success = True
        try:
            if "singleSubmit" not in df.data:
                df.add_error(None, "Invalid form submission")
                success = False

            elif df.is_valid():

                cdt = None
                if df.cleaned_data['compound_datatype'] != CompoundDatatype.RAW_ID:
                    cdt = CompoundDatatype.objects.get(pk=df.cleaned_data['compound_datatype'])

                with transaction.atomic():
                    ds = Dataset.create_dataset(
                        file_path=None,
                        user=request.user,
                        cdt=cdt,
                        keep_file=True,
                        name=df.cleaned_data['name'],
                        description=df.cleaned_data['description'],
                        file_source=None,
                        check=True,
                        file_handle=df.cleaned_data['dataset_file'],
                        instance=ds
                    )
                    ds.grant_from_json(df.cleaned_data["permissions"])

                    ds.validate_uniqueness_on_upload()
            else:
                success = False

        except (AttributeError, ValidationError, ValueError) as e:
            LOGGER.exception(e.message)
            success = False
            df.add_error(None, e)

        if success:
            return HttpResponseRedirect("datasets")
        else:
            c.update({'singleDataset': df})

    else:  # return an empty formset for the user to fill in
        df = DatasetForm(user=request.user, prefix="single")
        c.update({'singleDataset': df})

    return HttpResponse(t.render(c))


class BulkDatasetDisplay:
    """
    Helper class for displaying
    """
    STATUS_FAIL = 1
    STATUS_SUCCESS = 0


@login_required
def datasets_add_archive(request):
    """
    Add datasets in bulk to db.  Redirect to /datasets_bulk view so user can examine upload status of each dataset.
    """
    # Redirect to page to allow user to view status of added datasets.
    c = RequestContext(request)
    t = loader.get_template('librarian/datasets_bulk.html')
    archive_add_dataset_form = None

    # If we got posted to, try to create DB entries
    if request.method == 'POST':
        try:
            archive_add_dataset_form = ArchiveAddDatasetForm(
                data=request.POST,
                files=request.FILES,
                user=request.user
            )

            # Try to add new datasets
            if archive_add_dataset_form.is_valid():
                add_results = archive_add_dataset_form.create_datasets(request.user)
            else:
                # TODO: change this
                c.update({'archiveAddDatasetForm': archive_add_dataset_form})
                return HttpResponse(t.render(c))

            # New datasets added, generate a response
            archive_display_results = []

            # Fill in default values for the form fields
            for i in range(len(add_results)):
                archive_display_result = {}
                uploaded_files = archive_add_dataset_form.cleaned_data["dataset_file"]
                if isinstance(add_results[i], basestring):
                    archive_display_result["name"] = ""
                    archive_display_result["description"] = ""
                    archive_display_result["orig_filename"] = ""
                    archive_display_result["filesize"] = ""
                    archive_display_result["md5"] = ""
                    archive_display_result["id"] = ""
                else:
                    archive_display_result["name"] = add_results[i].name
                    archive_display_result["description"] = add_results[i].description
                    # This is the original filename as uploaded by the client, not the filename as stored
                    # on the file server.
                    archive_display_result["orig_filename"] = uploaded_files[i].name
                    archive_display_result["filesize"] = add_results[i].get_formatted_filesize()
                    archive_display_result["md5"] = add_results[i].compute_md5()
                    archive_display_result["id"] = add_results[i].id

                archive_display_results.extend([archive_display_result])

            BulkDatasetUpdateFormSet = formset_factory(form=BulkDatasetUpdateForm, max_num=len(archive_display_results))
            bulk_dataset_update_formset = BulkDatasetUpdateFormSet(initial=archive_display_results)

            # Fill in the attributes that are not fields in the form
            # These are not set by the BulkDatasetUpdateFormSet(initial=...) parameter
            for i in range(0, len(add_results)):
                dataset_form = bulk_dataset_update_formset[i]
                if dataset_form.initial.get("name"):
                    dataset_form.dataset = add_results[i]
                    dataset_form.err_msg = ""
                    dataset_form.status = BulkDatasetDisplay.STATUS_SUCCESS
                else:
                    dataset_form.dataset = Dataset()
                    dataset_form.err_msg = add_results[i]
                    dataset_form.status = BulkDatasetDisplay.STATUS_FAIL

            c.update({"bulk_dataset_formset": bulk_dataset_update_formset})

        except ValidationError as e:
            LOGGER.exception(e.message)
            archive_add_dataset_form.add_error(None, e)
            t = loader.get_template('librarian/datasets_add_archive.html')
            c.update({'archiveAddDatasetForm': archive_add_dataset_form})

    else:  # return an empty form for the user to fill in
        t = loader.get_template('librarian/datasets_add_archive.html')
        archive_dataset_form = ArchiveAddDatasetForm(user=request.user)
        c.update({'archiveAddDatasetForm': archive_dataset_form})

    return HttpResponse(t.render(c))


@login_required
def datasets_add_bulk(request):
    """
    Add datasets in bulk to db.  Redirect to /datasets_bulk view so user can examine upload status of each dataset.
    """
    # Redirect to page to allow user to view status of added datasets.
    c = RequestContext(request)
    t = loader.get_template('librarian/datasets_bulk.html')
    if request.method == 'POST':
        try:
            # Add new datasets.
            bulk_add_dataset_form = BulkAddDatasetForm(data=request.POST, files=request.FILES, user=request.user)
            if bulk_add_dataset_form.is_valid():
                add_results = bulk_add_dataset_form.create_datasets(request.user)
            else:
                # The form is already annotated with the appropriate errors, so we can bail.
                c.update({'bulkAddDatasetForm': bulk_add_dataset_form})
                return HttpResponse(t.render(c))

            # Generate response.
            bulk_display_results = []
            # Fill in default values for the form fields
            for i in range(len(add_results)):
                bulk_display_result = {}
                uploaded_files = bulk_add_dataset_form.cleaned_data["dataset_files"]
                if isinstance(add_results[i], basestring):
                    bulk_display_result["name"] = ""
                    bulk_display_result["description"] = ""
                    bulk_display_result["orig_filename"] = ""
                    bulk_display_result["filesize"] = ""
                    bulk_display_result["md5"] = ""
                    bulk_display_result["id"] = ""
                else:
                    bulk_display_result["name"] = add_results[i].name
                    bulk_display_result["description"] = add_results[i].description
                    # This is the original filename as uploaded by the client, not the filename as stored
                    # on the file server.
                    bulk_display_result["orig_filename"] = uploaded_files[i].name
                    bulk_display_result["filesize"] = add_results[i].get_formatted_filesize()
                    bulk_display_result["md5"] = add_results[i].compute_md5()
                    bulk_display_result["id"] = add_results[i].id

                bulk_display_results.extend([bulk_display_result])
            BulkDatasetUpdateFormSet = formset_factory(form=BulkDatasetUpdateForm, max_num=len(bulk_display_results))
            bulk_dataset_update_formset = BulkDatasetUpdateFormSet(initial=bulk_display_results)

            # Fill in the attributes that are not fields in the form
            # These are not set by the BulkDatasetUpdateFormSet(initial=...) parameter

            for i in range(0, len(add_results)):
                dataset_form = bulk_dataset_update_formset[i]
                if dataset_form.initial.get("name"):
                    dataset_form.dataset = add_results[i]
                    dataset_form.err_msg = ""
                    dataset_form.status = BulkDatasetDisplay.STATUS_SUCCESS
                else:
                    dataset_form.dataset = Dataset()
                    dataset_form.err_msg = add_results[i]
                    dataset_form.status = BulkDatasetDisplay.STATUS_FAIL

            c.update({"bulk_dataset_formset": bulk_dataset_update_formset})

        except ValidationError as e:
            LOGGER.exception(e.message)
            bulk_add_dataset_form.add_error(None, e)
            t = loader.get_template('librarian/datasets_add_bulk.html')
            c.update({'bulkAddDatasetForm': bulk_add_dataset_form})

    else:  # return an empty form for the user to fill in
        t = loader.get_template('librarian/datasets_add_bulk.html')
        bulk_dataset_form = BulkAddDatasetForm(user=request.user)
        c.update({'bulkAddDatasetForm': bulk_dataset_form})

    return HttpResponse(t.render(c))


@login_required
def datasets_bulk(request):
    """
    View recently added bulk datasets in /datasets_bulk.html view so that user can keep editing the
    recently bulk-added datasets without having to filter through all datasets in the /datasets.html page.
    Now the user wants to edit those bulk datasets.  Redirect to /datasets_update_bulk.html
    :param request:
    :return:
    """
    c = RequestContext(request)
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

    return HttpResponse(t.render(c))


@login_required
def dataset_lookup(request, md5_checksum=None):
    if md5_checksum is None and request.method == 'POST':
        checksum = hashlib.md5()
        if 'file' in request.FILES:
            for chunk in request.FILES['file'].chunks():
                checksum.update(chunk)
            md5_checksum = checksum.hexdigest()

    datasets = librarian.models.Dataset.filter_by_user(request.user).filter(
        MD5_checksum=md5_checksum).exclude(file_source=None)

    datasets_as_inputs = []
    runs = Run.objects.filter(inputs__dataset__MD5_checksum=md5_checksum)

    for run in runs:
        for input in [x for x in run.inputs.all() if x.dataset.MD5_checksum == md5_checksum]:
            breakout = False
            for d in datasets_as_inputs:
                if d["run_id"] == run.id and d["dataset"].id == input.dataset.id:
                    breakout = True
                    continue
            if breakout:
                continue

            datasets_as_inputs += [{
                "run": run.run,
                "pipeline": run.pipeline,
                "dataset": input.dataset
            }]

    t = loader.get_template('librarian/dataset_lookup.html')
    c = RequestContext(request, {'datasets': datasets, 'datasets_as_inputs': datasets_as_inputs, 'md5': md5_checksum})

    return HttpResponse(t.render(c))


@login_required
def lookup(request):
    t = loader.get_template("librarian/lookup.html")
    c = RequestContext(request, {})
    return HttpResponse(t.render(c))
