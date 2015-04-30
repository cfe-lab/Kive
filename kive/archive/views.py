"""
archive views
"""
import hashlib
import logging
import mimetypes
import os

from django.contrib.auth.decorators import login_required
from django.core.servers.basehttp import FileWrapper
from django.core.exceptions import ValidationError
from django.core.urlresolvers import reverse
from django.db import transaction
from django.forms.formsets import formset_factory
from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.template import loader, RequestContext
from rest_framework.authentication import SessionAuthentication, BasicAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status as rf_status

from archive.forms import DatasetForm, BulkAddDatasetForm, BulkDatasetUpdateForm
from archive.models import Dataset, MethodOutput
from archive.serializers import DatasetSerializer
import librarian.models
from metadata.models import CompoundDatatype
from metadata.serializers import CompoundDatatypeInputSerializer
from portal.views import admin_check
import json

LOGGER = logging.getLogger(__name__)


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication))
@permission_classes((IsAuthenticated,))
def api_dataset_home(request):
    dataset_dir = {
        'directory': {
            name: reverse(name) for name in [
                'api_get_dataset',
                'api_dataset_add',
                'api_get_cdts']
        }
    }
    return Response(dataset_dir)


@login_required
def datasets(request):
    """
    Display a list of all Datasets in database
    """
    t = loader.get_template('archive/datasets.html')

    accessible_SDs = librarian.models.SymbolicDataset.filter_by_user(request.user)
    datasets = Dataset.objects.filter(symbolicdataset__in=accessible_SDs)

    c = RequestContext(request, {
        'datasets': datasets,
        'dataset_json':  json.dumps(DatasetSerializer(datasets, many=True).data)
    })

    c['is_user_admin'] = admin_check(request.user)

    return HttpResponse(t.render(c))


@api_view(['GET', 'POST'])
@authentication_classes((SessionAuthentication, BasicAuthentication))
@permission_classes((IsAuthenticated,))
def api_get_datasets(request, page=0):
    pagesize = 100
    page = int(page)

    accessible_sds = librarian.models.SymbolicDataset.filter_by_user(request.user)
    datasets = Dataset.objects.filter(symbolicdataset__in=accessible_sds)

    if page >= 0:
        datasets = datasets[page*pagesize: (page+1)*pagesize]

    next_url = None
    if len(datasets) == pagesize:
        next_url = reverse('api_get_dataset_page', kwargs={'page': page+1})
    dataset_list = {
        'next': next_url,
        'datasets': DatasetSerializer(datasets, many=True).data,
    }
    return Response(dataset_list)


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication))
@permission_classes((IsAuthenticated,))
def api_get_cdts(request):
    cdts = CompoundDatatype.objects.all()
    cdt_dir = {
        'compoundtypes': CompoundDatatypeInputSerializer(cdts, many=True).data,
    }
    return Response(cdt_dir)


def _build_download_response(source_file):
    file_chunker = FileWrapper(source_file) # stream file in chunks to avoid overloading memory
    mimetype = mimetypes.guess_type(source_file.url)[0]
    response = HttpResponse(file_chunker, content_type=mimetype)
    response['Content-Length'] = source_file.size
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(os.path.basename(source_file.name))
    return response


@login_required
def dataset_download(request, dataset_id):
    """
    Retrieve the file associated with the dataset for client download.
    """
    try:
        accessible_SDs = librarian.models.SymbolicDataset.filter_by_user(request.user)
        dataset = Dataset.objects.get(symbolicdataset__in=accessible_SDs, pk=dataset_id)
    except Dataset.DoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id))

    return _build_download_response(dataset.dataset_file)


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication))
@permission_classes((IsAuthenticated,))
def api_dataset_download(request, dataset_id):
    try:
        accessible_SDs = librarian.models.SymbolicDataset.filter_by_user(request.user)
        dataset = Dataset.objects.get(symbolicdataset__in=accessible_SDs, pk=dataset_id)
    except Dataset.DoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id)) # TODO: JSON ERROR

    return _build_download_response(dataset.dataset_file)


@login_required
def dataset_view(request, dataset_id):
    """
    Display the file associated with the dataset in the browser.
    """
    try:
        accessible_SDs = librarian.models.SymbolicDataset.filter_by_user(request.user)
        dataset = Dataset.objects.get(symbolicdataset__in=accessible_SDs, pk=dataset_id)
    except Dataset.DoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id))

    if dataset.symbolicdataset.is_raw():
        return _build_raw_viewer(request, dataset.dataset_file, dataset.name, dataset.get_absolute_url())

    # If we have a mismatched output, we do an alignment
    # over the columns
    col_matching, processed_rows = None, dataset.rows(True)
    if not dataset.content_matches_header:
        col_matching, insert = dataset.column_alignment()
        processed_rows = dataset.rows(data_check=True, insert_at=insert)

    t = loader.get_template("archive/dataset_view.html")
    c = RequestContext(request, {"dataset": dataset, 'column_matching': col_matching, 'processed_rows': processed_rows})
    return HttpResponse(t.render(c))

def _build_raw_viewer(request, file, name, download=None):
    t = loader.get_template("archive/raw_view.html")
    c = RequestContext(request, {"file": file, "name": name, 'download': download})
    return HttpResponse(t.render(c))
    

@login_required
def stdout_download(request, methodoutput_id):
    """
    Display the standard output associated with the method output in the browser.
    """
    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return _build_download_response(methodoutput.output_log)

@login_required
def stdout_view(request, methodoutput_id):
    """
    Display the standard output associated with the method output in the browser.
    """
    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return _build_raw_viewer(request, methodoutput.output_log, 'Standard out', methodoutput.get_absolute_log_url())

@login_required
def stderr_download(request, methodoutput_id):
    """
    Display the standard output associated with the method output in the browser.
    """
    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return _build_download_response(methodoutput.error_log)

@login_required
def stderr_view(request, methodoutput_id):
    """
    Display the standard error associated with the method output in the browser.
    """
    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return _build_raw_viewer(request, methodoutput.error_log, 'Standard error', methodoutput.get_absolute_error_url())


@login_required
def datasets_add(request):
    """
    Add datasets to db.
    """
    c = RequestContext(request)
    if request.method == 'POST':
        single_dataset_form = DatasetForm(request.POST, request.FILES, user=request.user, prefix="single")

        success = True
        try:
            if "singleSubmit" not in single_dataset_form.data:
                single_dataset_form.add_error(None, "Invalid form submission")
                success = False
            elif single_dataset_form.is_valid():
                single_dataset_form.create_dataset(request.user)
            else:
                success = False

        except (AttributeError, ValidationError, ValueError) as e:
            LOGGER.exception(e.message)
            success = False
            single_dataset_form.add_error(None, e)

        if success:
            return HttpResponseRedirect("datasets")
        else:
            t = loader.get_template('archive/datasets_add.html')
            c.update({'singleDataset': single_dataset_form})


    else:  # return an empty formset for the user to fill in
        single_dataset_form = DatasetForm(user=request.user, prefix="single")
        t = loader.get_template('archive/datasets_add.html')
        c.update({'singleDataset': single_dataset_form})

    return HttpResponse(t.render(c))


@api_view(['post'])
@authentication_classes((SessionAuthentication, BasicAuthentication))
@permission_classes((IsAuthenticated,))
def api_dataset_add(request):
    single_dataset_form = DatasetForm(request.POST, request.FILES, user=request.user, prefix="")

    symdataset = None
    if single_dataset_form.is_valid():
        symdataset = single_dataset_form.create_dataset(request.user)

    if symdataset is None:
        return Response({'errors': single_dataset_form.errors}, status=rf_status.HTTP_400_BAD_REQUEST)

    resp = {
        'dataset': DatasetSerializer(symdataset.dataset).data
    }

    return Response(resp)


class BulkDatasetDisplay:
    """
    Helper class for displaying
    """
    STATUS_FAIL = 1
    STATUS_SUCCESS = 0


@login_required
def datasets_add_bulk(request):
    """
    Add datasets in bulk to db.  Redirect to /datasets_bulk view so user can examine upload status of each dataset.
    """
    # Redirect to page to allow user to view status of added datasets.
    c = RequestContext(request)
    t = loader.get_template('archive/datasets_bulk.html')
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
                    bulk_display_result["description"] =  ""
                    bulk_display_result["orig_filename"] =  ""
                    bulk_display_result["filesize"] =  ""
                    bulk_display_result["md5"] =  ""
                    bulk_display_result["id"] = ""
                else:
                    bulk_display_result["name"] =  add_results[i].name
                    bulk_display_result["description"] =  add_results[i].description
                    # This is the original filename as uploaded by the client, not the filename as stored
                    # on the file server.
                    bulk_display_result["orig_filename"] = uploaded_files[i].name
                    bulk_display_result["filesize"] =  add_results[i].get_formatted_filesize()
                    bulk_display_result["md5"] =  add_results[i].compute_md5()
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
            t = loader.get_template('archive/datasets_add_bulk.html')
            c.update({'bulkAddDatasetForm': bulk_add_dataset_form})

    else:  # return an empty form for the user to fill in
        t = loader.get_template('archive/datasets_add_bulk.html')
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
    t = loader.get_template('archive/datasets_bulk.html')
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
            t = loader.get_template("archive/datasets_bulk.html")

    else:
        # You must access the /datasets_bulk.html page by adding datasets in bulk form /datasets_add_bulk.html
        # A GET to /datasets_bulk.html will only redirect to you the /dataset_add_bulk.html page
        t = loader.get_template('archive/datasets_add_bulk.html')
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

    datasets = librarian.models.SymbolicDataset.filter_by_user(request.user).filter(MD5_checksum=md5_checksum)
    t = loader.get_template('archive/dataset_lookup.html')
    c = RequestContext(request, {'datasets': datasets, 'md5': md5_checksum})

    return HttpResponse(t.render(c))


@login_required
def lookup(request):
    t = loader.get_template("archive/lookup.html")
    c = RequestContext(request, {})
    return HttpResponse(t.render(c))


# def datasets_update_bulk(request):
#     """
#     Edit recently added bulk datasets in /datasets_udate_bulk.html
#     """
#     t = loader.get_template('archive/datasets_update_bulk.html')
#     if request.method == 'POST':  # User wants to submit edits to to datasets
#         try:
#             DatasetModelFormset = modelformset_factory(model=Dataset, form=BulkDatasetUpdateForm)
#             bulk_dataset_modelformset = DatasetModelFormset(request.POST)
#
#             if bulk_dataset_modelformset.is_valid():
#                 with transaction.atomic():
#                     bulk_dataset_modelformset.save()
#             else:
#                 raise ValueError("Invalid form items")
#
#             c = Context({'datasets.formset': bulk_dataset_modelformset})
#
#         except Exception, e:
#             update_error = "Error while adding datasets.  " + str(e)
#             c = Context({'datasets.formset': bulk_dataset_modelformset, 'update_error': update_error})
#             LOGGER.exception(e.message)
#
#     else:
#         # Prepopulate formset with the sucessfully added bulk datasets
#         DatasetModelFormset =modelformset_factory(model=Dataset, form=BulkDatasetUpdateForm)
#         bulk_dataset_modelformset = DatasetModelFormset(request.GET)
#
#         c = Context({'datasets.formset': bulk_dataset_modelformset})
#
#     c.update(csrf(request))
#     return HttpResponse(t.render(c))
