"""
archive views
"""
from django.http import HttpResponse, HttpResponseRedirect, HttpResponseBadRequest
from django.template import loader, Context
from django.core.context_processors import csrf
from django.core.servers.basehttp import FileWrapper
from django.core.exceptions import ValidationError
from django.forms.models import modelformset_factory
from django.forms.formsets import formset_factory
from django.db import transaction
from django.contrib.auth.decorators import login_required

import mimetypes
import os
import logging

from archive.models import Dataset
from archive.forms import DatasetForm, BulkAddDatasetForm, BulkDatasetUpdateForm


LOGGER = logging.getLogger(__name__)


@login_required
def datasets(request):
    """
    Display a list of all Datasets in database
    """
    t = loader.get_template('archive/datasets.html')
    datasets = Dataset.objects.all()
    c = Context({'datasets': datasets, "user": request.user})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


@login_required
def dataset_download(request, dataset_id):
    """
    Retrieve the file associated with the dataset for client download.
    """
    dataset = Dataset.objects.filter(id=dataset_id).get()

    file_chunker = FileWrapper(dataset.dataset_file)  # stream file in chunks to avoid overloading memory
    mimetype = mimetypes.guess_type(dataset.dataset_file.url)[0]
    response = HttpResponse(file_chunker, content_type=mimetype)
    response['Content-Length'] = dataset.get_filesize()
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(os.path.basename(dataset.dataset_file.name))
    return response


@login_required
def dataset_view(request, dataset_id):
    """
    Display the file associated with the dataset in the browser.
    """
    t = loader.get_template("archive/dataset_view.html")
    dataset = Dataset.objects.filter(id=dataset_id).get()
    header = []
    if not dataset.symbolicdataset.is_raw():
        for column in dataset.symbolicdataset.compounddatatype.members.order_by("column_idx"):
            header.append(column.column_name)

    c = Context({"dataset": dataset, "header": header, "user": request.user})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


@login_required
def datasets_add(request):
    """
    Add datasets to db.
    """

    if request.method == 'POST':
        single_dataset_form = DatasetForm(request.POST, request.FILES, prefix="single")

        success = True
        try:
            if "singleSubmit" not in single_dataset_form.data:
                single_dataset_form.add_error(None, "Invalid form submission")
                success = False
            elif single_dataset_form.is_valid():
                single_dataset_form.create_dataset(request.user)
            else:
                success = False

        except (AttributeError, ValidationError) as e:
            LOGGER.exception(e.message)
            success = False
            single_dataset_form.add_error(None, e)

        if success:
            return HttpResponseRedirect("datasets")
        else:
            t = loader.get_template('archive/datasets_add.html')
            c = Context({'singleDataset': single_dataset_form})


    else:  # return an empty formset for the user to fill in
        single_dataset_form = DatasetForm(prefix="single")
        t = loader.get_template('archive/datasets_add.html')
        c = Context({'singleDataset': single_dataset_form})

    c.push(user=request.user)
    c.update(csrf(request))
    return HttpResponse(t.render(c))


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
    t = loader.get_template('archive/datasets_bulk.html')
    if request.method == 'POST':
        try:
            # Add new datasets.
            bulk_add_dataset_form = BulkAddDatasetForm(data=request.POST, files=request.FILES)
            if bulk_add_dataset_form.is_valid():
                add_results = bulk_add_dataset_form.create_datasets(request.user)
            else:
                # The form is already annotated with the appropriate errors, so we can bail.
                c = Context({'bulkAddDatasetForm': bulk_add_dataset_form, "user": request.user})
                c.update(csrf(request))
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

            c = Context({"bulk_dataset_formset": bulk_dataset_update_formset})

        except ValidationError as e:
            LOGGER.exception(e.message)
            bulk_add_dataset_form.add_error(None, e)
            t = loader.get_template('archive/datasets_add_bulk.html')
            c = Context({'bulkAddDatasetForm': bulk_add_dataset_form})

    else:  # return an empty form for the user to fill in
        t = loader.get_template('archive/datasets_add_bulk.html')
        bulk_dataset_form = BulkAddDatasetForm()
        c = Context({'bulkAddDatasetForm': bulk_dataset_form})

    c.push({"user": request.user})
    c.update(csrf(request))
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
            c = Context({'bulk_dataset_formset': bulk_dataset_update_formset})
            t = loader.get_template("archive/datasets_bulk.html")

    else:
        # You must access the /datasets_bulk.html page by adding datasets in bulk form /datasets_add_bulk.html
        # A GET to /datasets_bulk.html will only redirect to you the /dataset_add_bulk.html page
        t = loader.get_template('archive/datasets_add_bulk.html')
        bulk_dataset_form = BulkAddDatasetForm()
        c = Context({'bulkAddDatasetForm': bulk_dataset_form})

    c.push({"user": request.user})
    c.update(csrf(request))
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
