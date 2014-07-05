"""
archive views
"""
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.core.context_processors import csrf
from archive.models import Dataset
from archive.forms import DatasetForm, BulkDatasetForm
import logging

LOGGER = logging.getLogger(__name__)


def datasets(request):
    """
    Display a list of all Datasets in database
    """
    t = loader.get_template('archive/datasets.html')
    datasets = Dataset.objects.all()
    c = Context({'datasets': datasets})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def datasets_add(request):
    """
    Add datasets to db.
    """

    if request.method == 'POST':
        single_dataset_form = DatasetForm(request.POST, request.FILES, prefix="single")
        bulk_dataset_form = BulkDatasetForm(request.POST, request.FILES, prefix="bulk")

        try:

            if "singleSubmit" in single_dataset_form.data and single_dataset_form.is_valid():
                    single_dataset_form.create_dataset()
            elif "bulkSubmit" in bulk_dataset_form.data and bulk_dataset_form.is_valid():
                    bulk_dataset_form.create_datasets()
            else:
                raise Exception("Invalid form submission")

            datasets = Dataset.objects.all()    # Once saved, let user browse table of all datasets
            t = loader.get_template('archive/datasets.html')
            c = Context({'datasets': datasets})
        except Exception, e:
            create_error = "Error while adding datasets.  " + str(e)
            t = loader.get_template('archive/datasets_add.html')
            c = Context({'singleDataset': single_dataset_form, 'bulkDataset': bulk_dataset_form, 'create_error': create_error})
            LOGGER.exception(e.message)

    else:  # return an empty formset for the user to fill in
        single_dataset_form = DatasetForm(prefix="single")
        bulk_dataset_form = BulkDatasetForm(prefix="bulk")
        t = loader.get_template('archive/datasets_add.html')
        c = Context({'singleDataset': single_dataset_form, 'bulkDataset': bulk_dataset_form})

    c.update(csrf(request))
    return HttpResponse(t.render(c))
