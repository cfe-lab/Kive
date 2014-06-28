"""
archive views
"""
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.core.context_processors import csrf
from archive.models import Dataset
from django.forms.models import model_to_dict
from django.forms.formsets import formset_factory
from archive.forms import DatasetForm, BulkDatasetForm


from librarian.models import SymbolicDataset
from django.contrib.auth.models import User
from metadata.models import CompoundDatatype



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
    Display a list of all Datasets in database
    """
    t = loader.get_template('archive/datasets_add.html')
    dataset_formset_factory = formset_factory(DatasetForm)
    if request.method == 'POST':
        t = loader.get_template('archive/datasets_add.html')
        username = "shipyard"  # TODO:  do not hardcode this

        # TODO:  do the files get uploaded in anyway?
        # TODO:  what if the file exists on the client but not on the webserver?
        dataset_formset = dataset_formset_factory(request.POST, request.FILES)


        # get the dataset files
        for dataset_form in dataset_formset:
            dataset_form.save()
            # file_location = dataset_form['dataset_file']
            # name = dataset_form['name']
            # description = dataset_form['description']
            # datatype_id = dataset_form['datatype']
            #
            # # Don't let user save dataset that already has the same name or file location
            # # TODO:  what is the definition of uniqueness for DataSets?
            # # There is no db uniqueness constraint on name and file location.
            # # there is only an MD5 check but the user should be allowed to modify datasets.
            # if Dataset.objects.exists(name=name):
            #     raise Exception("Dataset already exists with same name")
            # elif Dataset.objects.exists(dataset_file=file_location):
            #     raise Exception("Dataset already exists with same file location")
            #
            # # TODO:  allower user to modify existing file location of dataset?
            #
            # compound_datatype_obj = None
            # if datatype_id != Dataset.COMPOUND_DATA_TYPE_ID_RAW:
            #     compound_datatype_obj = CompoundDatatype.objects.get(pk=datatype_id)
            #
            # user_obj = User.objects.get(username=username)
            # SymbolicDataset.create_SD(file_location, cdt=compound_datatype_obj, make_dataset=True, user=user_obj,
            #                           name=name, description=description, created_by=None, check=True)

    else:  # return an empty formset for the user to fill in
        dataset_formset = dataset_formset_factory()

    c = Context({'datasets': dataset_formset})
    c.update(csrf(request))
    return HttpResponse(t.render(c))