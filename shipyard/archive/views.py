"""
archive views
"""
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.core.context_processors import csrf
from archive.models import Dataset

def datasets(request):
    """
    Display a list of all Datasets in database
    """
    t = loader.get_template('archive/datasets.html')
    datasets = Dataset.objects.filter(content_type==None)
    c = Context()
    c.update(csrf(request))
    return HttpResponse(t.render(c))


