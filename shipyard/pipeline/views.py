"""
pipeline views
"""

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.core.context_processors import csrf
from method.models import Method, MethodFamily
from metadata.models import CompoundDatatype

def pipelines(request):
    """
    Display a list of all code resources (parents) in database
    """
    t = loader.get_template('pipeline/pipelines.html')
    method_families = MethodFamily.objects.all()
    compound_datatypes = CompoundDatatype.objects.all()
    c = Context({'method_families': method_families, 'compound_datatypes': compound_datatypes})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
