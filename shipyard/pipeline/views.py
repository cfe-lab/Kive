"""
pipeline views
"""

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.core.context_processors import csrf
from method.models import Method, MethodFamily

def pipelines(request):
    """
    Display a list of all code resources (parents) in database
    """
    t = loader.get_template('pipeline/pipelines.html')
    method_families = MethodFamily.objects.all()
    methods = Method.objects.all()
    c = Context({'method_families': method_families, 'methods': methods})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
