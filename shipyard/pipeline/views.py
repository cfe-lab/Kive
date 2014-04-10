"""
pipeline views
"""

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.core.context_processors import csrf
from method.models import MethodFamily
from metadata.models import CompoundDatatype
from pipeline.models import Pipeline
import json

def pipelines(request):
    """
    Display existing pipeline families, represented by the
    root members (without parent).
    """
    t = loader.get_template('pipeline/pipelines.html')
    pipelines = Pipeline.objects.filter(revision_parent=None)
    c = Context({'pipelines': pipelines})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def pipeline_add(request):
    """
    Most of the heavy lifting is done by JavaScript and HTML5.
    I don't think we need to use forms here.
    """
    t = loader.get_template('pipeline/pipeline_add.html')
    method_families = MethodFamily.objects.all()
    compound_datatypes = CompoundDatatype.objects.all()
    c = Context({'method_families': method_families, 'compound_datatypes': compound_datatypes})
    c.update(csrf(request))

    if request.method == 'POST':
        query = request.POST.dict()
        print query
        response_data = {'foo': 'bar'}
        return HttpResponse(json.dumps(response_data), content_type='application/json')
    else:
        return HttpResponse(t.render(c))


def pipeline_exec(request):
    t = loader.get_template('pipeline/pipeline_exec.html')
    method_families = MethodFamily.objects.all()
    compound_datatypes = CompoundDatatype.objects.all()
    c = Context({'method_families': method_families, 'compound_datatypes': compound_datatypes})
    c.update(csrf(request))
    return HttpResponse(t.render(c))