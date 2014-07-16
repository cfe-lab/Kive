"""
pipeline views
"""

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.core.context_processors import csrf
from method.models import *
from metadata.models import *
from pipeline.models import *
from django.core.exceptions import ValidationError
import json

logger = logging.getLogger(__name__)

def pipelines(request):
    """
    Display existing pipeline families, represented by the
    root members (without parent).
    """
    t = loader.get_template('pipeline/pipelines.html')
    families = PipelineFamily.objects.all()
    #pipelines = Pipeline.objects.filter(revision_parent=None)
    c = Context({'families': families})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def pipeline_add(request):
    """
    Most of the heavy lifting is done by JavaScript and HTML5.
    I don't think we need to use forms here.
    """
    t = loader.get_template('pipeline/pipeline_add.html')
    method_families = MethodFamily.objects.all().order_by('name')
    compound_datatypes = CompoundDatatype.objects.all()
    c = Context({'method_families': method_families, 'compound_datatypes': compound_datatypes})
    c.update(csrf(request))

    if request.method == 'POST':
        form_data = json.loads(request.body)
        try:
            Pipeline.create_from_dict(form_data)
            response_data = {"status": "success", "error_msg": ""}
        except PipelineSerializationException as e:
            response_data = {"status": "failure", "error_msg": str(e)}
        return HttpResponse(json.dumps(response_data), content_type='application/json')
    else:
        return HttpResponse(t.render(c))


def pipeline_revise(request, id):
    """
    Display all revisions in this PipelineFamily.
    Use an AJAX transaction to load the actual Pipeline object
    from the database to front-end to render as HTML5 Canvas
    objects.
    """
    t = loader.get_template('pipeline/pipeline_revise.html')
    method_families = MethodFamily.objects.all().order_by('name')
    compound_datatypes = CompoundDatatype.objects.all()

    # retrieve this pipeline from database
    family = PipelineFamily.objects.filter(pk=id)[0]
    revisions = Pipeline.objects.filter(family=family)

    c = Context({'family': family, 'revisions': revisions,
                 'method_families': method_families, 'compound_datatypes': compound_datatypes})
    c.update(csrf(request))

    if request.method == 'POST':
        form_data = json.loads(request.body)
        print form_data
        try:
            parent_pk = form_data['revision_parent_pk']
            parent_revision = Pipeline.objects.get(pk=parent_pk)
            parent_revision.revise_from_dict(form_data)
            response_data = {'status': 'success', 'error_msg': ''}
        except PipelineSerializationException as e:
            response_data = {'status': 'failure', 'error_msg': str(e)}
        return HttpResponse(json.dumps(response_data), content_type='application/json')

    return HttpResponse(t.render(c))


def pipeline_exec(request):
    t = loader.get_template('pipeline/pipeline_exec.html')
    method_families = MethodFamily.objects.all()
    compound_datatypes = CompoundDatatype.objects.all()
    c = Context({'method_families': method_families, 'compound_datatypes': compound_datatypes})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
