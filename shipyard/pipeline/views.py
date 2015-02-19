"""
pipeline views
"""

from django.http import HttpResponse
from django.template import loader, RequestContext
from django.contrib.auth.decorators import login_required

import json
import logging

from method.models import MethodFamily
from metadata.models import CompoundDatatype, KiveUser
from pipeline.models import Pipeline, PipelineFamily, PipelineSerializationException
import metadata.forms

logger = logging.getLogger(__name__)


@login_required
def pipelines(request):
    """
    Display existing pipeline families, represented by the
    root members (without parent).
    """
    t = loader.get_template('pipeline/pipelines.html')
    user_plus = KiveUser.kiveify(request.user)
    families = PipelineFamily.objects.filter(user_plus.access_query()).distinct()

    c = RequestContext(request, {'families': families})
    return HttpResponse(t.render(c))


def prepare_pipeline_dict(request_body, user):
    """
    Helper that creates a dictionary representation of a Pipeline.

    For now, everything we produce is shared, with no users or groups granted
    any special privileges.
    """
    form_data = json.loads(request_body)
    form_data["user"] = user.pk
    return form_data


@login_required
def pipeline_add(request):
    """
    Most of the heavy lifting is done by JavaScript and HTML5.
    I don't think we need to use forms here.
    """
    t = loader.get_template('pipeline/pipeline_add.html')
    user_plus = KiveUser.kiveify(request.user)
    method_families = MethodFamily.objects.filter(user_plus.access_query()).distinct().order_by('name')
    compound_datatypes = CompoundDatatype.objects.filter(user_plus.access_query()).distinct()
    acf = metadata.forms.AccessControlForm()
    c = RequestContext(request, {'method_families': method_families, 'compound_datatypes': compound_datatypes,
                                 "access_control_form": acf})

    if request.method == 'POST':
        form_data = prepare_pipeline_dict(request.body, request.user)
        try:
            Pipeline.create_from_dict(form_data)
            response_data = {"status": "success", "error_msg": ""}
        except PipelineSerializationException as e:
            response_data = {"status": "failure", "error_msg": str(e)}
        return HttpResponse(json.dumps(response_data), content_type='application/json')
    else:
        return HttpResponse(t.render(c))


@login_required
def pipeline_revise(request, id):
    """
    Display all revisions in this PipelineFamily.
    Use an AJAX transaction to load the actual Pipeline object
    from the database to front-end to render as HTML5 Canvas
    objects.
    """
    t = loader.get_template('pipeline/pipeline_revise.html')
    user_plus = KiveUser.kiveify(request.user)
    method_families = MethodFamily.objects.filter(user_plus.access_query()).distinct().order_by('name')
    compound_datatypes = CompoundDatatype.objects.filter(user_plus.access_query()).distinct()
    acf = metadata.forms.AccessControlForm()

    # Retrieve this pipeline from database.

    four_oh_four = False
    try:
        family = PipelineFamily.objects.get(pk=id)
        if not family.can_be_accessed(request.user):
            four_oh_four = True
    except PipelineFamily.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} cannot be accessed".format(id))

    revisions = Pipeline.objects.filter(user_plus.access_query(), family=family).distinct()

    c = RequestContext(
        request,
        {'family': family, 'revisions': revisions, 'method_families': method_families,
         'compound_datatypes': compound_datatypes, "access_control_form": acf})

    if request.method == 'POST':
        form_data = prepare_pipeline_dict(request.body, request.user)
        try:
            parent_pk = form_data['revision_parent_pk']
            parent_revision = Pipeline.objects.get(pk=parent_pk)
            parent_revision.revise_from_dict(form_data)
            response_data = {'status': 'success', 'error_msg': ''}
        except PipelineSerializationException as e:
            response_data = {'status': 'failure', 'error_msg': str(e)}
        return HttpResponse(json.dumps(response_data), content_type='application/json')

    return HttpResponse(t.render(c))


@login_required
def pipeline_exec(request):
    t = loader.get_template('pipeline/pipeline_exec.html')
    user_plus = KiveUser.kiveify(request.user)
    method_families = MethodFamily.objects.filter(user_plus.access_query()).distinct()
    compound_datatypes = CompoundDatatype.objects.filter(user_plus.access_query()).distinct()
    c = RequestContext(request, {'method_families': method_families, 'compound_datatypes': compound_datatypes})
    return HttpResponse(t.render(c))

