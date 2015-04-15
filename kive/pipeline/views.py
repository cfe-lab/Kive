"""
pipeline views
"""

from django.http import HttpResponse, Http404
from django.template import loader, RequestContext
from django.contrib.auth.decorators import login_required, user_passes_test

import json
import logging

from method.models import MethodFamily
from metadata.models import CompoundDatatype
from pipeline.models import Pipeline, PipelineFamily, PipelineSerializationException
import metadata.forms
from portal.views import developer_check

logger = logging.getLogger(__name__)


@login_required
@user_passes_test(developer_check)
def pipelines(request):
    """
    Display existing pipeline families, represented by the
    root members (without parent).
    """
    t = loader.get_template('pipeline/pipelines.html')
    families = PipelineFamily.filter_by_user(request.user)

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
@user_passes_test(developer_check)
def pipeline_add(request):
    """
    Most of the heavy lifting is done by JavaScript and HTML5.
    I don't think we need to use forms here.
    """
    t = loader.get_template('pipeline/pipeline_add.html')
    method_families = MethodFamily.filter_by_user(request.user).order_by('name')
    compound_datatypes = CompoundDatatype.filter_by_user(request.user)
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
@user_passes_test(developer_check)
def pipeline_revise(request, id):
    """
    Display all revisions in this PipelineFamily.
    Use an AJAX transaction to load the actual Pipeline object
    from the database to front-end to render as HTML5 Canvas
    objects.
    """
    t = loader.get_template('pipeline/pipeline_revise.html')
    method_families = MethodFamily.filter_by_user(request.user).order_by('name')
    compound_datatypes = CompoundDatatype.filter_by_user(request.user)
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

    revisions = Pipeline.filter_by_user(request.user).filter(family=family)

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
@user_passes_test(developer_check)
def pipeline_exec(request):
    t = loader.get_template('pipeline/pipeline_exec.html')
    method_families = MethodFamily.filter_by_user(request.user)
    compound_datatypes = CompoundDatatype.filter_by_user(request.user)
    c = RequestContext(request, {'method_families': method_families, 'compound_datatypes': compound_datatypes})
    return HttpResponse(t.render(c))

