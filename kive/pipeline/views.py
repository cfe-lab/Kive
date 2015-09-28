"""
pipeline views
"""

from django.http import HttpResponse, Http404
from django.template import loader, RequestContext
from django.contrib.auth.decorators import login_required, user_passes_test

from rest_framework.renderers import JSONRenderer

import json
import logging

from method.models import MethodFamily
from metadata.models import CompoundDatatype, AccessControl
from pipeline.models import Pipeline, PipelineFamily
import metadata.forms
from portal.views import developer_check, admin_check
from pipeline.serializers import PipelineFamilySerializer, PipelineSerializer

logger = logging.getLogger(__name__)


@login_required
@user_passes_test(developer_check)
def pipeline_families(request):
    """
    Display existing pipeline families, represented by the
    root members (without parent).
    """
    t = loader.get_template('pipeline/pipeline_families.html')
    families = PipelineFamily.filter_by_user(request.user)
    families_json = json.dumps(
        PipelineFamilySerializer(
            families,
            context={"request": request},
            many=True).data
    )

    c = RequestContext(
        request,
        {
            'pipeline_families': families_json,
            "is_user_admin": admin_check(request.user)
        })
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def pipelines(request, id):
    """
    Display a list of all Pipelines within a given PipelineFamily.
    """
    four_oh_four = False
    try:
        family = PipelineFamily.objects.get(pk=id)
        if not family.can_be_accessed(request.user) and not admin_check(request.user):
            four_oh_four = True
    except MethodFamily.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        # Redirect back to the resources page.
        raise Http404("ID {} cannot be accessed".format(id))

    member_pipelines = AccessControl.filter_by_user(
        request.user,
        is_admin=False,
        queryset=family.members.all())

    pipelines_json = JSONRenderer().render(
        PipelineSerializer(member_pipelines, many=True, context={"request": request}).data
    )

    t = loader.get_template('pipeline/pipelines.html')
    c = RequestContext(request,
                       {
                           'family': family,
                           "pipelines": pipelines_json,
                           "is_user_admin": admin_check(request.user)
                       })
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

def _get_compound_datatypes(request):
    """ Get a sorted list of (name, id) pairs for compound datatypes. """
    compound_datatypes = [(cdt.short_name, cdt.pk)
                          for cdt in CompoundDatatype.filter_by_user(request.user)]
    compound_datatypes.sort()
    return compound_datatypes

@login_required
@user_passes_test(developer_check)
def pipeline_new(request):
    """
    Most of the heavy lifting is done by JavaScript and HTML5.
    I don't think we need to use forms here.
    """
    t = loader.get_template('pipeline/pipeline.html')
    method_families = MethodFamily.filter_by_user(request.user).order_by('name')
    acf = metadata.forms.AccessControlForm()
    c = RequestContext(request, {'method_families': method_families,
                                 'compound_datatypes': _get_compound_datatypes(request),
                                 "access_control_form": acf})

    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def pipeline_add(request, id=None):
    """
    Creates a new Pipeline belonging to an existing PipelineFamily.
    """
    t = loader.get_template('pipeline/pipeline.html')
    method_families = MethodFamily.filter_by_user(request.user).order_by('name')

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

    family_users_allowed = [x.pk for x in family.users_allowed.all()]
    family_groups_allowed = [x.pk for x in family.groups_allowed.all()]
    acf = metadata.forms.AccessControlForm(
        initial={
            "permissions": [family_users_allowed, family_groups_allowed]
        }
    )

    c = RequestContext(
        request,
        {
            "family": family,
            'method_families': method_families,
            'compound_datatypes': _get_compound_datatypes(request),
            "access_control_form": acf
        }
    )

    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def pipeline_revise(request, id):
    """
    Make a new Pipeline based on the one specified by id.

    Use an AJAX transaction to load the actual Pipeline object
    from the database to front-end to render as HTML5 Canvas
    objects.
    """
    t = loader.get_template('pipeline/pipeline.html')
    method_families = MethodFamily.filter_by_user(request.user).order_by('name')

    # Retrieve this pipeline from database.
    four_oh_four = False
    try:
        parent_revision = Pipeline.objects.get(pk=id)
        if not parent_revision.can_be_accessed(request.user):
            four_oh_four = True
    except Pipeline.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} cannot be accessed".format(id))

    parent_users_allowed = [x.pk for x in parent_revision.users_allowed.all()]
    parent_groups_allowed = [x.pk for x in parent_revision.groups_allowed.all()]
    acf = metadata.forms.AccessControlForm(
        initial={
            "permissions": [parent_users_allowed, parent_groups_allowed]
        }
    )

    parent_revision_json = json.dumps(
        PipelineSerializer(
            parent_revision,
            context={"request": request}
        ).data
    )
    c = RequestContext(
        request,
        {
            "family": parent_revision.family,
            "parent_revision": parent_revision,
            "parent_revision_json": parent_revision_json,
            'method_families': method_families,
            'compound_datatypes': _get_compound_datatypes(request),
            "access_control_form": acf
        }
    )

    return HttpResponse(t.render(c))



