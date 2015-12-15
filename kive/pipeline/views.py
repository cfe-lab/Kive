"""
pipeline views
"""

from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.template import loader, RequestContext
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.exceptions import ValidationError

import json
import logging

from method.models import MethodFamily
from metadata.models import CompoundDatatype
from pipeline.models import Pipeline, PipelineFamily
import metadata.forms
from portal.views import developer_check, admin_check
from pipeline.serializers import PipelineSerializer
from pipeline.forms import PipelineFamilyDetailsForm, PipelineDetailsForm

LOGGER = logging.getLogger(__name__)


@login_required
@user_passes_test(developer_check)
def pipeline_families(request):
    """
    Display existing pipeline families, represented by the
    root members (without parent).
    """
    t = loader.get_template('pipeline/pipeline_families.html')
    c = RequestContext(
        request,
        {
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

    addable_users, addable_groups = family.other_users_groups()

    if request.method == 'POST':
        # We are attempting to update the CodeResource's metadata/permissions.
        pf_form = PipelineFamilyDetailsForm(
            request.POST,
            addable_users=addable_users,
            addable_groups=addable_groups,
            instance=family
        )

        if pf_form.is_valid():
            try:
                family.name = pf_form.cleaned_data["name"]
                family.description = pf_form.cleaned_data["description"]
                family.save()
                family.grant_from_json(pf_form.cleaned_data["permissions"])
                family.clean()

                # Success -- go back to the resources page.
                return HttpResponseRedirect('/pipeline_families')
            except (AttributeError, ValidationError, ValueError) as e:
                LOGGER.exception(e.message)
                pf_form.add_error(None, e)

    else:
        pf_form = PipelineFamilyDetailsForm(
            addable_users=addable_users,
            addable_groups=addable_groups,
            initial={"name": family.name, "description": family.description}
        )

    t = loader.get_template('pipeline/pipelines.html')
    c = RequestContext(request,
                       {
                           "family": family,
                           "family_form": pf_form,
                           "is_admin": admin_check(request.user),
                           "is_owner": request.user == family.user
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


@login_required
@user_passes_test(developer_check)
def pipeline_view(request, id):
    """
    View a Pipeline or edit its metadata/permissions.
    """
    four_oh_four = False
    try:
        pipeline = Pipeline.objects.get(pk=id)
        if not pipeline.can_be_accessed(request.user):
            four_oh_four = True
    except Pipeline.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} is not accessible".format(id))

    addable_users, addable_groups = pipeline.other_users_groups()
    addable_users, addable_groups = pipeline.family.intersect_permissions(addable_users, addable_groups)

    if pipeline.revision_parent is not None:
        addable_users, addable_groups = pipeline.revision_parent.intersect_permissions(addable_users, addable_groups)

    atomic_steps, psics, pocs = pipeline.get_all_atomic_steps_cables()

    for step in atomic_steps:
        for step_input in step.transformation.inputs.all():
            step_input_cdt = step_input.get_cdt()
            if step_input_cdt is not None:
                addable_users, addable_groups = step_input_cdt.intersect_permissions(addable_users, addable_groups)

    for psic in psics:
        cable_in_cdt = psic.source.get_cdt()
        if cable_in_cdt is not None:
            addable_users, addable_groups = cable_in_cdt.intersect_permissions(addable_users, addable_groups)

    for poc in pocs:
        if poc.output_cdt is not None:
            addable_users, addable_groups = poc.output_cdt.intersect_permissions(addable_users, addable_groups)

    if request.method == 'POST':
        # We are attempting to update the Pipeline's metadata/permissions.
        pipeline_form = PipelineDetailsForm(
            request.POST,
            addable_users=addable_users,
            addable_groups=addable_groups,
            instance=pipeline
        )

        if pipeline_form.is_valid():
            try:
                pipeline.revision_name = pipeline_form.cleaned_data["revision_name"]
                pipeline.revision_desc = pipeline_form.cleaned_data["revision_desc"]
                pipeline.save()
                pipeline.grant_from_json(pipeline_form.cleaned_data["permissions"])
                pipeline.clean()

                # Success -- go back to the CodeResource page.
                return HttpResponseRedirect('/pipelines/{}'.format(pipeline.family.pk))
            except (AttributeError, ValidationError, ValueError) as e:
                LOGGER.exception(e.message)
                pipeline_form.add_error(None, e)

    else:
        pipeline_form = PipelineDetailsForm(
            addable_users=addable_users,
            addable_groups=addable_groups,
            initial={
                "revision_name": pipeline.revision_name,
                "revision_desc": pipeline.revision_desc
            }
        )

    t = loader.get_template("pipeline/pipeline_view.html")
    c = RequestContext(
        request,
        {
            "pipeline": pipeline,
            "pipeline_form": pipeline_form,
            "pipeline_dict": json.dumps(PipelineSerializer(
                pipeline,
                context={"request": request}
            ).data),
            "is_owner": pipeline.user == request.user,
            "is_admin": admin_check(request.user)
        }
    )
    return HttpResponse(t.render(c))
