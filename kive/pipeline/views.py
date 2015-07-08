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
    Display a list of all Pipelines within a given MethodFamily.
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

    return HttpResponse(t.render(c))


# @login_required
# @user_passes_test(developer_check)
# def method_add(request, id=None):
#     """
#     Generate forms for adding Methods, and validate and process POST data returned
#     by the user.  Allows for an arbitrary number of input and output forms.
#
#     [id] : User is adding a new Method to an existing family
#            without a specified parent Method (different CodeResource)
#            If id is None, then user is creating a new MethodFamily.
#     """
#     creating_user = request.user
#     if id:
#         four_oh_four = False
#         try:
#             this_family = MethodFamily.objects.get(pk=id)
#             if not this_family.can_be_accessed(creating_user):
#                 four_oh_four = True
#         except MethodFamily.DoesNotExist:
#             four_oh_four = True
#         if four_oh_four:
#             raise Http404("ID {} is inaccessible".format(id))
#
#         header = "Add a new Method to MethodFamily '%s'" % this_family.name
#     else:
#         this_family = None
#         header = 'Start a new MethodFamily with an initial Method'
#
#     t = loader.get_template('method/method_add.html')
#     c = RequestContext(request)
#     if request.method == 'POST':
#         family_form, method_form, input_form_tuples, output_form_tuples = create_method_forms(
#             request.POST, creating_user, family=this_family)
#         if not _method_forms_check_valid(family_form, method_form, input_form_tuples, output_form_tuples):
#             # Bail out now if there are any problems.
#             c.update(
#                 {
#                     'family_form': family_form,
#                     'method_form': method_form,
#                     'input_forms': input_form_tuples,
#                     'output_forms': output_form_tuples,
#                     'family': this_family,
#                     'header': header
#                 })
#             return HttpResponse(t.render(c))
#
#         # Next, attempt to build the Method and its associated MethodFamily (if necessary),
#         # inputs, and outputs.
#         create_method_from_forms(
#             family_form, method_form, input_form_tuples, output_form_tuples, creating_user,
#             family=this_family
#         )
#
#         if _method_forms_check_valid(family_form, method_form, input_form_tuples, output_form_tuples):
#             # Success!
#             if id:
#                 return HttpResponseRedirect('/methods/{}'.format(id))
#             else:
#                 return HttpResponseRedirect('/method_families')
#
#     else:
#         # Prepare a blank set of forms for rendering.
#         family_form = MethodFamilyForm()
#         method_form = MethodForm(user=creating_user)
#         input_form_tuples = [
#             (TransformationXputForm(auto_id='id_%s_in_0'), XputStructureForm(user=creating_user,
#                                                                              auto_id='id_%s_in_0'))
#         ]
#         output_form_tuples = [
#             (TransformationXputForm(auto_id='id_%s_out_0'), XputStructureForm(user=creating_user,
#                                                                               auto_id='id_%s_out_0'))
#         ]
#
#     c.update(
#         {
#             'family_form': family_form,
#             'method_form': method_form,
#             'input_forms': input_form_tuples,
#             'output_forms': output_form_tuples,
#             'family': this_family,
#             'header': header
#         })
#     return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def pipeline_revise(request, id):
    """
    Make a new Pipeline based on the one specified by id.

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
        parent_revision = Pipeline.objects.get(pk=id)
        if not parent_revision.can_be_accessed(request.user):
            four_oh_four = True
    except Pipeline.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} cannot be accessed".format(id))

    parent_revision_json = json.dumps(
        PipelineSerializer(
            parent_revision,
            context={"request": request}
        ).data
    )
    c = RequestContext(
        request,
        {
            "parent_revision": parent_revision,
            "parent_revision_json": parent_revision_json,
            'method_families': method_families,
            'compound_datatypes': compound_datatypes,
            "access_control_form": acf
        }
    )

    return HttpResponse(t.render(c))



