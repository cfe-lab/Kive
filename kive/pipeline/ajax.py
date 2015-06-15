from django.http import HttpResponse, Http404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.db import transaction

from rest_framework import permissions
from rest_framework.decorators import detail_route
from rest_framework.response import Response

import json

from method.models import MethodFamily, Method
from pipeline.models import Pipeline, PipelineFamily
from portal.views import developer_check, admin_check
from metadata.models import KiveUser, AccessControl

from pipeline.serializers import PipelineFamilySerializer, PipelineSerializer
from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet,\
    CleanCreateModelMixin


class PipelineFamilyViewSet(RemovableModelViewSet):
    queryset = PipelineFamily.objects.all()
    serializer_class = PipelineFamilySerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

    @detail_route(methods=["get"])
    def pipelines(self, request, pk=None):
        if self.request.QUERY_PARAMS.get('is_granted') == 'true':
            is_admin = False
        else:
            is_admin = admin_check(self.request.user)

        member_pipelines = AccessControl.filter_by_user(
            request.user,
            is_admin=is_admin,
            queryset=self.get_object().members.all())

        member_serializer = PipelineSerializer(
            member_pipelines, many=True, context={"request": request})
        return Response(member_serializer.data)


class PipelineViewSet(CleanCreateModelMixin,
                      RemovableModelViewSet):
    queryset = Pipeline.objects.all()
    serializer_class = PipelineSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

    def get_queryset(self):
        prefetchd = Pipeline.objects.prefetch_related(
            'steps__transformation__method__family',
            'steps__transformation__pipeline__family',
            'steps__transformation__method__inputs__structure__compounddatatype__members__datatype',
            'steps__transformation__method__outputs__structure__compounddatatype__members__datatype',
            'steps__transformation__method__family',
            'steps__cables_in__custom_wires',
            'steps__cables_in__dest__transformationinput',
            'steps__cables_in__dest__transformationoutput',
            'steps__cables_in__source__transformationinput',
            'steps__cables_in__source__transformationoutput',
            'steps__outputs_to_delete',
            'inputs__structure',
            'outcables__source__structure',
            'outcables__source__transformationinput',
            'outcables__source__transformationoutput',
            'outcables__custom_wires__source_pin',
            'outcables__custom_wires__dest_pin').\
            select_related(
            'steps__transformation__pipeline',
            'steps__transformation__method',
            'outcables__pipeline',
            'outcables__output_cdt',
            'outcables__source'
            'inputs__transformation'
        )

        return prefetchd

    # Override perform_create to call complete_clean, not just clean.
    @transaction.atomic
    def perform_create(self, serializer):
        new_pipeline = serializer.save()
        new_pipeline.complete_clean()


@login_required
@user_passes_test(developer_check)
def populate_method_revision_dropdown (request):
    """
    copied from Method ajax.py
    """
    if request.is_ajax():
        response = HttpResponse()
        method_family_id = request.POST.get('mf_id')
        if method_family_id != '':
            method_family = MethodFamily.objects.get(pk=method_family_id)

            # Go through all Methods with this family and retrieve their primary key and revision_name.
            method_dicts = []
            for curr_method in Method.objects.filter(family=method_family).order_by('-pk'):
                driver = curr_method.driver
                parent = driver.coderesource
                method_dicts.append({"pk": curr_method.pk, "model": "method.method",
                                     "fields": {'driver_number': driver.revision_number,
                                                'driver_name': driver.revision_name,
                                                'filename': parent.filename,
                                                'method_number': curr_method.revision_number,
                                                'method_name': curr_method.revision_name,
                                                'method_desc': curr_method.revision_desc
                                                }})

            response.write(json.dumps(method_dicts))
        return response
    else:
        raise Http404


@login_required
@user_passes_test(developer_check)
def get_method_io (request):
    """
    handles ajax request from pipelines.html
    populates a dictionary with information about this method's transformation
    inputs and outputs, returns as JSON.

    TODO: this function is no longer used thanks to changes to get_pipeline.
    """
    if request.is_ajax():
        method_id = request.POST.get('mid')
        method = Method.objects.filter(pk=method_id)[0]

        inputs = []

        for input in method.inputs.all():
            strct = None

            if input.has_structure:
                structure = input.structure
                cdt_pk = structure.compounddatatype.pk
                cdt_label = str(structure.compounddatatype)
                strct = {'compounddatatype': cdt_pk, 'cdt_label': cdt_label}

            inputs.append({
                'dataset_idx': input.dataset_idx,
                'dataset_name': input.dataset_name,
                'structure': strct
            })

        outputs = []
        for output in method.outputs.all():
            strct = None
            if output.has_structure:
                structure = output.structure
                cdt_pk = structure.compounddatatype.pk
                cdt_label = str(structure.compounddatatype)
                strct = {'compounddatatype': cdt_pk, 'cdt_label': cdt_label}

            outputs.append({
                'dataset_idx': output.dataset_idx,
                'dataset_name': output.dataset_name,
                'structure': strct
            })

        response_data = {'inputs': inputs, 'outputs': outputs}
        return HttpResponse(json.dumps(response_data), content_type='application/json')
    else:
        raise Http404


def get_method_xputs(method):
    """Get the inputs and outputs of a Method as a dictionary."""
    result = []
    for method_xputs in [method.inputs.all(), method.outputs.all()]:
        xputs = {}
        for xput in method_xputs:
            if not xput.has_structure:
                cdt_pk = None
                cdt_label = "raw"
            else:
                structure = xput.structure
                cdt_pk = structure.compounddatatype.pk
                cdt_label = str(structure.compounddatatype)
            xputs.update(
                {
                    xput.dataset_idx: {
                        'datasetname': xput.dataset_name,
                        'cdt_pk': cdt_pk,
                        'cdt_label': cdt_label
                    }
                }
            )
        result.append(xputs)
    return {'inputs': result[0], 'outputs': result[1]}


@login_required
def get_pipeline(request):
    if request.is_ajax():
        response = HttpResponse()
        pipeline_revision_id = request.POST.get('pipeline_id')  # TODO: Split this off into a form?
        user = KiveUser.kiveify(request.user)

        if pipeline_revision_id != '':
            # Get and check permissions
            pipeline_revision = Pipeline.objects.filter(
                user.access_query(),
                pk=pipeline_revision_id)

            if pipeline_revision.count() == 0:
                raise Http404
            pipeline_revision = pipeline_revision.first()

            pipeline_dict = pipeline_revision.represent_as_dict()
            steps = pipeline_revision.steps\
                .select_related('transformation__pipeline',
                                'transformation__method')\
                .prefetch_related('transformation__method__inputs__structure__compounddatatype__members__datatype',
                                  'transformation__method__outputs__structure__compounddatatype__members__datatype',
                                  'transformation__method__family')

            # Hack to reduce number of ajax calls in interface.
            for step in steps:
                if not step.is_subpipeline:
                    method = step.transformation.definite
                    pipeline_dict["pipeline_steps"][step.step_num-1].update(get_method_xputs(method))
            # End of hack.

            return HttpResponse(json.dumps(pipeline_dict), content_type='application/json')
        return response
    else:
        raise Http404


@login_required
@user_passes_test(developer_check)
def activate_pipeline(request):
    """
    Make this pipeline revision the published version.
    :param request:
    :return:
    """
    if request.is_ajax():
        #response = HttpResponse()
        pipeline_revision_id = request.POST.get('pipeline_id')
        if pipeline_revision_id != '':
            pipeline_revision = Pipeline.objects.get(pk=pipeline_revision_id)
            pipeline_family = pipeline_revision.family
            pipeline_family.published_version = None if pipeline_revision.is_published_version else pipeline_revision

            pipeline_family.full_clean()
            pipeline_family.save()

            return HttpResponse(json.dumps({'is_published': pipeline_revision.is_published_version}), content_type='application/json')
        # else
        return HttpResponse()
    else:
        return Http404

