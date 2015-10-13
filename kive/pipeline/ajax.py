import json

from django.http import HttpResponse, Http404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.exceptions import ValidationError as DjangoValidationError
from django.db import transaction
from django.db.models import Q
from rest_framework import permissions
from rest_framework.decorators import detail_route
from rest_framework.exceptions import APIException
from rest_framework.response import Response

from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet,\
    CleanCreateModelMixin, convert_validation, StandardPagination
from metadata.models import AccessControl
from method.models import MethodFamily, Method
from pipeline.models import Pipeline, PipelineFamily
from pipeline.serializers import PipelineFamilySerializer, PipelineSerializer,\
    PipelineStepUpdateSerializer
from portal.views import developer_check, admin_check



class PipelineFamilyViewSet(CleanCreateModelMixin,
                            RemovableModelViewSet):
    """ Pipeline Families that contain the different versions of each pipeline
    
    Query parameters for the list view:
    * is_granted=true - For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search for pipeline families. n starts at 0 and increases by 1 for each
        added filter.
    * filters[n][key]=smart&filters[n][val]=match - pipeline families where the
        pipeline family name or description contain the value (case insensitive)
    """
    queryset = PipelineFamily.objects.all()
    serializer_class = PipelineFamilySerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination

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

    def partial_update(self, request, pk=None):
        """
        Defines PATCH functionality on a PipelineFamily.
        """
        if "published_version" in request.data:
            # This is a PATCH to change the published version.
            return self.change_published_version(request)

        return Response({"message": "No action taken."})

    def change_published_version(self, request):
        family_to_publish = self.get_object()
        if request.data.get("published_version") == "" or request.data.get("published_version") is None:
            new_published_version = None
        else:
            new_published_version = Pipeline.objects.get(pk=request.data["published_version"])

        family_to_publish.published_version = new_published_version
        family_to_publish.save()
        response_msg = 'PipelineFamily "{}" published_version set to "{}".'.format(
            family_to_publish,
            new_published_version
        )
        return Response({'message': response_msg})
    
    def filter_queryset(self, queryset):
        queryset = super(PipelineFamilyViewSet, self).filter_queryset(queryset)
        i = 0
        while True:
            key = self.request.GET.get('filters[{}][key]'.format(i))
            if key is None:
                break
            value = self.request.GET.get('filters[{}][val]'.format(i), '')
            queryset = self._add_filter(queryset, key, value)
            i += 1
        return queryset
    
    def _add_filter(self, queryset, key, value):
        if key == 'smart':
            return queryset.filter(Q(name__icontains=value) |
                                   Q(description__icontains=value))
        raise APIException('Unknown filter key: {}'.format(key))


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

    @detail_route(methods=['get'], suffix='Step Updates')
    def step_updates(self, request, pk=None):
        updates = self.get_object().find_step_updates()
        return Response(PipelineStepUpdateSerializer(updates,
                                                     context={'request': request},
                                                     many=True).data)
    
    # Override perform_create to call complete_clean, not just clean.
    @transaction.atomic
    def perform_create(self, serializer):
        """
        Handle creation and cleaning of a new object.
        """
        try:
            new_pipeline = serializer.save()
            new_pipeline.complete_clean()
        except DjangoValidationError as ex:
            raise convert_validation(ex)

    def partial_update(self, request, pk=None):
        """
        Defines PATCH functionality on a Pipeline.
        """
        if "published" in request.data:
            # This is a PATCH to publish/unpublish this Pipeline.
            return self.change_published_version(request)

        return Response({"message": "No action taken."})

    def change_published_version(self, request):
        pipeline_to_change = self.get_object()
        if request.data.get("published") == "" or request.data.get("published") is None:
            return Response({"message": "published is unspecified."})

        publish_update = request.data.get("published", "false") == "true"
        pipeline_to_change.published = publish_update
        pipeline_to_change.save()
        response_msg = 'Pipeline "{}" has been {}published.'.format(
            pipeline_to_change,
            "" if publish_update else "un"
        )
        return Response({'message': response_msg})


@login_required
@user_passes_test(developer_check)
def populate_method_revision_dropdown(request):
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
