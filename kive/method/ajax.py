from django.http import HttpResponse, Http404
from django.core import serializers
from django.contrib.auth.decorators import login_required, user_passes_test

from rest_framework import viewsets, permissions, mixins
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from method.models import CodeResourceRevision, MethodFamily, Method
import method.serializers
from metadata.models import AccessControl
from portal.views import developer_check
from portal.ajax import IsDeveloperOrGrantedReadOnly


@login_required
@user_passes_test(developer_check)
def populate_revision_dropdown(request):
    """
    resource_add.html template can render multiple forms for CodeResourceDependency that
     have fields for CodeResource and CodeResourceRevision.  We want to only populate the
     latter with the revisions that correspond to the CodeResource selected in the first
     drop-down.  The 'change' event triggers an Ajax request that this function will handle
     and return a JSON object with the revision info.
    """
    if request.is_ajax():
        response = HttpResponse()
        coderesource_id = request.GET.get('cr_id')
        if coderesource_id != '':
            # pk (primary key) implies id__exact
            response.write(
                serializers.serialize(
                    "json",
                    CodeResourceRevision.filter_by_user(request.user).filter(
                        coderesource__pk=coderesource_id
                    ).order_by("-revision_number"),
                    fields=('pk', 'revision_number', 'revision_name')
                )
            )
        return response
    else:
        raise Http404


class MethodFamilyViewSet(mixins.DestroyModelMixin,
                          viewsets.ReadOnlyModelViewSet):
    queryset = MethodFamily.objects.all()
    serializer_class = method.serializers.MethodFamilySerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

    # FIXME replace in the future with inheritance
    def get_queryset(self):
        is_admin = self.request.QUERY_PARAMS.get('is_admin') == 'true'
        return MethodFamily.filter_by_user(self.request.user,
                                           is_admin=is_admin,
                                           queryset=self.queryset)

    # FIXME replace in the future with inheritance
    @detail_route(methods=['get'])
    def removal_plan(self, request, pk=None):
        method_family = self.get_object()
        removal_plan = method_family.build_removal_plan()
        counts = {key: len(targets) for key, targets in removal_plan.iteritems()}
        return Response(counts)

    # FIXME replace in the future with inheritance
    @list_route(methods=['get'])
    def granted(self, request):
        queryset = AccessControl.filter_by_user(
            request.user,
            False,
            self.filter_queryset(self.get_queryset()))

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def perform_destroy(self, instance):
        instance.remove()

    @detail_route(methods=["get"])
    def methods(self, request, pk=None):
        member_methods = AccessControl.filter_by_user(
            request.user,
            False,
            self.get_object().members.all())

        member_serializer = method.serializers.MethodSerializer(
            member_methods, many=True, context={"request": request})
        return Response(member_serializer.data)


class MethodViewSet(mixins.DestroyModelMixin,
                    viewsets.ReadOnlyModelViewSet):
    queryset = Method.objects.all()
    serializer_class = method.serializers.MethodSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

    # FIXME replace in the future with inheritance
    def get_queryset(self):
        is_admin = self.request.QUERY_PARAMS.get('is_admin') == 'true'
        return Method.filter_by_user(self.request.user,
                                     is_admin=is_admin,
                                     queryset=self.queryset)

    # FIXME replace in the future with inheritance
    @detail_route(methods=['get'])
    def removal_plan(self, request, pk=None):
        method = self.get_object()
        removal_plan = method.build_removal_plan()
        counts = {key: len(targets) for key, targets in removal_plan.iteritems()}
        return Response(counts)

    # FIXME replace in the future with inheritance
    @list_route(methods=['get'])
    def granted(self, request):
        queryset = AccessControl.filter_by_user(
            request.user,
            False,
            self.filter_queryset(self.get_queryset()))

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def perform_destroy(self, instance):
        instance.remove()
