"""
Handle Ajax transaction requests from metadata templates.
"""

from django.http import HttpResponse
from django.core import serializers
from django.contrib.auth.decorators import login_required, user_passes_test
from rest_framework import viewsets, permissions, mixins
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from metadata.models import Datatype, get_builtin_types, CompoundDatatype,\
    AccessControl
from metadata.serializers import CompoundDatatypeSerializer
from portal.views import developer_check
from portal.ajax import IsDeveloperOrGrantedReadOnly


@login_required
@user_passes_test(developer_check)
def get_python_type(request):
    """
    Return the lowest-level Python type (string, boolean, int, or
    float) given the Datatype restrictions set by the user.
    """
    if request.is_ajax():
        response = HttpResponse()
        query = request.POST
        restricts = query.getlist(u'restricts[]')

        # get Datatypes given these primary keys
        DTs = []
        for pk in restricts:
            this_datatype = Datatype.objects.filter(pk=pk)
            DTs.extend(this_datatype)

        # use get_builtin_types() method
        python_types = get_builtin_types(DTs)
        response.write(serializers.serialize("json", python_types, fields=('pk', 'name')))
        return response


class CompoundDatatypeViewSet(mixins.DestroyModelMixin,
                              viewsets.ReadOnlyModelViewSet):
    queryset = CompoundDatatype.objects.all()
    serializer_class = CompoundDatatypeSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

    @detail_route(methods=['get'])
    def removal_plan(self, request, pk=None):
        removal_plan = self.get_object().build_removal_plan()
        counts = {key: len(targets) for key, targets in removal_plan.iteritems()}
        return Response(counts)
    
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
