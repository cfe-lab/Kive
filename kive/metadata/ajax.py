"""
Handle Ajax transaction requests from metadata templates.
"""

from django.http import HttpResponse
from django.core import serializers
from django.contrib.auth.decorators import login_required, user_passes_test
from rest_framework import viewsets, permissions, mixins
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from metadata.models import Datatype, get_builtin_types, CompoundDatatype,\
    AccessControl
from metadata.serializers import CompoundDatatypeSerializer
from portal.views import developer_check

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
    permission_classes = (permissions.IsAuthenticated, )

    def get_queryset(self):
        is_admin = self.request.QUERY_PARAMS.get('is_admin') == 'true'
        return AccessControl.filter_by_user(self.request.user,
                                            is_admin=is_admin,
                                            queryset=self.queryset)

    @detail_route(methods=['get'])
    def removal_plan(self, request, pk=None):
        compounddatatype = self.get_object()
        removal_plan = compounddatatype.build_removal_plan()
        counts = {key: len(targets) for key, targets in removal_plan.iteritems()}
        return Response(counts)
    
    def perform_destroy(self, instance):
        instance.remove()
