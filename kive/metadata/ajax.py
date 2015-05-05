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
from portal.views import developer_check, admin_check
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
    """ Compound datatypes are used to define a CSV file format.
    
    Query parameters for the list view:
    
    * is_granted=true - For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    """
    queryset = CompoundDatatype.objects.all()
    serializer_class = CompoundDatatypeSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

    def get_queryset(self):
        if self.request.QUERY_PARAMS.get('is_granted') == 'true':
            is_admin = False
        else:
            is_admin = admin_check(self.request.user)
        return AccessControl.filter_by_user(self.request.user,
                                            is_admin=is_admin,
                                            queryset=self.queryset)

    @detail_route(methods=['get'])
    def removal_plan(self, request, pk=None):
        removal_plan = self.get_object().build_removal_plan()
        counts = {key: len(targets) for key, targets in removal_plan.iteritems()}
        return Response(counts)
    
    def perform_destroy(self, instance):
        instance.remove()
