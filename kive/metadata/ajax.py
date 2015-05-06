"""
Handle Ajax transaction requests from metadata templates.
"""

from django.http import HttpResponse
from django.core import serializers
from django.contrib.auth.decorators import login_required, user_passes_test
from rest_framework import permissions

from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet
from metadata.models import Datatype, get_builtin_types, CompoundDatatype
from metadata.serializers import DatatypeSerializer, CompoundDatatypeSerializer
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


class DatatypeViewSet(RemovableModelViewSet):
    """Datatypes are used to define the types of data in CSV entries.

    Query parameters are as for RemovableModelViewSet.
    """
    queryset = Datatype.objects.all()
    serializer_class = DatatypeSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

    
class CompoundDatatypeViewSet(RemovableModelViewSet):
    """ Compound datatypes are used to define a CSV file format.
    
    Query parameters for the list view:
    
    * is_granted=true - For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    """
    queryset = CompoundDatatype.objects.all()
    serializer_class = CompoundDatatypeSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
