"""
Handle Ajax transaction requests from metadata templates.
"""

from rest_framework import permissions

from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet, StandardPagination
from metadata.models import Datatype, get_builtin_types, CompoundDatatype
from metadata.serializers import DatatypeSerializer, CompoundDatatypeSerializer


class DatatypeViewSet(RemovableModelViewSet):
    """Datatypes are used to define the types of data in CSV entries.

    Query parameters:

    * is_granted - true For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * base_for[] - id of a data type that you want to find the restriction base
        for. For example, base_for[]=<id of natural number> would return integer.
        You can include more than one of these parameters, and the result will
        include restriction bases for all of the requested parameters.
    """
    queryset = Datatype.objects.all()
    serializer_class = DatatypeSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination

    def filter_queryset(self, queryset):
        queryset = super(DatatypeViewSet, self).filter_queryset(queryset)
        restricting_datatypes = []
        base_for_values = self.request.GET.getlist('base_for')
        base_for_values.extend(self.request.GET.getlist('base_for[]'))
        for base_for in base_for_values:
            restricting_datatypes.append(Datatype.objects.get(pk=int(base_for)))
        if restricting_datatypes:
            base_datatypes = get_builtin_types(restricting_datatypes)
            base_pks = [datatype.pk for datatype in base_datatypes]
            queryset = queryset.filter(pk__in=base_pks)
        return queryset


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
    pagination_class = StandardPagination
