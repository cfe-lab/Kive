"""
Handle Ajax transaction requests from metadata templates.
"""
import itertools

from django.db.models import Q

from rest_framework import permissions
from rest_framework.exceptions import APIException

from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet, StandardPagination, \
    SearchableModelMixin
from metadata.models import Datatype, get_builtin_types, CompoundDatatype, CompoundDatatypeMember
from metadata.serializers import DatatypeSerializer, CompoundDatatypeSerializer


class DatatypeViewSet(RemovableModelViewSet, SearchableModelMixin):
    """Datatypes are used to define the types of data in CSV entries.

    Query parameters:

    * is_granted - true For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * base_for[] - id of a data type that you want to find the restriction base
        for. For example, base_for[]=<id of natural number> would return integer.
        You can include more than one of these parameters, and the result will
        include restriction bases for all of the requested parameters.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    * filters[n][key]=smart&filters[n][val]=match - name or description contains the value (case
        insensitive)
    * filters[n][key]=name&filters[n][val]=match - name contains the value (case
        insensitive)
    * filters[n][key]=description&filters[n][val]=match - description contains the value (case
        insensitive)
    * filters[n][key]=user&filters[n][val]=match - username of creator contains the value (case
        insensitive)
    """
    queryset = Datatype.objects.all()
    serializer_class = DatatypeSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination

    def filter_queryset(self, queryset):
        queryset = super(DatatypeViewSet, self).filter_queryset(queryset)

        # First, if base_for[] is specified, we're getting the builtin types
        # that this Datatype is based on.
        restricting_datatypes = []
        base_for_values = self.request.GET.getlist('base_for')
        base_for_values.extend(self.request.GET.getlist('base_for[]'))
        for base_for in base_for_values:
            restricting_datatypes.append(Datatype.objects.get(pk=int(base_for)))
        if restricting_datatypes:
            base_datatypes = get_builtin_types(restricting_datatypes)
            base_pks = [datatype.pk for datatype in base_datatypes]
            queryset = queryset.filter(pk__in=base_pks)

        # Now, we can apply the filters.
        return self.apply_filters(queryset)

    def _add_filter(self, queryset, key, value):
        """
        Filter the specified queryset by the specified key and value.
        """
        if key == 'smart':
            return queryset.filter(Q(name__icontains=value) | Q(description__icontains=value))
        if key == 'name':
            return queryset.filter(name__icontains=value)
        if key == 'description':
            return queryset.filter(description__icontains=value)
        if key == "user":
            return queryset.filter(user__username__icontains=value)

        raise APIException('Unknown filter key: {}'.format(key))


class CompoundDatatypeViewSet(RemovableModelViewSet, SearchableModelMixin):
    """Compound datatypes are used to define a CSV file format.

    Query parameters for the list view:

    * is_granted=true - For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    """
    queryset = CompoundDatatype.objects.all()
    serializer_class = CompoundDatatypeSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination

    def filter_queryset(self, queryset):
        queryset = super(CompoundDatatypeViewSet, self).filter_queryset(queryset)
        return self.apply_filters(queryset)

    def _add_filter(self, queryset, key, value):
        """
        Filter the specified queryset by the specified key and value.
        """
        all_pks = queryset.values_list("id", flat=True)
        CDT_pks = None
        if key == "smart":
            matching_members = CompoundDatatypeMember.objects.filter(
                (Q(column_name__icontains=value) |
                 Q(datatype__name__icontains=value)) &
                Q(compounddatatype__in=all_pks)
            )
            CDTs_with_user = queryset.filter(Q(user__username__icontains=value))
            CDT_pks = itertools.chain(
                matching_members.values_list("compounddatatype", flat=True),
                CDTs_with_user.values_list("id", flat=True)
            )
            return queryset.filter(pk__in=CDT_pks)

        if key == "name":
            matching_members = CompoundDatatypeMember.objects.filter(
                compounddatatype__in=all_pks,
                column_name__icontains=value
            )

        if key == "datatype":
            matching_members = CompoundDatatypeMember.objects.filter(
                compounddatatype__in=all_pks,
                datatype__name__icontains=value
            )

        if CDT_pks:
            CDT_pks = matching_members.values_list("compounddatatype", flat=True)
            return queryset.filter(pk__in=CDT_pks)

        if key == "user":
            return queryset.filter(Q(user__username__icontains=value))

        raise APIException('Unknown filter key: {}'.format(key))
