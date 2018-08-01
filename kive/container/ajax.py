from django.db.models import Q
from rest_framework import permissions
from rest_framework.exceptions import APIException

from container.models import ContainerFamily
from container.serializers import ContainerFamilySerializer
from kive.ajax import CleanCreateModelMixin, RemovableModelViewSet, \
    SearchableModelMixin, IsDeveloperOrGrantedReadOnly, StandardPagination


class ContainerFamilyViewSet(CleanCreateModelMixin,
                             RemovableModelViewSet,
                             SearchableModelMixin):
    """ A container family is a set of Singularity containers that are all
    built from different versions of the same source.

    Query parameters:

    * is_granted - true For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    * filters[n][key]=smart&filters[n][val]=match - name, git, or
        description contains the value (case insensitive)
    * filters[n][key]=name&filters[n][val]=match - name contains the value (case
        insensitive)
    * filters[n][key]=git&filters[n][val]=match - git contains the value (case
        insensitive)
    * filters[n][key]=description&filters[n][val]=match - description contains
        the value (case insensitive)
    * filters[n][key]=user&filters[n][val]=match - username of creator contains
        the value (case insensitive)
    """
    queryset = ContainerFamily.objects.all()
    serializer_class = ContainerFamilySerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination

    def filter_queryset(self, queryset):
        queryset = super(ContainerFamilyViewSet, self).filter_queryset(queryset)
        return self.apply_filters(queryset)

    @staticmethod
    def _add_filter(queryset, key, value):
        """
        Filter the specified queryset by the specified key and value.
        """
        if key == 'smart':
            return queryset.filter(Q(name__icontains=value) |
                                   Q(git__icontains=value) |
                                   Q(description__icontains=value))
        if key == 'name':
            return queryset.filter(name__icontains=value)
        if key == 'git':
            return queryset.filter(git__icontains=value)
        if key == 'description':
            return queryset.filter(description__icontains=value)
        if key == "user":
            return queryset.filter(user__username__icontains=value)

        raise APIException('Unknown filter key: {}'.format(key))
