import mimetypes
import os
from wsgiref.util import FileWrapper

from django.db.models import Q
from django.db.models.aggregates import Count
from django.http import HttpResponse
from rest_framework import permissions
from rest_framework.decorators import action
from rest_framework.response import Response

from container.models import ContainerFamily, Container, ContainerApp
from container.serializers import ContainerFamilySerializer, ContainerSerializer, \
    ContainerAppSerializer
from kive.ajax import CleanCreateModelMixin, RemovableModelViewSet, \
    SearchableModelMixin, IsDeveloperOrGrantedReadOnly, StandardPagination
from metadata.models import AccessControl
from portal.views import admin_check


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
    queryset = ContainerFamily.objects.annotate(
        num_containers=Count('containers'))
    serializer_class = ContainerFamilySerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination
    filters = dict(
        smart=lambda queryset, value: queryset.filter(
            Q(name__icontains=value) |
            Q(git__icontains=value) |
            Q(description__icontains=value)),
        name=lambda queryset, value: queryset.filter(
            name__icontains=value),
        git=lambda queryset, value: queryset.filter(
            git__icontains=value),
        description=lambda queryset, value: queryset.filter(
            description__icontains=value),
        user=lambda queryset, value: queryset.filter(
            user__username__icontains=value))

    @action(detail=True)
    def containers(self, request, pk=None):
        if self.request.query_params.get('is_granted') == 'true':
            is_admin = False
        else:
            is_admin = admin_check(self.request.user)

        family_members = AccessControl.filter_by_user(
            request.user,
            is_admin=is_admin,
            queryset=Container.objects.filter(family_id=pk))

        return Response(
            ContainerSerializer(family_members,
                                many=True,
                                context={"request": request}).data)


class ContainerViewSet(CleanCreateModelMixin,
                       RemovableModelViewSet,
                       SearchableModelMixin):
    """ A Singularity container.

    Query parameters:

    * is_granted - true For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    * filters[n][key]=family_id&filters[n][val]=match - parent family's id equals
        the value
    * filters[n][key]=smart&filters[n][val]=match - family name, tag, or
        description contains the value (case insensitive)
    * filters[n][key]=name&filters[n][val]=match - family name contains the
        value (case insensitive)
    * filters[n][key]=tag&filters[n][val]=match - tag contains the value (case
        insensitive)
    * filters[n][key]=description&filters[n][val]=match - description contains
        the value (case insensitive)
    * filters[n][key]=user&filters[n][val]=match - username of creator contains
        the value (case insensitive)
    """
    queryset = Container.objects.all()
    serializer_class = ContainerSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination
    filters = dict(
        family_id=lambda queryset, value: queryset.filter(
            family_id=value),
        smart=lambda queryset, value: queryset.filter(
            Q(tag__icontains=value) |
            Q(description__icontains=value)),
        tag=lambda queryset, value: queryset.filter(
            tag__icontains=value),
        description=lambda queryset, value: queryset.filter(
            description__icontains=value),
        user=lambda queryset, value: queryset.filter(
            user__username__icontains=value))

    # noinspection PyUnusedLocal
    @action(detail=True)
    def download(self, request, pk=None):
        container = self.get_object()
        container.file.open()
        try:
            # Stream file in chunks to avoid overloading memory.
            file_chunker = FileWrapper(container.file)

            mimetype = mimetypes.guess_type(container.file.name)[0]
            response = HttpResponse(file_chunker, content_type=mimetype)
            response['Content-Length'] = container.file.size
            response['Content-Disposition'] = 'attachment; filename="{}"'.format(
                os.path.basename(container.file.name))
        finally:
            container.file.close()
        return response


class ContainerAppViewSet(CleanCreateModelMixin,
                          RemovableModelViewSet,
                          SearchableModelMixin):
    """ An app within a Singularity container.

    Query parameters:

    * is_granted - true For administrators, this limits the list to only include
        records that the user has been explicitly granted access to, via their
        parent containers. For other users, this has no effect.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    * filters[n][key]=container_id&filters[n][val]=match - parent container's
        id equals the value
    * filters[n][key]=smart&filters[n][val]=match - app name or description
        contains the value (case insensitive)
    * filters[n][key]=name&filters[n][val]=match - app name contains the
        value (case insensitive)
    * filters[n][key]=description&filters[n][val]=match - description contains
        the value (case insensitive)
    """
    queryset = ContainerApp.objects.all()
    serializer_class = ContainerAppSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination
    filters = dict(
        container_id=lambda queryset, value: queryset.filter(
            container_id=value),
        smart=lambda queryset, value: queryset.filter(
            Q(name__icontains=value) |
            Q(description__icontains=value)),
        name=lambda queryset, value: queryset.filter(
            name__icontains=value),
        description=lambda queryset, value: queryset.filter(
            description__icontains=value))

    def filter_granted(self, queryset):
        """ Apps don't have permissions, so filter by parent containers. """
        granted_containers = Container.filter_by_user(self.request.user)

        return queryset.filter(container_id__in=granted_containers)
