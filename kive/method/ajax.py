from rest_framework import permissions, status
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet, CleanCreateModelMixin, StandardPagination
from method.models import CodeResourceRevision, Method, MethodFamily, CodeResource
from method.serializers import MethodSerializer, MethodFamilySerializer, \
    CodeResourceSerializer, CodeResourceRevisionSerializer
from metadata.models import AccessControl
from archive.views import _build_download_response
from portal.views import admin_check


class CodeResourceViewSet(RemovableModelViewSet):
    queryset = CodeResource.objects.all()
    serializer_class = CodeResourceSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination

    @detail_route(methods=["get"])
    def revisions(self, request, pk=None):
        if self.request.QUERY_PARAMS.get('is_granted') == 'true':
            is_admin = False
        else:
            is_admin = admin_check(self.request.user)

        revisions = AccessControl.filter_by_user(
            request.user,
            is_admin=is_admin,
            queryset=self.get_object().revisions.all())

        return Response(
            CodeResourceRevisionSerializer(revisions, many=True, context={"request": request}).data
        )


class CodeResourceRevisionViewSet(CleanCreateModelMixin, RemovableModelViewSet):
    queryset = CodeResourceRevision.objects.all()
    serializer_class = CodeResourceRevisionSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination

    @detail_route(methods=['get'])
    def download(self, request, pk=None):
        """
        Download the file pointed to by this CodeResourceRevision.
        """
        accessible_CRRs = CodeResourceRevision.filter_by_user(request.user)
        CRR = self.get_object()

        if not accessible_CRRs.filter(pk=CRR.pk).exists():
            return Response(None, status=status.HTTP_404_NOT_FOUND)
        elif not CRR.content_file:
            return Response({"errors": "This CodeResourceRevision has no content file."},
                            status=status.HTTP_403_FORBIDDEN)

        return _build_download_response(CRR.content_file)


class MethodFamilyViewSet(RemovableModelViewSet):
    queryset = MethodFamily.objects.all()
    serializer_class = MethodFamilySerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
    pagination_class = StandardPagination

    @detail_route(methods=["get"])
    def methods(self, request, pk=None):
        if self.request.QUERY_PARAMS.get('is_granted') == 'true':
            is_admin = False
        else:
            is_admin = admin_check(self.request.user)

        member_methods = AccessControl.filter_by_user(
            request.user,
            is_admin=is_admin,
            queryset=self.get_object().members.all())

        member_serializer = MethodSerializer(
            member_methods, many=True, context={"request": request})
        return Response(member_serializer.data)


class MethodViewSet(CleanCreateModelMixin, RemovableModelViewSet):
    queryset = Method.objects.all()
    serializer_class = MethodSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)
