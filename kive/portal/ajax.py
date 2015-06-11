from rest_framework import permissions, mixins

from kive.ajax import RemovableModelViewSet
from portal.views import admin_check

import portal.serializers
import portal.models


class StagedFileViewSet(mixins.CreateModelMixin, RemovableModelViewSet):
    queryset = portal.models.StagedFile.objects.all()
    serializer_class = portal.serializers.StagedFileSerializer
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        is_admin = admin_check(self.request.user)
        if is_admin:
            return self.queryset
        return self.queryset.filter(user=self.request.user)