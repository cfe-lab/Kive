from rest_framework import permissions, mixins
from rest_framework.viewsets import ReadOnlyModelViewSet

from django.contrib.auth.models import User

from kive.ajax import RemovableModelViewSet
from kive.serializers import UserSerializer
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


class UserViewSet(ReadOnlyModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer