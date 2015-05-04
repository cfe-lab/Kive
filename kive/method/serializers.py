from rest_framework import serializers
from method.models import MethodFamily

from kive.serializers import TinyUserSerializer, GroupSerializer


class MethodFamilyTableSerializer(serializers.ModelSerializer):
    url = serializers.SerializerMethodField()
    user = TinyUserSerializer()
    users_allowed = serializers.SerializerMethodField()
    groups_allowed = serializers.SerializerMethodField()
    num_revisions = serializers.SerializerMethodField()

    class Meta:
        model = MethodFamily
        fields = ("name", "description", "url", "user", "users_allowed", "groups_allowed", "num_revisions")

    def get_users_allowed(self, obj):
        if not obj:
            return None
        return TinyUserSerializer(obj.users_allowed, many=True).data

    def get_groups_allowed(self, obj):
        if not obj:
            return None
        return GroupSerializer(obj.groups_allowed, many=True).data

    def get_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()

    def get_num_revisions(self, obj):
        if not obj:
            return None
        return obj.num_revisions