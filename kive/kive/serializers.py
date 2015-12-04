from django.contrib.auth.models import User, Group
from rest_framework import serializers


class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ('id', 'name')


class TinyUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ('id', 'username')


class UserSerializer(serializers.ModelSerializer):
    groups = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ('id', 'username', 'email', 'groups')

    def get_groups(self, obj):
        if not obj:
            return None
        return [GroupSerializer(x).data for x in obj.groups.all()]


class PermissionsSerializer(serializers.Serializer):
    """
    Serializer that encapsulates users and groups.

    This is useful when populating a PermissionsField.
    """
    users = TinyUserSerializer(many=True)
    groups = GroupSerializer(many=True)


class AccessControlSerializer(serializers.Serializer):
    """
    Mixin that adds SlugRelatedFields to AccessControl-based ModelSerializers.
    """
    user = serializers.SlugRelatedField(
        slug_field="username",
        read_only=True,
        default=serializers.CurrentUserDefault()
    )
    users_allowed = serializers.SlugRelatedField(
        slug_field="username",
        queryset=User.objects.all(),
        many=True,
        allow_null=True,
        required=False
    )
    groups_allowed = serializers.SlugRelatedField(
        slug_field="name",
        queryset=Group.objects.all(),
        many=True,
        allow_null=True,
        required=False
    )