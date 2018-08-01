from rest_framework import serializers
from rest_framework.fields import URLField

from container.models import ContainerFamily
from kive.serializers import AccessControlSerializer


class ContainerFamilySerializer(AccessControlSerializer,
                                serializers.ModelSerializer):
    absolute_url = URLField(source='get_absolute_url')
    # TODO: removal plan here and in fields
    # removal_plan = serializers.HyperlinkedIdentityField(
    #     view_name='dockerimage-removal-plan')

    class Meta:
        model = ContainerFamily
        fields = (
            "name",
            "description",
            "git",
            "url",
            "absolute_url",
            "user",
            "users_allowed",
            "groups_allowed")
