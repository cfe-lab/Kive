from rest_framework import serializers
from rest_framework.fields import URLField

from container.models import ContainerFamily, Container
from kive.serializers import AccessControlSerializer


class ContainerFamilySerializer(AccessControlSerializer,
                                serializers.ModelSerializer):
    absolute_url = URLField(source='get_absolute_url', read_only=True)
    # TODO: removal plan here and in fields
    # removal_plan = serializers.HyperlinkedIdentityField(
    #     view_name='dockerimage-removal-plan')

    class Meta:
        model = ContainerFamily
        fields = (
            "id",
            "url",
            "absolute_url",
            "name",
            "description",
            "git",
            "user",
            "users_allowed",
            "groups_allowed")


class ContainerSerializer(AccessControlSerializer,
                          serializers.ModelSerializer):
    # TODO: removal plan here and in fields
    # removal_plan = serializers.HyperlinkedIdentityField(
    #     view_name='dockerimage-removal-plan')

    absolute_url = URLField(source='get_absolute_url', read_only=True)
    family = serializers.SlugRelatedField(
        slug_field='name',
        queryset=ContainerFamily.objects.all())
    family_url = serializers.HyperlinkedRelatedField(
        source='family',
        view_name='containerfamily-detail',
        lookup_field='pk',
        read_only=True)
    download_url = serializers.HyperlinkedIdentityField(
        view_name='container-download')

    class Meta:
        model = Container
        fields = ('id',
                  'url',
                  'download_url',
                  'absolute_url',
                  'family',
                  'family_url',
                  'file',
                  'tag',
                  'description',
                  'md5',
                  'created',
                  'user',
                  'users_allowed',
                  'groups_allowed')
