from rest_framework import serializers
from rest_framework.fields import URLField

from container.models import ContainerFamily, Container
from kive.serializers import AccessControlSerializer


class ContainerFamilySerializer(AccessControlSerializer,
                                serializers.ModelSerializer):
    absolute_url = URLField(source='get_absolute_url', read_only=True)
    num_containers = serializers.IntegerField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='containerfamily-removal-plan')

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
            "groups_allowed",
            "num_containers",
            "removal_plan")


class ContainerSerializer(AccessControlSerializer,
                          serializers.ModelSerializer):
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
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='container-removal-plan')

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
                  'groups_allowed',
                  'removal_plan')
