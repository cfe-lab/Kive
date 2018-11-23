from rest_framework import serializers
from rest_framework.fields import URLField

from container.models import ContainerFamily, Container, ContainerApp
from kive.serializers import AccessControlSerializer


class ContainerFamilySerializer(AccessControlSerializer,
                                serializers.ModelSerializer):
    absolute_url = URLField(source='get_absolute_url', read_only=True)
    num_containers = serializers.IntegerField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='containerfamily-removal-plan')
    containers = serializers.HyperlinkedIdentityField(
        view_name="containerfamily-containers")

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
            "containers",
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


class ContainerAppSerializer(serializers.ModelSerializer):
    absolute_url = URLField(source='get_absolute_url', read_only=True)
    container = serializers.HyperlinkedRelatedField(
        view_name='container-detail',
        lookup_field='pk',
        queryset=Container.objects.all())
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='containerapp-removal-plan')

    class Meta:
        model = ContainerApp
        fields = ('id',
                  'url',
                  'absolute_url',
                  'container',
                  'name',
                  'description',
                  'inputs',
                  'outputs',
                  'removal_plan')

    def save(self, **kwargs):
        app = super(ContainerAppSerializer, self).save(**kwargs)
        app.write_inputs(self.initial_data.get('inputs', ''))
        app.write_outputs(self.initial_data.get('outputs', ''))
        return app
