from rest_framework import serializers
from rest_framework.fields import URLField

from container.models import ContainerFamily, Container, ContainerApp, ContainerRun, Batch, ContainerDataset, \
    ContainerArgument, ContainerLog
from kive.serializers import AccessControlSerializer
from librarian.models import Dataset


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
    family = serializers.HyperlinkedRelatedField(
        view_name='containerfamily-detail',
        lookup_field='pk',
        queryset=ContainerFamily.objects.all())
    family_name = serializers.SlugRelatedField(
        source='family',
        slug_field='name',
        read_only=True)
    parent = serializers.HyperlinkedRelatedField(
        view_name='container-detail',
        lookup_field='pk',
        queryset=Container.objects.filter(file_type=Container.SIMG),
        required=False,
        allow_null=True)
    download_url = serializers.HyperlinkedIdentityField(
        view_name='container-download')
    num_apps = serializers.IntegerField(read_only=True)
    app_list = serializers.HyperlinkedIdentityField(
        view_name='container-app-list')
    content = serializers.HyperlinkedIdentityField(
        view_name='container-content')
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='container-removal-plan')

    class Meta:
        model = Container
        fields = ('id',
                  'url',
                  'download_url',
                  'absolute_url',
                  'family',
                  'family_name',
                  'parent',
                  'file',
                  'file_type',
                  'tag',
                  'description',
                  'md5',
                  'num_apps',
                  'created',
                  'user',
                  'users_allowed',
                  'groups_allowed',
                  'app_list',
                  'content',
                  'removal_plan')

    def create(self, validated_data):
        container = super(ContainerSerializer, self).create(validated_data)
        if container.file_type != Container.SIMG:
            container.create_app_from_content()
        return container


class ContainerAppSerializer(serializers.ModelSerializer):
    absolute_url = URLField(source='get_absolute_url', read_only=True)
    container = serializers.HyperlinkedRelatedField(
        view_name='container-detail',
        lookup_field='pk',
        queryset=Container.objects.all())
    container_name = serializers.SlugRelatedField(
        source='container',
        slug_field='display_name',
        read_only=True)
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='containerapp-removal-plan')
    argument_list = serializers.HyperlinkedIdentityField(
        view_name='containerapp-argument-list')

    class Meta:
        model = ContainerApp
        fields = ('id',
                  'url',
                  'absolute_url',
                  'container',
                  'container_name',
                  'name',
                  'description',
                  'threads',
                  'memory',
                  'inputs',
                  'outputs',
                  'argument_list',
                  'removal_plan')

    def save(self, **kwargs):
        app = super(ContainerAppSerializer, self).save(**kwargs)
        app.write_inputs(self.initial_data.get('inputs', ''))
        app.write_outputs(self.initial_data.get('outputs', ''))
        return app


class ContainerArgumentSerializer(serializers.ModelSerializer):
    app = serializers.HyperlinkedRelatedField(
        view_name='containerapp-detail',
        lookup_field='pk',
        queryset=ContainerApp.objects.all())

    class Meta(object):
        model = ContainerArgument
        fields = ('id',
                  'url',
                  'name',
                  'type',
                  'position',
                  'app',
                  'allow_multiple')


class ContainerAppChoiceSerializer(serializers.ModelSerializer):
    class Meta:
        model = ContainerApp
        fields = ('id',
                  'url',
                  'name',
                  'description',
                  'threads',
                  'memory')


class ContainerChoiceSerializer(AccessControlSerializer,
                                serializers.ModelSerializer):
    apps = ContainerAppChoiceSerializer(many=True, read_only=True)

    class Meta:
        model = Container
        fields = ('id',
                  'url',
                  'file',
                  'tag',
                  'description',
                  'md5',
                  'created',
                  'user',
                  'users_allowed',
                  'groups_allowed',
                  'apps')


class ContainerFamilyChoiceSerializer(AccessControlSerializer,
                                      serializers.ModelSerializer):
    containers = ContainerChoiceSerializer(many=True, read_only=True)

    class Meta:
        model = ContainerFamily
        fields = (
            "id",
            "url",
            "name",
            "description",
            "git",
            "user",
            "users_allowed",
            "groups_allowed",
            "containers")


class ContainerDatasetSerializer(serializers.ModelSerializer):
    run = serializers.HyperlinkedRelatedField(
        view_name='containerrun-detail',
        lookup_field='pk',
        queryset=ContainerRun.objects.all(),
        required=False)  # Not required when nested inside a run.
    argument = serializers.HyperlinkedRelatedField(
        view_name='containerargument-detail',
        lookup_field='pk',
        queryset=ContainerArgument.objects.all())
    dataset = serializers.HyperlinkedRelatedField(
        view_name='dataset-detail',
        lookup_field='pk',
        queryset=Dataset.objects.all())
    dataset_name = serializers.SlugRelatedField(
        source='dataset',
        slug_field='name',
        read_only=True)
    dataset_purged = serializers.SlugRelatedField(
        source='dataset',
        slug_field='is_purged',
        read_only=True)
    argument_name = serializers.SlugRelatedField(
        source='argument',
        slug_field='name',
        read_only=True)
    argument_type = serializers.SlugRelatedField(
        source='argument',
        slug_field='type',
        read_only=True)

    class Meta:
        model = ContainerDataset
        fields = ('run',
                  'argument',
                  'argument_name',
                  'argument_type',
                  'dataset',
                  'dataset_name',
                  'dataset_purged',
                  'name',
                  'created')


class ContainerRunSerializer(AccessControlSerializer,
                             serializers.ModelSerializer):
    datasets = ContainerDatasetSerializer(many=True,
                                          required=False,
                                          write_only=True)
    dataset_list = serializers.HyperlinkedIdentityField(
        view_name='containerrun-dataset-list')
    log_list = serializers.HyperlinkedIdentityField(
        view_name='containerrun-log-list')
    absolute_url = URLField(source='get_absolute_url', read_only=True)
    app = serializers.HyperlinkedRelatedField(
        view_name='containerapp-detail',
        lookup_field='pk',
        queryset=ContainerApp.objects.all(),
        required=False)
    app_name = serializers.SlugRelatedField(
        source='app',
        slug_field='display_name',
        read_only=True)
    batch = serializers.HyperlinkedRelatedField(
        view_name='batch-detail',
        lookup_field='pk',
        queryset=Batch.objects.all(),
        required=False)
    batch_name = serializers.SlugRelatedField(
        source='batch',
        slug_field='name',
        read_only=True)
    batch_absolute_url = serializers.SlugRelatedField(
        source='batch',
        slug_field='absolute_url',
        read_only=True)
    original_run = serializers.HyperlinkedRelatedField(
        view_name='containerrun-detail',
        lookup_field='pk',
        queryset=ContainerRun.objects.all(),
        required=False)
    stopped_by = serializers.SlugRelatedField(
        slug_field='username',
        read_only=True)
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='containerrun-removal-plan')

    class Meta:
        model = ContainerRun
        fields = ('id',
                  'url',
                  'absolute_url',
                  'name',
                  'description',
                  'batch',
                  'batch_name',
                  'batch_absolute_url',
                  'original_run',
                  'app',
                  'app_name',
                  'state',
                  'priority',
                  'slurm_job_id',
                  'return_code',
                  'stopped_by',
                  'is_redacted',
                  'start_time',
                  'end_time',
                  'user',
                  'users_allowed',
                  'groups_allowed',
                  'removal_plan',
                  'dataset_list',
                  'log_list',
                  'datasets')
        read_only_fields = ('state',
                            'slurm_job_id',
                            'return_code',
                            'start_time',
                            'end_time')

    def create(self, validated_data):
        """Create a Run and the inputs it contains."""
        datasets = validated_data.pop("datasets", [])

        original_run = validated_data.get('original_run')
        if original_run is not None:
            validated_data['app'] = original_run.app
        run = super(ContainerRunSerializer, self).create(validated_data)
        run.validate_restrict_access(run.get_access_limits())
        dataset_serializer = ContainerDatasetSerializer()
        for dataset in datasets:
            dataset['run'] = run
            dataset_serializer.create(dataset)
        if original_run is not None:
            for container_dataset in original_run.datasets.filter(
                    argument__type='I'):
                container_dataset.id = None  # Make a copy.
                container_dataset.dataset = container_dataset.find_rerun_dataset()
                container_dataset.run = run
                container_dataset.save()
        return run


class ContainerLogSerializer(serializers.ModelSerializer):
    run = serializers.HyperlinkedRelatedField(
        view_name='containerrun-detail',
        lookup_field='pk',
        queryset=ContainerRun.objects.all())
    download_url = serializers.HyperlinkedIdentityField(
        view_name='containerlog-download')

    class Meta:
        model = ContainerLog
        fields = ('id',
                  'url',
                  'download_url',
                  'run',
                  'type',
                  'size')


class BatchSerializer(AccessControlSerializer,
                      serializers.ModelSerializer):
    runs = ContainerRunSerializer(many=True, required=False)
    # absolute_url = URLField(source='get_absolute_url', read_only=True)
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='batch-removal-plan')
    copy_permissions_to_runs = serializers.BooleanField(default=True, write_only=True)

    class Meta:
        model = Batch
        fields = ('id',
                  'url',
                  'name',
                  'description',
                  'user',
                  'users_allowed',
                  'groups_allowed',
                  'removal_plan',
                  'copy_permissions_to_runs',
                  'runs')

    def create(self, validated_data):
        """Create a RunBatch and the Runs it contains."""
        run_dictionaries = validated_data.pop("runs", [])
        users_allowed = validated_data.pop("users_allowed", [])
        groups_allowed = validated_data.pop("groups_allowed", [])
        copy_permissions_to_runs = validated_data.pop("copy_permissions_to_runs")

        batch = Batch(**validated_data)
        batch.save()
        batch.users_allowed.add(*users_allowed)
        batch.groups_allowed.add(*groups_allowed)

        run_serializer = ContainerRunSerializer()
        for run_data in run_dictionaries:
            if (len(run_data.get("users_allowed", [])) == 0 and
                    len(run_data.get("groups_allowed", [])) == 0 and
                    copy_permissions_to_runs):
                run_data["users_allowed"] = users_allowed
                run_data["groups_allowed"] = groups_allowed

            run_data["batch"] = batch
            run_serializer.create(run_data)

        return batch
