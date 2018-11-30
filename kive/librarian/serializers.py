import os

from django.template.defaultfilters import filesizeformat
from rest_framework import serializers

from librarian.models import Dataset, ExternalFileDirectory
from metadata.models import CompoundDatatype

from kive.serializers import AccessControlSerializer


class ExternalFileDirectorySerializer(serializers.ModelSerializer):
    class Meta(object):
        model = ExternalFileDirectory
        fields = (
            'pk',
            'url',
            'name',
            'path'
        )


class ExternalFileDirectoryListFilesSerializer(ExternalFileDirectorySerializer):
    """
    Gives a list of file choices within this ExternalFileDirectory.

    This is intended to be used to look at a single ExternalFileDirectory
    at a time, as the list_files field may be too slow and/or provide
    too much output.
    """
    class Meta(object):
        model = ExternalFileDirectory
        fields = (
            'pk',
            'url',
            'name',
            'path',
            'list_files'
        )


class DatasetSerializer(AccessControlSerializer, serializers.ModelSerializer):

    compounddatatype = serializers.PrimaryKeyRelatedField(
        source="structure.compounddatatype",
        queryset=CompoundDatatype.objects.all(),
        required=False
    )

    filename = serializers.SerializerMethodField()
    filesize = serializers.IntegerField(source='get_filesize', read_only=True)
    filesize_display = serializers.SerializerMethodField()

    download_url = serializers.HyperlinkedIdentityField(view_name='dataset-download')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='dataset-removal-plan')
    redaction_plan = serializers.HyperlinkedIdentityField(view_name='dataset-redaction-plan')

    save_in_db = serializers.NullBooleanField(write_only=True, required=False)
    externalfiledirectory = serializers.SlugRelatedField(
        slug_field="name",
        queryset=ExternalFileDirectory.objects.all(),
        allow_null=True,
        required=False
    )

    class Meta(object):
        model = Dataset
        fields = (
            'id',
            'url',
            'name',
            'description',
            'dataset_file',
            'externalfiledirectory',
            'external_path',
            'save_in_db',
            'filename',
            'date_created',
            'download_url',
            'compounddatatype',
            'filesize',
            'filesize_display',
            'MD5_checksum',
            'has_data',
            'is_redacted',
            'uploaded',

            'user',  # inherited
            'users_allowed',
            'groups_allowed',

            'removal_plan',
            'redaction_plan'
        )

    def __init__(self, *args, **kwargs):
        super(DatasetSerializer, self).__init__(*args, **kwargs)
        self.fields["compounddatatype"].queryset = CompoundDatatype.filter_by_user(self.context["request"].user)

    def get_filename(self, obj):
        if not obj:
            return

        if obj.dataset_file:
            return os.path.basename(obj.dataset_file.name)
        elif obj.external_path:
            return os.path.basename(obj.external_path)

    def get_filesize_display(self, obj):
        if obj:
            return filesizeformat(obj.get_filesize())

    def validate(self, data):
        df_exists = "dataset_file" in data
        ep_exists = "external_path" in data
        efd_exists = "externalfiledirectory" in data

        if df_exists:
            errors = []
            if ep_exists:
                errors.append("external_path should not be specified if dataset_file is")
            if efd_exists:
                errors.append("externalfiledirectory should not be specified if dataset_file is")
            if errors:
                raise serializers.ValidationError(errors)

        if ep_exists and not efd_exists:
            raise serializers.ValidationError("externalfiledirectory must be specified")

        elif efd_exists and not ep_exists:
            raise serializers.ValidationError("external_path must be specified")

        return super(DatasetSerializer, self).validate(data)

    def create(self, validated_data):
        """
        Create a Dataset object from deserialized and validated data.
        """
        cdt = None
        if "structure" in validated_data:
            cdt = validated_data["structure"].get("compounddatatype", None)

        # The default behaviour for keep_file depends on the mode of creation.
        keep_file = True
        file_path = validated_data.get("external_path", "")
        efd = validated_data.get("externalfiledirectory", None)
        # Both or neither are specified (this is enforced in serializer validation).
        if file_path:
            file_path = os.path.join(efd.path, file_path)
            keep_file = False  # don't retain a copy by default

        # Override the default if specified.
        keep_file = validated_data.get("save_in_db", keep_file)

        dataset = Dataset.create_dataset(
            file_path=file_path,
            user=self.context["request"].user,
            users_allowed=validated_data["users_allowed"],
            groups_allowed=validated_data["groups_allowed"],
            cdt=cdt,
            keep_file=keep_file,
            name=validated_data["name"],
            description=validated_data["description"],
            file_source=None,
            check=True,
            file_handle=validated_data.get("dataset_file", None),  # should be freshly opened so cursor is at start
            externalfiledirectory=efd
        )
        return dataset
