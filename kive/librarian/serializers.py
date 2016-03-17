import os

from django.template.defaultfilters import filesizeformat
from rest_framework import serializers

from librarian.models import Dataset, ExternalFileDirectory
from metadata.models import CompoundDatatype

from kive.serializers import AccessControlSerializer


class ExternalFileDirectorySerializer(serializers.ModelSerializer):
    class Meta():
        model = ExternalFileDirectory
        fields = (
            'name',
            'path',
            'display_name'
        )
        extra_kwargs = {
            'display_name': {'write_only': True}
        }


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

    save_in_db = serializers.BooleanField(default=False, write_only=True)

    class Meta():
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
        if obj:
            return os.path.basename(obj.dataset_file.name)

    def get_filesize_display(self, obj):
        if obj:
            return filesizeformat(obj.get_filesize())

    def validate(self):
        df_exists = "dataset_file" in validated_data
        ep_exists = "external_path" in validated_data
        efd_exists = "externalfiledirectory" in validated_data

        if df_exists:
            errors = []
            if ep_exists:
                errors.append("external_path should not be specified if dataset_file is")
            if efd_exists:
                errors.append(" externalfiledirectory should not be specified if dataset_file is")
            if errors:
                raise serializers.ValidationError(errors)

        if ep_exists and not efd_exists:
            raise serializers.ValidationError("externalfiledirectory must be specified")

        elif efd_exists and not ep_exists:
            raise serializers.ValidationError("external_path must be specified")



    def create(self, validated_data):
        """
        Create a Dataset object from deserialized and validated data.
        """
        cdt = None
        if "structure" in validated_data:
            cdt = validated_data["structure"].get("compounddatatype", None)

        file_path = None
        efd = None
        if "external_path" in validated_data:
            # At this point, Dataset.clean has assured that externalfiledirectory is also specified.
            file_path = ""
        file_path = validated_data.get("external_path", None)

        dataset = Dataset.create_dataset(
            file_path=file_path,
            user=self.context["request"].user,
            users_allowed=validated_data["users_allowed"],
            groups_allowed=validated_data["groups_allowed"],
            cdt=cdt,
            keep_file=validated_data["save_in_db"],
            name=validated_data["name"],
            description=validated_data["description"],
            file_source=None,
            check=True,
            file_handle=validated_data["dataset_file"],
            is_external="external_path" in validated_data
        )
        return dataset
