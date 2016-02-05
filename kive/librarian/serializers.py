import os

from django.template.defaultfilters import filesizeformat
from rest_framework import serializers

from librarian.models import Dataset
from metadata.models import CompoundDatatype

from kive.serializers import AccessControlSerializer


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

    class Meta():
        model = Dataset
        fields = (
            'id',
            'url',
            'name',
            'description',
            "dataset_file",
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

    def create(self, validated_data):
        """
        Create a Dataset object from deserialized and validated data.
        """
        cdt = None
        if "structure" in validated_data:
            cdt = validated_data["structure"].get("compounddatatype", None)

        dataset = Dataset.create_dataset(
            file_path=None,
            user=self.context["request"].user,
            users_allowed=validated_data["users_allowed"],
            groups_allowed=validated_data["groups_allowed"],
            cdt=cdt,
            keep_file=True,
            name=validated_data["name"],
            description=validated_data["description"],
            file_source=None,
            check=True,
            file_handle=validated_data["dataset_file"]
        )
        return dataset
