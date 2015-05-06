from rest_framework import serializers
from archive.models import Dataset, Run
from kive.serializers import TinyUserSerializer, GroupSerializer
from metadata.serializers import CompoundDatatypeSerializer
from django.core.urlresolvers import reverse
import os


class TinyRunSerializer(serializers.ModelSerializer):
    class Meta:
        model = Run
        feild = ('id', )


class DatasetSerializer(serializers.ModelSerializer):

    user = TinyUserSerializer()
    compounddatatype = CompoundDatatypeSerializer(source='symbolicdataset.compounddatatype')
    filename = serializers.SerializerMethodField()
    filesize = serializers.IntegerField(source='get_filesize')
    users_allowed = serializers.StringRelatedField(many=True, source="symbolicdataset.users_allowed")
    groups_allowed = serializers.StringRelatedField(many=True, source="symbolicdataset.groups_allowed")
    download_url = serializers.HyperlinkedIdentityField(view_name='dataset-download')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='dataset-removal-plan')
    redaction_plan = serializers.HyperlinkedIdentityField(view_name='dataset-redaction-plan')

    class Meta:
        model = Dataset
        fields = ('id', 'url', 'name', 'description', 'filename', 'user', 'date_created', 'date_modified',
                  'download_url', 'compounddatatype', 'filesize', 'users_allowed', 'groups_allowed', 'removal_plan',
                  'redaction_plan')

    def get_filename(self, obj):
        if obj:
            return os.path.basename(obj.dataset_file.name)
