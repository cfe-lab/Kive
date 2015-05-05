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
    download_url = serializers.SerializerMethodField()
    view_url = serializers.SerializerMethodField()
    filename = serializers.SerializerMethodField()
    filesize = serializers.SerializerMethodField()
    users_allowed = serializers.SerializerMethodField()
    groups_allowed = serializers.SerializerMethodField()

    class Meta:
        model = Dataset
        fields = ('id', 'name', 'description', 'filename', 'user', 'date_created', 'date_modified', 'download_url',
                  'view_url', 'compounddatatype', 'filesize', 'users_allowed', 'groups_allowed')

    def get_filename(self, obj):
        if obj:
            return os.path.basename(obj.dataset_file.name)

    def get_compounddatatype(self, obj):
        if not obj:
            return None
        if obj.symbolicdataset.compounddatatype is None:
            return None
        print dir(self)
        return obj.symbolicdataset.compounddatatype

    def get_download_url(self, obj):
        if not obj:
            return None
        return reverse('api_dataset_download', kwargs={'dataset_id': obj.id})

    def get_view_url(self, obj):
        if not obj:
            return None
        return reverse('dataset_view', kwargs={'dataset_id': obj.id})

    def get_filesize(self, obj):
        if not obj:
            return 0
        return obj.get_filesize()

    def get_users_allowed(self, obj):
        if not obj:
            return None
        return TinyUserSerializer(obj.symbolicdataset.users_allowed, many=True).data

    def get_groups_allowed(self, obj):
        if not obj:
            return None
        return GroupSerializer(obj.symbolicdataset.groups_allowed, many=True).data
