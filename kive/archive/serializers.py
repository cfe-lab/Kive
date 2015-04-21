from rest_framework import serializers
from archive.models import Dataset, Run
from kive.serializers import TinyUserSerializer
from metadata.serializers import CompoundDatatypeInputSerializer
from django.core.urlresolvers import reverse
import os


class TinyRunSerializer(serializers.ModelSerializer):
    class Meta:
        model = Run
        feild = ('id', )


class DatasetSerializer(serializers.ModelSerializer):

    user = TinyUserSerializer()
    compounddatatype = serializers.SerializerMethodField()
    download_url = serializers.SerializerMethodField()
    filename = serializers.SerializerMethodField()

    class Meta:
        model = Dataset
        fields = ('id', 'name', 'filename', 'user', 'date_created', 'date_modified', 'download_url', 'compounddatatype')

    def get_filename(self, obj):
        if obj:
            return os.path.basename(obj.dataset_file.name)

    def get_compounddatatype(self, obj):
        if not obj:
            return None
        if obj.symbolicdataset.compounddatatype is None:
            return None
        return CompoundDatatypeInputSerializer(obj.symbolicdataset.compounddatatype).data

    def get_download_url(self, obj):
        if not obj:
            return None
        return reverse('api_dataset_download', kwargs={'dataset_id': obj.id})


