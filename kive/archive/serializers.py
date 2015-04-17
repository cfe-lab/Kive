from rest_framework import serializers
from archive.models import Dataset
from kive.serializers import TinyUserSerializer
from django.core.urlresolvers import reverse


class DatasetSerializer(serializers.ModelSerializer):

    user = TinyUserSerializer()
    download_url = serializers.SerializerMethodField()

    class Meta:
        model = Dataset
        fields = ('id', 'name', 'user', 'date_created', 'date_modified', 'download_url')

    def get_download_url(self, obj):
        if not obj:
            return None
        return reverse('dataset_download', kwargs={'dataset_id': obj.id})

