from rest_framework import serializers
from fleet.models import RunToProcess
from archive.serializers import TinyRunSerializer
from django.core.urlresolvers import reverse


class RunToProcessSerializer(serializers.ModelSerializer):
    run = TinyRunSerializer()
    run_status = serializers.SerializerMethodField()

    class Meta:
        model = RunToProcess
        fields = ('id', 'run', 'run_status')

    def get_run_status(self, obj):
        if obj:
            return reverse('api_pipelines_runstat', kwargs={'rtp_id': obj.id})