from rest_framework import serializers
from fleet.models import RunToProcess
from archive.serializers import TinyRunSerializer, RunOutputsSerializer


class RunToProcessSerializer(serializers.ModelSerializer):
    run = TinyRunSerializer()
    run_status = serializers.HyperlinkedIdentityField(view_name='runtoprocess-run-status')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='runtoprocess-removal-plan')
    run_outputs = serializers.HyperlinkedIdentityField(view_name='runtoprocess-run-outputs')

    class Meta:
        model = RunToProcess
        fields = ('id', 'url', 'run', 'run_status', 'run_outputs', 'removal_plan')

class RunToProcessOutputsSerializer(serializers.ModelSerializer):
    run = RunOutputsSerializer()
    
    class Meta:
        model = RunToProcess
        fields = ('id', 'run')