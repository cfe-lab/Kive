from rest_framework import serializers
from fleet.models import RunToProcess
from archive.serializers import TinyRunSerializer


class RunToProcessSerializer(serializers.ModelSerializer):
    run = TinyRunSerializer()
    run_status = serializers.HyperlinkedIdentityField(view_name='runtoprocess-run-status')
    run_results = serializers.HyperlinkedIdentityField(view_name='runtoprocess-run-results')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='runtoprocess-removal-plan')

    class Meta:
        model = RunToProcess
        fields = ('id', 'url', 'run', 'run_status', 'run_results', 'removal_plan')
