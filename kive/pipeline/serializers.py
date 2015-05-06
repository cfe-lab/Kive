from rest_framework import serializers
from pipeline.models import PipelineFamily, Pipeline

from transformation.serializers import TransformationInputSerializer
from kive.serializers import TinyUserSerializer


class PipelineSerializer(serializers.ModelSerializer):

    inputs = TransformationInputSerializer(many=True)
    user = TinyUserSerializer()
    removal_plan = serializers.HyperlinkedIdentityField(view_name='pipeline-removal-plan')

    class Meta:
        model = Pipeline
        fields = ('id', 'url', 'revision_name', 'revision_number', 'inputs', 'user', 'removal_plan')


class PipelineFamilySerializer(serializers.ModelSerializer):

    members = PipelineSerializer(many=True)
    published_version = PipelineSerializer()
    removal_plan = serializers.HyperlinkedIdentityField(view_name='pipelinefamily-removal-plan')

    class Meta:
        model = PipelineFamily
        fields = ('id', 'url', 'name', 'members', 'published_version', 'removal_plan')

