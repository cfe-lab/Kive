from rest_framework import serializers
from pipeline.models import PipelineFamily, Pipeline

from transformation.serializers import TransformationInputSerializer
from kive.serializers import TinyUserSerializer

class PipelineSerializer(serializers.ModelSerializer):

    inputs = TransformationInputSerializer(many=True)
    user = TinyUserSerializer()

    class Meta:
        model = Pipeline
        fields = ('id', 'url', 'revision_name', 'revision_number', 'inputs', 'user')


class PipelineFamilySerializer(serializers.ModelSerializer):

    members = PipelineSerializer(many=True)
    published_version = PipelineSerializer()

    class Meta:
        model = PipelineFamily
        fields = ('id', 'url', 'name', 'members', 'published_version')

