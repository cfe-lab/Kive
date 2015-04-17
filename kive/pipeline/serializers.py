from rest_framework import serializers
from pipeline.models import PipelineFamily, Pipeline

from transformation.serializers import TransformationInputSerializer
from kive.serializers import TinyUserSerializer
from django.core.urlresolvers import reverse


class PipelineSerializer(serializers.ModelSerializer):

    inputs = serializers.SerializerMethodField()
    user = TinyUserSerializer()

    class Meta:
        model = Pipeline
        fields = ('id', 'revision_name', 'revision_number', 'inputs', 'user')

    def get_inputs(self, obj):
        if not obj:
            return None
        return TransformationInputSerializer(obj.inputs, many=True).data


class PipelineFamilySerializer(serializers.ModelSerializer):

    members = serializers.SerializerMethodField()
    published_version = PipelineSerializer()

    class Meta:
        model = PipelineFamily
        fields = ('id', 'name', 'members', 'published_version')

    def get_members(self, obj):
        if not obj:
            return None
        return PipelineSerializer(obj.members, many=True).data

