from rest_framework import serializers
from pipeline.models import PipelineFamily, Pipeline

from transformation.serializers import TransformationInputSerializer


class PipelineSerializer(serializers.ModelSerializer):

    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    inputs = TransformationInputSerializer(many=True)
    removal_plan = serializers.HyperlinkedIdentityField(view_name='pipeline-removal-plan')

    class Meta:
        model = Pipeline
        fields = (
            'id', 'url', 'revision_name', 'revision_number', 'inputs',
            'user', "users_allowed", "groups_allowed",
            'removal_plan'
        )


class PipelineFamilySerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)

    members = PipelineSerializer(many=True)
    published_version = PipelineSerializer()
    removal_plan = serializers.HyperlinkedIdentityField(view_name='pipelinefamily-removal-plan')
    absolute_url = serializers.SerializerMethodField()
    num_revisions = serializers.SerializerMethodField()

    class Meta:
        model = PipelineFamily
        fields = ('id', 'url', 'name', "description", "user", "users_allowed", "groups_allowed",
                  'members', 'published_version', "absolute_url", 'removal_plan', "num_revisions")

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()

    def get_num_revisions(self, obj):
        if not obj:
            return None
        return obj.num_revisions