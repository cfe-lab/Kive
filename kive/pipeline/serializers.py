from rest_framework import serializers
from pipeline.models import PipelineFamily, Pipeline, CustomCableWire, PipelineStepInputCable

from transformation.serializers import TransformationInputSerializer


class PipelineSerializer(serializers.ModelSerializer):

    user = serializers.StringRelatedField()
    family_name = serializers.StringRelatedField(source='family.name')
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    inputs = TransformationInputSerializer(many=True)
    removal_plan = serializers.HyperlinkedIdentityField(view_name='pipeline-removal-plan')

    class Meta:
        model = Pipeline
        fields = (
            'id', 'url', 'revision_name', 'revision_number', 'inputs',
            'user', "users_allowed", "groups_allowed",
            'removal_plan', 'family_name'
        )


class PipelineFamilySerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)

    members = PipelineSerializer(many=True)
    published_version = PipelineSerializer(allow_null=True)
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


class CustomCableWireSerializer(serializers.ModelSerializer):
    source_idx = serializers.SerializerMethodField()
    dest_idx = serializers.SerializerMethodField()

    class Meta:
        model = CustomCableWire
        fields = ("cable", "source_pin", "dest_pin", "source_idx", "dest_idx")
        read_only_fields = ("cable", "source_pin", "dest_pin")

    def get_source_idx(self, obj):
        if not obj:
            return None
        return obj.source_pin.column_idx

    def get_dest_idx(self, obj):
        if not obj:
            return None
        return obj.dest_pin.column_idx


class PipelineStepInputCableSerializer(serializers.ModelSerializer):

    source_dataset_name = serializers.SerializerMethodField()
    dest_dataset_name = serializers.SerializerMethodField()
    custom_wires = CustomCableWireSerializer(many=True)

    class Meta:
        model = PipelineStepInputCable
        fields = ("pipelinestep", "dest", "source_step", "source", "source_step",
                  "source_dataset_name", "dest_dataset_name", "custom_wires", "keep_output")

    def get_source_dataset_name(self, obj):
        if not obj:
            return None
        return obj.source.definite.dataset_name

    def get_dest_dataset_name(self, obj):
        if not obj:
            return None
        return obj.dest.definite.dataset_name