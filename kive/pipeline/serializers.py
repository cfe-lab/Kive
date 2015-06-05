from rest_framework import serializers
from pipeline.models import PipelineFamily, Pipeline, CustomCableWire, PipelineStepInputCable,\
    PipelineStep, PipelineOutputCable

from transformation.serializers import TransformationInputSerializer, TransformationOutputSerializer
from kive.serializers import AccessControlSerializer


class CustomCableWireSerializer(serializers.ModelSerializer):
    source_idx = serializers.SerializerMethodField()
    dest_idx = serializers.SerializerMethodField()

    class Meta:
        model = CustomCableWire
        fields = ("source_pin", "dest_pin", "source_idx", "dest_idx")

    def get_source_idx(self, obj):
        if not obj:
            return None
        return obj.source_pin.column_idx

    def get_dest_idx(self, obj):
        if not obj:
            return None
        return obj.dest_pin.column_idx


class PipelineStepInputCableSerializer(serializers.ModelSerializer):

    source_dataset_name = serializers.CharField(source="source.definite.dataset_name", read_only=True)
    dest_dataset_name = serializers.CharField(source="dest.dataset_name", read_only=True)
    custom_wires = CustomCableWireSerializer(many=True, allow_null=True, required=False)

    class Meta:
        model = PipelineStepInputCable
        fields = (
            "source_step",
            "source",
            "source_dataset_name",
            "dest",
            "dest_dataset_name",
            "custom_wires",
            "keep_output"
        )


class PipelineStepSerializer(serializers.ModelSerializer):
    cables_in = PipelineStepInputCableSerializer(many=True)

    class Meta:
        model = PipelineStep
        fields = (
            "transformation",
            "step_num",
            "outputs_to_delete",
            "x",
            "y",
            "name",
            "cables_in"
        )


class PipelineOutputCableSerializer(serializers.ModelSerializer):

    source_dataset_name = serializers.CharField(source="source.dataset_name", read_only=True)
    custom_wires = CustomCableWireSerializer(many=True, allow_null=True, required=False)

    x = serializers.FloatField(write_only=True)
    y = serializers.FloatField(write_only=True)

    class Meta:
        model = PipelineOutputCable
        fields = (
            "output_idx",
            "output_name",
            "output_cdt",
            "x",
            "y",
            "source_step",
            "source",
            "source_dataset_name",
            "custom_wires"
        )


# This is analogous to CRRevisionNumberGetter.
class PipelineRevisionNumberGetter(object):
    """
    Handles retrieving the default revision number for a new Method.

    This is completely analogous to CRRevisionNumberGetter, and if that breaks
    due to changes in the internals of DRF, this probably will too.
    """
    def set_context(self, rev_num_field):
        self.pipelinefamily = PipelineFamily.objects.get(
            name=rev_num_field.parent.initial_data["family"]
        )

    def __call__(self):
        return self.pipelinefamily.num_revisions + 1


class PipelineSerializer(AccessControlSerializer,
                         serializers.ModelSerializer):

    family = serializers.SlugRelatedField(
        slug_field='name',
        queryset=PipelineFamily.objects.all()
    )
    inputs = TransformationInputSerializer(many=True)
    outputs = TransformationOutputSerializer(many=True, read_only=True)

    revision_number = serializers.IntegerField(read_only=True, required=False)

    steps = PipelineStepSerializer(many=True)
    outcables = PipelineOutputCableSerializer(many=True)

    removal_plan = serializers.HyperlinkedIdentityField(view_name='pipeline-removal-plan')

    # This is as per CodeResourceRevisionSerializer.
    revision_number = serializers.IntegerField(
        read_only=True,
        default=PipelineRevisionNumberGetter()
    )

    class Meta:
        model = Pipeline
        fields = (
            'id',
            'url',
            'family',
            'revision_name',
            "revision_desc",
            'revision_number',
            "revision_parent",
            "revision_DateTime",
            'user',
            "users_allowed",
            "groups_allowed",
            'inputs',
            "outputs",
            "steps",
            "outcables",
            'removal_plan',
        )

    def create(self, validated_data):
        """
        Create a Pipeline from deserialized and validated data.
        """
        inputs = validated_data.pop("inputs")
        steps = validated_data.pop("steps")
        outcables = validated_data.pop("outcables")

        users_allowed = validated_data.pop("users_allowed")
        groups_allowed = validated_data.pop("groups_allowed")

        # First, create the Pipeline.
        pipeline = Pipeline.objects.create(**validated_data)
        pipeline.users_allowed.add(*users_allowed)
        pipeline.groups_allowed.add(*groups_allowed)

        # Create the inputs.
        # fields = ("transformation", "dataset_name", "dataset_idx", "x", "y", "structure")
        for input_data in inputs:
            # Ignore the value given for "transformation".
            # input_data.pop("transformation")
            structure_data = None
            if "structure" in input_data:
                structure_data = input_data.pop("structure")
            curr_input = pipeline.inputs.create(**input_data)

            if structure_data is not None:
                # Ignore the "transf_xput" specified.
                # structure_data.pop("transf_xput")
                curr_input.structure.create(**structure_data)

        # Next, create the PipelineSteps.
        for step_data in steps:
            # Ignore the value given for "pipeline" -- obviously it's this one.
            # step_data.pop("pipeline")

            cables = step_data.pop("cables_in")
            # This is a ManyToManyField so it must be populated after the step
            # itself is created.
            outputs_to_delete = step_data.pop("outputs_to_delete")

            curr_step = pipeline.steps.create(**step_data)
            curr_step.outputs_to_delete.add(*outputs_to_delete)

            for cable_data in cables:
                # Ignore "pipelinestep".
                # cable_data.pop("pipelinestep")

                custom_wires = cable_data.pop("custom_wires")
                curr_cable = curr_step.cables_in.create(**cable_data)

                for wire_data in custom_wires:
                    # Ignore "cable".
                    wire_data.pop("cable")
                    curr_cable.custom_wires.create(**wire_data)

        # Lastly, create the PipelineOutputCables and associated data.
        for outcable_data in outcables:
            # Ignore the value given for "pipeline".
            # outcable_data.pop("pipeline")
            custom_wires = outcable_data.pop("custom_wires")
            x = outcable_data.pop("x")
            y = outcable_data.pop("y")
            curr_outcable = pipeline.outcables.create(**outcable_data)

            for wire_data in custom_wires:
                # Ignore "cable".
                # wire_data.pop("cable")
                curr_outcable.custom_wires.create(**wire_data)

            curr_outcable.create_output(x=x, y=y)

        return pipeline


class PipelineFamilySerializer(AccessControlSerializer,
                               serializers.ModelSerializer):
    # published_version = PipelineSerializer(allow_null=True)
    removal_plan = serializers.HyperlinkedIdentityField(view_name='pipelinefamily-removal-plan')
    absolute_url = serializers.SerializerMethodField()

    # members = PipelineSerializer(many=True, read_only=True)
    members_url = serializers.HyperlinkedIdentityField(view_name='pipelinefamily-pipelines')

    class Meta:
        model = PipelineFamily
        fields = ('id',
                  'url',
                  'name',
                  "description",
                  "user",
                  "users_allowed",
                  "groups_allowed",
                  'published_version',
                  "absolute_url",
                  'removal_plan',
                  "num_revisions",
                  'members',
                  'members_url')

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()

    def get_num_revisions(self, obj):
        if not obj:
            return None
        return obj.num_revisions