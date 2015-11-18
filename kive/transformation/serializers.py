from rest_framework import serializers
from transformation.models import TransformationXput, TransformationInput, \
    TransformationOutput, XputStructure


class XputStructureSerializer(serializers.ModelSerializer):
    class Meta:
        model = XputStructure
        fields = (
            "compounddatatype",
            "min_row",
            "max_row"
        )

class TransformationXputSerializer(serializers.ModelSerializer):
    structure = XputStructureSerializer(allow_null=True, required=False)

    class Meta:
        model = TransformationXput
        fields = ("x", "y", "structure")


# It's recommended in the documentation to explicitly declare the Meta classes for
# these classes.
class TransformationInputSerializer(TransformationXputSerializer):
    class Meta:
        model = TransformationInput
        fields = ("dataset_name", "dataset_idx", "x", "y", "structure")


class TransformationOutputSerializer(TransformationXputSerializer):
    class Meta:
        model = TransformationOutput
        fields = ("dataset_name", "dataset_idx", "x", "y", "structure")
