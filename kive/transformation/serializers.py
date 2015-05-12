from rest_framework import serializers
from transformation.models import TransformationXput, TransformationInput, TransformationOutput, XputStructure


class TransformationXputSerializer(serializers.ModelSerializer):
    min_row = serializers.SerializerMethodField(allow_null=True)
    max_row = serializers.SerializerMethodField(allow_null=True)
    compounddatatype = serializers.SerializerMethodField(allow_null=True)

    class Meta:
        model = TransformationXput
        fields = ('compounddatatype', "x", "y", "min_row", "max_row")

    def get_min_row(self, obj):
        if not obj:
            return None
        try:
            return obj.structure.min_row
        except XputStructure.DoesNotExist:
            pass

    def get_max_row(self, obj):
        if not obj:
            return None
        try:
            return obj.structure.max_row
        except XputStructure.DoesNotExist:
            pass

    def get_compounddatatype(self, obj):
        if not obj:
            return None
        try:
            return obj.structure.compounddatatype.pk
        except XputStructure.DoesNotExist:
            pass

# It's recommended in the documentation to explicitly declare the Meta classes for
# these classes.
class TransformationInputSerializer(TransformationXputSerializer):
    class Meta:
        model = TransformationInput
        fields = ('dataset_name', 'dataset_idx', 'compounddatatype', "x", "y", "min_row", "max_row")


class TransformationOutputSerializer(TransformationXputSerializer):
    class Meta:
        model = TransformationOutput
        fields = ('dataset_name', 'dataset_idx', 'compounddatatype', "x", "y", "min_row", "max_row")


