from rest_framework import serializers
from transformation.models import TransformationInput

from metadata.serializers import CompoundDatatypeInputSerializer


class TransformationInputSerializer(serializers.ModelSerializer):

    compounddatatype = CompoundDatatypeInputSerializer()

    class Meta:
        model = TransformationInput
        fields = ('dataset_name', 'dataset_idx', 'compounddatatype')