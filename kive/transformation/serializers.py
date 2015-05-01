from rest_framework import serializers
from transformation.models import TransformationInput

from metadata.serializers import CompoundDatatypeSerializer


class TransformationInputSerializer(serializers.ModelSerializer):

    compounddatatype = CompoundDatatypeSerializer()

    class Meta:
        model = TransformationInput
        fields = ('dataset_name', 'dataset_idx', 'compounddatatype')