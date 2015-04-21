from rest_framework import serializers
from metadata.models import CompoundDatatype


class CompoundDatatypeInputSerializer(serializers.ModelSerializer):

    representation = serializers.SerializerMethodField()

    class Meta:
        model = CompoundDatatype
        fields = ('id', 'representation')

    def get_representation(self, obj):
        if obj:
            return str(obj)