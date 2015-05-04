from rest_framework import serializers
from metadata.models import CompoundDatatype


class CompoundDatatypeSerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    representation = serializers.SerializerMethodField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='compounddatatype-removal-plan')

    class Meta:
        model = CompoundDatatype
        fields = ('id',
                  'url',
                  'representation',
                  'user',
                  'users_allowed',
                  'groups_allowed',
                  'removal_plan'
                  )

    def get_representation(self, obj):
        if obj:
            return str(obj)