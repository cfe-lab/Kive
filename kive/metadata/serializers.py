from rest_framework import serializers
from metadata.models import CompoundDatatype, Datatype


class DatatypeSerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='datatype-removal-plan')
    restricts = serializers.StringRelatedField(many=True)

    class Meta:
        model = Datatype
        fields = (
            'id',
            'url',
            'user',
            'users_allowed',
            'groups_allowed',
            'removal_plan',
            "absolute_url",
            "restricts",
            "date_created",
            "name",
            "description"
        )


class CompoundDatatypeSerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    representation = serializers.SerializerMethodField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='compounddatatype-removal-plan')

    class Meta:
        model = CompoundDatatype
        fields = (
            'id',
            'url',
            'representation',
            'user',
            'users_allowed',
            'groups_allowed',
            'removal_plan',
            'absolute_url',
            'name'
        )

    def get_representation(self, obj):
        if obj:
            return str(obj)