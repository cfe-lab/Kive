from rest_framework import serializers
from kive.serializers import AccessControlSerializer
from metadata.models import CompoundDatatype, Datatype, CompoundDatatypeMember,\
    AccessControl


class DatatypeSerializer(AccessControlSerializer. serializers.ModelSerializer):
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


class CompoundDatatypeSerializer(AccessControlSerializer, serializers.ModelSerializer):
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
            'name',
            'CDTMs'
        )

    def get_representation(self, obj):
        if obj:
            return str(obj)

    def validate(self, data):
        """
        Check that the indices and permissions are coherent.
        """
        indices = sorted([x["column_idx"] for x in data["CDTMs"]])
        if indices != range(1, len(indices) + 1):
            raise serializers.ValidationError("Column indices must be consecutive starting from 1")

        errors = []
        column_dts = [x["datatype"] for x in data["CDTMs"]]
        prohibited_users, prohibited_groups = AccessControl.validate_restrict_access_raw(
            data["user"],
            data["users_allowed"],
            data["groups_allowed"],
            [column_dts]
        )

        errors.extend(["User {} cannot be granted access".format(x) for x in prohibited_users])
        errors.extend(["Group {} cannot be granted access".format(x) for x in prohibited_groups])

        if len(errors) > 0:
            raise serializers.ValidationError(errors)

        return data

    def create(self, validated_data):
        """
        Create a CompoundDatatype from validated data.
        """
        member_dictionaries = validated_data.pop("CDTMs", [])
        users_allowed = validated_data.pop("users_allowed", [])
        groups_allowed = validated_data.pop("groups_allowed", [])

        cdt = CompoundDatatype(**validated_data)
        cdt.save()
        cdt.users_allowed.add(*users_allowed)
        cdt.groups_allowed.add(*groups_allowed)

        for member_dict in member_dictionaries:
            member_dict.pop("compounddatatype", None)
            member = CompoundDatatypeMember(compounddatatype=cdt, **member_dict)
            member.save()

        return cdt


class CompoundDatatypeMemberSerializer(serializers.ModelSerializer):
    class Meta:
        model = CompoundDatatypeMember
        fields = (
            "compounddatatype",
            "datatype",
            "column_name",
            "column_idx",
            "blankable"
        )
        extra_kwargs = {
            "compounddatatype": {"required": False}
        }