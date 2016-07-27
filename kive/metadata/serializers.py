from rest_framework import serializers

from django.contrib.auth.models import User, Group

from kive.serializers import AccessControlSerializer
from metadata.models import CompoundDatatype, Datatype, CompoundDatatypeMember,\
    AccessControl


class DatatypeSerializer(AccessControlSerializer, serializers.ModelSerializer):
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


class CompoundDatatypeMemberSerializer(serializers.ModelSerializer):
    class Meta:
        model = CompoundDatatypeMember
        fields = (
            "datatype",
            "column_name",
            "column_idx",
            "blankable"
        )


class CompoundDatatypeSerializer(AccessControlSerializer, serializers.ModelSerializer):
    representation = serializers.SerializerMethodField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='compounddatatype-removal-plan')
    members = CompoundDatatypeMemberSerializer(many=True, required=False)

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
            'members'
        )

    def get_representation(self, obj):
        if obj:
            return str(obj)

    def validate(self, data):
        """
        Check that the indices and permissions are coherent.
        """
        members = data.get("members", [])
        indices = sorted([x["column_idx"] for x in members])
        if indices != range(1, len(indices) + 1):
            raise serializers.ValidationError("Column indices must be consecutive starting from 1")

        errors = []
        column_dts = [x["datatype"] for x in members]
        prohibited_users, prohibited_groups = AccessControl.validate_restrict_access_raw(
            data["user"],
            data.get("users_allowed", User.objects.none()),
            data.get("groups_allowed", Group.objects.none()),
            column_dts
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
        member_dictionaries = validated_data.pop("members", [])
        users_allowed = validated_data.pop("users_allowed", [])
        groups_allowed = validated_data.pop("groups_allowed", [])

        cdt = CompoundDatatype(**validated_data)
        cdt.save()
        cdt.users_allowed.add(*users_allowed)
        cdt.groups_allowed.add(*groups_allowed)

        for member_dict in member_dictionaries:
            member = CompoundDatatypeMember(compounddatatype=cdt, **member_dict)
            member.save()

        return cdt