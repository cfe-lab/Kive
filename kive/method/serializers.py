from rest_framework import serializers
from method.models import Method, MethodFamily, CodeResource, CodeResourceRevision


class CodeResourceSerializer(serializers.ModelSerializer):

    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesource-removal-plan')

    class Meta:
        model = CodeResource
        fields = ('id', 'url', 'removal_plan')


class CodeResourceRevisionSerializer(serializers.ModelSerializer):

    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesourcerevision-removal-plan')

    class Meta:
        model = CodeResourceRevision
        fields = ('id', 'url', 'removal_plan')


class MethodFamilySerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    num_revisions = serializers.SerializerMethodField()
    absolute_url = serializers.SerializerMethodField()
    family_link = serializers.SerializerMethodField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='methodfamily-removal-plan')
    methods = serializers.HyperlinkedIdentityField(view_name="methodfamily-methods")

    class Meta:
        model = MethodFamily
        fields = (
            "name", "description", "url", "user", "users_allowed", "groups_allowed", "num_revisions",
            "absolute_url", "family_link", "removal_plan", "methods"
        )

    def get_num_revisions(self, obj):
        if not obj:
            return None
        return obj.num_revisions

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()

    # FIXME need to update this when permissions.js is updated.
    def get_family_link(self, obj):
        if not obj:
            return None
        return '<a href="{}">{}</a>'.format(obj.get_absolute_url(), obj.name)


class MethodSerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    absolute_url = serializers.SerializerMethodField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='method-removal-plan')

    class Meta:
        model = Method
        fields = (
            "revision_name", "revision_number", "revision_desc", "user", "users_allowed", "groups_allowed",
            "url", "absolute_url", "removal_plan"
        )

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()
