from rest_framework import serializers
from method.models import Method, MethodFamily, CodeResource, CodeResourceRevision


class CodeResourceSerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesource-removal-plan')
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    revisions = serializers.HyperlinkedIdentityField(view_name="coderesource-revisions")
    last_revision_date = serializers.DateTimeField()
    absolute_url = serializers.SerializerMethodField()

    class Meta:
        model = CodeResource
        fields = ('id', 'url', 'name', 'last_revision_date', 'filename', 'description', 'user', 'revisions',
                  'removal_plan', 'users_allowed', 'groups_allowed', 'num_revisions', 'absolute_url')

    def get_num_revisions(self, obj):
        if not obj:
            return None
        return obj.num_revisions

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()


class CodeResourceRevisionSerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    code_resource_name = serializers.StringRelatedField(source='coderesource.name')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesourcerevision-removal-plan')
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    absolute_url = serializers.SerializerMethodField()

    class Meta:
        model = CodeResourceRevision
        fields = ('id',
                  'url',
                  'revision_name',
                  'display_name',
                  'user',
                  'removal_plan',
                   'users_allowed',
                  'groups_allowed',
                  'absolute_url',
                  'revision_number',
                  'revision_desc',
                  'revision_DateTime',
                  'code_resource_name')

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()


class MethodFamilySerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    num_revisions = serializers.SerializerMethodField()
    absolute_url = serializers.SerializerMethodField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='methodfamily-removal-plan')
    methods = serializers.HyperlinkedIdentityField(view_name="methodfamily-methods")

    class Meta:
        model = MethodFamily
        fields = (
            "name", "description", "url", "user", "users_allowed", "groups_allowed", "num_revisions",
            "absolute_url", "removal_plan", "methods"
        )

    def get_num_revisions(self, obj):
        if not obj:
            return None
        return obj.num_revisions

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()


class MethodSerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField()
    family_name = serializers.StringRelatedField(source="family.name")
    users_allowed = serializers.StringRelatedField(many=True)
    groups_allowed = serializers.StringRelatedField(many=True)
    absolute_url = serializers.SerializerMethodField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='method-removal-plan')

    class Meta:
        model = Method
        fields = (
            "revision_name",
            "display_name",
            "revision_number",
            "revision_desc",
            "user",
            "users_allowed",
            "groups_allowed",
            "url",
            "absolute_url",
            "removal_plan",
            "family_name"
        )

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()
