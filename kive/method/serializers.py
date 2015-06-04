from django.contrib.auth.models import User, Group

from rest_framework import serializers
from method.models import Method, MethodFamily, CodeResource, CodeResourceRevision, CodeResourceDependency
from transformation.serializers import TransformationInputSerializer, TransformationOutputSerializer

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


class CodeResourceDependencySerializer(serializers.ModelSerializer):

    class Meta:
        model = CodeResourceDependency
        fields = (
            "coderesourcerevision",
            "requirement",
            "depPath",
            "depFileName"
        )


class CodeResourceRevisionSerializer(serializers.ModelSerializer):
    user = serializers.SlugRelatedField(slug_field="username",
                                        queryset=User.objects.all())
    coderesource = serializers.SlugRelatedField(slug_field='name',
                                                queryset=CodeResource.objects.all())
    users_allowed = serializers.SlugRelatedField(
        slug_field="username",
        queryset=User.objects.all(),
        many=True,
        allow_null=True,
        required=False)
    groups_allowed = serializers.SlugRelatedField(
        slug_field="name",
        queryset=Group.objects.all(),
        many=True,
        allow_null=True,
        required=False
    )
    dependencies = CodeResourceDependencySerializer(
        many=True,
        allow_null=True,
        required=False
    )

    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesourcerevision-removal-plan')
    absolute_url = serializers.SerializerMethodField()

    class Meta:
        model = CodeResourceRevision
        fields = ('id',
                  'url',
                  "coderesource",
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
                  "content_file",
                  "dependencies"
        )
        # revision_DateTime, removal_plan, absolute_url, and display_name are already read_only.
        read_only_fields = (
            "revision_number",
        )
        extra_kwargs = {
            "content_file": {"use_url": False}
        }

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()

    # This is a nested serializer so we need to customize the create method.
    def create(self, validated_data):
        """
        Create a CodeResourceRevision from the validated deserialized data.

        Note that no cleaning occurs here.  That will fall to the calling method.
        """
        crr_data = validated_data
        users_allowed = crr_data.pop("users_allowed")
        groups_allowed = crr_data.pop("groups_allowed")
        dependencies = crr_data.pop("dependencies")
        crr = CodeResourceRevision.objects.create(**crr_data)

        crr.users_allowed.add(*users_allowed)
        crr.groups_allowed.add(*groups_allowed)

        for dep_data in dependencies:
            # Note that we ignore the value of dep_data["coderesourcerevision"].
            crr.dependencies.create(
                requirement=dep_data["requirement"],
                depPath=dep_data["depPath"],
                depFileName=dep_data["depFileName"]
            )

        return crr


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
    user = serializers.SlugRelatedField(slug_field="username",
                                        queryset=User.objects.all())

    family = serializers.SlugRelatedField(slug_field="name",
                                          queryset=MethodFamily.objects.all())

    users_allowed = serializers.SlugRelatedField(
        slug_field="username",
        queryset=User.objects.all(),
        many=True,
        allow_null=True,
        required=False)
    groups_allowed = serializers.SlugRelatedField(
        slug_field="name",
        queryset=Group.objects.all(),
        many=True,
        allow_null=True,
        required=False
    )

    inputs = TransformationInputSerializer(many=True, allow_null=True, required=False)
    outputs = TransformationOutputSerializer(many=True, allow_null=True, required=False)

    revision_number = serializers.IntegerField(read_only=True, required=False)

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
            "family",
            "driver",
            "reusable",
            "threads",
            "inputs",
            "outputs"
        )
        # removal_plan, absolute_url, and display_name are already read_only.

    def get_absolute_url(self, obj):
        if not obj:
            return None
        return obj.get_absolute_url()

    # Due to nesting of inputs and outputs, we need to customize the create method.
    def create(self, validated_data):
        """
        Create a Method from the validated deserialized data.

        Note that no cleaning occurs here.  That will fall to the calling method.
        """
        method_data = validated_data
        users_allowed = method_data.pop("users_allowed")
        groups_allowed = method_data.pop("groups_allowed")
        inputs = method_data.pop("inputs")
        outputs = method_data.pop("outputs")

        method = Method.objects.create(**method_data)

        method.users_allowed.add(*users_allowed)
        method.groups_allowed.add(*groups_allowed)

        def create_xput(xput_data, xput_manager):
            # Note that we ignore the value of xput_data["transformation"].
            xput_data.pop("transformation")
            structure_data = None
            try:
                structure_data = xput_data.pop("structure")
            except KeyError:
                pass

            curr_xput = xput_manager.create(**xput_data)
            if structure_data is not None:
                # Here we ignore the transf_xput ID passed to the structure.
                structure_data.pop("transf_xput")
                curr_xput.structure.create(**structure_data)

        for input_data in inputs:
            create_xput(input_data, method.inputs)

        for output_data in outputs:
            create_xput(output_data, method.outputs)

        return method
