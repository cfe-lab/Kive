from django.contrib.auth.models import User, Group

from rest_framework import serializers
from method.models import Method, MethodFamily, CodeResource, CodeResourceRevision, CodeResourceDependency
from transformation.serializers import TransformationInputSerializer, TransformationOutputSerializer
from kive.serializers import AccessControlSerializer


class CodeResourceSerializer(AccessControlSerializer,
                             serializers.ModelSerializer):
    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesource-removal-plan')
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
            "requirement",
            "depPath",
            "depFileName"
        )


# Note: set_context doesn't seem to appear in the documentation anywhere --
# we had to go into the DRF source code.
class CRRevisionNumberGetter(object):
    """
    Handles retrieving the default revision number for a new CodeResourceRevision.

    This is defined as per
    http://www.django-rest-framework.org/api-guide/serializers/#specifying-read-only-fields
    """
    def set_context(self, rev_num_field):
        self.coderesource = CodeResource.objects.get(
            name=rev_num_field.parent.initial_data["coderesource"]
        )

    def __call__(self):
        return self.coderesource.num_revisions + 1


class CodeResourceRevisionSerializer(AccessControlSerializer,
                                     serializers.ModelSerializer):
    coderesource = serializers.SlugRelatedField(slug_field='name',
                                                queryset=CodeResource.objects.all())

    dependencies = CodeResourceDependencySerializer(
        many=True,
        allow_null=True,
        required=False
    )

    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesourcerevision-removal-plan')
    absolute_url = serializers.SerializerMethodField()

    # As per
    # http://www.django-rest-framework.org/api-guide/serializers/#specifying-read-only-fields
    # this field must be explicitly defined, with read_only=True and a default specified.
    revision_number = serializers.IntegerField(
        read_only=True,
        default=CRRevisionNumberGetter()
    )

    class Meta:
        model = CodeResourceRevision
        fields = (
            'id',
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

        extra_kwargs = {
            "content_file": {"use_url": False},
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


class MethodFamilySerializer(AccessControlSerializer,
                             serializers.ModelSerializer):
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


# This is analogous to CRRevisionNumberGetter.
class MethodRevisionNumberGetter(object):
    """
    Handles retrieving the default revision number for a new Method.

    This is completely analogous to CRRevisionNumberGetter, and if that breaks
    due to changes in the internals of DRF, this probably will too.
    """
    def set_context(self, rev_num_field):
        self.methodfamily = MethodFamily.objects.get(
            name=rev_num_field.parent.initial_data["family"]
        )

    def __call__(self):
        return self.methodfamily.num_revisions + 1


class MethodSerializer(AccessControlSerializer,
                       serializers.ModelSerializer):
    family = serializers.SlugRelatedField(slug_field="name",
                                          queryset=MethodFamily.objects.all())

    inputs = TransformationInputSerializer(many=True, allow_null=True, required=False)
    outputs = TransformationOutputSerializer(many=True, allow_null=True, required=False)

    absolute_url = serializers.SerializerMethodField()
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='method-removal-plan')

    # This is as in CodeResourceRevisionSerializer.
    revision_number = serializers.IntegerField(
        read_only=True,
        default=MethodRevisionNumberGetter()
    )

    class Meta:
        model = Method
        fields = (
            "revision_name",
            "display_name",
            "revision_number",
            "revision_desc",
            "revision_DateTime",
            "revision_parent",
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
