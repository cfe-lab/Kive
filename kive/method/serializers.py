from django.db import transaction
from django.core.files import File

from rest_framework import serializers

from method.models import Method, MethodDependency, MethodFamily, CodeResource, CodeResourceRevision
from transformation.models import XputStructure
from transformation.serializers import TransformationInputSerializer, TransformationOutputSerializer
from kive.serializers import AccessControlSerializer
import portal.models


class CodeResourceSerializer(AccessControlSerializer,
                             serializers.ModelSerializer):
    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesource-removal-plan')
    revisions = serializers.HyperlinkedIdentityField(view_name="coderesource-revisions")

    class Meta:
        model = CodeResource
        fields = (
            'id',
            'url',
            'name',
            'last_revision_date',
            'filename',
            'description',
            'user',
            'revisions',
            'removal_plan',
            'users_allowed',
            'groups_allowed',
            'num_revisions',
            'absolute_url'
        )


class MethodDependencySerializer(serializers.ModelSerializer):

    class Meta:
        model = MethodDependency
        fields = (
            "requirement",
            "path",
            "filename"
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
        return self.coderesource.next_revision()


class CodeResourceRevisionSerializer(AccessControlSerializer,
                                     serializers.ModelSerializer):
    coderesource = serializers.SlugRelatedField(slug_field='name',
                                                queryset=CodeResource.objects.all())

    download_url = serializers.HyperlinkedIdentityField(view_name='coderesourcerevision-download')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='coderesourcerevision-removal-plan')

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
            'view_url',
            'revision_number',
            'revision_desc',
            'revision_DateTime',
            "content_file",
            "download_url",
            "MD5_checksum"
        )
        # revision_DateTime, removal_plan, absolute_url, and display_name are already read_only.
        read_only_fields = ("content_file", "MD5_checksum")
        extra_kwargs = {
            "content_file": {"use_url": False},
        }

    def __init__(self, *args, **kwargs):
        super(CodeResourceRevisionSerializer, self).__init__(*args, **kwargs)
        request = self.context.get("request", None)
        if request is not None:
            # Set the queryset of the coderesource field.
            cr_field = self.fields["coderesource"]
            cr_field.queryset = CodeResource.filter_by_user(request.user)

    # This is a nested serializer so we need to customize the create method.
    def create(self, validated_data):
        """
        Create a CodeResourceRevision from the validated deserialized data.

        Note that no cleaning occurs here.  That will fall to the calling method.
        """
        crr_data = validated_data
        users_allowed = crr_data.pop("users_allowed") if "users_allowed" in crr_data else []
        groups_allowed = crr_data.pop("groups_allowed") if "groups_allowed" in crr_data else []

        with transaction.atomic():
            crr = CodeResourceRevision(**crr_data)
            crr.users_allowed.add(*users_allowed)
            crr.groups_allowed.add(*groups_allowed)

        return crr


class MethodFamilySerializer(AccessControlSerializer,
                             serializers.ModelSerializer):
    removal_plan = serializers.HyperlinkedIdentityField(
        view_name='methodfamily-removal-plan')
    methods = serializers.HyperlinkedIdentityField(view_name="methodfamily-methods")

    class Meta:
        model = MethodFamily
        fields = (
            "name",
            "description",
            "url",
            "user",
            "users_allowed",
            "groups_allowed",
            "num_revisions",
            "absolute_url",
            "removal_plan",
            "methods"
        )


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
        return self.methodfamily.next_revision()


class MethodSerializer(AccessControlSerializer,
                       serializers.ModelSerializer):
    family = serializers.SlugRelatedField(slug_field="name",
                                          queryset=MethodFamily.objects.all())
    family_id = serializers.PrimaryKeyRelatedField(read_only=True)

    inputs = TransformationInputSerializer(many=True, allow_null=True, required=False)
    outputs = TransformationOutputSerializer(many=True, allow_null=True, required=False)

    dependencies = MethodDependencySerializer(
        many=True,
        allow_null=True,
        required=False
    )

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
            "id",
            "url",
            "absolute_url",
            "view_url",
            "removal_plan",
            "family_id",
            "family",
            "driver",
            "reusable",
            "threads",
            "memory",
            "dependencies",
            "inputs",
            "outputs"
        )

    def __init__(self, *args, **kwargs):
        super(MethodSerializer, self).__init__(*args, **kwargs)
        request = self.context.get("request")
        if request is not None:
            curr_user = request.user
    
            # Set the querysets of the related model fields.
            revision_parent_field = self.fields["revision_parent"]
            revision_parent_field.queryset = Method.filter_by_user(curr_user)
    
            family_field = self.fields["family"]
            family_field.queryset = MethodFamily.filter_by_user(curr_user)
    
            driver_field = self.fields["driver"]
            driver_field.queryset = CodeResourceRevision.filter_by_user(curr_user)

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
        dependencies = method_data.pop("dependencies") if "dependencies" in method_data else []

        method = Method.objects.create(**method_data)
        method.users_allowed.add(*users_allowed)
        method.groups_allowed.add(*groups_allowed)

        def create_xput(xput_data, xput_manager):
            structure_data = None
            try:
                structure_data = xput_data.pop("structure")
            except KeyError:
                pass

            curr_xput = xput_manager.create(**xput_data)
            if structure_data is not None:
                new_structure = XputStructure(transf_xput=curr_xput, **structure_data)
                new_structure.save()

        for input_data in inputs:
            create_xput(input_data, method.inputs)

        for output_data in outputs:
            create_xput(output_data, method.outputs)

        for dep_data in dependencies:
            method.dependencies.create(**dep_data)

        return method
