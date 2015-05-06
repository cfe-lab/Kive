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


class MethodSerializer(serializers.ModelSerializer):

    removal_plan = serializers.HyperlinkedIdentityField(view_name='method-removal-plan')

    class Meta:
        model = Method
        fields = ('id', 'url', 'removal_plan')


class MethodFamilySerializer(serializers.ModelSerializer):

    removal_plan = serializers.HyperlinkedIdentityField(view_name='methodfamily-removal-plan')

    class Meta:
        model = MethodFamily
        fields = ('id', 'url', 'removal_plan')

