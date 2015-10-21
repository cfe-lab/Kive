from django.contrib.auth.models import User, Group
from django.utils import timezone
from django.db import transaction

from rest_framework import serializers
from fleet.models import RunToProcess, RunToProcessInput
from pipeline.models import Pipeline
from metadata.models import who_cannot_access
from transformation.models import TransformationInput
from archive.serializers import TinyRunSerializer, RunOutputsSerializer
from kive.serializers import AccessControlSerializer


class RunToProcessInputSerializer(serializers.ModelSerializer):
    class Meta:
        model = RunToProcessInput
        fields = ("symbolicdataset", "index")


class RunToProcessSerializer(AccessControlSerializer, serializers.ModelSerializer):
    run_status = serializers.HyperlinkedIdentityField(view_name='runtoprocess-run-status')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='runtoprocess-removal-plan')
    run_outputs = serializers.HyperlinkedIdentityField(view_name='runtoprocess-run-outputs')

    sandbox_path = serializers.CharField(read_only=True, required=False)
    inputs = RunToProcessInputSerializer(many=True)

    class Meta:
        model = RunToProcess
        fields = (
            'id',
            'url',
            'pipeline',
            'time_queued',
            'name',
            'run',
            'sandbox_path',
            'purged',
            'run_status',
            'run_outputs',
            'removal_plan',
            'user',
            'users_allowed',
            'groups_allowed',
            'inputs'
        )
        read_only_fields = (
            "run",
            "purged",
            "time_queued",
        )

    def validate(self, data):
        """
        Check that the run is correctly specified.

        First, check that the inputs are correctly specified; then,
        check that the permissions are OK.
        """
        pipeline = Pipeline.objects.get(pk=data["pipeline"])

        if len(data["inputs"]) != pipeline.inputs.count():
            raise serializers.ValidationError(
                'Number of inputs must equal the number of Pipeline inputs'
            )

        inputs_sated = [x["index"] for x in data["inputs"]]
        if len(inputs_sated) != len(set(inputs_sated)):
            raise serializers.ValidationError(
                'Pipeline inputs must be uniquely specified'
            )

        all_access_controlled_objects = [pipeline]
        errors = []
        for rtp_input in data["inputs"]:
            curr_idx = rtp_input["index"]
            curr_SD = rtp_input["symbolicdataset"]
            try:
                corresp_input = pipeline.inputs.get(dataset_idx=curr_idx)
            except TransformationInput.DoesNotExist:
                errors.append('Pipeline {} has no input with index {}'.format(pipeline, curr_idx))

            if curr_SD.is_raw() and corresp_input.is_raw():
                continue
            elif (not curr_SD.is_raw() and not corresp_input.is_raw()):
                if curr_SD.get_cdt().is_restriction(corresp_input.get_cdt()):
                    continue
            else:
                errors.append('Input {} is incompatible with SymbolicDataset {}'.format(corresp_input, curr_SD))

            all_access_controlled_objects.append(curr_SD)

        if len(errors) > 0:
            raise serializers.ValidationError(errors)

        # Check that the specified user, users_allowed, and groups_allowed are all okay.
        users_without_access, groups_without_access = who_cannot_access(
            self.context["request"].user,
            User.objects.filter(username__in=data.get("users_allowed", [])),
            Group.objects.filter(name__in=data.get("groups_allowed", [])),
            all_access_controlled_objects)

        if len(users_without_access) != 0:
            errors.append("User(s) {} may not be granted access".format(list(users_without_access)))

        if len(groups_without_access) != 0:
            errors.append("Group(s) {} may not be granted access".format(list(groups_without_access)))

        if len(errors) > 0:
            raise serializers.ValidationError(errors)

        return data

    # We don't place this in a transaction; when it's called from a ViewSet, it'll already be
    # in one.
    def create(self, validated_data):
        """
        Create a RunToProcess, i.e. add a job to the work queue.
        """
        inputs = validated_data.pop("inputs")
        users_allowed = validated_data.pop("users_allowed", [])
        groups_allowed = validated_data.pop("groups_allowed", [])

        # First, create the RunToProcess with the current time.
        rtp = RunToProcess(time_queued=timezone.now(), **validated_data)
        rtp.save()
        rtp.users_allowed.add(*users_allowed)
        rtp.groups_allowed.add(*groups_allowed)

        # Create the inputs.
        for input_data in inputs:
            rtp.inputs.create(**input_data)

        # If this throws an error, we'll break out of the transaction.
        rtp.clean()
        return rtp


class RunToProcessOutputsSerializer(serializers.ModelSerializer):
    run = RunOutputsSerializer()
    
    class Meta:
        model = RunToProcess
        fields = ('id', 'run')