import os

from django.template.defaultfilters import filesizeformat
from django.utils import timezone
from django.contrib.auth.models import User, Group
from rest_framework import serializers
from rest_framework.reverse import reverse

from archive.models import Run, MethodOutput, RunInput, RunBatch
from transformation.models import TransformationInput
from metadata.models import who_cannot_access, everyone_group
from datachecking.models import BadData, MD5Conflict, ContentCheckLog

from constants import runstates

from kive.serializers import AccessControlSerializer


class TinyRunSerializer(serializers.ModelSerializer):

    class Meta:
        model = Run
        fields = ('id',)


class MethodOutputSerializer(serializers.ModelSerializer):

    output_redaction_plan = serializers.HyperlinkedIdentityField(
        view_name='methodoutput-output-redaction-plan')
    error_redaction_plan = serializers.HyperlinkedIdentityField(
        view_name='methodoutput-error-redaction-plan')
    code_redaction_plan = serializers.HyperlinkedIdentityField(
        view_name='methodoutput-code-redaction-plan')

    class Meta:
        model = MethodOutput
        fields = ('id',
                  'url',
                  'output_redacted',
                  'error_redacted',
                  'code_redacted',
                  'output_redaction_plan',
                  'error_redaction_plan',
                  'code_redaction_plan')


class _RunDataset(object):
    def __init__(self,
                 step_name,
                 name,
                 type,
                 display=None,
                 id=None,
                 size="removed",
                 date="removed",
                 url=None,
                 redaction_plan=None,
                 is_ok=True,
                 filename=None,
                 errors=None):
        self.step_name = str(step_name)
        self.name = name
        self.display = str(display or name)
        self.type = type
        self.id = id
        self.size = size
        self.date = date
        self.url = url
        self.redaction_plan = redaction_plan
        self.is_ok = is_ok
        self.errors = errors or []
        self.filename = filename

    def set_dataset(self, dataset, request):
        self.id = dataset.id
        self.size = dataset.get_filesize()
        self.date = dataset.date_created
        self.url = reverse('dataset-detail',
                           kwargs={'pk': dataset.id},
                           request=request)
        self.redaction_plan = reverse('dataset-redaction-plan',
                                      kwargs={'pk': dataset.id},
                                      request=request)
        with dataset.get_open_file_handle() as data_handle:
            self.filename = os.path.basename(data_handle.name)

    def set_redacted(self):
        self.size = self.date = 'redacted'

    def set_missing_output(self):
        self.size = self.date = 'not created'
        self.errors.append(self.size)
        self.is_ok = False


class RunOutputsSerializer(serializers.ModelSerializer):
    """ Serialize a run with a focus on the outputs. """

    output_summary = serializers.SerializerMethodField()
    input_summary = serializers.SerializerMethodField()

    class Meta:
        model = Run
        fields = ('id', 'output_summary', 'input_summary')

    def get_input_summary(self, run):
        """Get a list of objects that summarize all the inputs for a run."""

        request = self.context.get('request', None)
        inputs = []
        pipeline_inputs = run.pipeline.inputs

        for i, input in enumerate(run.inputs.order_by('index')):
            has_data = input.dataset.has_data()
            if has_data:
                input_name = input.dataset.name
            else:
                pipeline_input = pipeline_inputs.get(dataset_idx=input.index)
                input_name = pipeline_input.dataset_name
            input_data = _RunDataset(step_name=(i == 0 and 'Run inputs' or ''),
                                     name=input_name,
                                     display='{}: {}'.format(i+1, input_name),
                                     type='dataset')
            if has_data:
                input_data.set_dataset(input.dataset, request)
            inputs += [input_data]

        for input in inputs:
            input.is_invalid = not input.is_ok and input.id is not None

            try:
                input.size += 0
                # It's a number, so format it nicely, along with date.
                input.size = filesizeformat(input.size)
                input.date = timezone.localtime(input.date).strftime(
                    '%d %b %Y %H:%M:%S')
            except TypeError:
                pass  # Size was not a number, so leave it alone.

        return [inp.__dict__ for inp in inputs]

    def get_output_summary(self, run):
        """ Get a list of objects that summarize all the outputs from a run.

        Outputs include pipeline outputs, as well as output log, error log, and
        output cables for each step.
        """

        request = self.context.get('request', None)
        outputs = []
        for i, outcable in enumerate(run.outcables_in_order):
            if outcable.execrecord is not None:
                execrecordout = outcable.execrecord.execrecordouts.first()
                output = _RunDataset(
                    step_name=(i == 0 and 'Run outputs' or ''),
                    name=outcable.pipelineoutputcable.dest.dataset_name,
                    display=outcable.pipelineoutputcable.dest,
                    type='dataset')
                if execrecordout.dataset.has_data():
                    dataset = execrecordout.dataset
                    output.set_dataset(dataset, request)
                elif execrecordout.dataset.is_redacted():
                    output.set_redacted()

                outputs.append(output)

        for runstep in run.runsteps_in_order:
            execlog = runstep.get_log()
            if execlog is None:
                continue
            methodoutput = execlog.methodoutput
            step_prefix = 'step_{}_'.format(runstep.pipelinestep.step_num)
            # first handle the standard output file
            output = _RunDataset(step_name=runstep.pipelinestep,
                                 name=step_prefix + 'stdout',
                                 display='Standard out',
                                 type='stdout')
            if methodoutput.is_output_redacted():
                output.set_redacted()
                outputs.append(output)
            else:
                try:
                    # a set the state of the run based on the exit code
                    # NOTE: the javascript OutputsTable.js uses output.id to decide whether
                    # to display a 'View' and 'Download' link ==> Only set
                    # the id here iff we actually have a clickable/viewable output file.
                    output.id = None
                    output.is_ok = True
                    # we attach the return_code as an error to the standard output entry
                    # if its nonzero
                    retcode = methodoutput.return_code
                    if retcode is not None and retcode != 0:
                        output.errors.append('return code {}'.format(retcode))
                    if execlog.start_time is None:
                        output.display = 'Did not run'
                        output.date = None
                        output.size = None
                        output.errors.append('Did not run.')
                        output.is_ok = False
                    elif execlog.end_time is None:
                        output.display = 'Running'
                        output.date = None
                        output.size = None
                        output.is_ok = False
                    else:
                        if not methodoutput.output_log:
                            output.date = output.size = 'removed'
                            output.url = output.redaction_plan = None
                        else:
                            ismissing = False
                            try:
                                output.size = methodoutput.output_log.size
                            except OSError:
                                ismissing = True
                            if ismissing:
                                output.date = output.size = 'missing'
                                output.url = output.redaction_plan = None
                            else:
                                output.url = reverse('methodoutput-detail',
                                                     kwargs={'pk': methodoutput.id},
                                                     request=request)
                                output.id = methodoutput.id
                                output.date = execlog.end_time
                                output.redaction_plan = reverse(
                                    'methodoutput-output-redaction-plan',
                                    kwargs={'pk': methodoutput.id},
                                    request=request)
                    outputs.append(output)
                except ValueError as e:
                    print "stdout serializer", e
            # Now handle the stderr file
            output = _RunDataset(step_name="",
                                 name=step_prefix + 'stderr',
                                 display='Standard error',
                                 type='stderr')
            if methodoutput.is_error_redacted():
                output.set_redacted()
                outputs.append(output)
            else:
                try:
                    output.id = None
                    output.is_ok = True
                    if retcode is not None and retcode != 0:
                        output.errors.append('return code {}'.format(retcode))
                    if execlog.start_time is None:
                        output.display = 'Did not run'
                        output.date = None
                        output.size = None
                        output.errors.append('Did not run.')
                        output.is_ok = False
                    elif execlog.end_time is None:
                        output.display = 'Running'
                        output.date = None
                        output.size = None
                        output.is_ok = False
                    else:
                        if not methodoutput.error_log:
                            output.date = output.size = 'removed'
                            output.url = output.redaction_plan = None
                        else:
                            ismissing = False
                            try:
                                output.size = methodoutput.error_log.size
                            except OSError:
                                ismissing = True
                            if ismissing:
                                output.date = output.size = 'missing'
                                output.url = output.redaction_plan = None
                            else:
                                output.id = methodoutput.id
                                output.date = execlog.end_time
                                output.url = reverse('methodoutput-detail',
                                                     kwargs={'pk': methodoutput.id},
                                                     request=request)
                                output.redaction_plan = reverse(
                                    'methodoutput-error-redaction-plan',
                                    kwargs={'pk': methodoutput.id},
                                    request=request)
                    outputs.append(output)
                except ValueError as e:
                    print "stderr serializer", e

            if runstep.execrecord is not None:
                for execrecordout in runstep.execrecord.execrecordouts_in_order:
                    transform_output = execrecordout.generic_output.definite
                    output = _RunDataset(
                        step_name='',
                        name=step_prefix + transform_output.dataset_name,
                        display=execrecordout.generic_output,
                        is_ok=execrecordout.is_OK(),
                        type='dataset')

                    # Look for any failed checks.
                    content_checks = ContentCheckLog.objects.filter(
                        dataset=execrecordout.dataset)
                    bad_data = BadData.objects.filter(
                        contentchecklog__in=content_checks)
                    missing_data = bad_data.filter(missing_output=True)
                    corrupted_data = MD5Conflict.objects.filter(
                        integritychecklog__dataset=execrecordout.dataset)

                    if execrecordout.dataset.has_data():
                        output.set_dataset(execrecordout.dataset, request)
                    if corrupted_data.exists():
                        output.is_ok = False
                        output.errors.append('failed integrity check')
                    if bad_data.exists():
                        output.is_ok = False
                        output.errors.append('failed content check')
                    elif not content_checks.exists():
                        output.is_ok = False
                        output.errors.append('content not checked')
                    elif execrecordout.dataset.is_redacted():
                        output.set_redacted()
                    elif missing_data.exists():
                        output.set_missing_output()

                    outputs.append(output)
        for output in outputs:
            output.is_invalid = not output.is_ok and output.id is not None

            try:
                output.size += 0
                # It's a number, so format it nicely, along with date.
                output.size = filesizeformat(output.size)
            except TypeError:
                pass  # Size was not a number, so leave it alone.
            try:
                if output.date:
                    output.date = timezone.localtime(output.date).strftime(
                        '%d %b %Y %H:%M:%S')
            except Exception:
                pass

        return [output.__dict__ for output in outputs]


class RunInputSerializer(serializers.ModelSerializer):
    class Meta:
        model = RunInput
        fields = ("dataset", "index")


class RunSerializer(AccessControlSerializer, serializers.ModelSerializer):
    run_status = serializers.HyperlinkedIdentityField(view_name='run-run-status')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='run-removal-plan')
    run_outputs = serializers.HyperlinkedIdentityField(view_name='run-run-outputs')

    sandbox_path = serializers.CharField(read_only=True, required=False)
    inputs = RunInputSerializer(many=True)
    stopped_by = serializers.SlugRelatedField(
        slug_field="username",
        read_only=True
    )

    runbatch_name = serializers.CharField(
        source="runbatch.name",
        read_only=True
    )

    class Meta:
        model = Run
        fields = (
            'id',
            'url',
            'pipeline',
            'time_queued',
            'start_time',
            'end_time',
            'name',
            'description',
            'display_name',
            'sandbox_path',
            'purged',
            'run_status',
            'run_outputs',
            'removal_plan',
            'user',
            'users_allowed',
            'groups_allowed',
            'inputs',
            'stopped_by',
            'runbatch',
            'runbatch_name'
        )
        read_only_fields = (
            "purged",
            "time_queued",
            "start_time",
            "end_time"
        )

    def validate(self, data):
        """
        Check that the run is correctly specified.

        First, check that the inputs are correctly specified; then,
        check that the permissions are OK.
        """
        pipeline = data["pipeline"]

        posted_input_count = len(data["inputs"])
        pipeline_input_count = pipeline.inputs.count()
        if posted_input_count != pipeline_input_count:
            raise serializers.ValidationError(
                'Pipeline has {} inputs, but only received {}.'.format(
                    pipeline_input_count,
                    posted_input_count))

        inputs_sated = [x["index"] for x in data["inputs"]]
        if len(inputs_sated) != len(set(inputs_sated)):
            raise serializers.ValidationError(
                'Pipeline inputs must be uniquely specified'
            )

        errors = RunSerializer.validate_permissions(data)
        if len(errors) > 0:
            raise serializers.ValidationError(errors)

        return data

    # We don't place this in a transaction; when it's called from a ViewSet, it'll already be
    # in one.
    def create(self, validated_data):
        """
        Create a Run to process, i.e. add a job to the work queue.
        """
        return RunSerializer.create_from_validated_data(validated_data)

    @staticmethod
    def validate_permissions(data, users_allowed=None, groups_allowed=None):
        """
        Helper used by validate and RunBatch.validate that checks the permissions are OK.

        If users_allowed and groups_allowed are specified, then they're used instead of the
        corresponding entries in data.
        """
        # These are lists of users and groups, not usernames and group names.
        users_allowed = users_allowed or data.get("users_allowed", [])
        groups_allowed = groups_allowed or data.get("groups_allowed", [])

        pipeline = data["pipeline"]

        all_access_controlled_objects = [pipeline]
        errors = []
        for run_input in data["inputs"]:
            curr_idx = run_input["index"]
            curr_SD = run_input["dataset"]
            try:
                corresp_input = pipeline.inputs.get(dataset_idx=curr_idx)
            except TransformationInput.DoesNotExist:
                errors.append('Pipeline {} has no input with index {}'.format(pipeline, curr_idx))

            if curr_SD.is_raw() and corresp_input.is_raw():
                continue
            elif not curr_SD.is_raw() and not corresp_input.is_raw():
                if curr_SD.get_cdt().is_restriction(corresp_input.get_cdt()):
                    continue
            else:
                errors.append('Input {} is incompatible with Dataset {}'.format(corresp_input, curr_SD))

            all_access_controlled_objects.append(curr_SD)

        if len(errors) > 0:
            raise serializers.ValidationError(errors)

        # Check that the specified user, users_allowed, and groups_allowed are all okay.
        users_without_access, groups_without_access = who_cannot_access(
            data["user"],
            User.objects.filter(pk__in=[x.pk for x in users_allowed]),
            Group.objects.filter(pk__in=[x.pk for x in groups_allowed]),
            all_access_controlled_objects)

        if len(users_without_access) != 0:
            errors.append("User(s) {} may not be granted access".format(list(users_without_access)))

        if len(groups_without_access) != 0:
            errors.append("Group(s) {} may not be granted access".format(list(groups_without_access)))

        return errors

    @staticmethod
    def create_from_validated_data(validated_data):
        """
        Helper method used by create and also by RunBatchSerializer.create.
        """
        inputs = validated_data.pop("inputs")
        users_allowed = validated_data.pop("users_allowed", [])
        groups_allowed = validated_data.pop("groups_allowed", [])

        # First, create the Run to process with the current time.
        rtp = Run(time_queued=timezone.now(), **validated_data)
        rtp.save()
        rtp.users_allowed.add(*users_allowed)
        rtp.groups_allowed.add(*groups_allowed)

        # Create the inputs.
        for input_data in inputs:
            rtp.inputs.create(**input_data)

        # The ViewSet will call full_clean after this, and if it fails then the
        # transaction will be broken.
        return rtp


class RunProgressSerializer(RunSerializer):
    """
    Same as RunSerializer except run_status is computed instead of linked.
    """
    run_progress = serializers.SerializerMethodField()

    class Meta:
        model = Run
        fields = (
            'id',
            'url',
            'pipeline',
            'time_queued',
            'start_time',
            'end_time',
            'name',
            'description',
            'display_name',
            'sandbox_path',
            'purged',
            "run_status",
            'run_progress',
            'run_outputs',
            'removal_plan',
            'user',
            'users_allowed',
            'groups_allowed',
            'inputs',
            'stopped_by',
            'runbatch',
            'runbatch_name'
        )
        read_only_fields = (
            "purged",
            "time_queued",
            "start_time",
            "end_time"
        )

    def get_run_progress(self, obj):
        if obj is not None:
            return obj.get_run_progress()


class RunBatchSerializer(AccessControlSerializer, serializers.ModelSerializer):

    runs = RunSerializer(many=True, required=False)
    copy_permissions_to_runs = serializers.BooleanField(default=True, write_only=True)
    #
    # users_allowed = serializers.PrimaryKeyRelatedField(
    #     queryset=User.objects.all(),
    #     many=True,
    #     allow_null=True,
    #     required=False
    # )
    # groups_allowed = serializers.PrimaryKeyRelatedField(
    #     queryset=Group.objects.all(),
    #     many=True,
    #     allow_null=True,
    #     required=False
    # )

    class Meta:
        model = RunBatch
        fields = (
            "id",
            "url",
            "name",
            "description",
            "user",
            "users_allowed",
            "groups_allowed",
            "runs",
            "copy_permissions_to_runs"
        )

    def validate(self, data):
        """
        Check that the Runs are coherently specified with the RunBatch.

        In particular, check that the permissions specified for the Run
        do not exceed those of the RunBatch.
        """
        # If this is an update of a RunBatch, and we are trying to set the
        # permissions but the Runs aren't complete yet, we should fail immediately.
        permission_change_requested = (data.get("users_allowed") is not None or
                                       data.get("groups_allowed") is not None)
        if self.instance is not None:
            any_unfinished_runs = self.instance.runs.filter(_runstate__pk__in=runstates.COMPLETE_STATE_PKS).exists()
            if any_unfinished_runs and permission_change_requested:
                raise serializers.ValidationError("Permissions may not be modified while Runs are incomplete")

        # Note that we don't have to check the user because it's the same user
        # creating this RunBatch and the child Runs.
        batch_users_allowed = data.get("users_allowed") or []
        batch_groups_allowed = data.get("groups_allowed") or []

        errors = []
        run_dicts = data.get("runs", [])

        if self.instance is None:
            # We're defining Runs here.
            for i, run_data in enumerate(run_dicts, start=1):
                if (data.get("copy_permissions_to_runs") and
                        run_data.get("users_allowed") is None and
                        run_data.get("groups_allowed") is None):
                    # Here, we're overriding the permissions of this Run, so check that they
                    # don't exceed those of the Pipeline, inputs, etc.
                    original_errors = RunSerializer.validate_permissions(run_data, users_allowed=batch_users_allowed,
                                                                         groups_allowed=batch_groups_allowed)

                    errors = [
                        "{} to run {} (index {})".format(
                            error,
                            run_data.get("name", "[blank]"),
                            i
                        )
                        for error in original_errors
                    ]
                elif everyone_group() not in batch_groups_allowed:
                    # This Run has its own permissions defined, and the batch does not have
                    # Everyone permissions.  Check that they don't exceed
                    # those of the RunBatch.  Other validation will have already been taken
                    # care of by field validation on the "runs" field.
                    extra_users = set(run_data.get("users_allowed", [])).difference(batch_users_allowed)
                    extra_groups = set(run_data.get("groups_allowed", [])).difference(batch_groups_allowed)

                    if len(extra_users) != 0:
                        errors.append(
                            "User(s) {} may not be granted access to run {} (index {})".format(
                                list(extra_users),
                                run_data.get("name", "[blank]"),
                                i
                            )
                        )

                    if len(extra_groups) != 0:
                        errors.append(
                            "Group(s) {} may not be granted access to run {} (index {})".format(
                                list(extra_groups),
                                run_data.get("name", "[blank]"),
                                i
                            )
                        )

        elif permission_change_requested and data.get("copy_permissions_to_runs"):
            # This is an update, make sure that the permissions we're changing are OK with
            # each individual run.
            batch_users_allowed_qs = User.objects.filter(pk__in=[x.pk for x in batch_users_allowed])
            batch_groups_allowed_qs = Group.objects.filter(pk__in=[x.pk for x in batch_groups_allowed])
            for run in self.instance.runs.all():
                # Note that if the Everyone group is among eligible groups, eligible_users
                # and eligible_groups will have all Users and Groups in them.
                eligible_users, eligible_groups = run.eligible_permissions(include_runbatch=False)
                extra_users = batch_users_allowed_qs.exclude(pk__in=[x.pk for x in eligible_users])
                extra_groups = batch_groups_allowed_qs.exclude(pk__in=[x.pk for x in eligible_groups])

                if extra_users.exists():
                    errors.append(
                        "User(s) {} may not be granted access to run {}".format(
                            list(extra_users),
                            run
                        )
                    )

                if len(extra_groups) != 0:
                    errors.append(
                        "Group(s) {} may not be granted access to run {}".format(
                            list(extra_groups),
                            run
                        )
                    )

        if len(errors) > 0:
            raise serializers.ValidationError(errors)

        return data

    def create(self, validated_data):
        """Create a RunBatch and the Runs it contains."""
        run_dictionaries = validated_data.pop("runs", [])
        users_allowed = validated_data.pop("users_allowed", [])
        groups_allowed = validated_data.pop("groups_allowed", [])
        copy_permissions_to_runs = validated_data.pop("copy_permissions_to_runs")

        rb = RunBatch(**validated_data)
        rb.save()
        rb.users_allowed.add(*users_allowed)
        rb.groups_allowed.add(*groups_allowed)

        runs = []
        for run_data in run_dictionaries:
            if (len(run_data.get("users_allowed", [])) == 0 and
                    len(run_data.get("groups_allowed", [])) == 0 and
                    copy_permissions_to_runs):
                run_data["users_allowed"] = users_allowed
                run_data["groups_allowed"] = groups_allowed

            run_data["user"] = validated_data["user"]
            run_data["runbatch"] = rb
            runs.append(RunSerializer.create_from_validated_data(run_data))

        return rb

    def update(self, instance, validated_data):
        """
        Update a RunBatch (e.g. on a PATCH).

        If permissions are updated and copy_permissions_to_runs is specified,
        we add all of those permissions to the child runs.
        """
        instance.name = validated_data.get("name", instance.name)
        instance.description = validated_data.get("description", instance.description)
        instance.user = validated_data.get("user", instance.user)
        instance.save()

        users_allowed = validated_data.get("users_allowed", [])
        groups_allowed = validated_data.get("groups_allowed", [])
        copy_permissions_to_runs = validated_data.get("copy_permissions_to_runs")

        instance.users_allowed.add(*users_allowed)
        instance.groups_allowed.add(*groups_allowed)

        for run in instance.runs.all():
            if copy_permissions_to_runs:
                # Validation assures that these can all be added.
                run.users_allowed.add(*users_allowed)
                run.groups_allowed.add(*groups_allowed)

        return instance
