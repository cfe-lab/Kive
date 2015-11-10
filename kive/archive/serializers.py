import os

from django.core.exceptions import ValidationError
from django.template.defaultfilters import filesizeformat
from django.utils import timezone
from django.contrib.auth.models import User, Group
from rest_framework import serializers
from rest_framework.reverse import reverse

from archive.models import Dataset, Run, MethodOutput, RunInput
from transformation.models import TransformationInput
from librarian.models import SymbolicDataset
from pipeline.models import Pipeline
from metadata.models import CompoundDatatype, who_cannot_access

from kive.serializers import AccessControlSerializer


class TinyRunSerializer(serializers.ModelSerializer):

    class Meta:
        model = Run
        fields = ('id',)


class DatasetSerializer(serializers.ModelSerializer):

    user = serializers.SlugRelatedField(
        source="symbolicdataset.user",
        slug_field='username',
        read_only=True,
        default=serializers.CurrentUserDefault())

    compounddatatype = serializers.PrimaryKeyRelatedField(
        source="symbolicdataset.structure.compounddatatype",
        queryset=CompoundDatatype.objects.all(),
        required=False
    )

    filename = serializers.SerializerMethodField()
    filesize = serializers.IntegerField(source='get_filesize', read_only=True)
    filesize_display = serializers.SerializerMethodField()

    users_allowed = serializers.SlugRelatedField(
        source="symbolicdataset.users_allowed",
        slug_field='username',
        queryset=User.objects.all(),
        many=True, allow_null=True, required=False
    )
    groups_allowed = serializers.SlugRelatedField(
        source="symbolicdataset.groups_allowed",
        slug_field='name',
        queryset=Group.objects.all(),
        many=True, allow_null=True, required=False
    )

    download_url = serializers.HyperlinkedIdentityField(view_name='dataset-download')
    removal_plan = serializers.HyperlinkedIdentityField(view_name='dataset-removal-plan')
    redaction_plan = serializers.HyperlinkedIdentityField(view_name='dataset-redaction-plan')
    symbolic_id = serializers.IntegerField(source='symbolicdataset.id', read_only=True)

    class Meta():
        model = Dataset
        fields = (
            'id',
            'symbolic_id',
            'url',
            'name',
            'description',
            "dataset_file",
            'filename',
            'date_created',
            'date_modified',
            'download_url',
            'compounddatatype',
            'filesize',
            'filesize_display',

            'user',  # inherited
            'users_allowed',
            'groups_allowed',

            'removal_plan',
            'redaction_plan'
        )

    def __init__(self, *args, **kwargs):
        super(DatasetSerializer, self).__init__(*args, **kwargs)
        self.fields["compounddatatype"].queryset = CompoundDatatype.filter_by_user(self.context["request"].user)

    def get_filename(self, obj):
        if obj:
            return os.path.basename(obj.dataset_file.name)

    def get_filesize_display(self, obj):
        if obj:
            return filesizeformat(obj.get_filesize())

    def create(self, validated_data):
        """
        Create a Dataset object from deserialized and validated data.
        """
        cdt = None
        if "structure" in validated_data["symbolicdataset"]:
            cdt = validated_data["symbolicdataset"]["structure"].get("compounddatatype", None)

        users_allowed = None
        groups_allowed = None
        if "users_allowed" in validated_data["symbolicdataset"]:
            users_allowed = validated_data["symbolicdataset"]["users_allowed"]

        if "groups_allowed" in validated_data["symbolicdataset"]:
            groups_allowed = validated_data["symbolicdataset"]["groups_allowed"]

        symbolicdataset = SymbolicDataset.create_SD(
            file_path=None,
            user=self.context["request"].user,
            users_allowed=users_allowed,
            groups_allowed=groups_allowed,
            cdt=cdt,
            make_dataset=True,
            name=validated_data["name"],
            description=validated_data["description"],
            created_by=None,
            check=True,
            file_handle=validated_data["dataset_file"]
        )
        return symbolicdataset.dataset


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
                 filename=None):
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
        self.filename = filename

    def set_dataset(self, dataset, request):
        self.id = dataset.id
        self.size = dataset.dataset_file.size
        self.date = dataset.date_created
        self.url = reverse('dataset-detail',
                           kwargs={'pk': dataset.id},
                           request=request)
        self.redaction_plan = reverse('dataset-redaction-plan',
                                      kwargs={'pk': dataset.id},
                                      request=request)
        self.filename = os.path.basename(dataset.dataset_file.name)

    def set_redacted(self):
        self.size = self.date = 'redacted'


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

        for i, input in enumerate(run.inputs.all()):
            has_data = input.symbolicdataset.has_data()
            if has_data:
                input_name = input.symbolicdataset.dataset.name
            else:
                pipeline_input = pipeline_inputs.get(dataset_idx=input.index)
                input_name = pipeline_input.dataset_name
            input_data = _RunDataset(step_name=(i == 0 and 'Run inputs' or ''),
                                     name=input_name,
                                     display='{}: {}'.format(i+1, input_name),
                                     type='dataset')
            if has_data:
                input_data.set_dataset(input.symbolicdataset.dataset, request)
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
                if execrecordout.symbolicdataset.has_data():
                    dataset = execrecordout.symbolicdataset.dataset
                    output.set_dataset(dataset, request)
                elif execrecordout.symbolicdataset.is_redacted():
                    output.set_redacted()

                outputs.append(output)

        for runstep in run.runsteps_in_order:
            execlog = runstep.get_log()
            if execlog is None:
                continue
            methodoutput = execlog.methodoutput
            step_prefix = 'step_{}_'.format(runstep.pipelinestep.step_num)

            output = _RunDataset(step_name=runstep.pipelinestep,
                                 name=step_prefix + 'stdout',
                                 display='Standard out',
                                 type='stdout')
            if methodoutput.is_output_redacted():
                output.set_redacted()
                outputs.append(output)
            else:
                try:
                    output.id = methodoutput.id
                    output.size = methodoutput.output_log.size
                    output.date = execlog.end_time
                    output.url = reverse('methodoutput-detail',
                                         kwargs={'pk': methodoutput.id},
                                         request=request)
                    output.redaction_plan = reverse(
                        'methodoutput-output-redaction-plan',
                        kwargs={'pk': methodoutput.id},
                        request=request)
                    outputs.append(output)
                except ValueError:
                    pass
            output = _RunDataset(step_name="",
                                 name=step_prefix + 'stderr',
                                 display='Standard error',
                                 type='stderr')
            if methodoutput.is_error_redacted():
                output.set_redacted()
                outputs.append(output)
            else:
                try:
                    output.id = methodoutput.id
                    output.size = methodoutput.error_log.size
                    output.date = execlog.end_time
                    output.url = reverse('methodoutput-detail',
                                         kwargs={'pk': methodoutput.id},
                                         request=request)
                    output.redaction_plan = reverse(
                        'methodoutput-error-redaction-plan',
                        kwargs={'pk': methodoutput.id},
                        request=request)
                    outputs.append(output)
                except ValueError:
                    pass
            if runstep.execrecord is not None:
                for execrecordout in runstep.execrecord.execrecordouts_in_order:
                    transform_output = execrecordout.generic_output.definite
                    output = _RunDataset(
                        step_name='',
                        name=step_prefix + transform_output.dataset_name,
                        display=execrecordout.generic_output,
                        is_ok=execrecordout.is_OK(),
                        type='dataset')
                    if execrecordout.symbolicdataset.has_data():
                        dataset = execrecordout.symbolicdataset.dataset
                        output.set_dataset(dataset, request)
                    elif execrecordout.symbolicdataset.is_redacted():
                        output.set_redacted()

                    outputs.append(output)
        for output in outputs:
            output.is_invalid = not output.is_ok and output.id is not None

            try:
                output.size += 0
                # It's a number, so format it nicely, along with date.
                output.size = filesizeformat(output.size)
                output.date = timezone.localtime(output.date).strftime(
                    '%d %b %Y %H:%M:%S')
            except TypeError:
                pass  # Size was not a number, so leave it alone.

        return [output.__dict__ for output in outputs]


class RunInputSerializer(serializers.ModelSerializer):
    class Meta:
        model = RunInput
        fields = ("symbolicdataset", "index")


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
            'stopped_by'
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
        for run_input in data["inputs"]:
            curr_idx = run_input["index"]
            curr_SD = run_input["symbolicdataset"]
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
        Create a Run to process, i.e. add a job to the work queue.
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
            'stopped_by'
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
