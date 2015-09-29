import sys
import threading
import shutil

from django.db import models, transaction
from django.core.validators import MinValueValidator
from django.core.exceptions import ValidationError
from django.utils import timezone

from archive.models import ExecLog, Run
from librarian.models import SymbolicDataset
from pipeline.models import Pipeline
import metadata.models
import fleet.exceptions

# This is an experimental replacement for the runfleet admin command.
# Disable it by setting worker_count to 0.
worker_count = 0

if worker_count > 0 and sys.argv[-1] == "runserver":
    # import here, because it causes problems when OpenMPI isn't loaded
    import fleet.workers
    
    manage_script = sys.argv[0]
    manager = fleet.workers.Manager(worker_count, manage_script)
    manager_thread = threading.Thread(target=manager.main_procedure)
    manager_thread.daemon = True
    manager_thread.start()


# Create your models here.
class RunToProcess(metadata.models.AccessControl):
    """
    Represents a run that is ready to be processed.

    This table in the database functions as a queue of work to perform, which will
    then be served by the Manager of the fleet.  The required information to start
    a run are:
     - user
     - users allowed access
     - groups allowed access
     - pipeline
     - inputs
     - sandbox path (default None)
    We also need to track the time that these are created, so we can do them in order.

    We also track some metadata to allow tracking of the queue of pipelines to run.
    Occasionally we should reap this table to remove stuff that's finished.
    """
    # The information needed to perform a run:
    # - pipeline
    # - inputs
    # - sandbox_path (default is None)
    # (access information is in the superclass).
    pipeline = models.ForeignKey(Pipeline)
    sandbox_path = models.CharField(max_length=256, default="", blank=True, null=False)
    time_queued = models.DateTimeField(auto_now_add=True)
    run = models.OneToOneField(Run, null=True, related_name="runtoprocess")
    purged = models.BooleanField(default=False)

    def clean(self):
        self.validate_restrict_access([self.pipeline])

        for rtp_input in self.inputs.all():
            rtp_input.clean()

        if hasattr(self, "not_enough_CPUs"):
            self.not_enough_CPUs.clean()

    @property
    @transaction.atomic
    def started(self):
        return (self.run is not None) or hasattr(self, "not_enough_CPUs")

    @property
    @transaction.atomic
    def running(self):
        return self.started and not self.run.is_complete()

    @property
    @transaction.atomic
    def finished(self):
        return (self.started and self.run.is_complete()) or hasattr(self, "not_enough_CPUs")
    
    @property
    def display_name(self):
        try:
            pipeline_name = self.pipeline.family.name
        except Pipeline.DoesNotExist:
            pipeline_name = "Run"
        inputs = self.inputs.select_related('symbolicdataset__dataset')
        first_input = inputs.order_by('index').first()
        if not (first_input and first_input.symbolicdataset.has_data()):
            if self.time_queued:
                return "{} at {}".format(pipeline_name, self.time_queued)
            return pipeline_name
        first_input_name = first_input.symbolicdataset.dataset.name
        return '{} on {}'.format(pipeline_name, first_input_name) 

    @transaction.atomic
    def get_run_progress(self, detailed=False):
        """
        Return a dictionary describing the Run's current state.

        If detailed is True, then the returned dictionary contains
         dictionaries for the run components and cables denoting
         their completion/success status (indexed by id)
        @return {'id': run_id, 'status': s, 'name': n, 'start': t, 'end': t,
            'user': u}
        """
        result = {'name': self.display_name, 'rtp_id': self.id}
        if hasattr(self, "not_enough_CPUs"):
            esc = self.not_enough_CPUs
            result['status'] = "Too many threads ({} from {})".format(
                esc.threads_requested,
                esc.max_available
            )
            return result
        
        if hasattr(self, 'user'):
            result['user'] = self.user.username
        
        if not self.started:
            result['status'] = '?'
            return result

        run = self.run
        status = ""
        step_progress = {}
        cable_progress = {}
        input_list = {}

        for _input in self.inputs.all():
            if _input.symbolicdataset.has_data():
                input_list[_input.index] = {"dataset_id": _input.symbolicdataset.dataset.id,
                                            "dataset_name": _input.symbolicdataset.dataset.name,
                                            "md5": _input.symbolicdataset.MD5_checksum}


        # One of the steps is in progress?
        total_steps = run.pipeline.steps.count()
        runsteps = sorted(run.runsteps.all(), key=lambda x: x.pipelinestep.step_num)

        for step in runsteps:
            step_status = ""
            log_char = ""

            if not step.is_marked_complete():
                try:
                    step.log.id
                    log_char = "+"
                    step_status = "RUNNING"
                except ExecLog.DoesNotExist:
                    log_char = ":"
                    step_status = "READY"

            elif not step.is_marked_successful():
                log_char = "!"
                step_status = "FAILURE"
            else:
                log_char = "*"
                step_status = "CLEAR"

            status += log_char
            if detailed:
                step_progress[step.pipelinestep.transformation.pk] = {'status': step_status, 'log_id': None}
                try:
                    step_progress[step.pipelinestep.transformation.pk]['log_id'] = step.execrecord.generator.\
                        methodoutput.id
                except:
                    pass

        # Just finished a step, but didn't start the next one?
        status += "." * (total_steps - len(runsteps))
        status += "-"

        # Which outcables are in progress?
        cables = sorted(run.pipeline.outcables.all(), key=lambda x: x.output_idx)
        for pipeline_cable in cables:
            run_cables = filter(lambda x: x.run == run, pipeline_cable.poc_instances.all())
            log_char = ""
            step_status = ""
            if len(run_cables) <= 0:
                log_char = "."
                step_status = "WAITING"
            elif run_cables[0].is_marked_complete():
                log_char = "*"
                step_status = "CLEAR"
            else:
                try:
                    run_cables[0].log.id
                    log_char = "+"
                    step_status = "RUNNING"
                except ExecLog.DoesNotExist:
                    log_char = ":"
                    step_status = "READY"

            # Log the status
            status += log_char
            if detailed:
                cable_progress[pipeline_cable.id] = {'status': step_status, 'dataset_id': None, 'md5': None}
                try:
                    symbolicdataset = run_cables[0].execrecord.execrecordouts.first().symbolicdataset
                    cable_progress[pipeline_cable.id]['dataset_id'] = symbolicdataset.dataset.pk \
                        if symbolicdataset.has_data() else None
                    cable_progress[pipeline_cable.id]['md5'] = symbolicdataset.MD5_checksum
                except:
                    pass

        if detailed:
            result['step_progress'] = step_progress
            result['output_progress'] = cable_progress
            result['inputs'] = input_list

        result['status'] = status
        result['id'] = run.id
        result['start'] = self._format_time(run.start_time)
        result['end'] = self._format_time(run.end_time)

        return result

    def _format_time(self, t):
        return t and timezone.localtime(t).strftime('%d %b %Y %H:%M')

    def build_removal_plan(self, removal_accumulator=None):
        if self.run is not None:
            return self.run.build_removal_plan(removal_accumulator=removal_accumulator)
        plan = removal_accumulator or metadata.models.empty_removal_plan()
        plan['Runs'].add(self)
        return plan

    def remove(self):
        """Remove this Run cleanly."""
        removal_plan = self.build_removal_plan()
        metadata.models.remove_helper(removal_plan)

    def collect_garbage(self):
        """
        Dispose of the sandbox used by the Run.
        """
        if self.sandbox_path == "":
            raise fleet.exceptions.SandboxActiveException(
                "Run (RunToProcess={}, Pipeline={}, queued {}, User={}) has not yet started".format(
                    self.pk, self.pipeline, self.time_queued, self.user)
                )
        elif not self.finished:
            raise fleet.exceptions.SandboxActiveException(
                "Run (RunToProcess={}, Pipeline={}, queued {}, User={}) is not finished".format(
                    self.pk, self.pipeline, self.time_queued, self.user)
                )

        # This may raise OSError; the caller should catch it.
        shutil.rmtree(self.sandbox_path)
        self.purged = True
        self.save()


class RunToProcessInput(models.Model):
    """
    Represents an input to a run to process.
    """
    runtoprocess = models.ForeignKey(RunToProcess, related_name="inputs")
    symbolicdataset = models.ForeignKey(SymbolicDataset, related_name="runtoprocessinputs")
    index = models.PositiveIntegerField()

    def clean(self):
        self.runtoprocess.validate_restrict_access([self.symbolicdataset])


class ExceedsSystemCapabilities(models.Model):
    """
    Denotes a RunToProcess that could not be run due to requesting too much from the system.
    """
    runtoprocess = models.OneToOneField(RunToProcess, related_name="not_enough_CPUs")
    threads_requested = models.PositiveIntegerField(validators=[MinValueValidator(1)])
    max_available = models.PositiveIntegerField(validators=[MinValueValidator(1)])

    def clean(self):
        if self.threads_requested <= self.max_available:
            raise ValidationError("Threads requested ({}) does not exceed maximum available ({})".format(
                self.threads_requested, self.max_available
            ))