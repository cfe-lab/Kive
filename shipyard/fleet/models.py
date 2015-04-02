import sys
import threading

from django.db import models, transaction
from django.core.validators import MinValueValidator
from django.core.exceptions import ValidationError
from django.utils import timezone

from archive.models import ExecLog, Run
from librarian.models import SymbolicDataset
from pipeline.models import Pipeline
import metadata.models

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
    run = models.ForeignKey(Run, null=True)

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
        if not first_input:
            if self.time_queued:
                return "{} at {}".format(pipeline_name, self.time_queued)
            return pipeline_name
        first_input_name = first_input.symbolicdataset.dataset.name
        return '{} on {}'.format(pipeline_name, first_input_name) 

    @transaction.atomic
    def get_run_progress(self):
        """
        Return a dictionary describing the Run's current state.
    
        @return {'id': run_id, 'status': s, 'name': n, 'start': t, 'end': t}
        """
        result = {'name': self.display_name}
        if hasattr(self, "not_enough_CPUs"):
            esc = self.not_enough_CPUs
            result['status'] = "Too many threads ({} from {})".format(
                esc.threads_requested,
                esc.max_available
            )
            return result
        
        if not self.started:
            result['status'] = '?'
            return result

        run = self.run

        status = ""

        # One of the steps is in progress?
        total_steps = run.pipeline.steps.count()
        runsteps = sorted(run.runsteps.all(), key=lambda x: x.pipelinestep.step_num)
        for step in runsteps:
            if not step.is_marked_complete():
                try:
                    step.log.id
                    status += "+"
                except ExecLog.DoesNotExist:
                    status += ":"
            elif not step.is_marked_successful():
                status += "!"
            else:
                status += "*"

        # Just finished a step, but didn't start the next one?
        status += "." * (total_steps - len(runsteps))
        status += "-"
        
        # Which outcables are in progress?
        cables = sorted(run.pipeline.outcables.all(), key=lambda x: x.output_idx)
        for pipeline_cable in cables:
            run_cables = filter(lambda x: x.run == run, pipeline_cable.poc_instances.all())
            if len(run_cables) <= 0:
                status += "."
            elif run_cables[0].is_marked_complete():
                status += "*"
            else:
                try:
                    run_cables[0].log.id
                    status += "+"
                except ExecLog.DoesNotExist:
                    status += ":"
        
        result['status'] = status
        result['id'] = run.id
        result['start'] = self._format_time(run.start_time)
        result['end'] = self._format_time(run.end_time)

        return result
    
    def _format_time(self, t):
        return t and timezone.localtime(t).strftime('%d %b %Y %H:%M')


class RunToProcessInput(models.Model):
    """
    Represents an input to a run to process.
    """
    runtoprocess = models.ForeignKey(RunToProcess, related_name="inputs")
    symbolicdataset = models.ForeignKey(SymbolicDataset)
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