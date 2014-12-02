from django.db import models, transaction
from django.contrib.auth.models import User
import pipeline.models
import librarian.models
import archive.models


# Create your models here.
class RunToProcess(models.Model):
    """
    Represents a run that is ready to be processed.

    This table in the database functions as a queue of work to perform, which will
    then be served by the Manager of the fleet.  The required information to start
    a run are:
     - user
     - pipeline
     - inputs
     - sandbox path (default None)
    We also need to track the time that these are created, so we can do them in order.

    We also track some metadata to allow tracking of the queue of pipelines to run.
    Occasionally we should reap this table to remove stuff that's finished.
    """
    # The information needed to perform a run:
    # - user
    # - pipeline
    # - inputs
    # - sandbox_path (default is None)
    user = models.ForeignKey(User)
    pipeline = models.ForeignKey(pipeline.models.Pipeline)
    sandbox_path = models.CharField(max_length=256, default="", blank=True, null=False)
    time_queued = models.DateTimeField(auto_now_add=True)
    run = models.ForeignKey(archive.models.Run, null=True)

    @property
    @transaction.atomic
    def started(self):
        return self.run is not None

    @property
    @transaction.atomic
    def running(self):
        return self.started and not self.run.is_complete()

    @property
    @transaction.atomic
    def finished(self):
        return self.started and self.run.is_complete()

    @transaction.atomic
    def get_run_progress(self):
        """
        Return a string describing the Run's current state.
        """
        if not self.started:
            return "Waiting"

        run = self.run

        # Run is finished?
        if run.is_complete():
            if run.successful_execution():
                return "Complete"
            return "{} ({})".format(*(run.describe_run_failure()))

        # One of the steps is in progress?
        total_steps = run.pipeline.steps.count()
        for i, step in enumerate(run.runsteps.order_by("pipelinestep__step_num"), start=1):
            if not step.is_complete():
                return "Running step {} of {}".format(i, total_steps)

        # Just finished a step, but didn't start the next one?
        if run.runsteps.count() < total_steps:
            return "Starting step {} of {}".format(run.runsteps.count()+1, total_steps)

        # One of the outcables is in progress?
        total_cables = run.pipeline.outcables.count()
        for i, cable in enumerate(run.runoutputcables.order_by("pipelineoutputcable__output_idx"), start=1):
            if not cable.is_complete():
                return "Creating output {} of {}".format(i, total_cables)

        # Just finished a cable, but didn't start the next one?
        if run.runoutputcables.count() < total_cables:
            return "Starting output {} of {}".format(run.runoutputcables.count()+1, total_cables)

        # Something is wrong.
        return "Unknown status"


class RunToProcessInput(models.Model):
    """
    Represents an input to a run to process.
    """
    runtoprocess = models.ForeignKey(RunToProcess, related_name="inputs")
    symbolicdataset = models.ForeignKey(librarian.models.SymbolicDataset)
    index = models.PositiveIntegerField()