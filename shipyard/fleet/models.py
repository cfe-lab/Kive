from django.db import models
from django.contrib.auth.models import User
import pipeline.models
import librarian.models


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
    """
    # The information needed to perform a run:
    # - user
    # - pipeline
    # - inputs
    # - sandbox_path (default is None)
    user = models.ForeignKey(User)
    pipeline = models.ForeignKey(pipeline.models.Pipeline)
    sandbox_path = models.CharField(null=False)
    time_started = models.DateTimeField(auto_now_add=True)


class RunToProcessInput(models.Model):
    """
    Represents an input to a run to process.
    """
    runtoprocess = models.ForeignKey(RunToProcess, related_name="inputs")
    symbolicdataset = models.ForeignKey(librarian.models.SymbolicDataset)
    index = models.PositiveIntegerField()