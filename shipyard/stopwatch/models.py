"""
stopwatch.models

Shipyard abstract class defining anything that has a start- and end-time.
All such classes should extend Stopwatch.
"""
from django.db import models, transaction
from django.utils import timezone
from django.core.exceptions import ValidationError

# Create your models here.
class Stopwatch(models.Model):
    # If the start_time is unset, we haven't started the clock yet.
    start_time = models.DateTimeField("start time",
                                      null=True,
                                      blank=True,
                                      help_text="Starting time")

    # If the end_time is unset, we're in the middle of execution.
    end_time = models.DateTimeField("end time",
                                    null=True,
                                    blank=True,
                                    help_text="Ending time")

    class Meta:
        abstract = True

    def clean(self):
        """
        Checks consistency of this Stopwatch object.
        """
        # end_time cannot be set if start_time is not.
        if self.start_time is None and self.end_time is not None:
            raise ValidationError('Stopwatch "{}" does not have a start time but it has an end time'.format(self))

        if self.end_time is not None and self.start_time > self.end_time:
            raise ValidationError('Stopwatch "{}" start time is later than its end time'.format(self))

    def has_started(self):
        """
        Checks whether this Stopwatch object has actually started.

        That is, check whether start_time is set.

        PRE: this object is clean.
        """
        return (self.start_time is not None)

    def has_ended(self):
        """
        Checks whether this Stopwatch object has been marked as ended.

        That is, check whether end_time is set.

        PRE: this object is clean.
        """
        return (self.end_time is not None)

    @transaction.atomic
    def start(self):
        """Start the stopwatch."""
        self.start_time = timezone.now()
        self.clean()
        self.save()

    @transaction.atomic
    def stop(self):
        """Stop the stopwatch."""
        self.end_time = timezone.now()
        self.clean()
        self.save()
