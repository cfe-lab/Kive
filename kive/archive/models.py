"""
archive.models

Shipyard data models relating to archiving information: Run, RunStep,
Dataset, etc.
"""
from __future__ import unicode_literals

from django.db import models, transaction
from django.db.models.signals import post_delete
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.core.urlresolvers import reverse
from django.utils.encoding import python_2_unicode_compatible

import logging
import itertools
import os
import time
import file_access_utils
import csv
from operator import attrgetter, itemgetter

from datachecking.models import ContentCheckLog, IntegrityCheckLog
import stopwatch.models
import metadata.models
from constants import maxlengths
import archive.signals


def update_complete_mark(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        result = func(*args, **kwargs)

        # Hopefully you've decorated the right object
        # and this exists
        if hasattr(self, '_complete'):
            self._complete = result
            # If there is an entry in the database
            if self.pk is not None:
                self.save(update_fields=["_complete"])
        return result
    return wrapper


def update_success_mark(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        result = func(*args, **kwargs)

        # Hopefully you've decorated the right object
        # and this exists
        if hasattr(self, '_successful'):
            self._successful = result
            # If there is an entry in the database
            if self.pk is not None:
                self.save(update_fields=["_successful"])
        return result
    return wrapper

@python_2_unicode_compatible
class Run(stopwatch.models.Stopwatch, metadata.models.AccessControl):
    """
    Stores data associated with an execution of a pipeline.

    Related to :model:`pipeline.models.Pipeline`
    Related to :model:`archive.models.RunStep`
    Related to :model:`archive.models.Dataset`
    """
    # user = models.ForeignKey(User, help_text="User who performed this run")
    pipeline = models.ForeignKey("pipeline.Pipeline", related_name="pipeline_instances",
                                 help_text="Pipeline used in this run")

    name = models.CharField("Run name", max_length=maxlengths.MAX_NAME_LENGTH)
    description = models.TextField("Run description", max_length=maxlengths.MAX_DESCRIPTION_LENGTH, blank=True)

    # If run was spawned within another run, parent_runstep denotes
    # the run step that initiated it
    parent_runstep = models.OneToOneField("RunStep", related_name="child_run", null=True, blank=True,
        help_text="Step of parent run initiating this one as a sub-run")

    # Implicitly, this also has start_time and end_time through inheritance.

    @property
    def top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        # Base case: this is the top-level Run. Otherwise, return the
        # top-level run of the parent RunStep.
        return self if self.parent_runstep is None else self.parent_runstep.top_level_run

    @property
    def runsteps_in_order(self):
        return self.runsteps.order_by("pipelinestep__step_num")

    @property
    def outcables_in_order(self):
        return self.runoutputcables.order_by("pipelineoutputcable__output_idx")

    def clean(self):
        """
        Checks coherence of the Run (possibly in an incomplete state).

        The procedure:
         - check coherence of start_time and end_time
         - if parent_runstep is not None, then pipeline should be
           consistent with it
         - RunSteps should all be clean, and should be consecutively
           numbered starting from 1
         - all associated RunStepOutputCables must be from RunSteps
           which are associated (ie. at least in progress), and must
           be clean
        """
        # Access to this Run must not exceed that of the pipeline.
        self.validate_restrict_access([self.pipeline])

        # If this is not a top-level run it must have the same access as the top-level run.
        my_top_level_run = self.top_level_run
        if self != my_top_level_run:
            self.validate_identical_access(my_top_level_run)

        # Check that start- and end-time are coherent.
        stopwatch.models.Stopwatch.clean(self)

        if (self.is_subrun() and self.pipeline != self.parent_runstep.pipelinestep.transformation.definite):
            raise ValidationError('Pipeline of Run "{}" is not consistent with its parent RunStep'.format(self))

        # Go through whatever steps are registered. All must be clean.
        for i, runstep in enumerate(self.runsteps.order_by("pipelinestep__step_num"), start=1):
            if runstep.pipelinestep.step_num != i:
                raise ValidationError('RunSteps of Run "{}" are not consecutively numbered starting from 1'
                        .format(self))
            # RunStepInputCables are cleaned within RunStep.clean()
            runstep.clean()

        # Can't have RunOutputCables from non-existent RunSteps.
        # TODO: Should this go in RunOutputCable.clean() ?
        for run_outcable in self.runoutputcables.all():
            source_step = run_outcable.pipelineoutputcable.source_step
            try:
                self.runsteps.get(pipelinestep__step_num=source_step)
            except RunStep.DoesNotExist:
                raise ValidationError('Run "{}" has a RunOutputCable from step {}, but no corresponding RunStep'
                                      .format(self, source_step))
            run_outcable.clean()

    def is_complete(self):
        """
        True if this run is complete; false otherwise.
        """
        # A run is complete if all of its component RunSteps and
        # RunOutputCables are complete, or if any one fails and the
        # rest are complete.  If anything is incomplete, immediately
        # bail and return False.
        anything_failed = False
        all_exist = True

        for step in self.pipeline.steps.all():
            corresp_rs = self.runsteps.filter(pipelinestep=step).first()
            if corresp_rs is None:
                all_exist = False
            elif not corresp_rs.is_complete():
                return False
            elif not corresp_rs.is_successful():
                anything_failed = True
        for outcable in self.pipeline.outcables.all():
            corresp_roc = self.runoutputcables.filter(pipelineoutputcable=outcable).first()
            if corresp_roc is None:
                all_exist = False
            elif not corresp_roc.is_complete():
                return False
            elif not corresp_roc.is_successful():
                anything_failed = True

        # At this point, all RunSteps and ROCs that exist are complete.
        if anything_failed:
            # This is the "unsuccessful complete" case.
            return True
        elif not all_exist:
            # This is the "successful incomplete" case.
            return False

        # Nothing failed and all exist; we are complete and successful.
        return True

    def complete_clean(self):
        """
        Checks completeness and coherence of a run.
        """
        self.clean()
        if not self.is_complete():
            raise ValidationError('Run "{}" is not complete'.format(self))

    def __str__(self):
        if self.is_subrun():
            unicode_rep = "Run with pipeline [{}] parent_runstep [{}]".format(self.pipeline, self.parent_runstep)
        else:
            unicode_rep = "Run with pipeline [{}]".format(self.pipeline)
        return unicode_rep

    def is_subrun(self):
        return self.parent_runstep is not None

    def successful_execution(self):
        """
        Checks if this Run is successful (so far).
        """
        # Check steps for success.
        for step in self.runsteps.all():
            if not step.is_successful():
                return False

        # All steps checked out.  Check outcables.
        for outcable in self.runoutputcables.all():
            if not outcable.is_successful():
                return False

        # So far so good.
        return True

    def get_coordinates(self):
        """
        Retrieves a tuple of pipeline coordinates of this Run.

        This tuple looks like (x_1, x_2, x_3, ...) where x_1 is the step number
        of the top-level run that this Run sits in; x_2 is the step number of
        the first-level-down sub-run this sits in, etc.  The length of the
        tuple is given by how deeply nested this Run is.

        Returns an empty tuple if this is a top-level run.
        """
        # Base case: this is a top-level run.  Return None.
        if self.parent_runstep == None:
            return ()
        # Otherwise, return the coordinates of the parent RunStep.
        return self.parent_runstep.get_coordinates()
    
    def describe_run_failure(self):
        """
        Return a tuple (error, reason) describing a Run failure.
    
        TODO: this is very rudimentary at the moment.
        - It does not take recovery into account - should report which step
          was actually executed and failed, not which step tried to recover
          and failed.
        - It does not take sub-pipelines into account.
        - Failure details for a cable are not reported.
        - Details of cell errors are not reported.
        """
        total_steps = self.pipeline.steps.count()
        error = ""
    
        # Check each step for failure.
        for i, runstep in enumerate(self.runsteps.order_by("pipelinestep__step_num"), start=1):
    
            if runstep.is_complete() and not runstep.is_successful():
                error = "Step {} of {} failed".format(i, total_steps)
    
                # Check each cable.
                total_cables = runstep.pipelinestep.cables_in.count()
                for j, runcable in enumerate(runstep.RSICs.order_by("PSIC__dest__dataset_idx"), start=1):
                    if not runcable.is_successful():
                        return (error, "Input cable {} of {} failed".format(j, total_cables))
    
                # Check the step execution.
                if not runstep.log:
                    return (error, "Recovery failed")
                return_code = runstep.log.methodoutput.return_code 
                if return_code != 0:
                    return (error, "Return code {}".format(return_code))
    
                # Check for bad output.
                for output in runstep.execrecord.execrecordouts.all():
                    try:
                        check = runstep.log.content_checks.get(symbolicdataset=output.symbolicdataset)
                    except ContentCheckLog.DoesNotExist:
                        try:
                            check = runstep.log.integrity_checks.get(symbolicdataset=output.symbolicdataset)
                        except IntegrityCheckLog.DoesNotExist:
                            continue
    
                    if check.is_fail():
                        return (error, "Output {}: {}".format(output.generic_output.definite.dataset_idx, check))
    
                # Something else went wrong with the step?
                return (error, "Unknown error")
                    
        # Check each output cable.
        total_cables = self.pipeline.outcables.count()
        for i, runcable in enumerate(self.runoutputcables.order_by("pipelineoutputcable__output_idx")):
            if not runcable.is_successful():
                return ("Output {} of {} failed".format(i, total_cables), "could not copy file")
    
        # Shouldn't reach here.
        return ("Unknown error", "Unknown reason")


class RunComponent(stopwatch.models.Stopwatch):
    """
    Class extended by both RunStep and RunCable.

    This class encapsulates much of the common function
    of the three "atomic" Run* classes.
    """
    execrecord = models.ForeignKey("librarian.ExecRecord", null=True, blank=True, related_name="used_by_components")
    reused = models.NullBooleanField(help_text="Denotes whether this reuses an ExecRecord", default=None)
    is_cancelled = models.BooleanField(help_text="Denotes whether this has been cancelled",
                                    default=False)

    _complete = models.BooleanField(help_text="Denotes whether this run component has been completed. Private use only",
                                    default=False)
    _successful = models.BooleanField(help_text="Denotes whether this has been successful. Private use only!",
                                      default=False)

    # Implicit:
    # - log: via OneToOneField from ExecLog
    # - invoked_logs: via FK from ExecLog
    # - outputs: via FK from Dataset

    # Implicit from Stopwatch: start_time, end_time.

    def __init__(self, *args, **kwargs):
        """Instantiate and set up a logger."""
        super(RunComponent, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def save(self, *args, **kwargs):
        if 'update_fields' not in kwargs:
            self._complete = self.is_complete()
            self._successful = self.is_successful()
        super(RunComponent, self).save(*args, **kwargs)

    def has_data(self):
        """
        Returns whether or not this instance has an associated Dataset.

        This is abstract and must be overridden.
        """
        pass

    @property
    def component(self):
        """Pipeline component represented by this RunComponent."""
        return self.definite.component

    @property
    def parent_run(self):
        """Run of which this RunComponent is part.

        This is abstract and must be overridden.
        """
        return self.definite.parent_run

    @property
    def top_level_run(self):
        return self.definite.top_level_run

    @property
    def is_step(self):
        try:
            self.runstep
        except RunStep.DoesNotExist:
            return False
        return True

    @property
    def is_incable(self):
        try:
            self.runsic
        except RunSIC.DoesNotExist:
            return False
        return True

    @property
    def is_outcable(self):
        try:
            self.runoutputcable
        except RunOutputCable.DoesNotExist:
            return False
        return True

    @property
    def is_cable(self):
        return self.is_incable or self.is_outcable

    @property
    def has_log(self):
        return hasattr(self, "log")

    @property
    def definite(self):
        if self.is_step:
            return self.runstep
        elif self.is_incable:
            return self.runsic
        elif self.is_outcable:
            return self.runoutputcable

    def link_execrecord(self, execrecord, reused, clean=True):
        """Link an ExecRecord to this RunComponent."""
        self.reused = reused
        self.execrecord = execrecord
        if clean:
            self.clean()
        self.save()

    def _clean_undecided_reused(self):
        """
        Check coherence of a RunComponent which has not decided whether or
        or not to reuse an ExecRecord:

         - if reused is None (no decision on reusing has been made),
           no log or invoked_logs should be associated, no data should be associated,
           and execrecord should not be set

        This is a helper for clean().

        PRE
        This RunComponent has reused = None (the decision to reuse an
        ExecRecord or not has not yet been made).
        """
        general_error = '{} "{}" has not decided whether or not to reuse an ExecRecord'.format(
            self.__class__.__name__, self)
        if self.has_log:
            raise ValidationError("{}; no log should have been generated".format(general_error))
        if self.invoked_logs.exists():
            raise ValidationError("{}; no steps or cables should have been invoked".format(general_error))
        if self.has_data():
            raise ValidationError("{}; no Datasets should be associated".format(general_error))
        if self.execrecord:
            raise ValidationError("{}; execrecord should not be set yet".format(general_error))

    def _clean_reused(self):
        """
        Check coherence of a RunComponent which has decided to reuse an
        ExecRecord:

         - if reused is True, no data should be associated.
         - also, there should be no invoked_logs.

        This is a helper for clean().

        PRE
        This RunComponent has reused = True (has decided to reuse an ExecRecord).
        """
        if self.has_data():
            raise ValidationError('{} "{}" reused an ExecRecord and should not have generated any Datasets'
                                  .format(self.__class__.__name__, self))
        if self.invoked_logs.exists():
            raise ValidationError('{} "{}" reused an ExecRecord; no steps or cables should have been invoked'
                                  .format(self.__class__.__name__, self))

    # Note: what clean() does in the not-reused case is specific to
    # the class, so the _clean_not_reused() method is extended
    # in RunStep and RunCable.
    def _clean_not_reused(self):
        """
        Check coherence of a RunComponent which has decided not to reuse an
        ExecRecord:

         - if the log is incomplete, there should be no Datasets or ExecRecord
         - if ExecRecord is in place then it must have invoked logs, and its own log must be complete
           (_clean_execlogs makes sure that all other logs are complete).

        This is a helper for clean().  Returns False if clean() should terminate at this step
        and True if it should continue.

        PRE
        This RunComponent has reused = False (has decided not to reuse an ExecRecord).
        """
        if not self.has_log or not self.log.is_complete():

            general_error = '{} "{}" is not reused and does not have a complete log'.format(
                self.__class__.__name__, self)
            if self.has_data():
                raise ValidationError("{} so should not have generated any Datasets".format(general_error))
            if self.execrecord:
                raise ValidationError("{}; execrecord should not be set".format(general_error))
            return False

        # On the flipside....
        if (self.execrecord is not None and
                (not self.invoked_logs.exists() or self.has_log and not self.log.is_complete())):
            raise ValidationError(
                '{} "{}" is not reused and has not completed its own ExecLog but does have an ExecRecord'.format(
                    self.__class__.__name__, self))

        return True

    def _clean_execlogs(self):
        """Count and clean ExecLogs of Run(Step|SIC|OutputCable).

        Helper function to ensure a RunStep, RunSIC, or RunOutputCable
        has at most one ExecLog, and to clean it if it exists.  Also,
        clean all invoked_logs, and check coherence between log and
        invoked_logs; if there are invoked_logs and log is set, then
        log must also be among the invoked_logs.

        It also cleans all associated ContentCheckLogs and
        IntegrityCheckLogs in the process.

        Then, it checks that if log is complete then all of the
        invoked_logs must also be complete.
        """
        if self.has_log:
            self.log.clean()

        for invoked_log in self.invoked_logs.all():
            invoked_log.clean()

            # Clean all content/integrity checks, and make sure at most
            # one has been done for each output SymbolicDataset.
            outputs_checked = set([])
            for check in itertools.chain(invoked_log.content_checks.all(), 
                                         invoked_log.integrity_checks.all()):
                if check.symbolicdataset.pk in outputs_checked:
                    raise ValidationError('{} "{}" has multiple Integrity/ContentCheckLogs for output '
                                          'SymbolicDataset {} of ExecLog "{}"'
                                          .format(self.__class__.__name__, self, check.symbolicdataset, invoked_log))
                outputs_checked.add(check.symbolicdataset.pk)
                check.clean()

        # If log exists and there are invoked_logs, log should be among
        # the invoked logs.  If log exists, any preceding logs should
        # be complete and all tests should have passed (since they were
        # recoveries happening before we could carry out the execution
        # that log represents).
        if self.invoked_logs.exists() and self.has_log:
            if not self.invoked_logs.filter(pk=self.log.pk).exists():
                raise ValidationError(
                   'ExecLog of {} "{}" is not included with its invoked ExecLogs'.format(
                       self.__class__.__name__, self)
                )

            preceding_logs = self.invoked_logs.exclude(pk=self.log.pk)
            if not all([x.is_complete() for x in preceding_logs]):
                raise ValidationError(
                   'ExecLog of {} "{}" is set before all invoked ExecLogs are complete'.format(
                       self.__class__.__name__, self)
                )

            if not all([x.all_checks_passed() for x in preceding_logs]):
                raise ValidationError(
                   'Invoked ExecLogs preceding log of {} "{}" did not successfully pass all of their checks'.format(
                       self.__class__.__name__, self)
                )

    def _clean_has_execlog_no_execrecord_yet(self):
        """
        Check coherence after log is set but before execrecord is set.

        This is a helper called during the course of clean().

        PRE: log is set and complete, execrecord is not set yet.
        """
        # There should be no CCLs/ICLs yet.
        if self.log.integrity_checks.exists() or self.log.content_checks.exists():
            raise ValidationError(
                '{} "{}" does not have an ExecRecord so should not have any data checks'.format(
                    self.__class__.__name__, self)
            )

    def is_marked_complete(self):
        """
        Returns whether or not this run component has been marked
        as complete when it was last saved.
        """
        return self._complete

    def is_complete(self, **kwargs):
        """
        True if this RunComponent is complete; false otherwise.

        Note that this is overridden by RunStep.

        If this RunComponent is reused, then completeness == having an ER.

        If this RunComponent is not reused, then either all of its outputs
        have been checked with an ICL/CCL and passed, or some
        EL/ICL/CCL failed and the rest are complete (not all outputs
        have to have been checked).

        PRE: this RunComponent is clean.
        """
        # Has this been cancelled before even being attempted?
        if self.is_cancelled:
            return True

        # Is there an ExecRecord?  If not, check if this failed during
        # recovery and then completed.
        if self.execrecord is None:
            if not self.is_successful():

                for invoked_log in self.invoked_logs.all():
                    if not invoked_log.is_complete():
                        return False

                    if not all([x.is_complete() for x in invoked_log.integrity_checks.all()]):
                        return False

                    if not all([x.is_complete() for x in invoked_log.content_checks.all()]):
                        return False

                # All ELs, and ICLs/CCLs are complete, albeit
                # with a failure somewhere.
                return True

            # At this point we know that this is still a successful
            # execution that isn't complete.
            return False

        # From here on, we know there is an ExecRecord; therefore reused
        # is set.
        if self.reused:
            return True

        # From here on we know we are not reusing and ExecRecord is
        # set -- therefore log is set and complete.

        # Check that either every output has been successfully checked
        # or one+ has failed and the rest are complete.
        if self.log.all_checks_passed():
            return True

        # From here on we know that one of the following happened:
        # - the log was a failure
        # - at least one of the checks failed or was not performed.
        my_log = self.log
        if not my_log.is_successful():
            return True

        if (any([x.is_fail() for x in my_log.integrity_checks.all()]) or
                any([x.is_fail() for x in my_log.content_checks.all()])):
            if (all([x.is_complete() for x in my_log.integrity_checks.all()]) and
                    all([x.is_complete() for x in my_log.content_checks.all()])):
                return True

        # At this point, we know that it is unsuccessful and incomplete.
        return False

    def clean(self):
        """Confirm that this is one of RunStep or RunCable."""
        # If the ExecRecord is set, check that access on the top level Run does not exceed
        # that on the ExecRecord.
        if self.execrecord is not None:
            self.top_level_run.validate_restrict_access([self.execrecord.generating_run])

        if not self.is_step and not self.is_cable:
            raise ValidationError("RunComponent with pk={} is neither a step nor a cable".format(self.pk))

    def complete_clean(self):
        """
        Checks coherence and completeness of this RunComponent.
        """
        self.clean()
        if not self.is_complete():
            raise ValidationError('{} "{}" is not complete'.format(self.__class__.__name__, self))

    def is_marked_successful(self):
        """
        Returns whether or not this run component has been marked
        as successful when it was last saved.
        """
        return self._successful

    def is_successful(self, **kwargs):
        if self.is_cancelled:
            return False
        if self.reused:
            return self.successful_reuse()
        return self.successful_execution()

    def successful_reuse(self):
        """
        True if RunComponent is successful on reuse; False otherwise.

        PRE: this RunComponent is reused.
        """
        assert(self.reused)
        if self.execrecord is not None:
            return self.execrecord.outputs_OK() and not self.execrecord.has_ever_failed()
        # If there is no ExecRecord yet then this is trivially true.
        return True

    def successful_execution(self):
        """True if RunComponent is successful; False otherwise.

        Any RunComponent is failed if any of its invoked ExecLogs have
        failed, or if any CCLs/ICLs have failed.

        PRE: this RunComponent is clean, and so are all of its invoked_logs.
        (It's OK that they might not be complete.)
        PRE: this RunComponent is not reused.
        """
        assert(not self.reused)

        for invoked_log in self.invoked_logs.all():
            if not invoked_log.is_successful():
                return False
            icls = invoked_log.integrity_checks.all()
            if icls.exists() and any([x.is_fail() for x in icls]):
                return False
            ccls = invoked_log.content_checks.all()
            if ccls.exists() and any([x.is_fail() for x in ccls]):
                return False
        return True


@python_2_unicode_compatible
class RunStep(RunComponent):
    """
    Annotates the execution of a pipeline step within a run.

    Related to :model:`archive.models.Run`
    Related to :model:`librarian.models.ExecRecord`
    Related to :model:`pipeline.models.PipelineStep`
    """
    run = models.ForeignKey(Run, related_name="runsteps")
    pipelinestep = models.ForeignKey("pipeline.PipelineStep", related_name="pipelinestep_instances")

    # Implicit from RunComponent: start_time, end_time, execrecord,
    # reused, log, invoked_logs.

    class Meta:
        # Uniqueness constraint ensures you can't have multiple RunSteps for
        # a given PipelineStep within a Run.
        unique_together = ("run", "pipelinestep")

    def __str__(self):
        return "Runstep with PS [{}]".format(self.pipelinestep)

    @property
    def component(self):
        return self.pipelinestep

    @property
    def parent_run(self):
        return self.run

    @property
    def top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        return self.run.top_level_run

    @property
    def step_num(self):
        return self.pipelinestep.step_num

    @property
    def transformation(self):
        return self.pipelinestep.transformation

    @property
    def pipeline(self):
        return self.pipelinestep.pipeline

    @property
    def parent(self):
        return self.run

    @property
    def is_step(self):
        return True

    @property
    def is_incable(self):
        return False

    @property
    def is_outcable(self):
        return False

    @classmethod
    @transaction.atomic
    def create(cls, pipelinestep, run):
        """Create a new RunStep from a PipelineStep."""
        runstep = cls(pipelinestep=pipelinestep, run=run)
        runstep.start()
        runstep.clean()
        runstep.save()
        return runstep

    # TODO: fix for sub-pipelines
    def output_name(self, output):
        """Name for Dataset generated from a TransformationOutput."""
        assert self.pipelinestep.transformation.outputs.filter(pk=output.pk).exists()
        return "run{}_step{}_output{}".format(self.run.pk, self.step_num, output.dataset_name)

    def output_description(self, output):
        """Desc for Dataset generated from a TransformationOutput."""
        assert self.pipelinestep.transformation.outputs.filter(pk=output.pk).exists()
        desc = ('Generated data from a run of pipeline "{}" started at {} by {}\n'
                .format(self.pipeline, self.run.start_time, self.run.user))
        desc += "run: {}\n".format(self.run.pk)
        desc += "user: {}\n".format(self.run.user)
        desc += "step: {}\n".format(self.step_num)
        desc += "output: {}".format(output.dataset_name)
        return desc

    def has_subrun(self):
        """
        Does this RunStep contain a Run of a sub-Pipeline?
        """
        return hasattr(self, "child_run")

    def has_data(self):
        """True if associated output exists; False if not."""
        return self.outputs.all().exists()

    def keeps_output(self, output):
        """Whether the RunStep keeps the given output.

        INPUTS
        output      TransformationOutput to check
        
        PRE
        The provided output is a TransformationOutput of the RunStep's
        PipelineStep's Transformation.
        """
        assert self.pipelinestep.transformation.outputs.filter(pk=output.pk).exists()
        return not self.pipelinestep.outputs_to_delete.filter(pk=output.pk).exists()

    def _clean_with_subrun(self):
        """
        If this RunStep contains a subrun (child run), make sure it is
        in a coherent state, namely:

         - if pipelinestep is a pipeline, there should be no
           ExecLog or Datasets associated, reused = None, and
           execrecord = None

        This is a helper function for clean.

        PRE
        This RunStep has a child_run.
        """
        general_error = 'RunStep "{}" represents a sub-pipeline'.format(self)
        if self.has_log:
            raise ValidationError('{} so no log should be associated'.format(general_error))

        if self.outputs.all().exists():
            raise ValidationError('{} and should not have generated any data'.format(general_error))

        if self.reused is not None:
            raise ValidationError('{} so reused should not be set'.format(general_error))

        if self.execrecord is not None:
            raise ValidationError('{} so execrecord should not be set'.format(general_error))

    def _clean_cables_in(self):
        """
        Perform coherence checks for this RunStep's associated
        RunSIC's, namely:

         - If RSICs exist, check they are clean and complete

         - If all RSICs are not quenched, reused, child_run, and
           execrecord should not be set, no ExecLog should be
           associated, there should be no invoked_logs, and no
           Datasets should be associated

        This is a helper function for clean.

        OUTPUTS
        True if there are more checks to do within clean, False if
        clean should return right away.
        """
        for rsic in self.RSICs.all():
            rsic.complete_clean()

        if self.pipelinestep.cables_in.count() != self.RSICs.count():

            general_error = 'RunStep "{}" inputs not quenched'.format(self)
            if self.reused is not None or self.execrecord is not None:
                raise ValidationError("{}; reused and execrecord should not be set".format(general_error))
            if self.pipelinestep.transformation.__class__.__name__ == "Pipeline" and self.has_subrun():
                raise ValidationError("{}; child_run should not be set".format(general_error))
            if self.has_log:
                raise ValidationError("{}; no log should have been generated".format(general_error))
            if self.invoked_logs.exists():
                raise ValidationError("{}; no other steps or cables should have been invoked".format(general_error))
            if self.outputs.exists():
                raise ValidationError("{}; no data should have been generated".format(general_error))
            return False
        return True

    def _clean_with_method(self):
        """
        If this RunStep represents a Method (as opposed to a Pipeline),
        make sure it is in a coherent state:

         - if we haven't decided whether or not to reuse an ER and
           this is a method, no log or invoked_log should be
           associated, no Datasets should be associated, and
           execrecord should not be set

        (from here on, reused is assumed to be set)

         - else if we are reusing an ER and this is a method, check
           that:

           - there are no associated Datasets.
           - there are no associated invoked_logs.

         - else if we are not reusing an ER and this is a Method:

           - call RunComponent._clean_reused()

           (from here on ExecLog is assumed to be complete and clean)

           - clean any associated Datasets

        This is a helper function for clean.

        PRE
        This RunStep represents a Method.

        OUTPUTS
        True if there are more checks to do within clean, False if
        clean should return right away.
        """
        if self.reused is None:
            self._clean_undecided_reused()
            return False

        elif self.reused:
            self._clean_reused()

        else: # self.reused is False.
            if not RunComponent._clean_not_reused(self):
                return False
 
            for out_data in self.outputs.all():
                out_data.clean()
        return True

    def _clean_execrecord(self):
        """
        If this RunStep has an ExecRecord associated:

         - check that it is complete and clean
         - check that it's coherent with pipelinestep

        This is a helper function for clean.

        PRE
        This RunStep has an ExecRecord associated to it.
        """
        self.execrecord.complete_clean()

        # ExecRecord must point to the same transformation that this
        # RunStep points to.
        if self.pipelinestep.transformation.definite != self.execrecord.general_transf():
            raise ValidationError('RunStep "{}" points to transformation "{}" but corresponding ExecRecord does not'
                                  .format(self, self.pipelinestep))

    def _clean_outputs(self):
        """
        Check the coherence of this RunStep's outputs:

         - if an output is marked for deletion or missing, there
           should be no associated Dataset

         - else:
           - the corresponding ERO should have an associated Dataset.

         - any associated Dataset belongs to an ERO (this checks for
           Datasets that have been wrongly assigned to this RunStep).

        This is a helper function for clean.
        """
        # If there is no ExecLog there is no notion of missing outputs.
        outputs_missing = []
        if self.has_log:
            outputs_missing = self.log.missing_outputs()

        # Go through all of the outputs.
        for to in self.pipelinestep.transformation.outputs.all():
            # Get the associated ERO.
            corresp_ero = self.execrecord.execrecordouts.get(generic_output=to)

            if self.pipelinestep.outputs_to_delete.filter(dataset_name=to.dataset_name).exists():
                # This output is deleted; there should be no associated Dataset.
                if self.outputs.filter(symbolicdataset=corresp_ero.symbolicdataset).exists():
                    raise ValidationError('Output "{}" of RunStep "{}" is deleted; no data should be associated'
                                          .format(to, self))

            elif corresp_ero.symbolicdataset in outputs_missing:
                # This output is missing; there should be no associated Dataset.
                if self.outputs.filter(symbolicdataset=corresp_ero.symbolicdataset).exists():
                    raise ValidationError('Output "{}" of RunStep "{}" is missing; no data should be associated'
                                          .format(to, self))

            # The corresponding ERO should have existent data.
            elif not corresp_ero.symbolicdataset.has_data():
                raise ValidationError('ExecRecordOut "{}" of RunStep "{}" should reference existent data'
                                      .format(corresp_ero, self))

        # Check that any associated data belongs to an ERO of this ER
        # Supposed to be the datasets attached to this runstep (Produced by this runstep)
        for out_data in self.outputs.all():
            if not self.execrecord.execrecordouts.filter(symbolicdataset=out_data.symbolicdataset).exists():
                raise ValidationError('RunStep "{}" generated Dataset "{}" but it is not in its ExecRecord'
                                      .format(self, out_data))

    def clean(self):
        """
        Check coherence of this RunStep.

        The checks we perform, in sequence:
         - check that start_time and end_time are coherent
         - pipelinestep is consistent with run
         - if pipelinestep is a method, there should be no child_run

         - if pipelinestep is a pipeline, check for coherence (see 
           _clean_with_subrun)

         - if an EL is associated, check it is clean (see the module
           function _clean_execlogs)

         - if there are any invoked_logs, check they are clean; also
           check that this RunStep has an associated EL and that that
           EL is also one of the invoked_logs (also in _clean_execlogs)

         - check coherence of any CCLs and ICLs associated to its
           invoked_logs (in _clean_execlogs)

         - check coherence of RSICs (see _clean_inputs)

        (from here on all RSICs are assumed to be quenched)

         - if this is represents a Method, do relevant coherence checks 
           (see _clean_with_method).
         - else if this is a Pipeline:
           - clean child_run if it exists

        (from here on, log and invoked_logs are known to be complete
         and clean)

         - if execrecord is not set, no ICLs and CCLs should be
           associated with the associated log.

        (from here on, it is assumed that this is a Method and
         execrecord is set)

         - check the execrecord (see _clean_execrecord)
         - check the outputs (see _clean_outputs)

         - check for overquenching of the outputs by CCLs/ICLs

        Note: don't need to check inputs for multiple quenching due to
        uniqueness.  Quenching of outputs is checked by ExecRecord.
        """
        # Check that the times are coherent.
        stopwatch.models.Stopwatch.clean(self)
        RunComponent.clean(self)

        # Does pipelinestep belong to run.pipeline?
        if not self.run.pipeline.steps.filter(pk=self.pipelinestep.pk).exists():
            raise ValidationError('PipelineStep "{}" of RunStep "{}" does not belong to Pipeline "{}"'
                                  .format(self.pipelinestep, self, self.run.pipeline))

        # If the PipelineStep stores a method, it should have no
        # child_run (should not act as a parent runstep).
        if self.pipelinestep.transformation.is_method and self.has_subrun():
            raise ValidationError('PipelineStep of RunStep "{}" is not a Pipeline but a child run exists'
                                  .format(self))

        # TODO: Is there any difference between having a Pipeline as
        # your pipelinestep's transformation, and having a child_run?
        elif self.pipelinestep.transformation.is_pipeline:
            self._clean_with_subrun()

        # Clean all ExecLogs and their CCLs/ICLs, and make sure that
        # all preceding this step's ExecLog are complete and successful
        # before this one's is started.
        self._clean_execlogs()

        # If any inputs are not quenched, stop checking.
        if not self._clean_cables_in(): return

        # From here on, RSICs are assumed to be quenched.
        # Perform tests specific to the Method and Pipeline cases.
        if self.pipelinestep.transformation.is_method:
            if not self._clean_with_method(): return
        elif self.pipelinestep.transformation.is_pipeline:
            if self.has_subrun():
                self.child_run.clean()
            return

        # From here on, we know that this represents a Method, log is
        # assumed to be complete and clean, and so are the
        # invoked_logs().

        # Check that if there is no execrecord then log has no
        # associated CCLs or ICLs.  (It can't, as execution can't have
        # finished yet.)
        if self.has_log:
            if self.execrecord is None:
                self._clean_has_execlog_no_execrecord_yet()
                return

        if self.execrecord is None:
            return

        # From here on, the appropriate ER is known to be set.
        self._clean_execrecord()

        # If we reused an ExecRecord and it was a failure, then we can skip cleaning the outputs; otherwise
        # we clean them.
        usable_dict = self.check_ER_usable(self.execrecord)
        # This is the negative of (self.reused and not usable_dict["successful"])....
        if not self.reused or usable_dict["successful"]:
            self._clean_outputs()

    @update_complete_mark
    def is_complete(self, **kwargs):
        """
        True if RunStep is complete; False otherwise.

        This extends the procedure to check for completeness of a
        RunComponent.  In addition to the ways a RunComponent can fail, a
        RunStep can fail while its cables are running before it even
        gets to the recovery stage.  Also, if it represents
        a sub-Pipeline, then it simply checks if its child_run
        is complete.

        PRE: this RunStep must be clean.
        """
        # Sub-Pipeline case:
        if self.pipelinestep.transformation.is_pipeline:
            if self.has_subrun():
                return self.child_run.is_complete()
            # At this point, child_run hasn't been set yet, so we can
            # say that it isn't complete.
            return False

        # From here on we know we are in the Method case.  Check that
        # all PSICs have an RSIC that are complete and successful --
        # in which case go on and check the same stuff as RunComponent --
        # or that some RSIC failed and the rest are complete, and
        # return.  Any incomplete RSIC causes us to return False.
        all_cables_exist = True
        any_cables_failed = False
        for curr_cable in self.pipelinestep.cables_in.all():
            corresp_RSIC = self.RSICs.filter(PSIC=curr_cable).first()
            if corresp_RSIC is None:
                all_cables_exist = False
            elif not corresp_RSIC.is_complete():
                return False
            elif not corresp_RSIC.is_successful():
                any_cables_failed = True

        # At this point we know that all RSICs that exist are complete.
        if any_cables_failed:
            return True
        elif not all_cables_exist:
            return False

        # At this point we know that all RSICs exist, and are complete
        # and successful.  Proceed to check the RunComponent stuff.
        return RunComponent.is_complete(self)

    @update_success_mark
    def is_successful(self, **kwargs):
        return super(RunStep, self).is_successful()

    def successful_execution(self):
        """
        True if RunStep is successful; False otherwise.

        This inherits from RunComponent's method, with the additional
        wrinkle that a RunStep fails if any of its cables fails, or if
        its child_run has failed.

        PRE: this RunStep is clean.
        PRE: this RunStep is not reused.
        """
        input_cables = self.RSICs.all()
        if input_cables.exists():
            if any(not ic.is_successful() for ic in input_cables):
                return False

        # At this point we know that all the cables were successful;
        # we check for failure during recovery or during its own
        # execution.
        if not RunComponent.successful_execution(self):
            return False

        # In the case that this is a sub-Pipeline, check if child_run
        # is successful.
        try:
            self.child_run
            return self.child_run.successful_execution()
        except ObjectDoesNotExist:
            pass

        # No logs failed, and this wasn't a sub-Pipeline, so....
        return True

    def get_coordinates(self):
        """
        Retrieves a tuple of pipeline coordinates of this RunStep.

        The ith coordinate gives the step number in the ith-deeply-nested
        sub-Run that this RunStep belongs to.
        """
        # Get the coordinates of the parent Run.
        run_coords = self.run.get_coordinates()
        # Tack on the coordinate within that run.
        return run_coords + (self.pipelinestep.step_num,)

    def find_compatible_ERs(self, inputs_after_cable):
        assert self.transformation.is_method
        return self.pipelinestep.transformation.definite.find_compatible_ERs(inputs_after_cable, self)

    @transaction.atomic
    def check_ER_usable(self, execrecord):
        """
        Check that the specified ExecRecord may be reused.
        """
        result = {"fully reusable": False, "successful": True}
        # Case 1: ER was a failure.  In this case, we don't want to proceed,
        # so we return the failure for appropriate handling.
        if execrecord.outputs_failed_any_checks() or execrecord.has_ever_failed():
            self.logger.debug("ExecRecord found ({}) was a failure".format(execrecord))
            result["successful"] = False

        # Case 2: ER has fully checked outputs and provides the outputs needed.
        elif execrecord.outputs_OK() and execrecord.provides_outputs(self.pipelinestep.outputs_to_retain()):
            self.logger.debug("Completely reusing ExecRecord {}".format(execrecord))
            result["fully reusable"] = True

        return result

    @transaction.atomic
    def get_suitable_ER(self, input_SD):
        """
        Retrieve a suitable ExecRecord for this RunStep.

        If any of them are failed, we find the failed one with the most outputs having data, with
        ties broken by the smallest PK.
        If any of them are fully reusable, we find the fully reusable one satisfying the same criteria.
        Otherwise we find whichever one satisfies the same criteria.

        Return a tuple containing the ExecRecord along with its summary (as
        produced by check_ER_usable), or None if no appropriate ExecRecord is found.
        """
        execrecords = self.find_compatible_ERs(input_SD)
        failed = []
        fully_reusable = []
        other = []
        execrecords_sorted = sorted(execrecords, key=attrgetter("pk"))
        for er in execrecords_sorted:
            curr_summary = self.check_ER_usable(er)
            curr_entry = (er, curr_summary)
            if not curr_summary["successful"]:
                failed.append(curr_entry)
            elif curr_summary["fully reusable"]:
                fully_reusable.append(curr_entry)
            else:
                other.append(curr_entry)

        if len(failed) > 0:
            return _first_ER_h(failed)

        if len(fully_reusable) > 0:
            return _first_ER_h(fully_reusable)

        if len(other) > 0:
            return _first_ER_h(other)

        return (None, None)


def _first_ER_h(execrecord_summary_list):
    """
    Of the (ExecRecord, summary) pairs provided, return the one with the most outputs having real Datasets.

    Ties are broken according to their position in the list.
    """
    list_decorated = []
    for er, curr_summary in execrecord_summary_list:
        num_outputs = sum(1 if x.has_data() else 0 for x in er.execrecordouts.all())
        list_decorated.append((er, curr_summary, num_outputs))
    list_sorted = sorted(list_decorated, key=itemgetter(2))
    return (list_sorted[0][0], list_sorted[0][1])


class RunCable(RunComponent):
    """
    Class inherited by RunSIC and RunOutputCable.

    Since those classes share so much functionality, this
    abstract class will encapsulate that stuff and RSIC/ROC
    can extend it where necessary.
    """
    # Implicit from RunComponent: start_time, end_time, execrecord,
    # reused, log, invoked_logs, outputs.

    class Meta:
        abstract = True

    @classmethod
    def create(cls, cable, parent_record):
        if cable.is_incable:
            runcable = RunSIC.create(cable, parent_record)
        else:
            runcable = RunOutputCable.create(cable, parent_record)
        runcable.clean()
        runcable.save()
        runcable.start()
        return runcable

    def has_data(self):
        """True if associated output exists; False if not."""
        return self.outputs.exists()

    def keeps_output(self):
        """
        True if this RunCable retains its output; false otherwise.

        This is an abstract function that must be implemented by
        RunSIC and RunOutputCable
        """
        pass

    def _cable_type_str(self):
        """
        Helper to retrieve the class name of this cable.

        That is, a RunSIC will return "RunSIC" and a
        RunOutputCable will return "RunOutputCable".
        """
        return self.__class__.__name__

    def _pipeline_cable(self):
        """
        Abstract function that retrieves the PSIC/POC.
        """
        pass

    @property
    def component(self):
        return self.PSIC

    def is_trivial(self):
        return self.component.is_trivial()

    @update_complete_mark
    def is_complete(self, **kwargs):
        return super(RunCable, self).is_complete()

    @update_success_mark
    def is_successful(self, **kwargs):
        return super(RunCable, self).is_successful()

    def _clean_not_reused(self):
        """
        Check coherence of a RunCable which has decided not to reuse an
        ExecRecord:

         - if reused is False:

           - call RunComponent._clean_reused

           - if the cable is trivial, there should be no associated Dataset
           - otherwise, make sure there is at most one Dataset, and clean it
             if it exists

        This is a helper for clean().

        PRE
        This RunCable has reused = False (has decided not to reuse an
        ExecRecord).

        OUTPUT
        True if there are more checks to do within clean, False if clean
        should return right away.
        """
        if not RunComponent._clean_not_reused(self):
            return False

        # From here on, the ExecLog is known to be complete.

        # If this cable is trivial, there should be no data
        # associated.
        if self.has_data():
            if self._pipeline_cable().is_trivial():
                raise ValidationError(
                    '{} "{}" is trivial and should not have generated any Datasets'.format(
                        self._cable_type_str(), self)
                )

            # Otherwise, check that there is at most one Dataset
            # attached, and clean it.
            if self.outputs.count() > 1:
                raise ValidationError('{} "{}" should generate at most one Dataset'.format(
                    self._cable_type_str(), self))
            self.outputs.first().clean()
        return True

    def _clean_cable_coherent(self):
        """
        Checks that the cable is coherent with its parent.

        This is an abstract function that must be implemented
        by both RunSIC and RunOutputCable.
        """
        pass

    def _clean_with_execlog(self):
        """
        If this RunCable has an ExecLog (that is, code was run during its
        execution), make sure it is in a coherent state:

         - if this RunCable does not keep its output or its output is
           missing, there should be no existent data associated.


        This is a helper function for clean.

        PRE
        This RunCable has an ExecLog.
        """
        # If output of the cable not marked as kept, there shouldn't be a Dataset.
        if not self.keeps_output():
            if self.has_data():
                raise ValidationError(
                    '{} "{}" does not keep its output but a dataset was registered'.format(
                        self._cable_type_str(), self)
                )

        # If EL shows missing output, there shouldn't be a Dataset.
        elif self.log.missing_outputs():
            if self.has_data():
                raise ValidationError('{} "{}" had missing output but a dataset was registered'.format(
                    self._cable_type_str(), self))

    def _clean_without_execlog_reused_check_output(self):
        """
        Check coherence of a reused RunCable with no ExecLog.

        In this state, it should not have any registered output Datasets.

        This is a helper for clean().

        PRE: this RunCable is reused, has no ExecLog, and passes clean
        up to the point that this function is invoked.
        """
        # Case 1: Completely recycled ER (reused = true): it should
        # not have any registered dataset)
        if self.outputs.exists():
            raise ValidationError('{} "{}" was reused but has a registered dataset'.format(
                self._cable_type_str(), self
            ))

    def _clean_without_execlog_not_reused(self):
        """
        Check coherence of a non-reused RunCable without an ExecLog.

        In this state, execution is incomplete, so it should not have
        any outputs or an ExecRecord.

        This is a helper for clean().

        PRE: this RunCable is not reused, has no ExecLog, and passes
        clean up to the point that this function is invoked.
        """
        general_error = '{} "{}" not reused and has no ExecLog'.format(self._cable_type_str(), self)
        if self.outputs.exists():
            raise ValidationError("{}, but has a Dataset output".format(general_error))
        if self.execrecord is not None:
            raise ValidationError("{}, but has an ExecRecord".format(general_error))

    def _clean_execrecord(self):
        """
        Check coherence of the RunCable's associated ExecRecord.

        This is an abstract function that must be overridden by
        RunSIC and RunOutputCable, as most of this is case-specific.

        If the output of the cable is kept and either the record is reused or
        it isn't reused and no missing outputs are noted:
           - the corresponding ERO should have existent data associated

           - if the PSIC/POC is not trivial and this RunCable does not reuse an ER,
             then there should be existent data associated and it should also
             be associated to the corresponding ERO.

         - it must represent a PipelineCable

         - The cable is compatible with self.execrecord.general_transf()

        PRE
        This RunCable has an ExecRecord.
        """
        self.execrecord.complete_clean()

        # If output of the cable is kept and either the record is reused or
        # it isn't reused and no missing outputs are noted,
        # the corresponding ERO should have existent data.
        if self.keeps_output():
            if self.reused or len(self.log.missing_outputs()) == 0:

                # TODO: helper to get the ExecRecordOut without calling first().
                corresp_ero = self.execrecord.execrecordouts.first()
                if not corresp_ero.has_data():
                    raise ValidationError('{} "{}" keeps its output; ExecRecordOut "{}" should reference existent '
                                          'data'.format(self._cable_type_str(), self, corresp_ero))

                # If reused == False and the cable is not trivial,
                # there should be associated data, and it should match that
                # of corresp_ero.
                if not self.reused and not self._pipeline_cable().is_trivial():
                    if not self.has_data():
                        raise ValidationError('{} "{}" was not reused, trivial, or deleted; it should have '
                                              'produced data'.format(self._cable_type_str(), self))

                    if corresp_ero.symbolicdataset.dataset != self.outputs.first():
                        raise ValidationError('Dataset "{}" was produced by {} "{}" but is not in an ERO of '
                                              'ExecRecord "{}"'.format(self.outputs.first(), self._cable_type_str(),
                                              self, self.execrecord))

        # June 9, 2014: since PSICs are now allowed to use ERs of POCs and vice versa, the functionality
        # that was previously in RunSIC and RunOutputCable._clean_execrecord can now be folded into here.
        if not self.execrecord.general_transf().is_cable:
            raise ValidationError('ExecRecord of {} "{}" does not represent a PipelineCable'.format(
                self.__class__.__name__, self))

        elif not self.component.is_compatible(self.execrecord.general_transf()):
            #raise ValidationError('PSIC of RunSIC "{}" is incompatible with that of its ExecRecord'.format(self))
            raise ValidationError('{} of {} "{}" is incompatible with the cable of its ExecRecord'.format(
                self.component.__class__.__name__, self.__class__.__name__, self))

    def clean(self):
        """
        Check coherence of this RunCable.

        In sequence, the checks we perform:
         - check coherence of start_time and end_time
         - PSIC/POC belongs to runstep.pipelinestep/run.pipeline

         - if an ExecLog is attached, clean it

         - perform relevant coherence checks based on whether the RunCable
           has decided to reuse an ExecRecord (see _clean_undecided_reused,
           _clean_reused, and _clean_not_reused)

        (from here on execrecord is assumed to be set)

         - clean the ExecRecord(see _clean_execrecord)

         - perform relevant coherence checks based on whether the RunCable
           has an ExecLog (ie. code was run) (see _clean_with_execlog
           and _clean_without_execlog)
        """
        self.logger.debug("Initiating")

        RunComponent.clean(self)
        # Check coherence of the times.
        stopwatch.models.Stopwatch.clean(self)

        self._clean_cable_coherent()

        self._clean_execlogs()

        if self.reused is None:
            self._clean_undecided_reused()
        elif self.reused:
            self._clean_reused()
        elif not self._clean_not_reused():
            return

        self.logger.debug("Checking {}'s ExecLog".format(self._cable_type_str()))

        # Handle cases where the log either exists or does not exist.
        if not self.has_log:
            if self.reused:
                self._clean_without_execlog_reused_check_output()
            else:
                self._clean_without_execlog_not_reused()
                # We know we don't have an ER at this point so we stop.
                return
        else:
            self._clean_with_execlog()

        # At this point, we know that the log either exists or should
        # not exist (i.e. this is a reused cable).

        # If there is no execrecord defined but there is a log, then
        # check for spurious CCLs and ICLs and stop.
        if self.execrecord is None:
            if self.has_log:
                self._clean_has_execlog_no_execrecord_yet()
            return

        # Now, we know there to be an ExecRecord.
        self._clean_execrecord()

    def find_compatible_ERs(self, input_SD):
        """
        Find ExecRecords which may be used by this RunCable.

        INPUTS
        input_SD        SymbolicDataset to feed the cable

        OUTPUTS
        list of ExecRecords that are compatible with this cable and input (may be empty).
        """
        return self.component.find_compatible_ERs(input_SD, self)

    @transaction.atomic
    def check_ER_usable(self, execrecord):
        """
        Check that the specified ExecRecord is reusable (fully or not) or unsuccessful.
        """
        summary = {"fully reusable": False, "successful": True}

        output_SD = execrecord.execrecordouts.first().symbolicdataset

        # Terminal case 1: the found ExecRecord has failed some checks.  In this case,
        # we just return and the RunCable fails.
        if output_SD.any_failed_checks():
            self.logger.debug("The ExecRecord ({}) found is failed.".format(execrecord))
            summary["successful"] = False

        # Terminal case 2: the ExecRecord passed its checks and
        # provides the output we need.
        elif output_SD.is_OK() and (not self.keeps_output() or output_SD.has_data()):
            self.logger.debug("Can fully reuse ER {}".format(execrecord))
            summary["fully reusable"] = True

        return summary

    @transaction.atomic
    def get_suitable_ER(self, input_SD):
        """
        Retrieve a suitable ExecRecord for this RunCable.

        If any of them are failed, we find the failed one with the most outputs, with
        ties broken by the smallest PK.
        If any of them are fully reusable, we find the fully reusable one satisfying the same criteria.
        Otherwise we find whichever one satisfies the same criteria.

        Return a tuple containing the ExecRecord along with its summary (as
        produced by check_ER_usable), or None if no appropriate ExecRecord is found.
        """
        execrecords = self.find_compatible_ERs(input_SD)
        failed = []
        fully_reusable = []
        other = []
        execrecords_sorted = sorted(execrecords, key=attrgetter("pk"))
        for er in execrecords_sorted:
            curr_summary = self.check_ER_usable(er)
            if not curr_summary["successful"]:
                failed.append((er, curr_summary))
            elif curr_summary["fully reusable"]:
                fully_reusable.append((er, curr_summary))
            else:
                other.append((er, curr_summary))

        if len(failed) > 0:
            return _first_ER_h(failed)

        if len(fully_reusable) > 0:
            return _first_ER_h(fully_reusable)

        if len(other) > 0:
            return _first_ER_h(other)

        return (None, None)


class RunSIC(RunCable):
    """
    Annotates the action of a PipelineStepInputCable within a RunStep.

    Related to :model:`archive.models.RunStep`
    Related to :model:`librarian.models.ExecRecord`
    Related to :model:`pipeline.models.PipelineStepInputCable`
    """
    runstep = models.ForeignKey(RunStep, related_name="RSICs")
    PSIC = models.ForeignKey("pipeline.PipelineStepInputCable", related_name="psic_instances")

    # Implicit from RunCable: execrecord, reused, log, output, invoked_logs, start_time, end_time.

    class Meta:
        # Uniqueness constraint ensures that no POC is multiply-represented
        # within a run step.
        unique_together = ("runstep", "PSIC")

    @classmethod
    def create(cls, PSIC, runstep):
        runsic = cls(PSIC=PSIC, runstep=runstep)
        runsic.start()
        runsic.clean()
        runsic.save()
        return runsic

    def _pipeline_cable(self):
        """
        Retrieves the PSIC of this RunSIC.
        """
        return self.PSIC

    @property
    def component(self):
        return self.PSIC

    @property
    def parent_run(self):
        return self.runstep.run

    @property
    def top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        return self.runstep.top_level_run

    @property
    def pipeline(self):
        return self.PSIC.pipelinestep.pipeline

    @property
    def parent(self):
        return self.runstep

    @property
    def is_step(self):
        return False

    @property
    def is_incable(self):
        return True

    @property
    def is_outcable(self):
        return False

    # TODO: fix for sub-pipelines
    def output_name(self):
        return "run{}_step{}_input{}".format(self.parent_run.pk, self.runstep.step_num, self.PSIC.dest.dataset_idx)

    def output_description(self):
        run = self.top_level_run
        desc = ('Generated data from a run of pipeline "{}" started at {} by {}\n'
                .format(self.pipeline, run.start_time, run.user))
        desc += "run: {}\n".format(run.pk)
        desc += "user: {}\n".format(run.user)
        desc += "step: {}\n".format(self.runstep.step_num)
        desc += "input: {}".format(self.PSIC.dest.dataset_name)
        return desc

    def keeps_output(self):
        """
        True if the underlying PSIC retains its output; False otherwise.
        """
        if self.PSIC.is_trivial():
            return False
        return self.PSIC.keep_output

    def _clean_cable_coherent(self):
        """
        Checks that the PSIC and PipelineStep are coherent.
        """
        if (not self.runstep.pipelinestep.cables_in.filter(pk=self.PSIC.pk).exists()):
            raise ValidationError('PSIC "{}" does not belong to PipelineStep "{}"'
                                  .format(self.PSIC, self.runstep.pipelinestep))

    def get_coordinates(self):
        """
        Retrieves a tuple of pipeline coordinates of this RunSIC.

        This is simply the coordinates of the RunStep it belongs to.
        Implicitly, if you are comparing a RunSIC with a RunStep that has
        the same coordinates, the RunSIC is deemed to come first.
        """
        return self.runstep.get_coordinates()


class RunOutputCable(RunCable):
    """
    Annotates the action of a PipelineOutputCable within a run.

    Related to :model:`archive.models.Run`
    Related to :model:`librarian.models.ExecRecord`
    Related to :model:`pipeline.models.PipelineOutputCable`
    """
    run = models.ForeignKey(Run, related_name="runoutputcables")
    pipelineoutputcable = models.ForeignKey("pipeline.PipelineOutputCable", related_name="poc_instances")

    # Implicit from RunCable: execrecord, reused, log, output, invoked_logs, start_time, end_time.

    class Meta:
        # Uniqueness constraint ensures that no POC is
        # multiply-represented within a run.
        unique_together = ("run", "pipelineoutputcable")

    @classmethod
    def create(cls, pipelineoutputcable, run):
        runoutputcable = cls(pipelineoutputcable=pipelineoutputcable, run=run)
        runoutputcable.start()
        runoutputcable.clean()
        runoutputcable.save()
        return runoutputcable
    
    def __str__(self):
        return 'RunOutputCable("{}")'.format(
            self.pipelineoutputcable.output_name)

    def _pipeline_cable(self):
        """
        Retrieves the POC of this RunOutputCable.
        """
        return self.pipelineoutputcable

    @property
    def component(self):
        return self.pipelineoutputcable

    @property
    def parent_run(self):
        return self.run

    @property
    def top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        return self.run.top_level_run

    @property
    def pipeline(self):
        return self.pipelineoutputcable.pipeline

    @property
    def parent(self):
        return self.run

    @property
    def is_step(self):
        return False

    @property
    def is_incable(self):
        return False

    @property
    def is_outcable(self):
        return True

    # TODO: fix for sub-pipelines
    def output_name(self):
        return "run{}_output{}".format(self.run.pk, self.pipelineoutputcable.output_idx)

    def output_description(self):
        run = self.top_level_run
        desc = ('Generated data from a run of pipeline "{}" started at {} by {}\n'
                .format(self.pipeline, run.start_time, run.user))
        desc += "run: {}\n".format(run.pk)
        desc += "user: {}\n".format(run.user)
        desc += "output: {}".format(self.pipelineoutputcable.output_name)
        return desc

    def keeps_output(self):
        """
        True if the underlying POC retains its output; False otherwise.
        """
        if self.pipelineoutputcable.is_trivial():
            return False

        if self.run.parent_runstep is None:
            return True

        # At this point we know that this is a sub-Pipeline.  Check
        # if the parent PipelineStep deletes this output.
        return not self.run.parent_runstep.pipelinestep.outputs_to_delete.filter(
                dataset_idx=self.pipelineoutputcable.output_idx).exists()

    def _clean_cable_coherent(self):
        """
        Checks that the POC and Pipeline are coherent.
        """
        if not self.run.pipeline.outcables.filter(pk=self.pipelineoutputcable.pk).exists():
            raise ValidationError('POC "{}" does not belong to Pipeline "{}"'
                                  .format(self.pipelineoutputcable, self.run.pipeline))

    def get_coordinates(self):
        """
        Retrieves a tuple of pipeline coordinates of this RunOutputCable.

        This is simply the coordinates of the Run that contains it;
        implicitly, any ROCs with these coordinates is deemed to come
        after all of the RunSteps belonging to the same Run.
        """
        return self.run.get_coordinates()


def get_upload_path(instance, filename):
    """
    Helper method for uploading dataset_files for Dataset.
    This is outside of the Dataset class, since @staticmethod and other method decorators were used instead of the
    method pointer when this method was inside Dataset class.

    :param instance:  Dataset instance
    :param filename: Dataset.dataset_file.name
    :return:  The upload directory for Dataset files.
    """
    return instance.UPLOAD_DIR + os.sep + time.strftime('%Y_%m') + os.sep + filename


@python_2_unicode_compatible
class Dataset(models.Model):
    """
    Data files uploaded by users or created by transformations.

    Related to :model:`archive.models.RunStep`
    Related to :model:`archive.models.RunOutputCable`
    Related to :model:`librarian.models.SymbolicDataset`

    The clean() function should be used when a pipeline is executed to
    confirm that the dataset structure is consistent with what's
    expected from the pipeline definition.

    Pipeline.clean() checks that the pipeline is well-defined in theory,
    while Dataset.clean() ensures the Pipeline produces what is expected.
    """
    UPLOAD_DIR = "Datasets"  # This is relative to kive.settings.MEDIA_ROOT

    # The user who created this Dataset is stored in one of
    # a) the SymbolicDataset of this Dataset (if it's uploaded)
    # b) the parent Run of created_by (if it's generated) -- note that this may not be the same
    #    user that created the parent SymbolicDataset.
    name = models.CharField(max_length=maxlengths.MAX_NAME_LENGTH, help_text="Name of this Dataset.")
    description = models.TextField(help_text="Description of this Dataset.",
                                   max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
                                   blank=True)
    date_created = models.DateTimeField(auto_now_add=True, help_text="Date of Dataset creation.")
    date_modified = models.DateTimeField(auto_now_add=True, help_text="Date of Dataset modification.")

    # Four cases from which Datasets can originate:
    #
    # Case 1: uploaded
    # Case 2: from the transformation of a RunStep
    # Case 3: from the execution of a POC (i.e. from a ROC)
    # Case 4: from the execution of a PSIC (i.e. from a RunSIC)
    created_by = models.ForeignKey(RunComponent, related_name="outputs", null=True, blank=True)

    # Datasets are stored in the "Datasets" folder
    dataset_file = models.FileField(upload_to=get_upload_path, help_text="Physical path where datasets are stored",
                                    null=False, max_length=maxlengths.MAX_FILENAME_LENGTH)

    # Datasets always have a referring SymbolicDataset
    symbolicdataset = models.OneToOneField("librarian.SymbolicDataset", related_name="dataset")

    @property
    def user(self):
        if self.created_by is None:
            return self.symbolicdataset.user
        return self.created_by.parent_run.user

    def __str__(self):
        """
        Unicode representation of this Dataset.

        This looks like "[name] (created by [user] on [date])"
        """
        return "{} (created by {} on {})".format(self.name, self.user, self.date_created)

    def header(self):
        rows = self.all_rows()
        return next(rows)

    def rows(self, data_check=False, insert_at=None):
        rows = self.all_rows(data_check, insert_at)
        next(rows)  # skip header
        for row in rows:
            yield row

    def expected_header(self):
        header = []
        if not self.symbolicdataset.is_raw():
            header = [c.column_name for c in self.symbolicdataset.compounddatatype.members.order_by("column_idx")]
        return header

    @property
    def content_matches_header(self):
        observed = self.header()
        expected = self.expected_header()
        if len(observed) != len(expected):
            return False
        return not any([o != x for (o, x) in zip(observed, expected)])

    def column_alignment(self):
        """
        This function looks at the expected and observed headers for
        a dataset, and trys to align them if they don't match


        :return: a tuple whose first element is a list of tuples
        i.e (expected header name, observed header name), and
        whose second element is a list of gaps indicating where
        to insert blank fields in a row
        """
        expt = self.expected_header()
        obs = self.header()
        i, insert = 0, []

        if self.symbolicdataset.is_raw() and not self.content_matches_header:
            return None, None

        # Do a greedy 'hard matching' over the columns
        while i < max(len(expt), len(obs)) - 1:
            ex, ob = zip(*(map(None, expt, obs)[i:]))
            u_score = float('inf')
            l_score = float('inf')

            for j, val in enumerate(ob):
                if val == ex[0]:
                    u_score = j
            for j, val in enumerate(ex):
                if val == ob[0]:
                    l_score = j
            if l_score == u_score == float('inf'):
                pass
            elif u_score < l_score and u_score != float('inf'):
                [expt.insert(i, "") for _ in xrange(u_score)]
            elif l_score <= u_score and l_score != float('inf'):
                [obs.insert(i, "") for _ in xrange(l_score)]
                insert += [i] * l_score # keep track of where to insert columns in the resulting view
            i += 1

        # it would be nice to do a similar soft matching to try to
        # match columns that are close to being the same string

        # Pad out the arrays
        diff = abs(len(expt)-len(obs))
        if len(expt) > len(obs):
            obs += [""] * diff
        else:
            expt += [""] * diff

        return zip(expt, obs), insert

    def all_rows(self, data_check=False, insert_at=None):
        """
        Returns an iterator over all rows of this dataset

        If insert_at is specified, a blank field is inserted
        at each element of insert_at.
        """
        self.dataset_file.open('rU')
        cdt = self.symbolicdataset.compounddatatype

        with self.dataset_file:
            reader = csv.reader(self.dataset_file)
            for row in reader:
                if insert_at is not None:
                    [row.insert(pos, "") for pos in insert_at]
                if data_check:
                    row = map(None, row, cdt.check_constraints(row))
                yield row

    def validate_unique(self, *args, **kwargs):
        query = Dataset.objects.filter(symbolicdataset__MD5_checksum=self.symbolicdataset.MD5_checksum,
                                       name=self.name)
        if query.exclude(pk=self.pk).exists():
            raise ValidationError("A Dataset with that name and MD5 already exists.")
        super(Dataset, self).validate_unique(*args, **kwargs)

    def get_absolute_url(self):
        """
        :return str: URL to access the dataset_file
        """
        return reverse('dataset_download', kwargs={"dataset_id": self.id})

    def get_filesize(self):
        """
        :return int: size of dataset_file in bytes
        """
        return self.dataset_file.size

    def get_formatted_filesize(self):
        if self.dataset_file.size >= 1099511627776:
            return "{0:.2f}".format(self.dataset_file.size/1099511627776.0) + ' TB'
        if self.dataset_file.size >= 1073741824:
            return "{0:.2f}".format(self.dataset_file.size/1073741824.0) + ' GB'
        elif self.dataset_file.size >= 1048576:
            return "{0:.2f}".format(self.dataset_file.size/1048576.0) + ' MB'
        elif self.dataset_file.size >= 1024:
            return "{0:.2f}".format(self.dataset_file.size/1024.0) + ' KB'
        else:
            return str(self.dataset_file.size) + ' B'

    def clean(self):
        """
        Validate this Dataset for putting into the database.

        If this Dataset has an MD5 set, verify the dataset file integrity.
        Also, make sure its permissions match those of the creating RunComponent
        if there is one.
        """
        if self.created_by is not None:
            # Whatever run created this Dataset must have had access to the parent SymbolicDataset.
            self.created_by.definite.top_level_run.validate_restrict_access([self.symbolicdataset])

        if not self.check_md5():
            raise ValidationError('File integrity of "{}" lost. Current checksum "{}" does not equal expected checksum '
                                  '"{}"'.format(self, self.compute_md5(), self.symbolicdataset.MD5_checksum))

    def compute_md5(self):
        """Computes the MD5 checksum of the Dataset."""
        md5 = None
        try:
            self.dataset_file.open()
            md5 = file_access_utils.compute_md5(self.dataset_file.file)
        finally:
            self.dataset_file.close()

        return md5

    def check_md5(self):
        """
        Checks the MD5 checksum of the Dataset against its stored value.

        The stored value is in the Dataset's associated
        SymbolicDataset.  This will be used when regenerating data
        that once existed, as a coherence check.

        If there is no SymbolicDataset, then fails the check (returns False).
        """
        # Recompute the MD5, see if it equals what is already stored
        return self.symbolicdataset.MD5_checksum == self.compute_md5()


class ExecLog(stopwatch.models.Stopwatch):
    """
    Logs of Method/PSIC/POC execution.
    Records the start/end times of execution.
    Records *attempts* to run a computation, whether or not it succeeded.

    ExecLogs for methods will also link to a MethodOutput.
    """
    record = models.OneToOneField(RunComponent, related_name="log")
    invoking_record = models.ForeignKey(RunComponent, related_name="invoked_logs")

    # Since this inherits from Stopwatch, it has start_time and end_time.

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    @transaction.atomic
    def create(cls, record, invoking_record):
        execlog = cls(record=record, invoking_record=invoking_record)
        execlog.clean()
        execlog.save()
        if isinstance(record, RunStep) and not record.has_subrun():
            method_output = MethodOutput(execlog=execlog)
            method_output.clean()
            method_output.save()
        return execlog

    def clean(self):
        """
        Checks coherence of this ExecLog.

        If this ExecLog is for a RunStep, the RunStep represents a
        Method (as opposed to a Pipeline).  Moreover, the invoking
        record must not be earlier than the record it belongs to.
        Also, the end time must exceed the start time if both are set.
        """
        # First make sure the start- and end-times are coherent.
        stopwatch.models.Stopwatch.clean(self)

        if self.record.is_step and self.record.definite.pipelinestep.transformation.is_pipeline:
            raise ValidationError(
                'ExecLog "{}" does not correspond to a Method or cable'.
                format(self))

        if self.record.top_level_run != self.invoking_record.top_level_run:
            raise ValidationError(
                'ExecLog "{}" belongs to a different Run than its invoking RunStep/RSIC/ROC'.
                format(self)
            )

        # Check that invoking_record is not earlier than record.
        record_coords = self.record.definite.get_coordinates()
        invoking_record_coords = self.invoking_record.definite.get_coordinates()

        # We have to respect the hierarchy that in case of ties, RSIC is earlier than RunStep,
        # and both are earlier than ROC.
        tied = True
        for i in range(min(len(record_coords), len(invoking_record_coords))):
            if record_coords[i] > invoking_record_coords[i]:
                raise ValidationError(
                    'ExecLog "{}" is invoked earlier than the RunStep/RSIC/ROC it belongs to'.format(self)
                )
            elif record_coords[i] < invoking_record_coords[i]:
                tied = False
                break

        # In the case of a tie, we use the precedence that RunSICs are
        # earlier than RunSteps, and both are earlier than RunOutputCables.
        # RunSICs and RunOutputCables that are at the same level are OK.
        if tied:
            if self.record.is_outcable and not self.invoking_record.is_outcable:
                raise ValidationError('ExecLog "{}" is invoked earlier than the ROC it belongs to'.format(self))
            elif self.record.is_step and self.invoking_record.is_incable:
                raise ValidationError('ExecLog "{}" is invoked earlier than the RunStep it belongs to'.format(self))

    def is_complete(self):
        """
        Checks completeness of this ExecLog.

        The execution must have ended (i.e. end_time is
        set) and a MethodOutput must be in place if appropriate.
        """
        if not self.has_ended():
            return False

        if self.record.is_step and self.record.runstep.pipelinestep.transformation.is_method:
            if not hasattr(self, "methodoutput") or self.methodoutput is None:
                return False

        return True

    def complete_clean(self):
        """
        Checks completeness and coherence of this ExecLog.

        First, run clean; then, if this ExecLog is for a RunStep,
        check for the existence of a MethodOutput.
        """
        self.clean()

        if not self.is_complete():
            raise ValidationError('ExecLog "{}" is not complete'.format(self))

    def missing_outputs(self):
        """Returns output SDs missing output from this execution."""
        missing = []
        for ccl in self.content_checks.all():
            try:
                if ccl.baddata.missing_output:
                    missing.append(ccl.symbolicdataset)
            except ObjectDoesNotExist:
                pass

        self.logger.debug("returning missing outputs '{}'".format(missing))
        return missing

    def is_successful(self):
        """
        True if this execution is successful (so far); False otherwise.

        Note that the execution may still be in progress when we call this;
        this function tells us if anything has gone wrong so far.
        """
        # If this ExecLog has a MethodOutput, check its return code.
        try:
            if self.methodoutput.return_code is not None and self.methodoutput.return_code != 0:
                return False
        except ObjectDoesNotExist:
            pass

        # Having reached here, we are comfortable with the execution --
        # note that it may still be in progress!
        return True

    # FIXME: this isn't broken but it seems redundant and could just be folded directly
    # into all_checks_passed.  Do we ever find a use for this?
    def all_checks_performed(self):
        """
        True if every output of this ExecLog has been checked.

        If the parent record does not have an ExecRecord yet, return
        False; otherwise, use the ExecRecord to look up all of the SDs
        output by this execution, and check that all of the outputs
        have been tested appropriately.  That is, if the SD is
        originally created by this ExecLog (i.e. by its corresponding
        Run*) and is not raw, look for the CCL to appear in the list
        of the ExecLog's CCLs; if the SD was originally created
        before, check for this ExecLog to have a corresponding ICL.
        """
        if self.record.execrecord is None:
            return False

        # From here on, we know that this ExecLog corresponds to the
        # creation or filling-in of an ExecRecord.  Go through the
        # EROs and check that all of the corresponding ICL/CCLs are
        # present and passed.

        # FIXME REMOVE REDUNDANT??
        # # Get the SDs that were actually created during this EL's Run*.
        # record_outs = None
        # if type(self.record) == RunStep:
        #     record_outs = self.record.outputs.all()
        # else:
        #     record_outs = self.record.output.all()

        # Is this log the generator of the execrecord?  That is, is this
        # the very first time this execution was ever performed, and this
        # isn't either a "filling-in" or a recovery?
        if self.record.execrecord.generator == self:

            for ero in self.record.execrecord.execrecordouts.all():

                # If this was a trivial cable, then this didn't create the SD,
                # so just look for an ICL.  Otherwise, if the SD isn't raw, look
                # for a CCL.
                record_is_trivial_cable = False
                if self.record.is_cable and self.record.component.is_trivial():
                    record_is_trivial_cable = True

                if record_is_trivial_cable:
                    corresp_icls = self.integrity_checks.filter(symbolicdataset=ero.symbolicdataset)
                    if not corresp_icls.exists():
                        return False

                elif not ero.symbolicdataset.is_raw():
                    corresp_ccls = self.content_checks.filter(symbolicdataset=ero.symbolicdataset)
                    if not corresp_ccls.exists():
                        return False

        else:
            # This is either a filling-in or a recovery, so just look for ICLs.
            for ero in self.record.execrecord.execrecordouts.all():
                corresp_icls = self.integrity_checks.filter(symbolicdataset=ero.symbolicdataset)
                if not corresp_icls.exists():
                    return False

        # Now we've checked all of the outputs and they've all been
        # as expected, so....
        return True

    def all_checks_passed(self):
        """
        True if every output of this ExecLog has passed its check.

        First check that all checks have been performed; then check
        that all of the tests have passed.
        """
        if not self.all_checks_performed():
            return False

        # From here on, we know that this ExecLog corresponds to the
        # creation or filling-in of an ExecRecord.  Go through the
        # EROs and check that all of the corresponding ICL/CCLs are
        # present and passed.
        for ero in self.record.execrecord.execrecordouts.all():
            corresp_icls = self.integrity_checks.filter(symbolicdataset=ero.symbolicdataset)
            if corresp_icls.exists():
                if corresp_icls.first().is_fail():
                    return False

            corresp_ccls = self.content_checks.filter(symbolicdataset=ero.symbolicdataset)
            if corresp_ccls.exists():
                if corresp_ccls.first().is_fail():
                    return False

        return True


class MethodOutput(models.Model):
    """
    Logged output of the execution of a method.

    This stores the stdout and stderr output, as well as the process'
    return code.

    If the return code is -1, it indicates that an operating system level error
    was raised while trying to execute the code, ie., the code was not executable.
    In that case, stdout will be empty, and stderr will contain the Python stack
    trace produced when we tried to run the code with Popen.

    If the return code is None, it indicates that the code execution is
    in progress.
    """
    execlog = models.OneToOneField(ExecLog, related_name="methodoutput")
    return_code = models.IntegerField("return code", null=True)
    output_log = models.FileField("output log", upload_to="Logs",
                                  help_text="Terminal output of the RunStep Method, i.e. stdout.")
    error_log = models.FileField("error log", upload_to="Logs",
                                 help_text="Terminal error output of the RunStep Method, i.e. stderr.")

    def get_absolute_log_url(self):
        """
        :return str: URL to access the output log
        """
        return reverse('stdout_download', kwargs={"methodoutput_id": self.id})

    def get_absolute_error_url(self):
        """
        :return str: URL to access the output log
        """
        return reverse('stderr_download', kwargs={"methodoutput_id": self.id})

    @classmethod
    def create(cls, execlog):
        methodoutput = cls(execlog=execlog)
        methodoutput.clean()
        methodoutput.save()
        return methodoutput


# Register signals.
post_delete.connect(archive.signals.dataset_post_delete, sender=Dataset)
