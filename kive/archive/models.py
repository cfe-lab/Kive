"""
archive.models

Shipyard data models relating to archiving information: Run, RunStep,
Dataset, etc.
"""
from __future__ import unicode_literals

import itertools
import logging
from operator import attrgetter, itemgetter
import os
import time
from datetime import datetime, timedelta
import shutil

from django.db import models, transaction
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.core.urlresolvers import reverse
from django.core.validators import MinValueValidator
from django.conf import settings
from django.utils.encoding import python_2_unicode_compatible
from django.utils import timezone
from django.contrib.auth.models import User, Group

import archive.exceptions
from constants import maxlengths, groups, runstates, runcomponentstates
from datachecking.models import ContentCheckLog, IntegrityCheckLog
from librarian.models import Dataset, ExecRecord
import librarian.filewalker as filewalker
import metadata.models
import stopwatch.models
from pipeline.models import Pipeline
from method.models import Method


def empty_redaction_plan():
    return {
        "Datasets": set(),
        "ExecRecords": set(),
        "OutputLogs": set(),
        "ErrorLogs": set(),
        "ReturnCodes": set(),
        "ExternalFiles": set()
    }


def summarize_redaction_plan(redaction_plan):
    counts = {key: len(targets) for key, targets in redaction_plan.iteritems()}
    return counts


@transaction.atomic
def redact_helper(redaction_plan):
    # Check if anything that's currently running will be affected.
    if metadata.models.any_runs_in_progress(redaction_plan):
        raise archive.exceptions.RunNotFinished("Cannot redact: an affected run is still in progress")

    # Proceed in a fixed order.
    if "Datasets" in redaction_plan:
        for sd in redaction_plan["Datasets"]:
            sd.redact_this()

    if "ExecRecords" in redaction_plan:
        for er in redaction_plan["ExecRecords"]:
            # This marks all RunComponents using the ExecRecord as redacted.
            er.redact_this()

    if "OutputLogs" in redaction_plan:
        for log in redaction_plan["OutputLogs"]:
            log.methodoutput.redact_output_log()

    if "ErrorLogs" in redaction_plan:
        for log in redaction_plan["ErrorLogs"]:
            log.methodoutput.redact_error_log()

    if "ReturnCodes" in redaction_plan:
        for log in redaction_plan["ReturnCodes"]:
            log.methodoutput.redact_return_code()


class update_field(object):

    def __init__(self, field):
        self.field = field

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            this = args[0]
            result = func(*args, **kwargs)
            original_flag = getattr(this, self.field)

            if hasattr(this, self.field) and original_flag != result:
                if 'dont_save' in kwargs and kwargs['dont_save']:
                    return result

                setattr(this, self.field, result)

                if this.pk is not None:
                    try:
                        this.save(update_fields=[self.field])
                    except:
                        pass
            return result
        return wrapper


class RunState(models.Model):
    name = models.CharField(max_length=maxlengths.MAX_NAME_LENGTH)
    description = models.TextField()

    def __unicode__(self):
        return self.name


class RunComponentState(models.Model):
    name = models.CharField(max_length=maxlengths.MAX_NAME_LENGTH)
    description = models.TextField()

    def __unicode__(self):
        return self.name


@python_2_unicode_compatible
class RunBatch(metadata.models.AccessControl):

    name = models.CharField(
        "Name of this batch of runs",
        max_length=maxlengths.MAX_NAME_LENGTH,
        null=False,
        blank=True
    )
    description = models.TextField(
        "Batch description",
        max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
        null=False,
        blank=True
    )

    def __str__(self):
        return "{} (pk={})".format(self.name, self.pk)

    def all_runs_complete(self):
        """
        Checks whether all Runs in this batch are complete.
        """
        return not self.runs.exclude(_runstate_id__in=runstates.COMPLETE_STATE_PKS).exists()

    def eligible_permissions(self):
        """
        Determine which users and groups may be granted access to this RunBatch.

        The run's permissions can only be extended to those users and groups
        that have access to
        a) the Pipeline
        b) the input Datasets
        c) the top-level Runs of the ExecRecords it reuses
        d) the RunBatch
        """
        if not self.all_runs_complete():
            raise RuntimeError("Eligible permissions cannot be found until all runs are complete")

        # We don't use querysets here with many filter statements as it leads to very very long
        # query strings that can be problematic in deployment.
        addable_users = set(User.objects.all())
        addable_groups = set(Group.objects.all())
        for run in self.runs.all():
            run_addable_users, run_addable_groups = run.eligible_permissions(include_runbatch=False)
            addable_users = addable_users.intersection(set(run_addable_users))
            addable_groups = addable_groups.intersection(set(run_addable_groups))

        addable_users_qs = User.objects.filter(pk__in=[x.pk for x in addable_users])
        addable_groups_qs = Group.objects.filter(pk__in=[x.pk for x in addable_groups])
        return addable_users_qs, addable_groups_qs


@python_2_unicode_compatible
class Run(stopwatch.models.Stopwatch, metadata.models.AccessControl):
    """
    Stores data associated with an execution of a pipeline.

    When a Pipeline is queued up for execution, a Run is created and time_queued is
    set.  When the Run actually starts, the start_time attribute (from Stopwatch)
    gets set (i.e. we call .start()).

    Related to :model:`pipeline.models.Pipeline`
    Related to :model:`archive.models.RunStep`
    Related to :model:`archive.models.Dataset`
    """
    # Details required by the fleet for execution.  These will be meaningless if
    # this is not a top-level run.
    sandbox_path = models.CharField(max_length=256, default="", blank=True, null=False)
    time_queued = models.DateTimeField(default=timezone.now, null=True)
    purged = models.BooleanField(default=False)
    stopped_by = models.ForeignKey(User, help_text="User that stopped this Run", null=True, blank=True,
                                   related_name="runs_stopped")
    paused_by = models.ForeignKey(User, help_text="User that paused this Run", null=True, blank=True,
                                  related_name="runs_paused")

    pipeline = models.ForeignKey("pipeline.Pipeline", related_name="pipeline_instances",
                                 help_text="Pipeline used in this run")

    name = models.CharField("Run name", max_length=maxlengths.MAX_NAME_LENGTH, null=False, blank=True)
    description = models.TextField("Run description", max_length=maxlengths.MAX_DESCRIPTION_LENGTH, null=False,
                                   blank=True)

    # If run was spawned within another run, parent_runstep denotes
    # the run step that initiated it
    parent_runstep = models.OneToOneField("RunStep",
                                          related_name="child_run",
                                          null=True,
                                          blank=True,
                                          help_text="Step of parent run initiating this one as a sub-run")

    # State field to avoid the use of is_complete() and is_successful(), which can be slow.
    _runstate = models.ForeignKey(RunState, default=runstates.PENDING_PK, related_name="runs")

    runbatch = models.ForeignKey(
        RunBatch,
        help_text="Run batch that this Run is a part of",
        null=True,
        blank=True,
        related_name="runs",
        on_delete=models.SET_NULL
    )

    # Priority of this Run.  The priority levels are defined in settings, with defaults:
    # 0: BaseSlurmScheduler.PRIO_LOW
    # 1: BaseSlurmScheduler.PRIO_MEDIUM
    # 2: BaseSlurmScheduler.PRIO_HIGH
    # This integer is the index of the priority level to use, so 0, the default,
    # is the lowest priority level.
    priority = models.IntegerField(
        help_text="Priority of this Run (priority levels are defined in settings)",
        default=0
    )

    # Implicitly, this also has start_time and end_time through inheritance.

    def is_stopped(self):
        return self.stopped_by is not None

    def is_paused(self):
        return self.paused_by is not None

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
        Run.validate_permissions(self)
        # # Access to this Run must not exceed that of the pipeline or of the batch.
        # self.validate_restrict_access([self.pipeline])
        # if self.runbatch is not None:
        #     self.validate_restrict_access([self.runbatch])
        #
        # for rtp_input in self.inputs.all():
        #     rtp_input.clean()

        if hasattr(self, "not_enough_CPUs"):
            self.not_enough_CPUs.clean()

        # # If this is not a top-level run it must have the same access as the top-level run.
        # my_top_level_run = self.top_level_run
        # if self != my_top_level_run:
        #     self.validate_identical_access(my_top_level_run)

        # Check that start- and end-time are coherent.
        stopwatch.models.Stopwatch.clean(self)

        if self.is_subrun() and self.pipeline != self.parent_runstep.pipelinestep.transformation.definite:
            raise ValidationError('Pipeline of Run "{}" is not consistent with its parent RunStep'.format(self))

        # Go through whatever steps are registered. All must be clean.
        for i, runstep in enumerate(self.runsteps.order_by("pipelinestep__step_num"), start=1):
            if runstep.pipelinestep.step_num != i:
                raise ValidationError('RunSteps of Run "{}" are not consecutively numbered starting from 1'
                                      .format(self))
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

    @staticmethod
    def validate_permissions(run):
        """
        Check that the permissions set on this run are coherent.
        """
        # Access to this Run must not exceed that of the pipeline or of the batch.
        run.validate_restrict_access([run.pipeline])
        if run.runbatch is not None:
            run.validate_restrict_access([run.runbatch])

        # If this is not a top-level run it must have the same access as the top-level run.
        my_top_level_run = run.top_level_run
        if run != my_top_level_run:
            run.validate_identical_access(my_top_level_run)

    def is_complete(self):
        """
        True if this run is ended; False otherwise.

        By "ended" we mean Successful, Cancelled, Failed, or Quarantined.
        """
        return self._runstate_id in runstates.COMPLETE_STATE_PKS

    def is_pending(self):
        return self._runstate_id == runstates.PENDING_PK

    def is_running(self):
        return self._runstate_id == runstates.RUNNING_PK

    def is_successful(self):
        """
        Checks if this Run is successful.
        """
        return self._runstate_id == runstates.SUCCESSFUL_PK

    def is_failed(self):
        """
        Checks if this Run is failed.
        """
        return self._runstate_id == runstates.FAILED_PK

    def is_quarantined(self):
        """
        Checks if this Run is quarantined.
        """
        return self._runstate_id == runstates.QUARANTINED_PK

    def is_failing(self):
        return self._runstate_id == runstates.FAILING_PK

    def is_cancelling(self):
        return self._runstate_id == runstates.CANCELLING_PK

    def is_cancelled(self):
        return self._runstate_id == runstates.CANCELLED_PK

    def get_state_name(self):
        return self._runstate.name

    def all_inputs_have_data(self):
        """Return True if all datasets pass the has_data() test.
        Do this just before we start a run, as an external file
        might be missing or have been corrupted.
        """
        for _input in self.inputs.all():
            if not _input.dataset.has_data():
                return False
        return True

    @transaction.atomic
    def start(self, save=True, **kwargs):
        """
        Start this run, changing its state from Pending to Running.
        """
        assert self._runstate_id == runstates.PENDING_PK
        self._runstate_id = runstates.RUNNING_PK
        stopwatch.models.Stopwatch.start(self, save=save, **kwargs)

    @transaction.atomic
    def stop(self, save=True, **kwargs):
        """
        Stop this run, changing its state appropriately.
        """
        active_state_pks = [
            runstates.RUNNING_PK,
            runstates.CANCELLING_PK,
            runstates.FAILING_PK
        ]
        assert self._runstate_id in active_state_pks, self.get_state_name()
        if self._runstate_id == runstates.RUNNING_PK:
            # Check that there are no quarantined components.  We don't need to
            # recurse down into sub-run, because the cases where a sub-component gets
            # quarantined are:
            # - while the sub-Run is still in progress;
            #   then it will be quarantined/failed/cancelled on its call to stop().
            # - when the sub-Run is finished and successful;
            #   then it will have already been quarantined.
            # - when the sub-Run is failed/cancelled;
            #   then any ancestor runs (including this one) should already be
            #   Cancelling or Failing.
            if (self.runsteps.filter(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK).exists() or
                    self.runoutputcables.filter(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK).exists()):
                self._runstate = RunState.objects.get(pk=runstates.QUARANTINED_PK)
            else:
                self._runstate = RunState.objects.get(pk=runstates.SUCCESSFUL_PK)
        elif self._runstate_id == runstates.CANCELLING_PK:
            self._runstate = RunState.objects.get(pk=runstates.CANCELLED_PK)
        else:
            self._runstate = RunState.objects.get(pk=runstates.FAILED_PK)
        stopwatch.models.Stopwatch.stop(self, save=save, **kwargs)

    @transaction.atomic
    def cancel(self, save=True):
        """
        Mark this run as Cancelling.

        This does not affect the RunComponents.
        """
        assert self._runstate_id in [runstates.PENDING_PK,
                                     runstates.RUNNING_PK]
        self._runstate = RunState.objects.get(pk=runstates.CANCELLING_PK)
        if save:
            self.save()

    @transaction.atomic
    def mark_failure(self, save=True, recurse_upward=False):
        """
        Mark this run as Failing.

        This does not affect the RunComponents, but it optionally affects
        the parent Run, if this is a sub-Run.
        """
        assert self._runstate_id == runstates.RUNNING_PK
        self._runstate = RunState.objects.get(pk=runstates.FAILING_PK)
        if save:
            self.save()

        if recurse_upward and self.parent_runstep:
            self.parent_runstep.run.mark_failure(save=save, recurse_upward=True)

    @transaction.atomic
    def begin_recovery(self, save=True, recurse_upward=False):
        """
        Transition this run from Successful to Running on recovery of one of its components.

        This does not affect the RunComponents, but it optionally affects
        the parent Run, if this is a sub-Run.
        """
        assert self._runstate_id == runstates.SUCCESSFUL_PK
        assert self.has_ended()
        self._runstate = RunState.objects.get(pk=runstates.RUNNING_PK)
        if save:
            self.save()

        if recurse_upward and self.parent_runstep:
            self.parent_runstep.run.begin_recovery(save=save, recurse_upward=True)

    @transaction.atomic
    def finish_recovery(self, save=True, recurse_upward=False):
        """
        Transition this run's state when its recovering components are done.
        """
        assert self._runstate_id in [runstates.RUNNING_PK, runstates.FAILING_PK, runstates.CANCELLING_PK]
        assert self.has_ended()
        if self.is_running():
            self._runstate = RunState.objects.get(pk=runstates.SUCCESSFUL_PK)
        elif self.is_failing():
            self._runstate = RunState.objects.get(pk=runstates.FAILED_PK)
        else:
            self._runstate = RunState.objects.get(pk=runstates.CANCELLED_PK)

        if save:
            self.save()
        if recurse_upward and self.parent_runstep:
            self.parent_runstep.run.finish_recovery(save=save, recurse_upward=True)

    @transaction.atomic
    def quarantine(self, save=True, recurse_upward=False):
        """
        Transition this Run to a quarantined state.
        """
        assert self.is_successful()
        self._runstate = RunState.objects.get(pk=runstates.QUARANTINED_PK)
        if save:
            self.save()

        # Quarantine all ancestor runs.
        if recurse_upward and self.parent_runstep is not None and self.parent_runstep.run.is_successful():
            self.parent_runstep.run.quarantine(save=save, recurse_upward=True)

    @transaction.atomic
    def attempt_decontamination(self, save=True, recurse_upward=False):
        """
        Mark this quarantined Run as fixed.

        Optionally, attempt to decontaminate ancestor runs that are quarantined.
        """
        assert self.is_quarantined()

        # Look for components that are quarantined.
        for rs in self.runsteps.filter(_runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK):
            if rs.RSICs.filter(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK).exists():
                return
        if self.runsteps.filter(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK).exists():
            return
        elif self.runoutputcables.filter(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK).exists():
            return

        self._runstate = RunState.objects.get(pk=runstates.SUCCESSFUL_PK)
        if save:
            self.save()

        # Quarantine all ancestor runs.
        if recurse_upward and self.parent_runstep and self.parent_runstep.run.is_quarantined():
            self.parent_runstep.run.attempt_decontamination(save=save, recurse_upward=True)

    @classmethod
    def find_unstarted(cls):
        return cls.objects.filter(_runstate__pk=runstates.PENDING_PK)

    @property
    def display_name(self):
        """
        Produces a human-readable name for the Run.

        If the name field is not blank, use that; otherwise, give a string
        combining the Pipeline name and the first input name.
        """
        if self.name != "":
            return self.name

        try:
            pipeline_name = self.pipeline.family.name
        except Pipeline.DoesNotExist:
            pipeline_name = "Run"

        first_input = None
        for inp in self.inputs.all():
            first_input = inp
            break
        if not (first_input and first_input.dataset.has_data()):
            if self.time_queued:
                return "{} at {}".format(pipeline_name, self.time_queued)
            return pipeline_name
        first_input_name = first_input.dataset.name
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

        result = {'name': self.display_name,
                  'start': self._format_time(self.start_time),
                  'end': self._format_time(self.end_time)}
        if hasattr(self, "not_enough_CPUs"):
            esc = self.not_enough_CPUs
            result['status'] = "Too many threads ({} from {})".format(
                esc.threads_requested,
                esc.max_available
            )
            return result

        if hasattr(self, 'user'):
            result['user'] = self.user.username

        if self.is_pending():
            result['status'] = '?'
            return result

        status = ""
        step_progress = []
        cable_progress = {}
        input_list = {}

        for _input in self.inputs.all():
            if _input.dataset.has_data():
                input_list[_input.index] = {"dataset_id": _input.dataset.id,
                                            "dataset_name": _input.dataset.name,
                                            "md5": _input.dataset.MD5_checksum}

        # One of the steps is in progress?
        total_steps = self.pipeline.steps.count()
        runsteps = sorted(self.runsteps.all(),
                          key=lambda runstep: runstep.pipelinestep.step_num)

        for step in runsteps:
            if step.is_pending():
                log_char = "."
                step_status = "WAITING"

            elif step.is_running():
                try:
                    step.log.id
                    log_char = "+"
                    step_status = "RUNNING"
                except ExecLog.DoesNotExist:
                    log_char = ":"
                    step_status = "READY"

            elif step.is_cancelled():
                log_char = "x"
                step_status = "CANCELLED"

            elif step.is_failed():
                log_char = "!"
                step_status = "FAILURE"

            elif step.is_quarantined():
                log_char = "#"
                step_status = "QUARANTINED"

            else:
                log_char = "*"
                step_status = "CLEAR"

            status += log_char
            if detailed:
                step_progress.append({'status': step_status,
                                      'name': str(step.pipelinestep),
                                      'log_id': None})
                try:
                    step_progress[-1]['log_id'] = step.execrecord.generator.\
                        methodoutput.id
                except:
                    pass

        # Just finished a step, but didn't start the next one?
        status += "." * (total_steps - len(runsteps))
        status += "-"

        # Which outcables are in progress?
        cables = sorted(self.pipeline.outcables.all(),
                        key=attrgetter('output_idx'))
        run_output_cables = {c.pipelineoutputcable: c
                             for c in self.runoutputcables.all()}
        for pipeline_cable in cables:
            curr_roc = run_output_cables.get(pipeline_cable)
            if curr_roc is None:
                log_char = "."
                step_status = "WAITING"
            else:
                if curr_roc.is_pending():
                    log_char = "."
                    step_status = "WAITING"
                elif curr_roc.is_running():
                    try:
                        curr_roc.log.id
                        log_char = "+"
                        step_status = "RUNNING"
                    except ExecLog.DoesNotExist:
                        log_char = ":"
                        step_status = "READY"

                elif curr_roc.is_cancelled():
                    log_char = "x"
                    step_status = "CANCELLED"

                elif curr_roc.is_failed():
                    log_char = "!"
                    step_status = "FAILED"

                elif curr_roc.is_quarantined():
                    log_char = "#"
                    step_status = "QUARANTINED"

                else:
                    log_char = "*"
                    step_status = "CLEAR"

            # Log the status
            status += log_char
            if detailed:
                cable_progress[pipeline_cable.id] = {'status': step_status}

        if detailed:
            result['step_progress'] = step_progress
            result['output_progress'] = cable_progress
            result['inputs'] = input_list

        result['status'] = status
        result['id'] = self.pk

        return result

    @staticmethod
    def _format_time(t):
        return t and timezone.localtime(t).strftime('%d %b %Y %H:%M')

    def collect_garbage(self):
        """
        Dispose of the sandbox used by the Run.
        """
        if self.sandbox_path == "":
            raise archive.exceptions.SandboxActiveException(
                "Run (pk={}, Pipeline={}, queued {}, User={}) has no sandbox path".format(
                    self.pk, self.pipeline, self.time_queued, self.user)
                )
        elif not self.is_complete():
            raise archive.exceptions.SandboxActiveException(
                "Run (pk={}, Pipeline={}, queued {}, User={}) is not finished".format(
                    self.pk, self.pipeline, self.time_queued, self.user)
                )

        # This may raise OSError; the caller should catch it.
        shutil.rmtree(self.sandbox_path)
        self.purged = True
        self.save()

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
        if self.parent_runstep is None:
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
                        check = runstep.log.content_checks.get(dataset=output.dataset)
                    except ContentCheckLog.DoesNotExist:
                        try:
                            check = runstep.log.integrity_checks.get(dataset=output.dataset)
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

    def remove(self):
        """Remove this Run cleanly."""
        removal_plan = self.build_removal_plan()
        metadata.models.remove_helper(removal_plan)

    def build_removal_plan(self, removal_accumulator=None):
        """
        Create a manifest of objects removed when this Run is removed.
        """
        removal_plan = removal_accumulator or metadata.models.empty_removal_plan()
        assert self not in removal_plan["Runs"]
        removal_plan["Runs"].add(self)

        for runcomponent in itertools.chain(self.runsteps.all(), self.runoutputcables.all()):
            metadata.models.update_removal_plan(removal_plan, runcomponent.build_removal_plan_h(removal_plan))

        return removal_plan

    def increase_permissions_from_json(self, permissions_json):
        """
        Grant permission to all users and groups specified in the parameter.

        The permissions_json parameter should be a JSON string formatted as it would
        be by the permissions widget used in the UI.
        """
        self.grant_from_json(permissions_json)

        for runstep in self.runsteps.all():
            for rsic in runstep.RSICs.all():
                for ds in rsic.outputs.all():
                    ds.increase_permissions_from_json(permissions_json)

            if runstep.has_subrun():
                runstep.child_run.increase_permissions_from_json(permissions_json)

            for ds in runstep.outputs.all():
                ds.increase_permissions_from_json(permissions_json)

        for roc in self.runoutputcables.all():
            for ds in roc.outputs.all():
                ds.increase_permissions_from_json(permissions_json)

    def get_all_atomic_runcomponents(self):
        """
        Returns an iterable of all atomic RunComponents that belong to this Run.

        This includes RunComponents that belong to sub-Runs.
        """
        # This will be a list of querysets that we will use itertools.chain to join.
        rc_querysets = []

        atomic_step_pks = []
        for rs in self.runsteps.all():
            rc_querysets.append(rs.RSICs.all())
            if rs.has_subrun():
                sub_rcs = rs.child_run.get_all_atomic_runcomponents()
                rc_querysets.append(sub_rcs)
            else:
                atomic_step_pks.append(rs.pk)
        rc_querysets.append(RunStep.objects.filter(pk__in=atomic_step_pks))
        rc_querysets.append(self.runoutputcables.all())

        return itertools.chain(*rc_querysets)

    def eligible_permissions(self, include_runbatch=True):
        """
        Determine which users and groups may be granted access to this Run.

        The run's permissions can only be extended to those users and groups
        that have access to
        a) the Pipeline
        b) the input Datasets
        c) the top-level Runs of the ExecRecords it reuses
        d) the RunBatch
        """
        if not self.is_complete():
            raise RuntimeError("Eligible permissions cannot be found until the run is complete")

        # Start with the users/groups who don't have access to this Run...
        addable_users, addable_groups = self.other_users_groups()

        # ... and then refine it.
        addable_users, addable_groups = self.pipeline.intersect_permissions(addable_users, addable_groups)
        if include_runbatch and self.runbatch is not None:
            addable_users, addable_groups = self.runbatch.intersect_permissions(addable_users, addable_groups)
            self.validate_restrict_access([self.runbatch])

        for run_input in self.inputs.all():
            # SCO
            ds = run_input.dataset
            # self.validate_restrict_access(ds)
            # self.validate_restrict_access([self.pipeline])
            # if self.runbatch is not None:
            #     self.validate_restrict_access([self.runbatch])
            #
            
            addable_users, addable_groups = ds.intersect_permissions(
                addable_users,
                addable_groups
            )

        # Look for permissions on reused RunComponents.
        for rc in self.get_all_atomic_runcomponents():
            if rc.reused:
                orig_run = rc.execrecord.generating_run
                if orig_run != self:
                    addable_users, addable_groups = orig_run.intersect_permissions(
                        addable_users,
                        addable_groups
                    )

        if addable_groups.filter(pk=groups.EVERYONE_PK).exists():
            addable_users = User.objects.all()
            addable_groups = Group.objects.all()

        return addable_users, addable_groups

    def cancel_components(self, except_steps=None, except_incables=None, except_outcables=None):
        """
        Cancel all components (and sub-Runs) except for those specified.

        Returns True if all components are complete; False otherwise.

        PRE: anything that the fleet is currently running is specified among
        the exceptions.
        """
        except_steps = except_steps or []
        except_incables = except_incables or []
        except_outcables = except_outcables or []

        skip_step_pks = (x.pk for x in except_steps if x.parent_run == self)
        skip_incable_pks = (x.pk for x in except_incables if x.parent_run == self)
        skip_outcable_pks = (x.pk for x in except_outcables if x.parent_run == self)

        # In addition to the steps and outcables, we also need to check that all
        # incables are complete.
        incables_still_running = False
        for step in self.runsteps.exclude(pk__in=skip_step_pks).exclude(
                _runcomponentstate__pk__in=runcomponentstates.COMPLETE_STATE_PKS
        ):
            for rsic in step.RSICs.exclude(pk__in=skip_incable_pks).exclude(
                    _runcomponentstate__pk__in=runcomponentstates.COMPLETE_STATE_PKS
            ):
                rsic.cancel(save=True)

            if not step.has_subrun():
                step.cancel(save=True)

            else:
                if step.child_run.is_pending():
                    step.child_run.start()  # transition: Pending->Running

                if step.child_run.is_running():
                    step.child_run.cancel()  # transition: Running->Cancelling

                all_stopped = step.child_run.cancel_components(except_steps=except_steps,
                                                               except_incables=except_incables,
                                                               except_outcables=except_outcables)
                if all_stopped:
                    step.child_run.stop(save=True)  # transition: Cancelling->Cancelled or Failing->Failed
                    step.cancel(save=True)

        for outcable in self.runoutputcables.exclude(pk__in=skip_outcable_pks).exclude(
                _runcomponentstate__pk__in=runcomponentstates.COMPLETE_STATE_PKS
        ):
            outcable.cancel(save=True)

        if RunSIC.objects.filter(dest_runstep__in=self.runsteps.all()).exclude(
                _runcomponentstate__pk__in=runcomponentstates.COMPLETE_STATE_PKS
        ):
            incables_still_running = True

        # Return True if everything is complete and False otherwise.
        return not (
            incables_still_running or
            self.runsteps.exclude(
                _runcomponentstate__pk__in=runcomponentstates.COMPLETE_STATE_PKS
            ).exists() or
            self.runoutputcables.exclude(
                _runcomponentstate__pk__in=runcomponentstates.COMPLETE_STATE_PKS
            ).exists()
        )


class RunInput(models.Model):
    """
    Represents an input to a run.
    """
    run = models.ForeignKey(Run, related_name="inputs")
    dataset = models.ForeignKey(Dataset, related_name="runinputs")
    index = models.PositiveIntegerField()

    class Meta(object):
        ordering = ['index']

    def clean(self):
        self.run.validate_restrict_access([self.dataset])


class ExceedsSystemCapabilities(models.Model):
    """
    Denotes a Run that could not be run due to requesting too much from the system.
    """
    run = models.OneToOneField(Run, related_name="not_enough_CPUs")
    threads_requested = models.PositiveIntegerField(validators=[MinValueValidator(1)])
    max_available = models.PositiveIntegerField(validators=[MinValueValidator(1)])

    def clean(self):
        if self.threads_requested <= self.max_available:
            raise ValidationError("Threads requested ({}) does not exceed maximum available ({})".format(
                self.threads_requested, self.max_available
            ))


class RunComponent(stopwatch.models.Stopwatch):
    """
    Class extended by both RunStep and RunCable.

    This class encapsulates much of the common function
    of the three "atomic" Run* classes.
    """
    execrecord = models.ForeignKey("librarian.ExecRecord", null=True, blank=True, related_name="used_by_components")
    reused = models.NullBooleanField(help_text="Denotes whether this reuses an ExecRecord", default=None)

    # State field to avoid the use of is_complete() and is_successful(), which can be slow.
    # Note that if this is a RunStep and the sub-Run is "Cancelling" or "Failing" that
    # will still count as "Running" here.
    _runcomponentstate = models.ForeignKey(RunComponentState, default=runcomponentstates.PENDING_PK,
                                           related_name="runcomponents")

    _redacted = models.NullBooleanField(
        help_text="Denotes whether this has been redacted. Private use only!")

    # Implicit:
    # - log: via OneToOneField from ExecLog
    # - invoked_logs: via FK from ExecLog
    # - outputs: via FK from Dataset

    # Implicit from Stopwatch: start_time, end_time.

    def __init__(self, *args, **kwargs):
        """Instantiate and set up a logger."""
        super(RunComponent, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def __str__(self):
        return 'RunComponent id {}'.format(self.id)

    # State getter methods.
    def is_pending(self):
        """
        True if RunComponent is pending; False otherwise.
        """
        return self._runcomponentstate_id == runcomponentstates.PENDING_PK

    def is_running(self):
        """
        True if RunComponent is running; False otherwise.
        """
        return self._runcomponentstate_id == runcomponentstates.RUNNING_PK

    def is_successful(self):
        """
        True if RunComponent is successful; False otherwise.
        """
        retval = self._runcomponentstate_id == runcomponentstates.SUCCESSFUL_PK
        self.logger.debug("is_successful returning %d (state= %s)" % (retval, self._runcomponentstate_id))
        return retval

    def is_cancelled(self):
        """
        True if RunComponent is cancelled; False otherwise.
        """
        return self._runcomponentstate_id == runcomponentstates.CANCELLED_PK

    def is_failed(self):
        """
        True if RunComponent is failed; False otherwise.
        """
        return self._runcomponentstate_id == runcomponentstates.FAILED_PK

    def is_quarantined(self):
        """
        True if RunComponent is quarantined; False otherwise.
        """
        return self._runcomponentstate_id == runcomponentstates.QUARANTINED_PK

    def is_complete(self):
        """
        True if this RunComponent is complete; false otherwise.
        """
        return self._runcomponentstate_id in runcomponentstates.COMPLETE_STATE_PKS

    def get_state_name(self):
        return self._runcomponentstate.name

    # State transition methods.
    @transaction.atomic
    def start(self, save=True, **kwargs):
        """
        Start this RunComponent, changing its state from Pending to Running.
        """
        assert self._runcomponentstate_id == runcomponentstates.PENDING_PK
        stopwatch.models.Stopwatch.start(self, save=False, **kwargs)  # we save below if necessary
        self._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.RUNNING_PK)
        if save:
            self.save()

    @transaction.atomic
    def cancel_pending(self, save=True):
        """
        Cancel this pending RunComponent.

        This is to be used to terminate RunComponents that are still pending, not
        ones that are running.
        """
        assert self._runcomponentstate_id == runcomponentstates.PENDING_PK
        self._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.CANCELLED_PK)
        if save:
            self.save()

    @transaction.atomic
    def cancel_running(self, save=True):
        """
        Cancel this running RunComponent.

        This is to be used to terminate RunComponents that are running, not ones
        that are still pending.
        """
        assert self._runcomponentstate_id == runcomponentstates.RUNNING_PK, "{} != {}".format(
            self._runcomponentstate_id, runcomponentstates.RUNNING_PK
        )
        self._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.CANCELLED_PK)
        self.stop(save=save)

    @transaction.atomic
    def cancel(self, save=True):
        """
        Cancel this pending/running RunComponent.
        """
        assert self._runcomponentstate_id in [runcomponentstates.PENDING_PK, runcomponentstates.RUNNING_PK]
        if self.is_pending():
            self.cancel_pending(save=save)
        else:
            self.cancel_running(save=save)

    @transaction.atomic
    def begin_recovery(self, save=True, recurse_upward=False):
        """
        Mark a successful RunComponent as recovering.

        Optionally this will mark all parent Runs as running.
        """
        assert self._runcomponentstate_id == runcomponentstates.SUCCESSFUL_PK, (
            "RunComponentState {} != Successful".format(self._runcomponentstate)
        )
        assert self.has_ended()
        self._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.RUNNING_PK)
        if save:
            self.save()

        if recurse_upward:
            self.parent_run.begin_recovery(save=save, recurse_upward=True)

    @transaction.atomic
    def finish_successfully(self, save=True):
        """
        End this running RunComponent successfully.
        """
        assert self._runcomponentstate_id == runcomponentstates.RUNNING_PK, (
            "RunComponentState {} != Running".format(self._runcomponentstate)
        )
        self._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.SUCCESSFUL_PK)
        if not self.has_ended():
            self.stop(save=False)
        if save:
            self.save()

        if self.execrecord and self.execrecord.generator.record.is_quarantined():
            # This has to have been an execution that used a quarantined ExecRecord,
            # and it was successful, so we can decontaminate RunComponents.
            self.execrecord.decontaminate_runcomponents()

    @transaction.atomic
    def finish_failure(self, save=True, recurse_upward=False):
        """
        End this running RunComponent, marking it as failed.

        Optionally this will mark all parent Runs as failed too.
        """
        assert self._runcomponentstate_id == runcomponentstates.RUNNING_PK, (
            "RunComponentState {} != Running".format(self._runcomponentstate)
        )
        self._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.FAILED_PK)
        if not self.has_ended():
            self.stop(save=False)
        if save:
            self.save()

        # On recursion, mark any running ancestor runs as failing
        # (ones that are already failing or cancelling can be left alone).
        if recurse_upward and self.parent_run.is_running():
            self.parent_run.mark_failure(save=save, recurse_upward=True)

    @transaction.atomic
    def quarantine(self, save=True, recurse_upward=False):
        """
        Transition this component to a quarantined state.

        Optionally, quarantine ancestor runs that are still marked as Successful.
        """
        assert self._runcomponentstate_id == runcomponentstates.SUCCESSFUL_PK
        self._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.QUARANTINED_PK)
        if save:
            self.save()

        # Quarantine all ancestor runs.
        if recurse_upward and self.parent_run.is_successful():
            self.parent_run.quarantine(save=save, recurse_upward=True)

    @transaction.atomic
    def decontaminate(self, save=True, recurse_upward=False):
        """
        Mark this quarantined RunComponent as fixed.

        Optionally, attempt to decontaminate ancestor runs that are quarantined.
        """
        assert self._runcomponentstate_id == runcomponentstates.QUARANTINED_PK
        self._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.SUCCESSFUL_PK)
        if save:
            self.save()

        # Quarantine all ancestor runs.
        self.parent_run.refresh_from_db()
        if recurse_upward and self.parent_run.is_quarantined():
            self.parent_run.attempt_decontamination(save=save, recurse_upward=True)

    def has_data(self):
        """
        Returns whether or not this instance has an associated Dataset.

        This is abstract and must be overridden.
        """
        raise NotImplementedError()

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

    def is_step(self):
        try:
            self.runstep
        except RunStep.DoesNotExist:
            return False
        return True

    def is_incable(self):
        try:
            self.runsic
        except RunSIC.DoesNotExist:
            return False
        return True

    def is_outcable(self):
        try:
            self.runoutputcable
        except RunOutputCable.DoesNotExist:
            return False
        return True

    def is_cable(self):
        return self.is_incable() or self.is_outcable()

    def has_log(self):
        try:
            self.log
        except ExecLog.DoesNotExist:
            return False
        return True

    @property
    def definite(self):
        if self.is_step():
            return self.runstep
        elif self.is_incable():
            return self.runsic
        elif self.is_outcable():
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
        if self.has_log():
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
        if not self.has_log() or not self.log.is_complete():
            general_error = '{} "{}" is not reused and does not have a complete log'.format(
                self.__class__.__name__, self)
            if self.has_data():
                raise ValidationError("{} so should not have generated any Datasets".format(general_error))
            if self.execrecord:
                raise ValidationError("{}; execrecord should not be set".format(general_error))
            return False

        # On the flipside....
        if (self.execrecord is not None and
                (not self.invoked_logs.exists() or self.has_log() and not self.log.is_complete())):
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
        logs_to_check = self.invoked_logs.all()
        # Make sure that log is checked, and avoid checking it twice.
        if self.has_log():
            logs_to_check = itertools.chain([self.log], self.invoked_logs.exclude(pk=self.log.pk))

        for curr_log in logs_to_check:
            curr_log.clean()

            # There may be at most a single integrity check and content check for each output.
            # (Usually there will be only one of each but it's possible for both to occur
            # when there is lots of parallelism.)
            outputs_integrity_checked = set([])
            for check in curr_log.integrity_checks.all():
                if check.dataset.pk in outputs_integrity_checked:
                    raise ValidationError('{} "{}" has multiple IntegrityCheckLogs for output '
                                          'Dataset {} of ExecLog "{}"'
                                          .format(self.__class__.__name__, self, check.dataset, curr_log))
                outputs_integrity_checked.add(check.dataset.pk)
                check.clean()

            outputs_content_checked = set([])
            for check in curr_log.content_checks.all():
                if check.dataset.pk in outputs_content_checked:
                    raise ValidationError('{} "{}" has multiple ContentCheckLogs for output '
                                          'Dataset {} of ExecLog "{}"'
                                          .format(self.__class__.__name__, self, check.dataset, curr_log))
                outputs_content_checked.add(check.dataset.pk)
                check.clean()

        # If log exists and there are invoked_logs, log should be among
        # the invoked logs.  If log exists, any preceding logs should
        # be complete and all non-trivial cables' outputs' checks should
        # have passed (since they were recoveries happening before we could
        # carry out the execution that log represents).
        if self.invoked_logs.exists() and self.has_log():
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
            #
            # if not all([x.all_checks_passed() for x in preceding_logs]):
            #     raise ValidationError(
            #        'Invoked ExecLogs preceding log of {} "{}" did not successfully pass all of their checks'.format(
            #            self.__class__.__name__, self)
            #     )

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

    @update_field("_redacted")
    def is_redacted(self, use_cache=False):
        if use_cache and self._redacted is not None:
            return self._redacted

        if self.has_log() and self.log.is_redacted():
            return True
        if self.execrecord is not None and self.execrecord.is_redacted():
            return True
        return False

    def clean(self):
        """Confirm that this is one of RunStep or RunCable."""
        # If the ExecRecord is set, check that access on the top level Run does not exceed
        # that on the ExecRecord.
        if self.execrecord is not None:
            self.top_level_run.validate_restrict_access([self.execrecord.generating_run])

        if not self.is_step() and not self.is_cable():
            raise ValidationError("RunComponent with pk={} is neither a step nor a cable".format(self.pk))

    def complete_clean(self):
        """
        Checks coherence and completeness of this RunComponent.
        """
        self.clean()
        if not self.is_complete():
            raise ValidationError('{} "{}" is not complete'.format(self.__class__.__name__, self))

    def build_removal_plan_h(self, removal_accumulator=None):
        """
        Create a manifest of objects that will be removed by removing this RunComponent.
        """
        removal_plan = removal_accumulator or metadata.models.empty_removal_plan()

        for ds in self.outputs.all():
            if ds not in removal_plan["Datasets"]:
                metadata.models.update_removal_plan(removal_plan, ds.build_removal_plan(removal_plan))

        if self.has_log() and self.execrecord and self.execrecord.generator == self.log:
            if self.execrecord not in removal_plan["ExecRecords"]:
                metadata.models.update_removal_plan(removal_plan, self.execrecord.build_removal_plan(removal_plan))

        return removal_plan

    @transaction.atomic
    def redact(self):
        self._redacted = True
        self.save(update_fields=["_redacted"])

    def get_log(self):
        if self.has_log():
            return self.log
        if self.execrecord is not None:
            return self.execrecord.generator
        return None


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

    def is_step(self):
        return True

    def is_incable(self):
        return False

    def is_outcable(self):
        return False

    @classmethod
    @transaction.atomic
    def create(cls, pipelinestep, run, start=True):
        """Create a new RunStep from a PipelineStep."""
        runstep = cls(pipelinestep=pipelinestep, run=run)
        if start:
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
        if self.has_log():
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
            if self.pipelinestep.transformation.is_pipeline() and self.has_subrun():
                raise ValidationError("{}; child_run should not be set".format(general_error))
            if self.has_log():
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

        else:  # self.reused is False.
            if not self._clean_not_reused():
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
        exec_log = getattr(self, 'log', None)
        outputs_missing = [] if exec_log is None else exec_log.missing_outputs()

        # Go through all of the outputs.
        for to in self.pipelinestep.transformation.outputs.all():
            # Get the associated ERO.
            corresp_ero = self.execrecord.execrecordouts.get(generic_output=to)
            corresp_ds = corresp_ero.dataset

            if self.pipelinestep.outputs_to_delete.filter(dataset_name=to.dataset_name).exists():
                # This output is deleted; there should be no associated Dataset.
                if self.outputs.filter(pk=corresp_ds.pk).exists() and corresp_ds.has_data():
                    raise ValidationError('Output "{}" of RunStep "{}" is deleted; no data should be associated'
                                          .format(to, self))

            elif corresp_ds in outputs_missing:
                # This output is missing; there should be no associated Dataset.
                if self.outputs.filter(pk=corresp_ds.pk).exists() and corresp_ds.has_data():
                    raise ValidationError('Output "{}" of RunStep "{}" is missing; no data should be associated'
                                          .format(to, self))

        # Check that any associated data belongs to an ERO of this ER
        # Supposed to be the datasets attached to this runstep (Produced by this runstep)
        for out_data in self.outputs.all():
            if not self.execrecord.execrecordouts.filter(dataset=out_data).exists():
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
        if self.pipelinestep.transformation.is_method() and self.has_subrun():
            raise ValidationError('PipelineStep of RunStep "{}" is not a Pipeline but a child run exists'
                                  .format(self))

        # TODO: Is there any difference between having a Pipeline as
        # your pipelinestep's transformation, and having a child_run?
        elif self.pipelinestep.transformation.is_pipeline():
            self._clean_with_subrun()

        # Clean all ExecLogs and their CCLs/ICLs, and make sure that
        # all preceding this step's ExecLog are complete and successful
        # before this one's is started.
        self._clean_execlogs()

        # If any inputs are not quenched, stop checking.
        if not self._clean_cables_in():
            return

        # From here on, RSICs are assumed to be quenched.
        # Perform tests specific to the Method and Pipeline cases.
        if self.pipelinestep.transformation.is_method():
            if not self._clean_with_method():
                return
        elif self.pipelinestep.transformation.is_pipeline():
            if self.has_subrun():
                self.child_run.clean()
            return

        # From here on, we know that this represents a Method, log is
        # assumed to be complete and clean, and so are the
        # invoked_logs().

        # Check that if there is no execrecord then log has no
        # associated CCLs or ICLs.  (It can't, as execution can't have
        # finished yet.)
        if self.has_log():
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

    def find_compatible_ERs(self, input_datasets):
        """
        Find all ExecRecords that are compatible with this RunStep.

        Exclude redacted ones. Permissions of old run must include all
        permissions of new run.
        @param input_datasets: a list of datasets that have
            already been processed by the input cables. To be compatible, an
            ExecRecord must have the same inputs in the same order.
        @return: generator of ExecRecords
        """
        transformation = self.transformation
        assert transformation.is_method()
        if transformation.definite.reusable == Method.NON_REUSABLE:
            return

        execrecord_ids = RunStep.objects.filter(
            pipelinestep__transformation=transformation).values('execrecord_id')
        query = ExecRecord.objects.filter(id__in=execrecord_ids)
        for dataset_idx, dataset in enumerate(input_datasets, 1):
            query = query.filter(
                execrecordins__generic_input__transformationinput__dataset_idx=dataset_idx,
                execrecordins__dataset=dataset)

        new_run = self.top_level_run
        for execrecord in query.all():
            if not execrecord.is_redacted():
                extra_users, extra_groups = new_run.extra_users_groups(
                    [execrecord.generating_run])
                if not(extra_users or extra_groups):
                    yield execrecord

    @transaction.atomic
    def check_ER_usable(self, execrecord):
        """
        Check that the specified ExecRecord may be reused.
        """
        result = {"fully reusable": False, "successful": True}
        # Case 1: ER was a failure.  In this case, we don't want to proceed,
        # so we return the failure for appropriate handling.
        if execrecord.outputs_not_usable_in_run() or execrecord.generator.record.is_failed():
            self.logger.debug("ExecRecord found (%s) was a failure", execrecord)
            result["successful"] = False

        # Case 2: ER has fully checked outputs and provides the outputs needed.
        elif execrecord.outputs_OK() and execrecord.provides_outputs(self.pipelinestep.outputs_to_retain()):
            self.logger.debug("Completely reusing ExecRecord %s", execrecord)
            result["fully reusable"] = True

        return result

    @transaction.atomic
    def get_suitable_ER(self, input_datasets):
        """
        Retrieve a suitable ExecRecord for this RunStep.

        If any of them are failed, we find the failed one with the most outputs having data, with
        ties broken by the smallest PK.
        If any of them are fully reusable, we find the fully reusable one satisfying the same criteria.
        Otherwise we find whichever one satisfies the same criteria.

        Return a tuple containing the ExecRecord along with its summary (as
        produced by check_ER_usable), or None if no appropriate ExecRecord is found.
        """
        execrecords = self.find_compatible_ERs(input_datasets)
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

    def build_removal_plan_h(self, removal_accumulator=None):
        removal_plan = removal_accumulator or metadata.models.empty_removal_plan()
        for rsic in self.RSICs.all():
            metadata.models.update_removal_plan(removal_plan, rsic.build_removal_plan_h(removal_plan))

        return RunComponent.build_removal_plan_h(self, removal_plan)


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
        if cable.is_incable():
            runcable = RunSIC.create(cable, parent_record)
        else:
            runcable = RunOutputCable.create(cable, parent_record)
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
        if self.has_data() and self.outputs.first().has_data():
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
            # Check if the attached output has real data associated to it.
            if self.has_data() and self.outputs.first().has_data():
                raise ValidationError(
                    '{} "{}" does not keep its output but a dataset was registered'.format(
                        self._cable_type_str(), self)
                )

        # If EL shows missing output, there shouldn't be a Dataset.
        elif self.log.missing_outputs():
            if self.has_data() and self.outputs.first().has_data():
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
                    if not (self.has_data() and self.outputs.first().has_data()):
                        raise ValidationError('{} "{}" was not reused, trivial, or deleted; it should have '
                                              'produced data'.format(self._cable_type_str(), self))

                    if corresp_ero.dataset != self.outputs.first():
                        raise ValidationError('Dataset "{}" was produced by {} "{}" but is not in an ERO of '
                                              'ExecRecord "{}"'.format(self.outputs.first(),
                                                                       self._cable_type_str(),
                                                                       self,
                                                                       self.execrecord))

        # June 9, 2014: since PSICs are now allowed to use ERs of POCs and vice versa, the functionality
        # that was previously in RunSIC and RunOutputCable._clean_execrecord can now be folded into here.
        if not self.execrecord.general_transf().is_cable():
            raise ValidationError('ExecRecord of {} "{}" does not represent a PipelineCable'.format(
                self.__class__.__name__, self))

        elif not self.component.is_compatible(self.execrecord.general_transf()):
            raise ValidationError('{} of {} "{}" is incompatible with the cable of its ExecRecord'.format(
                self.component.__class__.__name__, self.__class__.__name__, self))

    def clean(self):
        """
        Check coherence of this RunCable.

        In sequence, the checks we perform:
         - check coherence of start_time and end_time
         - PSIC/POC belongs to dest_runstep.pipelinestep/run.pipeline

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
        if not self.has_log():
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
            if self.has_log():
                self._clean_has_execlog_no_execrecord_yet()
            return

        # Now, we know there to be an ExecRecord.
        self._clean_execrecord()

        # Attempt to clean the input_integrity_check.
        try:
            self.input_integrity_check.clean()
        except IntegrityCheckLog.DoesNotExist:
            pass

    def find_compatible_ERs(self, input_SD):
        """
        Find ExecRecords which may be used by this RunCable.

        INPUTS
        input_SD        Dataset to feed the cable

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

        output_SD = execrecord.execrecordouts.first().dataset

        # Terminal case 1: the found ExecRecord has failed some initial checks.  In this case,
        # we just return and the RunCable fails.
        if not output_SD.usable_in_run():
            self.logger.debug("The ExecRecord ({}) found has a bad output.".format(execrecord))
            summary["successful"] = False

        # Terminal case 2: the ExecRecord passed its checks and provides the output we need.
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

        return None, None


class RunSIC(RunCable):
    """
    Annotates the action of a PipelineStepInputCable within a RunStep.

    Related to :model:`archive.models.RunStep`
    Related to :model:`librarian.models.ExecRecord`
    Related to :model:`pipeline.models.PipelineStepInputCable`
    """
    dest_runstep = models.ForeignKey(RunStep, related_name="RSICs")
    PSIC = models.ForeignKey("pipeline.PipelineStepInputCable", related_name="psic_instances")

    # Implicit from RunCable: execrecord, reused, log, output, invoked_logs, start_time, end_time.

    class Meta:
        # Uniqueness constraint ensures that no POC is multiply-represented
        # within a run step.
        unique_together = ("dest_runstep", "PSIC")

    @classmethod
    def create(cls, PSIC, runstep):
        runsic = cls(PSIC=PSIC, dest_runstep=runstep)
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
        return self.dest_runstep.run

    @property
    def top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        return self.dest_runstep.top_level_run

    @property
    def pipeline(self):
        return self.PSIC.pipelinestep.pipeline

    @property
    def parent(self):
        return self.dest_runstep

    def is_step(self):
        return False

    def is_incable(self):
        return True

    def is_outcable(self):
        return False

    # TODO: fix for sub-pipelines
    def output_name(self):
        return "run{}_step{}_input{}".format(self.parent_run.pk, self.dest_runstep.step_num, self.PSIC.dest.dataset_idx)

    def output_description(self):
        run = self.top_level_run
        desc = ('Generated data from a run of pipeline "{}" started at {} by {}\n'
                .format(self.pipeline, run.start_time, run.user))
        desc += "run: {}\n".format(run.pk)
        desc += "user: {}\n".format(run.user)
        desc += "step: {}\n".format(self.dest_runstep.step_num)
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
        if (not self.dest_runstep.pipelinestep.cables_in.filter(pk=self.PSIC.pk).exists()):
            raise ValidationError('PSIC "{}" does not belong to PipelineStep "{}"'
                                  .format(self.PSIC, self.dest_runstep.pipelinestep))

    def get_coordinates(self):
        """
        Retrieves a tuple of pipeline coordinates of this RunSIC.

        This is simply the coordinates of the RunStep it belongs to.
        Implicitly, if you are comparing a RunSIC with a RunStep that has
        the same coordinates, the RunSIC is deemed to come first.
        """
        return self.dest_runstep.get_coordinates()


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

    def is_step(self):
        return False

    def is_incable(self):
        return False

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

        if self.record.is_step() and self.record.definite.pipelinestep.transformation.is_pipeline():
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
            if self.record.is_outcable() and not self.invoking_record.is_outcable():
                raise ValidationError('ExecLog "{}" is invoked earlier than the ROC it belongs to'.format(self))
            elif self.record.is_step() and self.invoking_record.is_incable():
                raise ValidationError('ExecLog "{}" is invoked earlier than the RunStep it belongs to'.format(self))

    def is_complete(self):
        """
        Checks completeness of this ExecLog.

        The execution must have ended (i.e. end_time is
        set) and a MethodOutput must be in place if appropriate.
        """
        if not self.has_ended():
            return False

        if self.record.is_step() and self.record.runstep.pipelinestep.transformation.is_method():
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
                    missing.append(ccl.dataset)
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
        # If this ExecLog has a MethodOutput, check whether its
        # integrity was compromised, and its return code.
        try:
            if not self.methodoutput.are_checksums_OK:
                return False
            elif self.methodoutput.return_code is not None and self.methodoutput.return_code != 0:
                return False
        except ObjectDoesNotExist:
            pass

        # If this ExecLog represents a RunSIC, check if it's got a failed input integrity check.
        if self.record.is_cable():
            try:
                if self.record.definite.input_integrity_check.is_fail():
                    return False
            except IntegrityCheckLog.DoesNotExist:
                pass

        # Having reached here, we are comfortable with the execution --
        # note that it may still be in progress!
        return True

    def all_checks_passed(self):
        """
        True if every non-trivial output of this ExecLog has passed its check.

        First check that all of the tests have passed. Then check that all
        checks have been performed.

        An exception is made for an ExecLog of a trivial cable.  Such cables
        don't check their outputs.
        """
        is_trivial_cable = self.record.is_cable() and self.record.component.is_trivial()
        if is_trivial_cable:
            return True

        if self.record.execrecord is None:
            return False
        is_original_run = self.record.execrecord.generator == self
        missing_content_checks = set()
        missing_integrity_checks = set()
        for ero in self.record.execrecord.execrecordouts.all():
            if not is_original_run:
                missing_integrity_checks.add(ero.dataset_id)
            elif not ero.dataset.is_raw():
                missing_content_checks.add(ero.dataset_id)

        for content_check in self.content_checks.all():
            if content_check.is_fail():
                return False
            missing_content_checks.discard(content_check.dataset_id)
        for integrity_check in self.integrity_checks.all():
            if integrity_check.is_fail():
                return False
            missing_integrity_checks.discard(integrity_check.dataset_id)

        return not (missing_integrity_checks or missing_content_checks)

    def is_redacted(self):
        try:
            return self.methodoutput.is_redacted()
        except ObjectDoesNotExist:
            pass
        return False

    def build_redaction_plan(self, output_log=True, error_log=True, return_code=True):
        """
        Redact the error/output log and/or the return code of the MethodOutput.

        Return lists of objects affected.
        """
        redaction_plan = empty_redaction_plan()
        try:
            if output_log and not self.methodoutput.is_output_redacted():
                redaction_plan["OutputLogs"].add(self)
            if error_log and not self.methodoutput.is_error_redacted():
                redaction_plan["ErrorLogs"].add(self)
            if return_code and not self.methodoutput.is_code_redacted():
                redaction_plan["ReturnCodes"].add(self)

            # Don't need to record RunComponent in the redaction plan, because
            # we don't report those.
        except MethodOutput.DoesNotExist:
            pass

        return redaction_plan

    def generated_execrecord(self):
        try:
            self.execrecord
        except ObjectDoesNotExist:
            return False
        return True


class SafeContext:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


class MethodOutput(models.Model):
    """
    Logged output of the execution of a method.

    This stores the stdout and stderr output, as well as the process'
    return code.

    If the return code is -1, it indicates that an operating system level error
    was raised while trying to execute the code, ie., the code was not executable.
    In that case, stdout will be empty, and stderr will contain the Python stack
    trace produced when we tried to run the code with Popen.

    If the return code is -2, it indicates that the execution was terminated.

    If the return code is None, it indicates that the code execution is
    in progress.
    """
    UPLOAD_DIR = "Logs"

    execlog = models.OneToOneField(ExecLog, related_name="methodoutput")
    return_code = models.IntegerField("return code", null=True)
    output_log = models.FileField("output log", upload_to=UPLOAD_DIR,
                                  help_text="Terminal output of the RunStep Method, i.e. stdout.",
                                  null=True, blank=True)
    error_log = models.FileField("error log", upload_to=UPLOAD_DIR,
                                 help_text="Terminal error output of the RunStep Method, i.e. stderr.",
                                 null=True, blank=True)

    are_checksums_OK = models.BooleanField(help_text="Do code checksums match originals?",
                                           default=True)

    install_failed = models.BooleanField(help_text="Did code fail to install?",
                                         default=False)

    output_redacted = models.BooleanField(default=False)
    error_redacted = models.BooleanField(default=False)
    code_redacted = models.BooleanField(default=False)

    logger = logging.getLogger('archive.MethodOutput')
    filepurger = filewalker.FilePurger(os.path.join(settings.MEDIA_ROOT, UPLOAD_DIR),
                                       grace_period_hrs=settings.LOGFILE_GRACE_PERIOD_HRS,
                                       walk_period_hrs=settings.LOGFILE_PURGE_SCAN_PERIOD_HRS,
                                       logger=logger)

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

    def is_redacted(self):
        return self.output_redacted or self.error_redacted or self.code_redacted

    def is_output_redacted(self):
        return self.output_redacted

    def is_error_redacted(self):
        return self.error_redacted

    def is_code_redacted(self):
        return self.code_redacted

    def redact_output_log(self):
        self.output_log.delete()
        self.output_redacted = True
        self.save()
        self.execlog.record.redact()

    def redact_error_log(self):
        self.error_log.delete()
        self.error_redacted = True
        self.save()
        self.execlog.record.redact()

    def redact_return_code(self):
        self.return_code = None
        self.code_redacted = True
        self.save()
        self.execlog.record.redact()

    @staticmethod
    def _active_log_files():
        """ Return the absolute file names of the log files of any running or pending
        runs.
        """
        pending_runs = archive.models.Run.objects.filter(start_time=None)
        running_runs = archive.models.Run.objects.filter(end_time=None)
        all_active_runs = pending_runs | running_runs
        for run in all_active_runs:
            for component in run.get_all_atomic_runcomponents():
                execlog = component.log if hasattr(component, 'log') else None
                if execlog is not None:
                    methodoutput = execlog.methodoutput if hasattr(execlog, 'methodoutput') else None
                    if methodoutput is not None:
                        yield os.path.join(settings.MEDIA_ROOT, methodoutput.error_log.name)
                        yield os.path.join(settings.MEDIA_ROOT, methodoutput.output_log.name)

    @classmethod
    def idle_logfile_purge(cls,
                           max_storage=settings.LOGFILE_MAX_STORAGE,
                           target_size=settings.LOGFILE_TARGET_STORAGE):
        # get batches of max 5 files at a time when we have to purge
        BATCH_SIZE = 5
        while True:
            time_to_stop = (yield None)
            cls.logger.debug('hello from idle_logfile_purge')

            exclude_set = set(MethodOutput._active_log_files())
            cls.logger.debug('Found %d active log files', len(exclude_set))
            for ff in exclude_set:
                cls.logger.debug("--%s", ff)
            # recalculate the total file size, while allowing for interruptions
            # the resulting total file size will be in cls.filepurger._totsize once
            # we have finished the file system scanning
            checker = cls.filepurger.regenerator(exclude_set)
            try:
                while True:
                    time_to_stop = (yield None)
                    checker.send(time_to_stop)
            except StopIteration:
                pass
            cls.logger.debug('finished regenerating file cache')
            cls.logger.debug("\n".join(["%s: %s" % itm for itm in cls.filepurger.get_scaninfo().items()]))
            if time.time() < time_to_stop and cls.filepurger._totsize > max_storage:
                cls.logger.info("Purge cycle started, total size = %d > max_storage=%d",
                                cls.filepurger._totsize, max_storage)
                tot_size_deleted = 0
                for ftup in cls.filepurger.next_to_purge(BATCH_SIZE, exclude_set,
                                                         target_size,
                                                         dodelete=True):
                    if ftup is None:
                        # we have run out of time: abruptly exit from the for loop
                        break
                    else:
                        # remove the file
                        abspath, fsize = ftup
                        tot_size_deleted += fsize
                        relpath = os.path.relpath(abspath, settings.MEDIA_ROOT)
                        cls.logger.debug("Looking for file '%s' in db" % relpath)
                        err_qset = MethodOutput.objects.filter(error_log=relpath)
                        out_qset = MethodOutput.objects.filter(output_log=relpath)
                        qs = err_qset | out_qset
                        methodoutput = qs.first()
                        if methodoutput is None:
                            # a file unknown to the database
                            # NOTE: must be defensive here: the file might have
                            # moved in the mean-time (#481)
                            cls.logger.debug("Deleting file '%s'" % abspath)
                            try:
                                with SafeContext():
                                    filedate = os.path.getmtime(abspath)
                                    file_age = datetime.now() - datetime.fromtimestamp(filedate)
                                    if file_age > timedelta(hours=1):
                                        cls.logger.warn('No MethodOutput matches file %r, deleting it.',
                                                        relpath)
                                        os.remove(abspath)
                            except OSError:
                                cls.warn("Failed to remove file %r", relpath)
                        else:
                            cls.logger.debug("Found a methodoutput id=%d, deleting '%s'" % (methodoutput.id, relpath))
                            if methodoutput.error_log == relpath:
                                methodoutput.error_log.delete(save=True)
                            elif methodoutput.output_log == relpath:
                                methodoutput.output_log.delete(save=True)
                            else:
                                cls.logger.error("mismatch in log file purge")
                            methodoutput.save()
                # -- report purging progress
                cls.logger.info("Purge cycle completed, deleted size %d", tot_size_deleted)
            else:
                cls.logger.info("Purge cycle skipped, total size = %d < max_storage=%d",
                                cls.filepurger._totsize, max_storage)
