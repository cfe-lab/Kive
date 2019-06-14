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
import six

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
    counts = {key: len(targets) for key, targets in six.iteritems(redaction_plan)}
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
                    except Exception:
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
        pass

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
                  'end': self._format_time(self.end_time),
                  'id': self.pk}
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

        if self.is_cancelled():
            result['status'] = 'CANCELLED'
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
                except Exception:
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
        if not self.is_complete():
            raise archive.exceptions.SandboxActiveException(
                "Run (pk={}, Pipeline={}, queued {}, User={}) is not finished".format(
                    self.pk, self.pipeline, self.time_queued, self.user)
                )

        if self.sandbox_path != "":
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
            name_to_show = self.top_level_run.name or "[blank]"
            unicode_rep = "{} (pk={}):{}".format(name_to_show, self.top_level_run.pk, self.get_coordinates())
        else:
            name_to_show = self.name or "[blank]"
            unicode_rep = "{} (pk={})".format(name_to_show, self.pk)
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
        pass

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
        pass


class RunInput(models.Model):
    """
    Represents an input to a run.
    """
    run = models.ForeignKey(Run, related_name="inputs")
    dataset = models.ForeignKey(Dataset, related_name="runinputs")
    index = models.PositiveIntegerField()

    objects = None  # Filled in later by Django.

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
        self.logger.debug("is_successful returning {} (state={})".format(retval, self._runcomponentstate_id))
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
        return True

    def is_incable(self):
        return False

    def is_outcable(self):
        return False

    def is_cable(self):
        return self.is_incable() or self.is_outcable()

    def has_log(self):
        return False

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
