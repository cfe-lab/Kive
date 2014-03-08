"""
archive.models

Shipyard data models relating to archiving information: Run, RunStep,
Dataset, etc.
"""

from django.db import models
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.core.exceptions import ValidationError

import hashlib
import logging

import file_access_utils

import method.models
import transformation.models
import stopwatch.models


def clean_execlogs(runx):
    """Count and clean ExecLogs of Run(Step|SIC|OutputCable).

    Helper function to ensure a RunStep, RunSIC, or RunOutputCable has
    at most one ExecLog, and to clean it if it exists.
    """
    if runx.log.exists():
       if runx.log.count() == 1:
           runx.log.first().complete_clean()
       else:
           raise ValidationError("{} \"{}\" has {} ExecLogs but should have only one".format(runx.__class__.__name__,
                                                                                             runx, runx.log.count()))


class Run(stopwatch.models.Stopwatch):
    """
    Stores data associated with an execution of a pipeline.

    Related to :model:`pipeline.models.Pipeline`
    Related to :model:`archive.models.RunStep`
    Related to :model:`archive.models.Dataset`
    """
    user = models.ForeignKey(User, help_text="User who performed this run")
    pipeline = models.ForeignKey("pipeline.Pipeline", related_name="pipeline_instances",
                                 help_text="Pipeline used in this run")

    name = models.CharField("Run name", max_length=256)
    description = models.TextField("Run description", blank=True)

    # If run was spawned within another run, parent_runstep denotes
    # the run step that initiated it
    parent_runstep = models.OneToOneField("RunStep", related_name="child_run", null=True, blank=True,
        help_text="Step of parent run initiating this one as a sub-run")

    # Implicitly, this also has start_time and end_time through inheritance.

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
        # Check that start- and end-time are coherent.
        stopwatch.models.Stopwatch.clean(self)

        if (self.is_subrun() and self.pipeline != self.parent_runstep.pipelinestep.transformation):
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
        # RunOutputCables are complete.
        for step in self.pipeline.steps.all():
            corresp_rs = self.runsteps.filter(pipelinestep=step)
            if not corresp_rs.exists() or not corresp_rs[0].is_complete():
                return False
        for outcable in self.pipeline.outcables.all():
            corresp_roc = self.runoutputcables.filter(pipelineoutputcable=outcable)
            if not corresp_roc.exists() or not corresp_roc[0].is_complete():
                return False
        return True

    def complete_clean(self):
        """
        Checks completeness and coherence of a run.
        """
        self.clean()
        if not self.is_complete():
            raise ValidationError('Run "{}" is not complete'.format(self))

    def __unicode__(self):
        if self.is_subrun():
            unicode_rep = u"Run with pipeline [{}] parent_runstep [{}]".format(self.pipeline, self.parent_runstep)
        else:
            unicode_rep = u"Run with pipeline [{}]".format(self.pipeline)
        return unicode_rep

    def is_subrun(self):
        return self.parent_runstep is not None

    def successful_execution(self):
        """
        Checks if this Run is successful (so far).

        PRE
        This Run is clean and complete.
        """
        # Check steps for success.
        for step in self.runsteps.all():
            if not step.successful_execution():
                return False

        # All steps checked out.  Check outcables.
        for outcable in self.runoutputcables.all():
            if not outcable.successful_execution():
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

    def get_top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        # Base case: this is the top-level Run.
        if self.parent_runstep is None:
            return self

        # Otherwise, return the top-level run of the parent RunStep.
        return self.parent_runstep.get_top_level_run()


class RunStep(stopwatch.models.Stopwatch):
    """
    Annotates the execution of a pipeline step within a run.

    Related to :model:`archive.models.Run`
    Related to :model:`librarian.models.ExecRecord`
    Related to :model:`pipeline.models.PipelineStep`
    """
    run = models.ForeignKey(Run, related_name="runsteps")

    # If this RunStep has a child_run, then this execrecord should be
    # null.
    execrecord = models.ForeignKey("librarian.ExecRecord", null=True, blank=True, related_name="runsteps")
    reused = models.NullBooleanField(default=None,
                                     help_text="Denotes whether this run step reuses a previous execution")
    pipelinestep = models.ForeignKey("pipeline.PipelineStep", related_name="pipelinestep_instances")

    log = generic.GenericRelation("ExecLog")

    invoked_logs = generic.GenericRelation("ExecLog",
                                           content_type_field="content_type_iel",
                                           object_id_field="object_id_iel")

    outputs = generic.GenericRelation("Dataset")

    # Implicit through inheritance: start_time, end_time.

    class Meta:
        # Uniqueness constraint ensures you can't have multiple RunSteps for
        # a given PipelineStep within a Run.
        unique_together = ("run", "pipelinestep")

    def __unicode__(self):
        unicode_rep = u"Runstep with PS [{}]".format(self.pipelinestep)
        return unicode_rep

    def has_subrun(self):
        """
        Does this RunStep contain a Run of a sub-Pipeline?
        """
        return hasattr(self, "child_run")

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
        if self.log.all().exists():
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
           associated and no Datasets should be associated

        This is a helper function for clean.

        OUTPUTS
        True if there are more checks to do within clean, False if
        clean should return right away.
        """
        for rsic in self.RSICs.all():
            rsic.complete_clean()

        if (self.pipelinestep.cables_in.count() != self.RSICs.count()):
            general_error = 'RunStep "{}" inputs not quenched'.format(self)
            if self.reused is not None or self.execrecord is not None:
                raise ValidationError("{}; reused and execrecord should not be set".format(general_error))
            if self.pipelinestep.transformation.__class__.__name__ == "Pipeline" and self.has_subrun():
                raise ValidationError("{}; child_run should not be set".format(general_error))
            if self.log.all().exists():
                raise ValidationError("{}; no log should have been generated".format(general_error))
            if self.outputs.all().exists():
                raise ValidationError("{}; no data should have been generated".format(general_error))
            return False
        return True

    def _clean_with_method(self):
        """
        If this RunStep represents a Method (as opposed to a Pipeline),
        make sure it is in a coherent state:

         - if we haven't decided whether or not to reuse an ER and
           this is a method, no log should be associated, no Datasets
           should be associated, and execrecord should not be set

        (from here on, reused is assumed to be set)

         - else if we are reusing an ER and this is a method, check
           that:

           - there are no associated Datasets.

         - else if we are not reusing an ER and this is a Method:

           - if there is no ExecLog or if it isn't complete, there
             should be no Datasets associated and ER should not be set

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
            general_error = 'RunStep "{}" has not decided whether or not to reuse an ExecRecord'.format(self)
            if self.log.all().exists():
                raise ValidationError("{}; no log should have been generated".format(general_error))

            if self.outputs.all().exists():
                raise ValidationError("{}; no data should have been generated".format(general_error))

            if self.execrecord is not None:
                raise ValidationError("{}; execrecord should not be set".format(general_error))
            return False

        elif self.reused:
            if self.outputs.all().exists():
                raise ValidationError('RunStep "{}" reused an ExecRecord and should not have generated any Datasets'
                                      .format(self))

        else: # self.reused is None
            if (not self.log.all().exists() or not self.log.first().is_complete()):
                general_error = 'RunStep "{}" does not have a complete log'.format(self)
                if self.outputs.all().exists():
                    raise ValidationError("{} so should not have generated any Datasets".format(general_error))

                if self.execrecord:
                    raise ValidationError("{}; execrecord should not be set".format(self))
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
        if self.pipelinestep.transformation != self.execrecord.general_transf():
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
        # If there is no exec log there is no notion of missing outputs
        outputs_missing = []
        if self.log.count() > 0:
            outputs_missing = self.log.first().missing_outputs()

        # Go through all of the outputs.
        to_type = ContentType.objects.get_for_model(transformation.models.TransformationOutput)

        for to in self.pipelinestep.transformation.outputs.all():
            # Get the associated ERO.
            corresp_ero = self.execrecord.execrecordouts.get(content_type=to_type, object_id=to.id)

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

         - if ELs is associated, check it is clean (see the module
           function clean_execlogs)
         - check coherence of RSIC's (see _clean_inputs)

        (from here on all RSICs are assumed to be quenched)

         - if this is represents a Method, do relevant coherence checks 
           (see _clean_with_method).
         - else if this is a Pipeline:
           - clean child_run if it exists

        (from here on, it is assumed that this is a Method and
         execrecord is set)

         - check the execrecord (see _clean_execrecord)
         - check the outputs (see _clean_outputs)

        Note: don't need to check inputs for multiple quenching due to
        uniqueness.  Quenching of outputs is checked by ExecRecord.
        """
        # Check that the times are coherent.
        stopwatch.models.Stopwatch.clean(self)

        # Does pipelinestep belong to run.pipeline?
        if not self.run.pipeline.steps.filter(pk=self.pipelinestep.pk).exists():
            raise ValidationError('PipelineStep "{}" of RunStep "{}" does not belong to Pipeline "{}"'
                                  .format(self.pipelinestep, self, self.run.pipeline))

        # If the PipelineStep stores a method, it should have no
        # child_run (should not act as a parent runstep).
        # TODO: Add a helper transformation.is_method() or
        # transformation.is_atomic()
        if self.pipelinestep.transformation.__class__.__name__ == "Method" and self.has_subrun():
            raise ValidationError('PipelineStep of RunStep "{}" is not a Pipeline but a child run exists'
                                  .format(self))

        # TODO: Is there any difference between having a Pipeline as
        # your pipelinestep's transformation, and having a child_run?
        elif self.pipelinestep.transformation.__class__.__name__ == "Pipeline":
            self._clean_with_subrun()

        clean_execlogs(self)

        # If any inputs are not quenched, stop checking.
        if not self._clean_cables_in(): return

        # From here on, RSICs are assumed to be quenched.
        if self.pipelinestep.transformation.__class__.__name__ == "Method":
            if not self._clean_with_method(): return

        # From here on, ExecLog is assumed to be complete and clean.
        elif self.pipelinestep.transformation.__class__.__name__ == "Pipeline":
            if self.has_subrun():
                self.child_run.clean()
            return

        # From here on, we know that this is a Method.
        if not self.execrecord: return

        # From here on, the appropriate ER is known to be set.
        self._clean_execrecord()
        self._clean_outputs()

    def is_complete(self):
        """
        True if RunStep is complete; False otherwise.

        This is similar to the procedure to check for completeness
        of RunSIC, with the additional wrinkle that a RunStep
        can fail while its cables are running before it even
        gets to the recovery stage.  Also, if it represents
        a sub-Pipeline, then it simply checks if its child_run
        is complete.
        """
        # Sub-Pipeline case:
        if self.has_subrun():
            return self.child_run.is_complete()

        # Method case:
        if self.execrecord is not None:
            return True

        # At this point we know that it is not finished successfully;
        # check whether it failed during either cables or during recovery.
        if not self.successful_execution():
            for cable in self.RSICs.all():
                if not (cable.is_complete() or not cable.has_started()):
                    return False

            for invoked_log in self.invoked_logs.all():
                if not (self.invoked_log.is_complete() or not self.invoked_log.has_started()):
                    return False

            # At this point, we know that it's failed but complete.
            return True

        # At this point, we know that it is successful but incomplete.
        return False

    def complete_clean(self):
        """
        Checks coherence and completeness of this RunStep.
        """
        self.clean()
        if not self.is_complete():
            raise ValidationError('RunStep "{}" is not complete'.format(self))

    def successful_execution(self):
        """
        True if RunStep is successful; False otherwise.

        The RunStep is failed if:
         - any of its cables failed (fail during cables)

         - any of its invoked_logs is failed (fail during either
           recovery of inputs or during its own execution if it's
           a Method)

         - its child_run has failed (if it fails during execution
           of the sub-Pipeline)

        PRE: this RunStep is clean and complete.
        """
        for cable in self.RSICs.all():
            if not cable.successful_execution():
                return False

        # At this point we know that all the cables were successful;
        # we check for failure during recovery or during its own
        # execution.
        for invoked_log in self.invoked_logs.all():
            if not invoked_log.is_successful():
                return False

        # In the case that this is a sub-Pipeline, check if child_run
        # is successful.
        if hasattr(self, "child_run"):
            return self.child_run.successful_execution()

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

    def get_top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        return self.run.get_top_level_run()


class RunCable(stopwatch.models.Stopwatch):
    """
    Abstract class inherited by RunSIC and RunOutputCable.

    Since those classes share so much functionality, this
    abstract class will encapsulate that stuff and RSIC/ROC
    can extend it where necessary.
    """
    execrecord = models.ForeignKey("librarian.ExecRecord", null=True, blank=True,
                                   related_name="%(app_label)s_%(class)s_related")
    reused = models.NullBooleanField(help_text="Denotes whether this run reused the action of an output cable",
                                     default=None)

    log = generic.GenericRelation("ExecLog")
    output = generic.GenericRelation("Dataset")

    invoked_logs = generic.GenericRelation("ExecLog",
                                           content_type_field="content_type_iel",
                                           object_id_field="object_id_iel")

    # Implicit from Stopwatch: start_time, end_time.

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        """Instantiate and set up a logger."""
        super(self.__class__, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def has_data(self):
        """True if associated output exists; False if not."""
        return self.output.all().exists()

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

    def _clean_undecided_reused(self):
        """
        Check coherence of a RunCable which has not decided whether or
        or not to reuse an ExecRecord:

         - if reused is None (no decision on reusing has been made),
           no log should be associated, no data should be associated,
           and execrecord should not be set

        This is a helper for clean().

        PRE
        This RunCable has reused = None (the decision to reuse an
        ExecRecord or not has not yet been made).
        """
        general_error = '{} "{}" has not decided whether or not to reuse an ExecRecord'.format(
            self._cable_type_str(), self)
        if self.log.all().exists():
            raise ValidationError("{}; no log should have been generated".format(general_error))
        if self.has_data():
            raise ValidationError("{}; no Datasets should be associated".format(general_error))
        if self.execrecord:
            raise ValidationError("{}; execrecord should not be set yet".format(general_error))

    def _clean_reused(self):
        """
        Check coherence of a RunCable which has decided to reuse an
        ExecRecord:

         - if reused is True, no data should be associated.

        This is a helper for clean().

        PRE
        This RunCable has reused = True (has decided to reuse an ExecRecord).
        """
        if self.has_data():
            raise ValidationError('{} "{}" reused an ExecRecord and should not have generated any Datasets'
                                  .format(self._cable_type_str(), self))

    def _clean_not_reused(self):
        """
        Check coherence of a RunCable which has decided not to reuse an
        ExecRecord:

         - if reused is False:

           - if no ExecLog is attached yet or it is not complete,
             there should be no associated Dataset

           (from here on ExecLog is known to be attached and complete)

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
        if not self.log.exists() or not self.log.first().is_complete():
            if self.has_data():
                raise ValidationError('{} "{}" does not have a complete log so should not have generated any '
                                      'Datasets'.format(self._cable_type_str(), self))
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
            if self.output.count() > 1:
                raise ValidationError('{} "{}" should generate at most one Dataset'.format(
                    self._cable_type_str(), self))
            self.output.first().clean()
        return True

    def _clean_execrecord(self):
        """
        Check coherence of the RunCable's associated ExecRecord.

        This is an abstract function that must be overridden by
        RunSIC and RunOutputCable, as most of this is case-specific.

        PRE
        This RunCable has an ExecRecord.
        """
        self.execrecord.complete_clean()

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

         - else:
           - the corresponding ERO should have existent data associated

           - if the PSIC/POC is not trivial and this RunCable does not reuse an ER,
             then there should be existent data associated and it should also
             be associated to the corresponding ERO.

        This is a helper function for clean.

        PRE
        This RunCable has an ExecLog AND an ExecRecord.
        """
        # If output of the cable not marked as kept, there shouldn't be a Dataset.
        if not self._pipeline_cable().keeps_output():
            if self.has_data():
                raise ValidationError(
                    '{} "{}" does not keep its output but a dataset was registered'.format(
                        self._cable_type_str(), self)
                )

        # If EL shows missing output, there shouldn't be a Dataset.
        elif self.log.first().missing_outputs():
            if self.has_data():
                raise ValidationError('{} "{}" had missing output but a dataset was registered'.format(
                    self._cable_type_str(), self))

        else:
            # The corresponding ERO should have existent data.
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

                if corresp_ero.symbolicdataset.dataset != self.output.first():
                    raise ValidationError('Dataset "{}" was produced by {} "{}" but is not in an ERO of '
                                          'ExecRecord "{}"'.format(self.output.first(), self._cable_type_str(),
                                          self, self.execrecord))

    def _clean_without_execlog(self):
        """
        If this RunCable has no ExecLog (that is, it either recycled an
        ExecRecord, or is incomplete), make sure it is coherent:

          - if it is reusing an ExecRecord, it should not have output
          - if it is not reusing an ExecRecord, it is incomplete,
            and should not have data or an ExecRecord.
        """
        # Case 1: Completely recycled ER (reused = true): it should
        # not have any registered dataset)
        if self.reused and self.output.exists():
            raise ValidationError('{} "{}" was reused but has a registered dataset'.format(
                self._cable_type_str(), self
            ))

        # Case 2: Still executing (reused = false): there should be
        # no RSIC.output and no ER
        if not self.reused:
            general_error = '{} "{}" not reused and has no ExecLog'.format(self._cable_type_str(), self)
            if self.output.exists():
                raise ValidationError("{}, but has a Dataset output".format(general_error))
            if self.execrecord.exists():
                raise ValidationError("{}, but has an ExecRecord".format(general_error))

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

        # Check coherence of the times.
        stopwatch.models.Stopwatch.clean(self)

        self._clean_cable_coherent()

        clean_execlogs(self)

        if self.reused is None:
            self._clean_undecided_reused()
        elif self.reused:
            self._clean_reused()
        elif not self._clean_not_reused():
            return

        # If there is no execrecord defined, then exit.
        if self.execrecord is None: return
        self._clean_execrecord()

        self.logger.debug("Checking {}'s ExecLog".format(self._cable_type_str()))

        if self.log.exists():
            self._clean_with_execlog()
        else:
            self._clean_without_execlog()

    def is_complete(self):
        """
        True if RunCable is complete; false otherwise.

        This checks something different depending on whether the RunCable
        is successful or not.  If it's successful, then the only thing
        to check is whether or not the ExecRecord is set.  If it's
        failed, then it checks different conditions:
         - if it failed during recover(), then there should be no ER
           but all ELs are either complete or were never started.
         - if it failed during its own execution, then there should be
           an ExecRecord.

        So, if there is an ExecRecord, then it's complete; if not, then
        check whether its failed and has only completed or never-started
        ExecLogs.
        """
        if self.execrecord is not None:
            return True

        # At this point we know that execrecord == None; check if it
        # failed during recovery and finished.
        if not self.successful_execution():
            for invoked_log in self.invoked_logs.all():
                if not (invoked_log.is_complete() or not invoked_log.has_started()):
                    return False

            # Now we made it through all of the invoked_logs and they were all
            # either complete or never started, so....
            return True

        # Now we know that it's successful but incomplete.
        return False

    def complete_clean(self):
        """Check completeness and coherence of this RunCable."""
        self.clean()
        if not self.is_complete():
            raise ValidationError('{} "{}" is not complete'.format(self._cable_type_str(), self))

    def successful_execution(self):
        """True if RunCable is successful; False otherwise.

        The RunCable is failed if any of its invoked ExecLogs have
        failed.

        PRE: this RunCable is clean, and so are all of its invoked_logs.
        (It's OK that they might not be complete.)
        """
        for invoked_log in self.invoked_logs.all():
            if not invoked_log.is_successful():
                return False
        return True


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

    def _pipeline_cable(self):
        """
        Retrieves the PSIC of this RunSIC.
        """
        return self.PSIC

    def keeps_output(self):
        """
        True if the underlying PSIC retains its output; False otherwise.
        """
        return self.PSIC.keep_output

    def _clean_execrecord(self):
        """
        Check coherence of the RunSIC's associated ExecRecord:

         - it must be complete and clean
         - it must represent a PSIC
         - PSIC is the same as (or compatible to) self.execrecord.general_transf()

        This is a helper for clean().

        PRE
        This RunSIC has an ExecRecord.
        """
        # At this point there must be an associated ER; check that it is
        # clean and complete.
        self.execrecord.complete_clean()

        # Check that PSIC and execrecord.general_transf() are compatible
        # given that the SymbolicDataset represented in the ERI is the
        # input to both.  (This must be true because our Pipeline was
        # well-defined.)
        # TODO: Helpers for transformation
        if self.execrecord.general_transf().__class__.__name__ != "PipelineStepInputCable":
            raise ValidationError('ExecRecord of RunSIC "{}" does not represent a PSIC'.format(self))

        elif not self.PSIC.is_compatible_given_input(self.execrecord.general_transf()):
            raise ValidationError('PSIC of RunSIC "{}" is incompatible with that of its ExecRecord'.format(self))

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

    def get_top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        return self.runstep.get_top_level_run()


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

    def _pipeline_cable(self):
        """
        Retrieves the POC of this RunOutputCable.
        """
        return self.pipelineoutputcable

    def keeps_output(self):
        """
        True if the underlying POC retains its output; False otherwise.
        """
        if self.run.parent_runstep is None:
            return True

        # At this point we know that this is a sub-Pipeline.  Check
        # if the parent PipelineStep deletes this output.
        if self.run.parent_runstep.pipelinestep.outputs_to_delete.filter(
                dataset_idx=self.pipelineoutputcable.output_idx).exists():
            return False
        return True

    def _clean_execrecord(self):
        """
        Check coherence of the RunOutputCable's associated ExecRecord:

         - it must be complete and clean
         - it must represent a POC
         - POC is the same as (or compatible to) self.execrecord.general_transf()

        This is a helper for clean().

        PRE
        This RunOutputCable has an ExecRecord.
        """
        # At this point there must be an associated ER; check that it is
        # clean and complete.
        self.execrecord.complete_clean()

        # ER must point to a cable compatible with the one this RunOutputCable points to.
        if self.execrecord.general_transf().__class__.__name__ != "PipelineOutputCable":
            raise ValidationError('ExecRecord of RunOutputCable "{}" does not represent a POC'.format(self))

        elif not self.pipelineoutputcable.is_compatible(self.execrecord.general_transf()):
            raise ValidationError('POC of RunOutputCable "{}" is incompatible with that of its ExecRecord'.format(self))

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

    def get_top_level_run(self):
        """
        Returns the top-level Run this belongs to.
        """
        return self.run.get_top_level_run()


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
    user = models.ForeignKey(
        User,
        help_text="User that uploaded this Dataset.")

    name = models.CharField(
        max_length=128,
        help_text="Description of this Dataset.")

    description = models.TextField()

    date_created = models.DateTimeField(
        "Date created",
        auto_now_add=True,
        help_text="Date of Dataset creation.")

    # Four cases from which Datasets can originate:
    #
    # Case 1: uploaded
    # Case 2: from the transformation of a RunStep
    # Case 3: from the execution of a POC (i.e. from a ROC)
    # Case 4: from the execution of a PSIC (i.e. from a RunSIC)
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {
            "model__in": ("RunStep", "RunOutputCable",
                          "RunSIC")
        },
        null=True,
        blank=True)
    object_id = models.PositiveIntegerField(null=True, blank=True)
    created_by = generic.GenericForeignKey("content_type", "object_id")

    # Datasets are stored in the "Datasets" folder
    dataset_file = models.FileField(
        upload_to="Datasets",
        help_text="Physical path where datasets are stored",
        null=False)

    # Datasets always have a referring SymbolicDataset
    symbolicdataset = models.OneToOneField(
        "librarian.SymbolicDataset",
        related_name="dataset")

    def __unicode__(self):
        """
        Unicode representation of this Dataset.

        This looks like "[name] (created by [user] on [date])"
        """
        return "{} (created by {} on {})".format(
            self.name, self.user, self.date_created)


    def clean(self):
        """If this Dataset has an MD5 set, verify the dataset file integrity"""
        if not self.check_md5():
            raise ValidationError(
                "File integrity of \"{}\" lost. Current checksum \"{}\" does not equal expected checksum \"{}\"".
                format(self, self.compute_md5(),
                       self.symbolicdataset.MD5_checksum))

    def compute_md5(self):
        """Computes the MD5 checksum of the Dataset."""
        md5gen = hashlib.md5()
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
        """
        # Recompute the MD5, see if it equals what is already stored
        return self.symbolicdataset.MD5_checksum == self.compute_md5()


class ExecLog(stopwatch.models.Stopwatch):
    """
    Logs of Method/PSIC/POC execution.
    Records the start/end times of execution.
    Records *attempts* to run a computation, whether or not it succeeded.

    ELs for methods will also link to a MethodOutput.
    """
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = { "model__in":
                            ("RunStep", "RunOutputCable","RunSIC")},
        related_name="type_belonging_to")
    object_id = models.PositiveIntegerField()
    record = generic.GenericForeignKey("content_type", "object_id")

    content_type_iel = models.ForeignKey(
        ContentType,
        limit_choices_to={"model__in": {"RunStep", "RunOutputCable", "RunSIC"}},
        related_name="type_invoked_by"
    )
    object_id_iel = models.PositiveIntegerField()
    invoking_record = generic.GenericForeignKey("content_type_iel", "object_id_iel")

    # Since this inherits from Stopwatch, it has start_time and end_time.

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

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

        if ((type(self.record) == RunStep) and
                (type(self.record.pipelinestep.transformation) !=
                 method.models.Method)):
            raise ValidationError(
                "ExecLog \"{}\" does not correspond to a Method or cable".
                format(self))

        if self.record.get_top_level_run() != self.invoking_record.get_top_level_run():
            raise ValidationError(
                'ExecLog "{}" belongs to a different Run than its invoking RunStep/RSIC/ROC'.
                format(self)
            )

        # Check that invoking_record is not earlier than record.
        record_coords = self.record.get_coordinates()
        invoking_record_coords = self.invoking_record.get_coordinates()

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
            if type(self.record) == RunOutputCable and type(self.invoking_record) != RunOutputCable:
                raise ValidationError('ExecLog "{}" is invoked earlier than the ROC it belongs to'.format(self))
            elif type(self.record) == RunStep and type(self.invoking_record) == RunSIC:
                raise ValidationError('ExecLog "{}" is invoked earlier than the RunStep it belongs to'.format(self))

    def is_complete(self):
        """
        If this is a RunStep, specifically a method, it must have a methodoutput to be complete.
        It also must have a defined start and end time.
        """
        if not self.has_ended():
            return False

        if (self.record.__class__.__name__ == "RunStep" and
                self.record.pipelinestep.transformation.__class__.__name__ == "Method"):
            if not hasattr(self, "methodoutput"):
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
            if hasattr(ccl, "baddata") and ccl.baddata.missing_output:
                missing.append(ccl.symbolicdataset)

        self.logger.debug("returning missing outputs '{}'".format(missing))
        return missing

    def is_successful(self):
        """
        True if this execution is successful (so far); False otherwise.

        Note that the execution may still be in progress when we call this;
        this function tells us if anything has gone wrong so far.
        """
        # If this ExecLog has a MethodOutput, check its return code.
        if (hasattr(self, "methodoutput") and
                self.methodoutput.return_code != 0):
            return False

        for icl in self.integrity_checks.all():
            if icl.is_fail():
                return False

        for ccl in self.content_checks.all():
            if ccl.is_fail():
                return False

        # Having reached here, we are comfortable with the execution --
        # note that it may still be in progress!
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
    execlog = models.OneToOneField(
        ExecLog,
        related_name="methodoutput")

    return_code = models.IntegerField("return code", null=True)

    output_log = models.FileField(
        "output log",
        upload_to="Logs",
        help_text="Terminal output of the RunStep Method, i.e. stdout.")

    error_log = models.FileField(
        "error log",
        upload_to="Logs",
        help_text="Terminal error output of the RunStep Method, i.e. stderr.")
