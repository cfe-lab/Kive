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

import file_access_utils

import method.models
import pipeline.models
import archive.models
import librarian.models
import transformation.models

class Run(models.Model):
    """
    Stores data associated with an execution of a pipeline.

    Related to :model:`pipeline.models.Pipeline`
    Related to :model:`archive.models.RunStep`
    Related to :model:`archive.models.Dataset`
    """
    user = models.ForeignKey(User, help_text="User who performed this run")
    start_time = models.DateTimeField("start time", auto_now_add=True,
                                      help_text="Time at start of run")
    pipeline = models.ForeignKey(
        pipeline.models.Pipeline,
        related_name="pipeline_instances",
        help_text="Pipeline used in this run")

    name = models.CharField("Run name", max_length=256)
    description = models.TextField("Run description", blank=True)
    
    # If run was spawned within another run, parent_runstep denotes
    # the run step that initiated it
    parent_runstep = models.OneToOneField(
        "RunStep",
        related_name="child_run",
        null=True,
        blank=True,
        help_text="Step of parent run initiating this one as a sub-run")

    # November 15, 2013: Runs no longer have ExecRecords.
    # execrecord = models.ForeignKey(
    #     "librarian.ExecRecord",
    #     null=True,
    #     blank=True,
    #     related_name="runs",
    #     help_text="Record of this run");

    # reused = models.NullBooleanField(
    #     help_text="Indicates whether this run uses the record of a previous execution",
    #     default=None);

    # November 15, 2013: changed to remove the ExecRecord and the
    # reused flag, and also to reflect that even a reused run must
    # have all its steps in place.
    def clean(self):
        """
        Checks coherence of the run (possibly in an incomplete state).

        The procedure:
         - if parent_runstep is not None, then pipeline should be
           consistent with it
         - check RSs; no RS should be associated without the previous
           ones being complete
         - if not all RSs are complete, no ROCs should be associated,
           ER should not be set
          (from here on all RSs are assumed to be complete)
           - clean all associated ROCs
        """
        if (self.parent_runstep != None and
                self.pipeline != self.parent_runstep.pipelinestep.transformation):
            raise ValidationError(
                "Pipeline of Run \"{}\" is not consistent with its parent RunStep".
                format(self))
                
        # Go through whatever steps are registered.
        most_recent_step = None
        steps_associated = None
        if self.runsteps.all().exists():
    
            # Check that steps are proceeding in order.  (Multiple quenching
            # of steps is taken care of already.)
            steps_associated = sorted(
                [rs.pipelinestep.step_num for rs in self.runsteps.all()])
    
            if steps_associated != range(1, len(steps_associated)+1):
                raise ValidationError(
                    "RunSteps of Run \"{}\" are not consecutively numbered starting from 1".
                    format(self))
    
            # All steps prior to the last registered one must be complete.
            for curr_step_num in steps_associated[:-1]:
                self.runsteps.get(pipelinestep__step_num=curr_step_num).complete_clean()
    
            # The most recent step should be clean.
            most_recent_step = self.runsteps.get(
                pipelinestep__step_num=steps_associated[-1])
            most_recent_step.clean()

        # If all steps are not complete, then no ROCs should be
        # associated.
        if (not self.runsteps.all().exists() or
                steps_associated[-1] < self.pipeline.steps.count() or 
                not most_recent_step.is_complete()):
            if self.runoutputcables.all().exists():
                raise ValidationError(
                    "Run \"{}\" has not completed all of its RunSteps, so there should be no associated RunOutputCables".
                    format(self))
            return
        
        # From this point on, all RunSteps are assumed to be complete.
        
        # Run clean on all of its outcables.
        for run_outcable in self.runoutputcables.all():
            run_outcable.clean()
    
    def is_complete(self):
        """True if this run is complete; false otherwise."""
        # A run is complete if all of its outcables are complete.
        for outcable in self.pipeline.outcables.all():
            corresp_roc = self.runoutputcables.filter(pipelineoutputcable=outcable)
            if not corresp_roc.exists() or not corresp_roc[0].is_complete():
                return False

        return True
            
    def complete_clean(self):
        """Checks completeness and coherence of a run."""
        self.clean()
        if not self.is_complete():
            raise ValidationError(
                "Run \"{}\" is not complete".format(self))

        
    # FIXME fill this in!
    def failed_execution(self):
        pass


class RunStep(models.Model):
    """
    Annotates the execution of a pipeline step within a run.

    Related to :model:`archive.models.Run`
    Related to :model:`librarian.models.ExecRecord`
    Related to :model:`pipeline.models.PipelineStep`
    """
    run = models.ForeignKey(Run, related_name="runsteps")

    # If this RunStep has a child_run, then this execrecord should be
    # null.
    execrecord = models.ForeignKey(
        librarian.models.ExecRecord,
        null=True, blank=True,
        related_name="runsteps")
    reused = models.NullBooleanField(
        default=None,
        help_text="Denotes whether this run step reuses a previous execution")
    pipelinestep = models.ForeignKey(
        pipeline.models.PipelineStep,
        related_name="pipelinestep_instances")

    start_time = models.DateTimeField("start time", auto_now_add=True,
                                      help_text="Time at start of run step")

    log = generic.GenericRelation("ExecLog")

    outputs = generic.GenericRelation("Dataset")

    class Meta:
        # Uniqueness constraint ensures you can't have multiple RunSteps for
        # a given PipelineStep within a Run.
        unique_together = ("run", "pipelinestep")

    def clean(self):
        """
        Check coherence of this RunStep.

        The checks we perform, in sequence:
         - pipelinestep is consistent with run
         - if pipelinestep is for a method, there should be no
           child_run

         - if pipelinestep is for a pipeline, there should be no
           ExecLog or Datasets associated, reused = None, and
           execrecord = None

         - if an ExecLog is associated, check that it is clean

         - if any RSICs exist, check they are clean and complete.
         
         - if all RSICs are not quenched, reused, child_run, and
           execrecord should not be set, no ExecLog should be
           associated and no Datasets should be associated

        (from here on all RSICs are assumed to be quenched)

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

         - else if this is a Pipeline:
           - clean child_run if it exists

        (from here on, it is assumed that this is a Method and
         execrecord is set)

         - check that it is complete and clean
         - check that it's coherent with pipelinestep
         
         - if an output is marked for deletion or missing, there
           should be no associated Dataset

         - else:
           - the corresponding ERO should have an associated Dataset.

         - any associated Dataset belongs to an ERO (this checks for
           Datasets that have been wrongly assigned to this RunStep).
        
        Note: don't need to check inputs for multiple quenching due to
        uniqueness.  Quenching of outputs is checked by ExecRecord.
        """
        # Does pipelinestep belong to run.pipeline?
        if not self.run.pipeline.steps.filter(pk=self.pipelinestep.pk).exists():
            raise ValidationError(
                "PipelineStep \"{}\" of RunStep \"{}\" does not belong to Pipeline \"{}\"".
                format(self.pipelinestep, self, self.run.pipeline))

        # If the PS stores a method, it should have no child_run.
        # (Should not act as a parent runstep)
        if (type(self.pipelinestep.transformation) == method.models.Method and
                hasattr(self,"child_run") == True):
            raise ValidationError(
                "PipelineStep of RunStep \"{}\" is not a Pipeline but a child run exists".
                format(self))

        elif (type(self.pipelinestep.transformation) ==
              pipeline.models.Pipeline):
            if self.log.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" represents a sub-pipeline so no log should be associated".
                    format(self))
            
            if self.outputs.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" represents a sub-pipeline and should not have generated any data".
                    format(self))

            if self.reused != None:
                raise ValidationError(
                    "RunStep \"{}\" represents a sub-pipeline so reused should not be set".
                    format(self))

            if self.execrecord != None:
                raise ValidationError(
                    "RunStep \"{}\" represents a sub-pipeline so execrecord should not be set".
                    format(self))

        if self.log.all().exists():
            self.log.clean()
        
        for rsic in self.RSICs.all():
            rsic.complete_clean()

        if (self.pipelinestep.cables_in.count() != self.RSICs.count()):
            if (self.reused != None or self.execrecord != None):
                raise ValidationError(
                    "RunStep \"{}\" inputs not quenched; reused and execrecord should not be set".
                    format(self))
            if (type(self.pipelinestep.transformation) == pipeline.models.Pipeline and
                    hasattr(self, "child_run")):
                raise ValidationError(
                    "RunStep \"{}\" inputs not quenched; child_run should not be set".
                    format(self))
            if self.log.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" inputs not quenched; no log should have been generated".
                    format(self))
            if self.outputs.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" inputs not quenched; no data should have been generated".
                    format(self))
            return

        # From here on, RSICs are assumed to be quenched.
        if (self.reused == None and type(
                self.pipelinestep.transformation) == method.models.Method):
            if self.log.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" has not decided whether or not to reuse an ExecRecord; no log should have been generated".
                    format(self))
            
            if self.outputs.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" has not decided whether or not to reuse an ExecRecord; no data should have been generated".
                    format(self))
                    
            if self.execrecord != None:
                raise ValidationError(
                    "RunStep \"{}\" has not decided whether or not to reuse an ExecRecord; execrecord should not be set".
                    format(self))
            return

        elif self.reused and type(
                self.pipelinestep.transformation) == method.models.Method:
            if self.outputs.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" reused an ExecRecord and should not have generated any Datasets".
                    format(self))

        elif not self.reused and type(
                self.pipelinestep.transformation) == method.models.Method:

            if (not self.log.all().exists() or
                    not self.log.all()[0].is_complete()):
                if self.outputs.all().exists():
                    raise ValidationError(
                        "RunStep \"{}\" does not have a complete log so should not have generated any Datasets".
                        format(self))
                
                if self.execrecord != None:
                    raise ValidationError(
                        "RunStep \"{}\" does not have a complete log; execrecord should not be set".
                        format(self))
                return

            # From here on, ExecLog is assumed to be complete and clean.
                
            for out_data in self.outputs.all():
                out_data.clean()
    
        elif type(self.pipelinestep.transformation) == pipeline.models.Pipeline:
            if hasattr(self, "child_run"):
                self.child_run.clean()
            return

        # From here on, we know that this is a Method.
        step_er = self.execrecord
        if step_er == None:
            return

        # From here on, the appropriate ER is known to be set.
        step_er.complete_clean()

        # ER must point to the same transformation that this runstep points to
        if self.pipelinestep.transformation != step_er.general_transf():
            raise ValidationError(
                "RunStep \"{}\" points to transformation \"{}\" but corresponding ER does not".
                format(self, self.pipelinestep))

        # November 18, 2013: get the missing outputs.
        outputs_missing = self.log.all()[0].missing_outputs()

        # Go through all of the outputs.
        to_type = ContentType.objects.get_for_model(
            transformation.models.TransformationOutput)
        
        for to in self.pipelinestep.transformation.outputs.all():
            # Get the associated ERO.
            corresp_ero = step_er.execrecordouts.get(
                content_type=to_type, object_id=to.id)
            
            if self.pipelinestep.outputs_to_delete.filter(
                    dataset_name=to.dataset_name).exists():
                # This output is deleted; there should be no associated Dataset.
                if self.outputs.filter(symbolicdataset=corresp_ero.symbolicdataset).exists():
                    raise ValidationError(
                        "Output \"{}\" of RunStep \"{}\" is deleted; no data should be associated".
                        format(to, self))

            elif to in outputs_missing:
                # This output is missing; there should be no associated Dataset.
                if self.outputs.filter(
                        symbolicdataset=corresp_ero.symbolicdataset).exists():
                    raise ValidationError(
                        "Output \"{}\" of RunStep \"{}\" is missing; no data should be associated".
                        format(to, self))
                
            else:
                # The corresponding ERO should have existent data.
                corresp_ero = step_er.execrecordouts.get(
                    content_type=to_type, object_id=to.id)
                if not corresp_ero.symbolicdataset.has_data():
                    raise ValidationError(
                        "ExecRecordOut \"{}\" of RunStep \"{}\" should reference existent data".
                        format(corresp_ero, self))

        # Check that any associated data belongs to an ERO of this ER
        for out_data in associated_datasets:
            if not step_er.execrecordouts.filter(
                    symbolicdataset=out_data.symbolicdataset).exists():
                raise ValidationError(
                    "RunStep \"{}\" generated Dataset \"{}\" but it is not in its ExecRecord".
                    format(self, out_data))

    def is_complete(self):
        """True if RunStep is complete; false otherwise."""
        # Sub-Pipeline case:
        if hasattr(self, "child_run"):
            return self.child_run.is_complete()
        # Method case:
        return step_er != None
    
    def complete_clean(self):
        """Checks coherence and completeness of this RunStep."""
        self.clean()
        if not self.is_complete():
            raise ValidationError(
                "RunStep \"{}\" is not complete".format(self))

    # FIXME fill this in!
    def failed_execution(self):
        """
        True if RunStep is failed; false otherwise.
        
        The RunStep is failed if any of its cables fail, or if the ER
        was tainted at the time of the RunStep's completion.
        """
        pass

class RunSIC(models.Model):
    """
    Annotates the action of a PipelineStepInputCable within a RunStep.

    Related to :model:`archive.models.RunStep`
    Related to :model:`librarian.models.ExecRecord`
    Related to :model:`pipeline.models.PipelineStepInputCable`
    """
    runstep = models.ForeignKey(RunStep, related_name="RSICs")
    execrecord = models.ForeignKey(
        librarian.models.ExecRecord,
        null=True,
        blank=True,
        related_name="RSICs")
    reused = models.NullBooleanField(
        help_text="Denotes whether this run reused the action of an output cable",
        default=None)
    PSIC = models.ForeignKey(
        pipeline.models.PipelineStepInputCable,
        related_name="psic_instances")

    start_time = models.DateTimeField(
        "start time", auto_now_add=True,
        help_text="Time at start of running this input cable")

    log = generic.GenericRelation("ExecLog")
    output = generic.GenericRelation("Dataset")

    class Meta:
        # Uniqueness constraint ensures that no POC is multiply-represented
        # within a run step.
        unique_together = ("runstep", "PSIC")

    def clean(self):
        """
        Check coherence of this RunSIC.

        In sequence, the checks we perform:
         - PSIC belongs to runstep.pipelinestep

         - if an ExecLog is attached, clean it
         
         - if reused is None (no decision on reusing has been made),
           no log should be associated, no data should be associated,
           and execrecord should not be set

         - else if reused is True, no data should be associated.
         - else if reused is False:

           - if no ExecLog is attached yet or it is not complete,
             there should be no associated Dataset

           (from here on ExecLog is known to be attached and complete)

           - if the cable is trivial, there should be no associated Dataset
           - otherwise, make sure there is at most one Dataset, and clean it
             if it exists
        (from here on execrecord is assumed to be set)
         - it must be complete and clean
         - PSIC is the same as (or compatible to) self.execrecord.general_transf()
         
         - if this RunSIC does not keep its output or its output is
           missing, there should be no existent data associated.
           
         - else:
           - the corresponding ERO should have existent data associated

           - if the PSIC is not trivial and this RunSIC does not reuse an ER,
             then there should be existent data associated and it should also
             be associated to the corresponding ERO.
        """
        if (not self.runstep.pipelinestep.cables_in.
                filter(pk=self.PSIC.pk).exists()):
            raise ValidationError(
                "PSIC \"{}\" does not belong to PipelineStep \"{}\"".
                format(self.PSIC, self.runstep.pipelinestep))

        if self.log.all().exists():
            self.log.complete_clean()

        if self.reused == None:
            if self.log.all().exists():
                raise ValidationError(
                    "RunSIC \"{}\" has not decided whether or not to reuse an ExecRecord; no log should have been generated".
                    format(self))
            if self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" has not decided whether or not to reuse an ExecRecord; no Datasets should be associated".
                    format(self))
            if self.execrecord != None:
                raise ValidationError(
                    "RunSIC \"{}\" has not decided whether or not to reuse an ExecRecord; execrecord should not be set yet".
                    format(self))

        elif self.reused:
            if self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" reused an ExecRecord and should not have generated any Datasets".
                    format(self))

        else:
            if (not self.log.all().exists() or
                    not self.log.all()[0].is_complete()):
                if self.has_data():
                    raise ValidationError(
                        "RunSIC \"{}\" does not have a complete log so should not have generated any Datasets".
                        format(self))
                return

            # From here on, the ExecLog is known to be complete.
            
            # If this cable is trivial, there should be no data
            # associated.
            if self.PSIC.is_trivial() and self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" is trivial and should not have generated any Datasets".
                    format(self))

            # Otherwise, check that there is at most one Dataset
            # attached, and clean it.
            elif self.has_data():
                if self.output.count() > 1:
                    raise ValidationError(
                        "RunSIC \"{}\" should generate at most one Dataset".
                        format(self))
                self.output.all()[0].clean()
        
        # If there is no execrecord defined, then exit.
        if self.execrecord == None:
            return
        
        # At this point there must be an associated ER; check that it is
        # clean and complete.
        self.execrecord.complete_clean()

        # Check that PSIC and execrecord.general_transf() are compatible
        # given that the SymbolicDataset represented in the ERI is the
        # input to both.  (This must be true because our Pipeline was
        # well-defined.)
        if (type(self.execrecord.general_transf()) !=
                pipeline.models.PipelineStepInputCable):
            raise ValidationError(
                "ExecRecord of RunSIC \"{}\" does not represent a PSIC".
                format(self.PSIC))
        
        elif not self.PSIC.is_compatible_given_input(
                self.execrecord.general_transf()):
            raise ValidationError(
                "PSIC of RunSIC \"{}\" is incompatible with that of its ExecRecord".
                format(self.PSIC))

        # Check whether this has a missing output.
        missing_output = len(self.log.missing_outputs()) != 0

        # If the output of this PSIC is not marked to keep, there should be
        # no data associated.
        if not self.PSIC.keep_output:
            if self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" does not keep its output; no data should be produced".
                    format(self))


        # Similar if the ExecLog shows missing output.
        elif missing_output:
            if self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" had a missing output; no data should be produced".
                    format(self))
            
        else:
            # The corresponding ERO should have existent data.
            corresp_ero = self.execrecord.execrecordouts.all()[0]
            if not corresp_ero.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" keeps its output; ExecRecordOut \"{}\" should reference existent data".
                    format(self, corresp_ero))

            # If reused == False and the cable is not trivial,
            # there should be associated data, and it should match that
            # of corresp_ero.
            if not self.reused and not self.PSIC.is_trivial():
                if not self.has_data():
                    raise ValidationError(
                        "RunSIC \"{}\" was not reused, trivial, or deleted; it should have produced data".
                        format(self))

                if corresp_ero.symbolicdataset.dataset != self.output.all()[0]:
                    raise ValidationError(
                        "Dataset \"{}\" was produced by RunSIC \"{}\" but is not in an ERO of ExecRecord \"{}\"".
                        format(self.output.all()[0], self, self.execrecord))

    def is_complete(self):
        """True if RunSIC is complete; false otherwise."""
        return self.execrecord != None

    def complete_clean(self):
        """Check completeness and coherence of this RunSIC."""
        self.clean()
        if not self.is_complete():
            raise ValidationError(
                "RunSIC \"{}\" has no ExecRecord".format(self))

    def has_data(self):
        """True if associated output exists; False if not."""
        return self.output.all().exists()

    # FIXME fill this in!
    def failed_execution(self):
        pass

class RunOutputCable(models.Model):
    """
    Annotates the action of a PipelineOutputCable within a run.

    Related to :model:`archive.models.Run`
    Related to :model:`librarian.models.ExecRecord`
    Related to :model:`pipeline.models.PipelineOutputCable`
    """
    run = models.ForeignKey(Run, related_name="runoutputcables")
    execrecord = models.ForeignKey(
        librarian.models.ExecRecord,
        null=True, blank=True,
        related_name="runoutputcables")
    reused = models.NullBooleanField(
        help_text="Denotes whether this run reused the action of an output cable",
        default=None)
    pipelineoutputcable = models.ForeignKey(
        pipeline.models.PipelineOutputCable,
        related_name="poc_instances")
    
    start_time = models.DateTimeField(
        "start time", auto_now_add=True,
        help_text="Time at start of running this output cable")

    log = generic.GenericRelation("ExecLog")
    output = generic.GenericRelation("Dataset")

    class Meta:
        # Uniqueness constraint ensures that no POC is
        # multiply-represented within a run.
        unique_together = ("run", "pipelineoutputcable")


    def clean(self):
        """
        Check coherence of this RunOutputCable.

        In sequence, the checks we perform are:
         - pipelineoutputcable belongs to run.pipeline

         - if an ExecLog is attached, complete_clean it

         - if no decision has been made on reuse, check that no log is
           associated, no data has been associated, and that ER is
           unset

         - else if it has been decided to reuse an ER, check that there
           are no associated datasets
         
         - else if it has been decided not to reuse an ER:
         
           - if this cable is trivial, there should be no associated
             dataset

           - if no ExecLog is attached yet or it is incomplete, there
             should be no associated dataset yet

           (from here on, ExecLog is known to be attached and complete)

           - otherwise, clean any associated dataset

        (after this point it is assumed that ER is set)
         - check that it is complete and clean
         - check that it's coherent with pipelineoutputcable

         - if this ROC's output was marked for deletion or
           missing output, then no data should be associated

         - else the corresponding ERO should have existent data
           associated

           - if this ROC did not reuse an ER and the cable is not
             trivial, then this ROC should have existent data
             associated and it should belong to the corresponding ERO
        """
        if (not self.run.pipeline.outcables.
                filter(pk=self.pipelineoutputcable.pk).exists()):
            raise ValidationError(
                "POC \"{}\" does not belong to Pipeline \"{}\"".
                format(self.pipelineoutputcable, self.run.pipeline))

        if self.log.all().exists():
            self.log.all()[0].complete_clean()

        if self.reused == None:
            if self.log.all().exists():
                raise ValidationError(
                    "RunOutputCable \"{}\" has not decided whether or not to reuse an ExecRecord; no ExecLog should be associated".
                    format(self))
            
            if self.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" has not decided whether or not to reuse an ExecRecord; no Datasets should be associated".
                    format(self))

            if self.execrecord != None:
                raise ValidationError(
                    "RunOutputCable \"{}\" has not decided whether or not to reuse an ExecRecord; execrecord should not be set yet".
                    format(self))

        elif self.reused:
            if self.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" reused an ExecRecord and should not have generated any Datasets".
                    format(self))
        else:
            # If this cable is trivial, there should be no data associated.
            if self.pipelineoutputcable.is_trivial():
                if self.has_data():
                    raise ValidationError(
                        "RunOutputCable \"{}\" is trivial and should not have generated any Datasets".
                        format(self))

            if (not self.log.all().exists() or
                    not self.log.all()[0].is_complete()):
                if self.has_data():
                    raise ValidationError(
                        "RunOutputCable \"{}\" does not have a complete log so should not have generated any Datasets".
                        format(self))

                return

            # From here on, ExecLog is known to be appropriately
            # attached and complete.

            # Otherwise, check that there is at most one Dataset
            # attached, and clean it.
            elif self.has_data():
                if self.output.count() > 1:
                    raise ValidationError(
                        "RunOutputCable \"{}\" should generate at most one Dataset".
                        format(self))
                self.output.all()[0].clean()

        if self.execrecord == None:
            return

        # self.execrecord is set, so complete_clean it.
        self.execrecord.complete_clean()

        # The ER must point to a cable that is compatible with the one
        # this RunOutputCable points to.
        if type(self.execrecord.general_transf()) != pipeline.models.PipelineOutputCable:
            raise ValidationError(
                "ExecRecord of RunOutputCable \"{}\" does not represent a POC".
                format(self))
        
        elif not self.pipelineoutputcable.is_compatible(self.execrecord.general_transf()):
            raise ValidationError(
                "POC of RunOutputCable \"{}\" is incompatible with that of its ExecRecord".
                format(self))

        is_deleted = False
        if self.run.parent_runstep != None:
            is_deleted = self.run.parent_runstep.pipelinestep.outputs_to_delete.filter(
                dataset_name=self.pipelineoutputcable.output_name).exists()

        # November 18, 2013: check if there was missing output
        # (i.e. some kind of messed up execution) in the ExecLog.
        missing_output = len(self.log.missing_outputs()) != 0
            
        # If the output of this ROC is marked for deletion, there should be no data
        # associated.
        if is_deleted:
            if self.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" is marked for deletion; no data should be produced".
                    format(self))

        # If the output of this ROC was missing on execution, there
        # should be no data associated.
        elif missing_output:
            if self.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" had a missing output; no data should be produced".
                    format(self))
            
        # If it isn't marked for deletion and the output isn't missing....
        else:
            # The corresponding ERO should have existent data.
            corresp_ero = self.execrecord.execrecordouts.get(execrecord=self.execrecord)
            if not corresp_ero.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" was not deleted and did not have missing output; ExecRecordOut \"{}\" should reference existent data".
                    format(self, corresp_ero))

            # If the step was not reused and the cable was not
            # trivial, there should be data associated to this ROC.
            if not self.reused and not self.pipelineoutputcable.is_trivial():
                if not self.has_data():
                    raise ValidationError(
                        "RunOutputCable \"{}\" was not reused, trivial, or deleted; it should have produced data".
                        format(self))

                # The associated data should belong to the ERO of
                # self.execrecord (which has already been checked for
                # completeness and cleanliness).
                if (not self.execrecord.execrecordouts.filter(
                        symbolicdataset=self.output.all()[0].symbolicdataset).
                        exists()):
                    raise ValidationError(
                        "Dataset \"{}\" was produced by RunOutputCable \"{}\" but is not in an ERO of ExecRecord \"{}\"".
                        format(self.output.all()[0], self, self.execrecord))

            

    def is_complete(self):
        """True if ROC is finished running; false otherwise."""
        return self.execrecord != None

    def complete_clean(self):
        """Check completeness and coherence of this RunOutputCable."""
        self.clean()
        if not self.is_complete():
            raise ValidationError(
                "RunOutputCable \"{}\" has no ExecRecord".format(self))

    def has_data(self):
        """True if associated output exists; False if not."""
        return self.output.all().exists()



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
        librarian.models.SymbolicDataset,
        related_name="dataset")

    def __unicode__(self):
        """
        Unicode representation of this Dataset.
        
        This looks like "[name] (created by [user] on [date])"
        """
        return "{} (created by {} on {})".format(
            self.name, self.user, self.date_created)


    def clean(self):
        """
        Check file integrity of this Dataset.
        """
        if not self.check_md5():
            raise ValidationError(
                "File integrity of \"{}\" lost.  Current checksum \"{}\" does not equal expected checksum \"{}\"".
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
    
class ExecLog(models.Model):
    """
    Logs of Method/PSIC/POC execution.

    Records the start and end times of execution; if the log is for
    a Method, links to a MethodOutput.

    This is meant to capture an *attempt* to run an atomic operation,
    whether or not it was successful.
    """
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {
            "model__in": ("RunStep", "RunOutputCable",
                          "RunSIC")
        })
    object_id = models.PositiveIntegerField()
    record = generic.GenericForeignKey("content_type", "object_id")

    start_time = models.DateTimeField(
        "start time", auto_now_add=True,
        help_text="Time at start of execution")

    end_time = models.DateTimeField(
        "end time",
        help_text="Time at end of execution")

    def clean(self):
        """
        Checks coherence of this ExecLog.

        If this ExecLog is for a RunStep, the RunStep represents a
        Method (as opposed to a Pipeline).
        """
        if ((type(self.record) == RunStep) and 
                (type(self.record.pipelinestep.transformation) !=
                 method.models.Method):
            raise ValidationError(
                "ExecLog \"{}\" does not correspond to a Method or cable".
                format(self))

    def is_complete(self):
        """True if ExecLog is complete; False otherwise."""
        if (type(self.record == RunStep) and
                type(self.record.pipelinestep.transformation) ==
                method.models.Method):
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
            raise ValidationError(
                "ExecLog \"{}\" represents a Method but has no associated MethodOutput".
                format(self))

    def missing_outputs(self):
        """Returns the output SDs missing output from this execution."""
        missing = []
        for ccl in content_checks:
            if hasattr(ccl, "baddata") and ccl.baddata.missing_output:
                missing.append(ccl.symbolicdataset)

        return missing

class MethodOutput(models.Model):
    """
    Logged output of the execution of a method.

    This stores the stdout and stderr output, as well as the process'
    return code.
    """
    execlog = models.OneToOneField(
        ExecLog,
        related_name="methodoutput")

    return_code = models.IntegerField("return code")
    
    output_log = models.FileField(
        "output log",
        upload_to="Logs",
        help_text="Terminal output of the RunStep Method, i.e. stdout.")
    
    error_log = models.FileField(
        "error log",
        upload_to="Logs",
        help_text="Terminal error output of the RunStep Method, i.e. stderr.")
