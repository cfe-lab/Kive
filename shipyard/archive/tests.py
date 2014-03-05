"""
Shipyard archive application unit tests.
"""

from django.test import TestCase
from django.core.files import File
from django.core.exceptions import ValidationError
from django.utils import timezone

import os, sys
import tempfile, shutil
import random
import logging
import time

from librarian.models import *
from archive.models import *
from method.models import *
from metadata.models import *
from pipeline.models import *
from method.tests import samplecode_path
import librarian.tests
from file_access_utils import compute_md5

# TODO: Put this someplace better, maybe shipyard/testing_utils.py?
from sandbox.tests_rm import clean_files

# Note that these tests use the exact same setup as librarian.
def make_complete_non_reused(record, input_SDs, output_SDs):
    """
    Helper function to do everything necessary to make a RunStep, 
    RunOutputCable, or RunStepInputCable complete, when it has not
    reused an ExecRecord (ie. make a new ExecRecord).
    """
    record_type = record.__class__.__name__
    record.reused = False

    execlog = ExecLog(record=record, start_time=timezone.now(), end_time=timezone.now())
    execlog.save()
    if record_type == "RunStep":
        MethodOutput(execlog=execlog, return_code=0).save()
    execrecord = ExecRecord(generator=execlog)
    execrecord.save()

    # TODO: This needs a helper get_pipeline_component or something.
    if record_type == "RunStep":
        inputs = list(record.pipelinestep.transformation.inputs.all())
        outputs = list(record.pipelinestep.transformation.outputs.all())
    elif record_type == "RunOutputCable":
        inputs = [record.pipelineoutputcable.source]
        transf = record.run.pipeline
        outputs = [transf.outputs.get(dataset_idx=record.pipelineoutputcable.output_idx)]
    else:
        inputs = [record.PSIC.source]
        outputs = [record.PSIC.dest]

    for i, inp in enumerate(inputs):
        execrecord.execrecordins.create(generic_input=inp, symbolicdataset=input_SDs[i])
    for i, outp in enumerate(outputs):
        execrecord.execrecordouts.create(generic_output=outp, symbolicdataset=output_SDs[i])
        if record_type == "RunOutputCable" and not record.pipelineoutputcable.is_trivial():
            record.output.add(output_SDs[i].dataset)

    record.execrecord = execrecord
    record.clean()
    record.save()

def complete_RSICs(runstep, input_SDs, output_SDs):
    """
    Helper function to create and complete all the RunSIC's needed for
    a given RunStep. input_SDs and output_SDs are lists of the input and
    output symbolic datasets for each cable, in order.
    """
    for i, cable in enumerate(runstep.pipelinestep.cables_in.order_by("dest__dataset_idx")):
        rsic = cable.psic_instances.create(runstep=runstep)
        make_complete_non_reused(rsic, [input_SDs[i]], [output_SDs[i]])

class RunStepTests(librarian.tests.LibrarianTestSetup):

    def tearDown(self):
        super(self.__class__, self).tearDown()
        clean_files()

    def test_runstep_many_execlogs(self):
        run = self.pE.pipeline_instances.create(user=self.myUser)
        run_step = self.step_E1.pipelinestep_instances.create(run=run)
        run_step.reused = False
        for i in range(2):
            run_step.log.create(start_time=timezone.now(),
                                end_time=timezone.now())
        self.assertRaisesRegexp(ValidationError,
                re.escape('RunStep "{}" has {} ExecLogs but should have only one'.
                          format(run_step, 2)),
                run_step.clean)

    def step_through_runstep_creation(self, bp):
        """
        Helper function to step through creation of a RunStep, breaking
        at a certain point (see the code for what these points are).
        """
        self.pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        if bp == "empty_runs": return

        self.step_E1_RS = self.step_E1.pipelinestep_instances.create(run=self.pE_run)
        if bp == "first_runstep": return

        self.E03_11_RSIC = self.E03_11.psic_instances.create(runstep=self.step_E1_RS)
        make_complete_non_reused(self.E03_11_RSIC, [self.raw_symDS], [self.raw_symDS])
        if bp == "first_rsic": return 
        
        make_complete_non_reused(self.step_E1_RS, [self.raw_symDS], [self.doublet_symDS])
        if bp == "first_runstep_complete": return

        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        if bp == "second_runstep": return

        complete_RSICs(self.step_E2_RS, [self.triplet_symDS, self.singlet_symDS], 
                                        [self.D1_in_symDS, self.singlet_symDS])
        if bp == "second_runstep_complete": return
        
        # Associate and complete sub-Pipeline.
        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
        self.step_D1_RS = self.step_D1.pipelinestep_instances.create(run=self.pD_run)
        complete_RSICs(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], 
                                        [self.D1_in_symDS, self.singlet_symDS])
        make_complete_non_reused(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], [self.C1_in_symDS])
        pD_ROC = self.pD.outcables.first().poc_instances.create(run=self.pD_run)
        make_complete_non_reused(pD_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
        if bp == "sub_pipeline": return

    def test_RunStep_clean_wrong_pipeline(self):
        """
        A RunStep which has a PipelineStep from one Pipeline, but a Run
        for a different Pipeline, is not clean.
        """
        self.step_through_runstep_creation("empty_runs")
        runstep = self.step_D1.pipelinestep_instances.create(run=self.pE_run)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('PipelineStep "{}" of RunStep "{}" does not belong to Pipeline "{}"'
                                          .format(self.step_D1, runstep, self.pE)),
                                runstep.clean)

    def test_RunStep_clean_child_run_for_method(self):
        """
        A RunStep which represents a Method should not have a child_run
        defined.
        """
        self.step_through_runstep_creation("first_runstep")
        self.pD_run.parent_runstep = self.step_E1_RS
        self.pD_run.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('PipelineStep of RunStep "{}" is not a Pipeline but a child run exists'
                                          .format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_clean_no_RunSICs(self):
        """
        A RunStep with no RunSIC's is clean.
        """
        self.step_through_runstep_creation("first_runstep")
        self.assertIsNone(self.step_E1_RS.clean())

    def test_RunStep_clean_incomplete_RunSIC(self):
        """
        A RunStep with an incomplete RunSIC is not clean.
        """
        self.step_through_runstep_creation("first_rsic")
        self.E03_11_RSIC.execrecord = None
        self.E03_11_RSIC.save()
        self.assertRaisesRegexp(ValidationError, re.escape('RunSIC "{}" has no ExecRecord'.format(self.E03_11_RSIC)),
                                self.step_E1_RS.clean)

    def test_RunStep_complete_RunSIC(self):
        """
        A RunStep with a complete RunSIC is clean.
        """
        self.step_through_runstep_creation("first_rsic")
        self.assertIsNone(self.step_E1_RS.clean())

    def test_RunStep_inputs_unquenched_with_data(self):
        """
        A RunStep with unquenched input cables, but an associated
        Dataset, is not clean.
        """
        self.step_through_runstep_creation("first_runstep")
        self.doublet_DS.created_by = self.step_E1_RS
        self.doublet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" inputs not quenched; no data should have been generated'
                                          .format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_input_unquenched_with_reused(self):
        """
        A RunStep with unquenched input cables, but which has decided
        not to reuse an ExecRecord, is not clean.
        """
        self.step_through_runstep_creation("first_runstep")
        self.step_E1_RS.reused = False
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" inputs not quenched; reused and execrecord should not be set'
                                          .format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_input_unquenched_with_execrecord(self):
        """
        A RunStep with unquenched input cables, but which has an
        associated ExecRecord, is not clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")
        self.E03_11_RSIC.delete()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" inputs not quenched; reused and execrecord should not be set'
                                          .format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_input_unquenched_with_child_run(self):
        """
        A RunStep with unquenched input cables, but which has a
        child_run, is not clean.
        """
        self.step_through_runstep_creation("second_runstep")
        pD_run.parent_runstep = step_E2_RS
        pD_run.save()
        self.assertRaisesRegexp(ValidationError, 
                                re.escape('RunStep "{}" inputs not quenched; child_run should not be set'
                                          .format(self.step_E2_RS)),
                                self.step_E2_RS.clean)

    def test_RunStep_clean_inputs_quenched(self):
        """
        A RunStep with all its inputs quenched is clean.
        """
        self.step_through_runstep_creation("second_runstep_complete")
        self.assertIsNone(self.step_E2_RS.clean())

    def test_RunStep_clean_undecided_reused_with_execrecord(self):
        """
        A RunStep which has not decided whether to reuse an ExecRecord,
        but which has one associated, is not clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")

        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_runstep = self.step_E1.pipelinestep_instances.create(run=other_run)
        rsic = self.E03_11.psic_instances.create(runstep=other_runstep)
        make_complete_non_reused(rsic, [self.raw_symDS], [self.raw_symDS])
        make_complete_non_reused(other_runstep, [self.raw_symDS], [self.doublet_symDS])

        self.step_E1_RS.reused = None
        runstep_PK = self.step_E1_RS.pk
        self.step_E1_RS.log.first().delete()
        print("*"*80)
        print(RunStep.objects.get(pk=runstep_PK))
        print("*"*80)
        self.step_E1_RS.execrecord = other_runstep.execrecord

        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'execrecord should not be set'.format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_clean_undecided_reused_with_execlog(self):
        """
        A RunStep which has not decided whether to reuse an ExecRecord,
        but which has an ExecLog, is not clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")
        self.step_E1_RS.reused = None
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'no log should have been generated'.format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_clean_undecided_reused_with_data(self):
        """
        A RunStep which has not decided whether to reuse an ExecRecord,
        but which has output SymbolicDatasets, is not clean.
        """
        # Give step_E1_RS a complete ExecLog.
        self.step_through_runstep_creation("first_runstep_complete")

        # To bypass the check for quenched inputs, we have to create
        # another ExecRecord which matches step_E1.
        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_runstep = self.step_E1.pipelinestep_instances.create(run=other_run)
        rsic = self.E03_11.psic_instances.create(runstep=other_runstep)
        make_complete_non_reused(rsic, [self.raw_symDS], [self.raw_symDS])
        make_complete_non_reused(other_runstep, [self.raw_symDS], [self.doublet_symDS])

        self.step_E1_RS.reused = None
        self.step_E1_RS.execrecord = other_runstep.execrecord

        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'no log should have been generated'.format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_clean_reused_with_data(self):
        """
        A RunStep which has decided to reuse an ExecRecord, but which
        has output SymbolicDatasets, is not clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")
        self.step_E1_RS.reused = True
        self.doublet_DS.created_by = self.step_E1_RS
        self.doublet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" reused an ExecRecord and should not have generated any Datasets'
                                          .format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_clean_subpipeline_with_execlog(self):
        """
        A RunStep which has a child run should have no ExecLog.
        """
        self.step_through_runstep_creation("sub_pipeline")
        ExecLog(record=self.step_E2_RS, start_time=timezone.now(), end_time=timezone.now()).save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" represents a sub-pipeline so no log should be associated'
                                          .format(self.step_E2_RS)),
                                self.step_E2_RS.clean)

    def test_RunStep_clean_subpipeline_with_dataset(self):
        """
        A RunStep which has a child run should have no associated output
        Datasets.
        """
        self.step_through_runstep_creation("second_runstep_complete")
        self.step_E2_RS.outputs.add(self.singlet_symDS.dataset)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" represents a sub-pipeline and should not have generated any '
                                          'data'.format(self.step_E2_RS)),
                                self.step_E2_RS.clean)

    def test_RunStep_clean_subpipeline_with_reused(self):
        """
        A RunStep which has a child run should not have set reused.
        """
        self.step_through_runstep_creation("second_runstep_complete")
        self.step_E2_RS.reused = True
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" represents a sub-pipeline so reused should not be set'
                                          .format(self.step_E2_RS)),
                                self.step_E2_RS.clean)

    def test_RunStep_clean_subpipeline_with_execrecord(self):
        """
        A RunStep which has a child run should not have an execrecord.
        """
        self.step_through_runstep_creation("second_runstep_complete")
        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_runstep = self.step_E2.pipelinestep_instances.create(run=other_run)
        execlog = ExecLog(record=other_runstep, start_time=timezone.now(), end_time=timezone.now())
        execlog.save()
        execrecord = ExecRecord(generator=execlog)
        execrecord.save()
        self.step_E2_RS.execrecord = execrecord
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" represents a sub-pipeline so execrecord should not be set'
                                          .format(self.step_E2_RS)),
                                self.step_E2_RS.clean)

    def test_RunStep_clean_subpipeline_good(self):
        """
        A RunStep representing a sub-pipeline, which has not set
        reused and has no ExecLog, is clean.
        """
        self.step_through_runstep_creation("second_runstep")
        self.assertIsNone(self.step_E2_RS.clean())

    def test_RunStep_clean_reused_no_execrecord(self):
        """
        A RunStep which has decided to reuse an ExecRecord, but doesn't
        have one associated yet, is clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")
        self.step_E1_RS.reused = True
        self.step_E1_RS.execrecord = None
        self.assertIsNone(self.step_E1_RS.clean())

    def test_RunStep_clean_non_reused_bad_data(self):
        """
        A RunStep which has decided not to reuse an ExecRecord,
        and has bad output data, is not clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")
        self.step_E1_RS.reused = False
        self.doublet_DS.created_by = self.step_E1_RS
        self.doublet_symDS.MD5_checksum = "foo"
        self.doublet_DS.save()
        self.doublet_symDS.save()
        with open(self.doublet_DS.dataset_file.name) as f:
            checksum = compute_md5(f)

        self.assertRaisesRegexp(ValidationError,
                                re.escape('File integrity of "{}" lost. Current checksum "{}" does not equal expected '
                                          'checksum "{}"'.format(self.doublet_DS, checksum, "foo")),
                                self.step_E1_RS.clean)

    def test_RunStep_clean_non_reused_good_data(self):
        """
        A RunStep which has decided not to reuse an ExecRecord, and has
        clean output data, is clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")
        self.doublet_symDS.MD5_checksum = self.doublet_DS.compute_md5()
        self.doublet_DS.save()
        self.doublet_symDS.save()
        self.assertIsNone(self.step_E1_RS.clean())    
        
    def test_RunStep_clean_good_child_run(self):
        """
        A RunStep with a child_run which is clean, is also clean.
        """
        self.step_through_runstep_creation("sub_pipeline")
        self.assertIsNone(self.step_E3_RS.clean())

    def test_RunStep_clean_good_child_run(self):
        """
        A RunStep with a child_run which is not clean, is also not
        clean. Note: this isn't quite the same as the original test,
        I'm having trouble hitting the "execrecord should not be set"
        error.
        """
        self.step_through_runstep_creation("sub_pipeline")
        self.step_D1_RS.reused = None
        self.step_D1_RS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'no log should have been generated'.format(self.step_D1_RS)),
                                self.step_E2_RS.clean)

    def test_RunStep_clean_bad_execrecord(self):
        """
        A RunStep whose ExecRecord is not clean, is also not clean.
        """
        self.step_through_runstep_creation("first_step_complete")
        execrecord = self.step_E1_RS.execrecord
        execrecord.execrecordins.first().delete()
        self.assertRaisesRegexp(ValidationError, 
                                re.escape('Input(s) to ExecRecord "{}" are not quenched'.format(execrecord)),
                                self.step_E1_RS.clean)

    def test_RunStep_clean_good_execrecord(self):
        """
        A RunStep representing a Method with a clean ExecRecord and no
        other problems, is clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")
        self.assertIsNone(self.step_E1_RS.clean())

    def test_RunStep_execrecord_wrong_transformation(self):
        """
        If a RunStep has an associated ExecRecord, it must point to
        the same transformation that the RunStep does.
        """
        self.step_through_runstep_creation("sub_pipeline")
        self.step_E1_RS.execrecord.generator = self.step_D1_RS
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" points to transformation "{}" but corresponding ER does not'
                                          .format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_deleted_output_with_data(self):
        """
        A RunStep with an output marked for deletion, should not have
        any Datasets associated to that output.
        """
        self.step_through_runstep_creation("first_step_complete")
        self.step_E1.outputs_to_delete.add(self.mA.outputs.get(dataset_name="A1_out"))
        output = self.step_E1.transformation.outputs.first()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Output "{}" of RunStep "{}" is deleted; no data should be associated'
                                          .format(output, self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_missing_output_with_data(self):
        """
        A RunStep with a missing output, should not have any Datasets
        associated to that output.
        """
        self.step_through_runstep_creation("first_step_complete")
        content_check = ContentCheckLog(symbolicdataset=self.doublet_symDS, execlog=self.step_E1_RS.log.first())
        content_check.save()
        BadData(contentchecklog=content_check, missing_output=True).save()

        output = self.step_E1.transformation.outputs.first()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Output "{}" of RunStep "{}" is missing; no data should be associated'
                                          .format(output, self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_kept_output_without_data(self):
        """
        A RunStep which keeps its outputs, but has an ExecRecordOut
        without data, is not clean.
        """
        self.step_through_runstep_creation("first_step_complete")
        ero = self.step_E1_RS.execrecord.execrecordouts.first()
        ero.symbolicdataset.dataset.delete()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecordOut "{}" of RunStep "{}" should reference existent data'
                                          .format(ero, self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_output_not_in_ExecRecord(self):
        """
        A RunStep with a Dataset not in its ExecRecord is not clean.
        """
        self.step_through_runstep_creation("first_step")
        self.triplet_DS.created_by = self.step_E1_RS
        self.triplet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" generated Dataset "{}" but it is not in its ExecRecord'
                                          .format(self.step_E1_RS, self.triplet_DS)),
                                self.step_E1_RS.clean)

    def test_RunStep_subpipeline_complete(self):
        """
        A RunStep with a complete and clean child run is itself clean
        and complete.
        """
        self.step_through_runstep_creation("sub_pipeline")
        self.assertTrue(step_E2_RS.is_complete())
        self.assertIsNone(step_E2_RS.complete_clean(), None)

    def test_RunStep_complete_clean_no_execrecord(self):
        """
        A RunStep with no ExecRecord is not complete.
        """
        self.step_through_runstep_creation("first_runstep")
        self.assertEquals(step_E1_RS.is_complete(), False)
        self.assertRaisesRegexp(ValidationError, 
                                re.escape('RunStep "{}" is not complete'.format(self.step_E1_RS)),
                                self.step_E1_RS.complete_clean)

    def test_RunStep_bad_clean_propagation(self):
        """
        A RunStep which is not clean, also should not pass
        complete_clean.
        """
        self.step_through_runstep_creation("first_runstep")
        self.doublet_DS.created_by = self.step_E1_RS
        self.doublet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" inputs not quenched; no data should have been generated'
                                          .format(self.step_E1_RS)),
                                self.step_E1_RS.complete_clean)

class RunTests(librarian.tests.LibrarianTestSetup):

    def tearDown(self):
        super(self.__class__, self).tearDown()
        clean_files()

    def step_through_run_creation(self, bp):
        """
        Helper function to step through creation of a Run. bp is a
        breakpoint - these are defined throughout (see the code).
        """
        # Empty Runs.
        self.pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        self.pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        if bp == "empty_runs": return

        # First RunStep associated.
        self.step_E1_RS = self.step_E1.pipelinestep_instances.create(run=self.pE_run)
        if bp == "first_step": return

        # First RunSIC associated and completed.
        step_E1_RSIC = self.step_E1.cables_in.first().psic_instances.create(runstep=self.step_E1_RS)
        make_complete_non_reused(step_E1_RSIC, [self.raw_symDS], [self.raw_symDS])
        if bp == "first_cable": return

        # First RunStep completed.
        make_complete_non_reused(self.step_E1_RS, [self.raw_symDS], [self.doublet_symDS])
        if bp == "first_step_complete": return

        # Second RunStep associated.
        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        if bp == "second_step": return

        # Sub-pipeline for step 2 - reset step_E2_RS.
        self.step_E2_RS.delete()
        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run, reused=None)
        complete_RSICs(self.step_E2_RS, [self.triplet_symDS, self.singlet_symDS], 
                                             [self.D1_in_symDS, self.singlet_symDS])
        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
        if bp == "sub_pipeline": return

        # Complete sub-Pipeline.
        self.step_D1_RS = self.step_D1.pipelinestep_instances.create(run=self.pD_run)
        complete_RSICs(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], 
                                             [self.D1_in_symDS, self.singlet_symDS])
        make_complete_non_reused(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], [self.C1_in_symDS])
        pD_ROC = self.pD.outcables.first().poc_instances.create(run=self.pD_run)
        make_complete_non_reused(pD_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
        if bp == "sub_pipeline_complete": return

        # Third RunStep associated.
        self.step_E3_RS = self.step_E3.pipelinestep_instances.create(run=self.pE_run)
        if bp == "third_step": return

        # Third RunStep completed.
        complete_RSICs(self.step_E3_RS, [self.C1_in_symDS, self.doublet_symDS], 
                                             [self.C1_in_symDS, self.C2_in_symDS])
        make_complete_non_reused(self.step_E3_RS, [self.C1_in_symDS, self.C2_in_symDS],
                                                       [self.singlet_symDS, self.raw_symDS, self.raw_symDS])
        if bp == "third_step_complete": return

        # Outcables associated.
        roc1 = self.pE.outcables.get(output_idx=1).poc_instances.create(run=self.pE_run)
        make_complete_non_reused(roc1, [self.C1_in_symDS], [self.doublet_symDS])
        if bp == "first_outcable": return

        roc2 = self.pE.outcables.get(output_idx=2).poc_instances.create(run=self.pE_run)
        make_complete_non_reused(roc2, [self.singlet_symDS], [self.singlet_symDS])
        roc3 = self.pE.outcables.get(output_idx=3).poc_instances.create(run=self.pE_run)
        make_complete_non_reused(roc3, [self.C3_out_symDS], [self.C3_out_symDS])

        if bp == "outcables_done": return

    def test_Run_is_subrun_True(self):
        """
        A Run which has a parent RunStep should register as being a subrun.
        """
        self.step_through_run_creation("sub_pipeline")
        self.assertTrue(self.pD_run.is_subrun())

    def test_Run_is_subrun_False(self):
        """
        A Run which has no parent RunStep should not be a subrun.
        """
        self.step_through_run_creation("empty_runs")
        self.assertFalse(self.pE_run.is_subrun())

    def test_Run_clean_not_started(self):
        """
        A Run which has been created, but nothing in it has executed 
        yet, is clean.
        """
        self.step_through_run_creation("empty_runs")
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_inconsistent_parent_runstep(self):
        """
        A sub-Run whose parent RunStep does not match its Pipeline is
        not clean.
        """
        self.step_through_run_creation("second_step")
        self.pD_run.parent_runstep = self.step_E1_RS
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Pipeline of Run "{}" is not consistent with its parent RunStep'
                                          .format(self.pD_run)),
                                self.pD_run.clean)

    def test_Run_clean_consistent_parent_runstep(self):
        """
        A sub-Run whose parent RunStep matches its Pipeline is clean.
        """
        self.step_through_run_creation("sub_pipeline")
        self.assertIsNone(self.pD_run.clean())

    def test_Run_clean_first_runstep_incomplete(self):
        """
        A Run whose first RunStep is associated and incomplete, and
        nothing else is, is clean.
        """
        self.step_through_run_creation("first_step")
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_first_runstep_complete(self):
        """
        A Run whose first RunStep is associated and complete, and
        nothing else is, is clean.
        """
        self.step_through_run_creation("first_step_complete")
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_second_runstep_incomplete(self):
        """
        A Run whose second RunStep is associated and incomplete, and
        whose first RunStep is complete, is clean.
        """
        self.step_through_run_creation("second_step")
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_previous_incomplete_runstep(self):
        """
        A Run whose second RunStep is associated, but whose first step
        is not complete, is clean.

        TODO: when we add the check to RunStep.clean that the
        SymbolicDatasets feeding it are present, this will fail by
        propagation. The test setup will need to be modified to put in
        place the inputs to steps 1 and 2.
        """
        self.step_through_run_creation("second_step")
        self.step_E1_RS.execrecord = None
        self.step_E1_RS.reused = None
        self.step_E1_RS.log.clear()
        self.step_E1_RS.save()
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_badly_numbered_steps(self):
        """
        A Run with steps not consecutively numbered from 1 to n is not
        clean.
        """
        self.step_through_run_creation("second_step")
        self.step_E1_RS.delete()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSteps of Run "{}" are not consecutively numbered starting from 1'
                                          .format(self.pE_run)),
                                self.pE_run.clean)

    def test_Run_clean_properly_numbered_steps(self):
        """
        A Run with steps consecutively numbered from 1 to n is clean.
        """
        self.step_through_run_creation("second_step")
        self.assertIsNone(self.pE_run.clean())

    def test_Run_bad_first_RunStep_propagation(self):
        """
        A Run whose first RunStep is not clean, and no other RunSteps
        are associated, is not clean.
        """
        self.step_through_run_creation("first_step_complete")
        self.step_E1_RS.reused = None
        self.step_E1_RS.RSICs.first().delete()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" inputs not quenched; reused and execrecord should not be set'
                                          .format(self.step_E1_RS)),
                                self.pE_run.clean)

    def test_Run_bad_second_RunStep_propagation(self):
        """
        A Run whose first RunStep is clean, but whose second RunStep
        is not, is not clean.
        """
        self.step_through_run_creation("second_step")
        self.step_E2_RS.reused = True
        self.step_E2_RS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" represents a sub-pipeline so reused should not be set'
                                          .format(self.step_E2_RS)),
                                self.pE_run.clean)

    def test_Run_clean_RunOutputCable_no_RunStep(self):
        """
        A Run with a RunOutputCable from a non-associated RunStep is not
        clean.
        """
        self.step_through_run_creation("sub_pipeline")
        self.E31_42.poc_instances.create(run=self.pE_run)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Run "{}" has a RunOutputCable from step {}, but no corresponding RunStep' 
                                          .format(self.pE_run, 3)),
                                self.pE_run.clean)

    def test_Run_clean_outcable_incomplete_last_RunStep(self):
        """
        A Run with a RunOutputCable, and a final RunStep which is clean
        but not complete, is clean.
        """
        self.step_through_run_creation("third_step")
        self.pE.outcables.first().poc_instances.create(run=self.pE_run)
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_two_complete_RunSteps(self):
        """
        A three step Run with two complete RunSteps, but no third
        RunStep, is clean.
        """
        self.step_through_run_creation("sub_pipeline")
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_all_RunSteps_complete_no_outcables(self):
        """
        A Run which has all its steps complete, but no RunOutputCables,
        is clean.
        """
        self.step_through_run_creation("third_step_complete")
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_bad_RunOutputCable_propagation(self):
        """
        A Run with an bad RunOutputCable is not clean (not quite the 
        same as the old test case, I can't make it work).
        """
        self.step_through_run_creation("outcables_done")
        cable1 = self.pE_run.runoutputcables.first()
        cable1.reused = None
        cable1.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'no ExecLog should be associated'.format(cable1)),
                                self.pE_run.clean)

    def test_Run_clean_one_complete_RunOutputCable(self):
        """
        A Run with one good RunOutputCable, and no others, is clean.
        """
        self.step_through_run_creation("first_outcable")
        self.assertIsNone(self.pE_run.clean())

    def test_Run_clean_all_complete_RunOutputCables(self):
        """
        A Run with all RunOutputCables complete and clean, is clean.
        """
        self.step_through_run_creation("outcables_done")
        self.assertIsNone(self.pE_run.clean())
        self.assertTrue(self.pE_run.is_complete())
        self.assertIsNone(self.pE_run.complete_clean())

class RunSICTests(librarian.tests.LibrarianTestSetup):

    def test_RSIC_many_execlogs(self):
        run = self.pE.pipeline_instances.create(user=self.myUser)
        runstep = self.pE.steps.first().pipelinestep_instances.create(run=run)
        cable = self.pE.steps.first().cables_in.first()
        rsic = cable.psic_instances.create(runstep=runstep)
        rsic.reused = False
        for i in range(2):
            rsic.log.create(start_time=timezone.now(),
                            end_time=timezone.now())
        self.assertRaisesRegexp(ValidationError,
                'RunSIC "{}" has {} ExecLogs but should have only one'.
                        format(rsic, 2),
                rsic.clean)

    def test_RSIC_clean_early(self):
        """Checks coherence of a RunSIC up to the point at which reused is set."""
        # Define some infrastructure.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        step_E3_RS = self.step_E3.pipelinestep_instances.create(
            run=pE_run)

        # Bad case: PSIC does not belong to the RunStep's PS.
        E01_21_RSIC = self.E01_21.psic_instances.create(
            runstep=step_E3_RS)
        self.assertRaisesRegexp(
            ValidationError,
            "PSIC .* does not belong to PipelineStep .*",
            E01_21_RSIC.clean)

        # Good case: PSIC and runstep are coherent; reused is not set yet.
        E11_32_RSIC = self.E11_32.psic_instances.create(runstep=step_E3_RS)
        self.assertEquals(E11_32_RSIC.clean(), None)

        # Bad case: reused is unset but there is data associated.
        self.doublet_DS.created_by = E11_32_RSIC
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* has not decided whether or not to reuse an ExecRecord; no Datasets should be associated",
            E11_32_RSIC.clean)
        # Reset....
        self.doublet_DS.created_by = None
        self.doublet_DS.save()

        # Bad case: ER is set before reused.
        E11_32_ER = self.ER_from_record(E11_32_RSIC)
        source = E11_32_ER.execrecordins.create(
            generic_input=self.mA.outputs.get(dataset_name="A1_out"),
            symbolicdataset=self.doublet_symDS)
        dest = E11_32_ER.execrecordouts.create(
            generic_output=self.C2_in,
            symbolicdataset=self.C2_in_symDS)
        E11_32_RSIC.execrecord = E11_32_ER
        E11_32_RSIC.log = E11_32_RSIC.log.none()
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* has not decided whether or not to reuse an ExecRecord; execrecord should not be set yet",
            E11_32_RSIC.clean)
        # Reset....
        E11_32_RSIC.execrecord = None

    def test_RSIC_clean_reused(self):
        """Checks coherence of a RunSIC reusing an ER after reused is set."""
        # Define some infrastructure.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        step_E3_RS = self.step_E3.pipelinestep_instances.create(run=pE_run)
        E11_32_RSIC = self.E11_32.psic_instances.create(runstep=step_E3_RS)
        E11_32_RSIC.reused = True

        E11_32_ER = self.ER_from_record(E11_32_RSIC)
        source = E11_32_ER.execrecordins.create(
            generic_input=self.mA.outputs.get(dataset_name="A1_out"),
            symbolicdataset=self.doublet_symDS)
        dest = E11_32_ER.execrecordouts.create(
            generic_output=self.C2_in,
            symbolicdataset=self.C2_in_symDS)
    
        # Bad case: Dataset is associated.
        self.doublet_DS.created_by = E11_32_RSIC
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* reused an ExecRecord and should not have generated any Datasets",
            E11_32_RSIC.clean)
        # Reset....
        self.doublet_DS.created_by = None
        self.doublet_DS.save()
        
        # Propagation test: ER is set and broken.
        E11_32_RSIC.execrecord = E11_32_ER
        dest.generic_output = self.C1_in
        dest.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Input .* is not the one fed by the PSIC of ExecRecord .*",
            E11_32_RSIC.clean)
        # Reset to proceed....
        dest.generic_output = self.C2_in
        dest.save()

        # Bad case: execrecord points to a PSIC that is incompatible.
        step_E2_RS = self.step_E2.pipelinestep_instances.create(run=pE_run)
        E02_22_RSIC = self.E02_22.psic_instances.create(runstep=step_E2_RS)
        E02_22_ER = self.ER_from_record(E02_22_RSIC)
        E02_22_ER.execrecordins.create(
            generic_input=self.E2_in,
            symbolicdataset=self.singlet_symDS)
        E02_22_ER.execrecordouts.create(
            generic_output=self.D2_in,
            symbolicdataset=self.singlet_symDS)
        E11_32_RSIC.execrecord = E02_22_ER
        self.assertRaisesRegexp(
            ValidationError,
            "PSIC of RunSIC .* is incompatible with that of its ExecRecord",
            E11_32_RSIC.clean)

        # Bad case: execrecord doesn't point to a PSIC.
        # We make a complete and clean ER for something else.
        # mA is step_E1 of pipeline pE.
        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=pE_run)
        mA_ER = self.ER_from_record(step_E1_RS)
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)
        E11_32_RSIC.execrecord = mA_ER
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecord of RunSIC .* does not represent a PSIC",
            E11_32_RSIC.clean)

        # Check of propagation:
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecord of RunSIC .* does not represent a PSIC",
            E11_32_RSIC.complete_clean)
        
        # Reset....
        E11_32_RSIC.execrecord = E11_32_ER

        # The bad case where PSIC does not keep its output (default)
        # but data is associated cannot happen because we already said
        # that we're reusing an ER.  We move on to....

        # Bad case: PSIC keeps its output, ERO does not have existent data.
        self.E11_32.keep_output = True
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* keeps its output; ExecRecordOut .* should reference existent data",
            E11_32_RSIC.clean)

        # Proceed....
        dest.symbolicdataset = self.E11_32_output_symDS
        dest.save()

        # Good case: ERO does have existent data.
        self.assertEquals(E11_32_RSIC.clean(), None)

        # Good case: RSIC is complete.
        self.assertEquals(E11_32_RSIC.is_complete(), True)
        self.assertEquals(E11_32_RSIC.complete_clean(), None)

        # Bad case: RSIC is incomplete.
        E11_32_RSIC.execrecord = None
        self.assertEquals(E11_32_RSIC.is_complete(), False)
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* has no ExecRecord",
            E11_32_RSIC.complete_clean)
        

    # October 16, 2013: modified to test parts involving Datasets being
    # attached.
    def test_RSIC_clean_not_reused(self):
        """Checks coherence of a RunSIC at all stages of its creation."""
        # Define some infrastructure.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        step_E3_RS = self.step_E3.pipelinestep_instances.create(
            run=pE_run)
        E11_32_RSIC = self.E11_32.psic_instances.create(runstep=step_E3_RS)
        E11_32_RSIC.reused = False

        E11_32_ER = self.ER_from_record(E11_32_RSIC)
        source = E11_32_ER.execrecordins.create(
            generic_input=self.mA.outputs.get(dataset_name="A1_out"),
            symbolicdataset=self.doublet_symDS)
        dest = E11_32_ER.execrecordouts.create(
            generic_output=self.C2_in,
            symbolicdataset=self.C2_in_symDS)

        # Good case: no ER is set.
        E11_32_RSIC.execrecord = None
        self.assertEquals(E11_32_RSIC.clean(), None)

        # Propagation test: ER is set and broken.
        E11_32_RSIC.execrecord = E11_32_ER
        dest.generic_output = self.C1_in
        dest.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Input .* is not the one fed by the PSIC of ExecRecord .*",
            E11_32_RSIC.clean)
        # Reset to proceed....
        dest.generic_output = self.C2_in
        dest.save()

        # Bad case: execrecord points to a PSIC that is incompatible.
        step_E2_RS = self.step_E2.pipelinestep_instances.create(run=pE_run)
        E02_22_RSIC = self.E02_22.psic_instances.create(runstep=step_E2_RS)
        E02_22_ER = self.ER_from_record(E02_22_RSIC)
        E02_22_ER.execrecordins.create(
            generic_input=self.E2_in,
            symbolicdataset=self.singlet_symDS)
        E02_22_ER.execrecordouts.create(
            generic_output=self.D2_in,
            symbolicdataset=self.singlet_symDS)
        E11_32_RSIC.execrecord = E02_22_ER
        self.assertRaisesRegexp(
            ValidationError,
            "PSIC of RunSIC .* is incompatible with that of its ExecRecord",
            E11_32_RSIC.clean)

        # Bad case: execrecord doesn't point to a PSIC.
        # We make a complete and clean ER for something else.
        # mA is step_E1 of pipeline pE.
        mA_RS = self.step_E1.pipelinestep_instances.create(run=pE_run)
        mA_ER = self.ER_from_record(mA_RS)
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)
        E11_32_RSIC.execrecord = mA_ER
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecord of RunSIC .* does not represent a PSIC",
            E11_32_RSIC.clean)

        # Check of propagation:
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecord of RunSIC .* does not represent a PSIC",
            E11_32_RSIC.complete_clean)
        
        # Reset....
        E11_32_RSIC.execrecord = E11_32_ER

        # Good case: PSIC does not keep its output, no data is associated.
        self.assertEquals(E11_32_RSIC.clean(), None)

        # Bad case: PSIC does not keep its output but data is associated.
        self.E11_32_output_DS.created_by = E11_32_RSIC
        self.E11_32_output_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* does not keep its output but a dataset was registered",
            E11_32_RSIC.clean)
        # Reset....
        self.E11_32_output_DS.created_by = None
        self.E11_32_output_DS.save()

        # Bad case: PSIC keeps its output, ERO has no existent data.
        self.E11_32.keep_output = True
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* keeps its output; ExecRecordOut .* should reference existent data",
            E11_32_RSIC.clean)

        # Bad case: PSIC keeps its output, ERO has existent data,
        # and cable is not trivial, but there is no associated data.
        dest.symbolicdataset = self.E11_32_output_symDS
        dest.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* was not reused, trivial, or deleted; it should have produced data",
            E11_32_RSIC.clean)

        # Bad case: as above, but Dataset associated is wrong.
        self.doublet_DS.created_by = E11_32_RSIC
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset .* was produced by RunSIC .* but is not in an ERO of ExecRecord .*",
            E11_32_RSIC.clean)
        # Reset....
        self.doublet_DS.created_by = None
        self.doublet_DS.save()
        
        # Good case: as above, with correct Dataset.
        self.E11_32_output_DS.created_by = E11_32_RSIC
        self.E11_32_output_DS.save()
        self.assertEquals(E11_32_RSIC.clean(), None)

        # Good case: RSIC is complete.
        self.assertEquals(E11_32_RSIC.is_complete(), True)
        self.assertEquals(E11_32_RSIC.complete_clean(), None)

        # Bad case: RSIC is incomplete.
        E11_32_RSIC.execrecord = None
        self.assertEquals(E11_32_RSIC.is_complete(), False)
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* has no ExecRecord",
            E11_32_RSIC.complete_clean)
        
        
class RunOutputCableTests(librarian.tests.LibrarianTestSetup):

    def test_ROC_many_execlogs(self):
        run = self.pE.pipeline_instances.create(user=self.myUser)
        run_output_cable = self.E31_42.poc_instances.create(run=run)
        run_output_cable.reused = False
        for i in range(2):
            run_output_cable.log.create(start_time=timezone.now(),
                                        end_time=timezone.now())
        self.assertRaisesRegexp(ValidationError,
                'RunOutputCable "{}" has {} ExecLogs but should have only one'.
                        format(run_output_cable, 2),
                run_output_cable.clean)

    def test_ROC_clean(self):
        """Checks coherence of a RunOutputCable at all stages of its creation."""
        # Define a run for pE so that this ROC has something to belong to.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)

        # Create a ROC for one of the POCs.
        E31_42_ROC = self.E31_42.poc_instances.create(run=pE_run)

        # Good case: POC belongs to the parent run's Pipeline.
        self.assertIsNone(E31_42_ROC.clean())

        # Bad case: POC belongs to another Pipeline.
        pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        E31_42_ROC.run = pD_run
        self.assertRaisesRegexp(
            ValidationError,
            "POC .* does not belong to Pipeline .*",
            E31_42_ROC.clean)

        # Reset the ROC.
        E31_42_ROC.run = pE_run

        # Bad case: reused is not set but data is associated.
        self.C1_out_DS.created_by = E31_42_ROC
        self.C1_out_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* has not decided whether or not to reuse an ExecRecord; no Datasets should be associated",
            E31_42_ROC.clean)
        # Reset....
        self.C1_out_DS.created_by = None
        self.C1_out_DS.save()

        # Bad case: reused is not set but execrecord is.
        #E31_42_ER = self.E31_42.execrecords.create()
        E31_42_ER = self.ER_from_record(E31_42_ROC)
        old_log = E31_42_ROC.log.first()
        E31_42_ROC.log = E31_42_ROC.log.none()
        E31_42_ROC.execrecord = E31_42_ER
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* has not decided whether or not to reuse an ExecRecord; execrecord should not be set yet",
            E31_42_ROC.clean)
        # Reset....
        E31_42_ROC.execrecord = None
        self.ER_from_record(E31_42_ROC)

        # Now set reused.  First we do the reused = True case.
        E31_42_ROC.reused = True
        # Bad case: ROC has associated data.
        self.singlet_DS.created_by = E31_42_ROC
        self.singlet_DS.save()

        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* reused an ExecRecord and should not have generated any Datasets",
            E31_42_ROC.clean)
        # Reset....
        self.singlet_DS.created_by = None
        self.singlet_DS.save()

        # Next, the reused = False case.
        E31_42_ROC.reused = False
        # Good case 1: trivial cable, no data.
        self.assertEquals(E31_42_ROC.clean(), None)

        # Bad case: trivial cable, data associated.
        self.singlet_DS.created_by = E31_42_ROC
        self.singlet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* is trivial and should not have generated any Datasets",
            E31_42_ROC.clean)
        # Reset....
        self.singlet_DS.created_by = None
        self.singlet_DS.save()
        
        # Good case 2: non-trivial cable, good data attached.
        E21_41_ROC = self.E21_41.poc_instances.create(run=pE_run, reused=False)
        self.doublet_DS.created_by = E21_41_ROC
        self.doublet_DS.save()
        E21_41_ER = self.ER_from_record(E21_41_ROC)
        self.assertEquals(E21_41_ROC.clean(), None)

        # Bad case: non-trivial cable, multiple datasets attached.
        self.E1_out_DS.created_by = E21_41_ROC
        self.E1_out_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* should generate at most one Dataset",
            E21_41_ROC.clean)
        # Reset....
        self.E1_out_DS.created_by = None
        self.E1_out_DS.save()
        
        # Propagation bad case: bad data attached.
        self.doublet_DS.symbolicdataset.MD5_checksum = "foo"
        self.doublet_DS.symbolicdataset.save()
        self.assertRaisesRegexp(
            ValidationError,
            "File integrity of .* lost.  Current checksum .* does not equal expected checksum .*",
            E21_41_ROC.clean)
        # Reset....
        self.doublet_symDS.MD5_checksum = self.doublet_DS.compute_md5()
        self.doublet_DS.created_by = None
        self.doublet_symDS.save()

        # Now set an ER.  Propagation bad case: ER is not complete and clean.
        E31_42_ROC.execrecord = E31_42_ER
        self.assertRaisesRegexp(
            ValidationError,
            "Input to ExecRecord .* is not quenched",
            E31_42_ROC.clean)
        
        # Propagation good case: continue on to examine the ER.
        # We define several ERs, good and bad.
        
        source = E31_42_ER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.mC.outputs.get(dataset_name="C1_out"))
        # This is a trivial outcable so its symbolic dataset should be
        # the same as the ERI.
        dest = E31_42_ER.execrecordouts.create(
            symbolicdataset=self.singlet_symDS,
            generic_output=self.pE.outputs.get(dataset_name="E2_out"))

        # Bad cases: we're looking at an ER for another transformation.
        # Create ER for mA, which is step_E1 of pipeline pE.
        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=pE_run)
        mA_ER = self.ER_from_record(step_E1_RS)
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)

        # Create an execrecord for another of the POCs.
        E21_41_ROC.log.clear()
        E21_41_ER = self.ER_from_record(E21_41_ROC)

        empty_sd_source = SymbolicDataset()
        empty_sd_source.save()
        structure = DatasetStructure(symbolicdataset=empty_sd_source, compounddatatype=self.triplet_cdt)
        structure.save()
        empty_sd_source.structure = structure

        empty_sd_dest = SymbolicDataset()
        empty_sd_dest.save()
        structure = DatasetStructure(symbolicdataset=empty_sd_dest, compounddatatype=self.doublet_cdt)
        structure.save()
        empty_sd_dest.structure = structure

        E21_41_ER_in = E21_41_ER.execrecordins.create(
            symbolicdataset=empty_sd_source,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        E21_41_ER_out = E21_41_ER.execrecordouts.create(
            symbolicdataset=empty_sd_dest,
            generic_output=self.pE.outputs.get(dataset_name="E1_out"))

        # Bad case 1: the ROC points to an ER linked to the wrong
        # thing (another POC).
        E31_42_ROC.execrecord = E21_41_ER
        error_msg = "POC of RunOutputCable .* is incompatible with that of its ExecRecord"
        self.assertRaisesRegexp(ValidationError, error_msg, E31_42_ROC.clean)

        # Bad case 2: the ROC points to an ER linked to another wrong
        # thing (not a POC).
        E31_42_ROC.execrecord = mA_ER
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecord of RunOutputCable .* does not represent a POC",
            E31_42_ROC.clean)
        
        # Good case: ROC and ER are consistent.  This lets us proceed.
        E31_42_ROC.execrecord = E31_42_ER

        # Now we check cases where an output is marked for deletion.
        # Since this only happens when a run is a sub-run, we define
        # the required infrastructure.

        # Take pD_run and use it as the child_run of step E2.
        # Consider cable D11_21, the only POC of pipeline D.
        step_E2_RS = self.step_E2.pipelinestep_instances.create(
            run=pE_run)
        pD_run.parent_runstep = step_E2_RS
        pD_run.save()
        D11_21_ROC = self.D11_21.poc_instances.create(run=pD_run)
        D11_21_ROC.reused = False
        D11_21_ER = self.ER_from_record(D11_21_ROC)
        empty_symDS = SymbolicDataset()
        empty_symDS.save()
        structure = DatasetStructure(symbolicdataset=empty_symDS, compounddatatype=self.triplet_cdt)
        structure.save()
        empty_symDS.structure = structure
        Dsource = D11_21_ER.execrecordins.create(
            generic_input=self.B1_out,
            symbolicdataset=empty_symDS)
        Ddest = D11_21_ER.execrecordouts.create(
            generic_output=self.pD.outputs.get(dataset_name="D1_out"),
            symbolicdataset=empty_symDS)
        D11_21_ROC.execrecord = D11_21_ER

        # Good case: the output of D11_21 is marked for deletion, and
        # no data is associated.
        step_E2_RS.pipelinestep.outputs_to_delete.add(
            self.pD.outputs.get(dataset_name="D1_out"))
        self.assertEquals(D11_21_ROC.clean(), None)

        # Bad case: output of D11_21 is marked for deletion, D11_21 is
        # not reused or trivial, and real data is associated.

        # Define some custom wiring for D11_21: swap the first two columns.
        self.D11_21.custom_outwires.create(
            source_pin=self.triplet_cdt.members.all()[0],
            dest_pin=self.triplet_cdt.members.all()[1])
        self.D11_21.custom_outwires.create(
            source_pin=self.triplet_cdt.members.all()[1],
            dest_pin=self.triplet_cdt.members.all()[0])
        
        self.triplet_3_rows_DS.created_by = D11_21_ROC
        self.triplet_3_rows_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* is marked for deletion; no data should be produced",
            D11_21_ROC.clean)
        # Reset....
        self.triplet_3_rows_DS.created_by = None
        self.triplet_3_rows_DS.save()

        # Bad case: output of D11_21 is not marked for deletion,
        # and the corresponding ERO does not have existent data.
        step_E2_RS.pipelinestep.outputs_to_delete.remove(
            self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut .* should reference existent data",
            D11_21_ROC.clean)
        # Set up to move on....
        Dsource.symbolicdataset = self.triplet_3_rows_symDS
        Dsource.save()
        Ddest.symbolicdataset = self.triplet_3_rows_symDS
        Ddest.save()
        
        # Good case: output of D11_21 is not marked for deletion, step is reused,
        # and no data is associated.
        D11_21_ROC.reused = True
        self.assertEquals(D11_21_ROC.clean(), None)
        
        # Good case: output of D11_21 is not marked for deletion, step is not reused
        # but cable is trivial, and no data is associated.
        D11_21_ROC.reused = False
        self.D11_21.custom_outwires.all().delete()
        self.assertEquals(D11_21_ROC.clean(), None)

        # Bad case: output of D11_21 is not marked for deletion, step
        # is not reused, cable is not trivial, but no associated data exists.
        self.D11_21.custom_outwires.create(
            source_pin=self.triplet_cdt.members.all()[0],
            dest_pin=self.triplet_cdt.members.all()[1])
        self.D11_21.custom_outwires.create(
            source_pin=self.triplet_cdt.members.all()[1],
            dest_pin=self.triplet_cdt.members.all()[0])
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* was not reused, trivial, or deleted; it should have produced data",
            D11_21_ROC.clean)
        
        # Bad case: associated data *does* exist, but is not the same
        # as that of the corresponding ERO.
        with open(os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), "rb") as f:
          md5 = file_access_utils.compute_md5(f)
        other_triplet_3_rows_symDS = SymbolicDataset(MD5_checksum=md5)
        other_triplet_3_rows_symDS.save()

        other_triplet_3_rows_DS = other_triplet_3_rows_DS = Dataset(
            user=self.myUser, name="triplet", description="lol",
            symbolicdataset=other_triplet_3_rows_symDS,
            created_by = D11_21_ROC)
        with open(os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), "rb") as f:
            other_triplet_3_rows_DS.dataset_file.save(
                "step_0_triplet_3_rows.csv", File(f))
        other_triplet_3_rows_DS.save()
        other_triplet_3_rows_DS_structure = DatasetStructure(
            symbolicdataset=other_triplet_3_rows_symDS,
            compounddatatype=self.triplet_cdt)
        other_triplet_3_rows_DS_structure.save()
        other_triplet_3_rows_DS.clean()

        self.assertRaisesRegexp(
            ValidationError,
            "Dataset .* is not in an ERO of ExecRecord .*",
            D11_21_ROC.clean)

        # Good case: associated data is the same as that of the
        # corresponding ERO.
        other_triplet_3_rows_DS.created_by = None
        other_triplet_3_rows_DS.save()
        self.triplet_3_rows_DS.created_by = D11_21_ROC
        self.triplet_3_rows_DS.save()

        self.assertEquals(D11_21_ROC.clean(), None)
        
        # Some checks in the top-level run case: make sure is_deleted
        # and whether cable is trivial is properly set.

        # Good case: trivial top-level cable, no data is associated.
        self.singlet_DS.created_by = None
        self.singlet_DS.save()
        self.assertEquals(E31_42_ROC.clean(), None)
        
        # Bad case: trivial top-level cable, data is associated.
        self.singlet_DS.created_by = E31_42_ROC
        self.singlet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* is trivial and should not have generated any Datasets",
            E31_42_ROC.clean)

        # Good case: non-trivial top-level cable, data is associated and it
        # matches that of the ERO.
        E21_41_ER_in.symbolicdataset = self.triplet_3_rows_symDS
        E21_41_ER_in.save()
        E21_41_ER_out.symbolicdataset = self.doublet_symDS
        E21_41_ER_out.save()
        E21_41_ROC.execrecord = E21_41_ER
        E21_41_ROC.save()
        self.doublet_DS.created_by = E21_41_ROC
        self.doublet_DS.save()
        self.assertIsNone(E21_41_ROC.clean())

        # Bad case: non-trivial top-level cable, no data is associated.
        self.doublet_DS.created_by = None
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* was not reused, trivial, or deleted; it should have produced data",
            E21_41_ROC.clean)

        # Now check that is_complete and complete_clean works.
        self.assertEquals(E21_41_ROC.is_complete(), True)
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* was not reused, trivial, or deleted; it should have produced data",
            E21_41_ROC.complete_clean)

        self.assertEquals(D11_21_ROC.is_complete(), True)
        self.assertEquals(D11_21_ROC.complete_clean(), None)
        
        D11_21_ROC.execrecord = None
        self.assertEquals(D11_21_ROC.is_complete(), False)
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* has no ExecRecord",
            D11_21_ROC.complete_clean)


class DatasetTests(librarian.tests.LibrarianTestSetup):

    def test_Dataset_check_MD5(self):
        # MD5 is now stored in symbolic dataset - even after the dataset was deleted
        self.assertEqual(self.raw_DS.compute_md5(), "7dc85e11b5c02e434af5bd3b3da9938e")

        # Initially, no change to the raw dataset has occured, so the md5 check will pass
        self.assertEqual(self.raw_DS.clean(), None)

        # The contents of the file are changed, disrupting file integrity
        self.raw_DS.dataset_file.close()
        self.raw_DS.dataset_file.open(mode='w')
        self.raw_DS.dataset_file.close()
        errorMessage = "File integrity of \".*\" lost.  Current checksum \".*\" does not equal expected checksum \".*\""
        self.assertRaisesRegexp(ValidationError,errorMessage, self.raw_DS.clean)
