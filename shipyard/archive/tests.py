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
from datachecking.models import ContentCheckLog, BadData
from method.tests import samplecode_path
import librarian.tests
from file_access_utils import compute_md5

# TODO: Put this someplace better, maybe shipyard/testing_utils.py?
from sandbox.tests_rm import clean_files

# Note that these tests use the exact same setup as librarian.

class ArchiveTestSetup(librarian.tests.LibrarianTestSetup):
    def setUp(self):
        super(ArchiveTestSetup, self).setUp()
        self.pE_run = self.pE.pipeline_instances.create(user=self.myUser)

    def make_complete_non_reused(self, record, input_SDs, output_SDs):
        """
        Helper function to do everything necessary to make a RunStep, 
        RunOutputCable, or RunStepInputCable complete, when it has not
        reused an ExecRecord (ie. make a new ExecRecord).
        """
        record_type = record.__class__.__name__
        record.reused = False
    
        execlog = ExecLog(record=record, invoking_record=record, start_time=timezone.now(), end_time=timezone.now())
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
            elif record_type == "RunStep":
                record.outputs.add(output_SDs[i].dataset)
    
        record.execrecord = execrecord
        record.clean()
        record.save()
    
    def complete_RSICs(self, runstep, input_SDs, output_SDs):
        """
        Helper function to create and complete all the RunSIC's needed for
        a given RunStep. input_SDs and output_SDs are lists of the input and
        output symbolic datasets for each cable, in order.
        """
        for i, cable in enumerate(runstep.pipelinestep.cables_in.order_by("dest__dataset_idx")):
            rsic = cable.psic_instances.create(runstep=runstep)
            self.make_complete_non_reused(rsic, [input_SDs[i]], [output_SDs[i]])

    def tearDown(self):
        super(ArchiveTestSetup, self).tearDown()
        clean_files()

class RunStepTests(ArchiveTestSetup):

    def test_runstep_many_execlogs(self):
        run = self.pE.pipeline_instances.create(user=self.myUser)
        run_step = self.step_E1.pipelinestep_instances.create(run=run)
        run_step.reused = False
        for i in range(2):
            run_step.log.create(invoking_record=run_step,
                                start_time=timezone.now(),
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
        if bp == "empty_runs": return

        self.step_E1_RS = self.step_E1.pipelinestep_instances.create(run=self.pE_run)
        if bp == "first_runstep": return

        self.E03_11_RSIC = self.E03_11.psic_instances.create(runstep=self.step_E1_RS)
        self.make_complete_non_reused(self.E03_11_RSIC, [self.raw_symDS], [self.raw_symDS])
        if bp == "first_rsic": return 
        
        self.make_complete_non_reused(self.step_E1_RS, [self.raw_symDS], [self.doublet_symDS])
        if bp == "first_runstep_complete": return

        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        if bp == "second_runstep": return

        self.complete_RSICs(self.step_E2_RS, [self.triplet_symDS, self.singlet_symDS], 
                                        [self.D1_in_symDS, self.singlet_symDS])
        if bp == "second_runstep_complete": return
        
        # Associate and complete sub-Pipeline.
        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
        self.step_D1_RS = self.step_D1.pipelinestep_instances.create(run=self.pD_run)
        self.complete_RSICs(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], 
                                        [self.D1_in_symDS, self.singlet_symDS])
        self.make_complete_non_reused(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], [self.C1_in_symDS])
        pD_ROC = self.pD.outcables.first().poc_instances.create(run=self.pD_run)
        self.make_complete_non_reused(pD_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
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
        self.assertRaisesRegexp(ValidationError,
                                re.escape('{} "{}" is not complete'.format("RunSIC", self.E03_11_RSIC)),
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
        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
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
        self.step_through_runstep_creation("first_rsic")

        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_runstep = self.step_E1.pipelinestep_instances.create(run=other_run)
        rsic = self.E03_11.psic_instances.create(runstep=other_runstep)
        self.make_complete_non_reused(rsic, [self.raw_symDS], [self.raw_symDS])
        self.make_complete_non_reused(other_runstep, [self.raw_symDS], [self.doublet_symDS])

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
        self.make_complete_non_reused(rsic, [self.raw_symDS], [self.raw_symDS])
        self.make_complete_non_reused(other_runstep, [self.raw_symDS], [self.doublet_symDS])

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
        ExecLog(record=self.step_E2_RS, invoking_record=self.step_E2_RS,
                start_time=timezone.now(), end_time=timezone.now()).save()
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
        execlog = ExecLog(record=other_runstep, invoking_record=other_runstep,
                          start_time=timezone.now(), end_time=timezone.now())
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
        self.step_E1_RS.outputs.clear()
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
        self.step_E1_RS.execrecord = self.step_D1_RS.execrecord
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" points to transformation "{}" but corresponding ExecRecord '
                                          'does not'.format(self.step_E1_RS, self.step_E1)),
                                self.step_E1_RS.clean)

    def test_RunStep_deleted_output_with_data(self):
        """
        A RunStep with an output marked for deletion, should not have
        any Datasets associated to that output.
        """
        self.step_through_runstep_creation("first_step_complete")
        self.step_E1.outputs_to_delete.add(self.mA.outputs.get(dataset_name="A1_out"))
        self.step_E1.save()
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
        self.assertTrue(self.step_E2_RS.is_complete())
        self.assertIsNone(self.step_E2_RS.complete_clean())

    def test_RunStep_complete_clean_no_execrecord(self):
        """
        A RunStep with no ExecRecord is not complete.
        """
        self.step_through_runstep_creation("first_runstep")
        self.assertFalse(self.step_E1_RS.is_complete())
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

class RunTests(ArchiveTestSetup):

    def step_through_run_creation(self, bp):
        """
        Helper function to step through creation of a Run. bp is a
        breakpoint - these are defined throughout (see the code).
        """
        # Empty Runs.
        self.pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        if bp == "empty_runs": return

        # First RunStep associated.
        self.step_E1_RS = self.step_E1.pipelinestep_instances.create(run=self.pE_run)
        if bp == "first_step": return

        # First RunSIC associated and completed.
        step_E1_RSIC = self.step_E1.cables_in.first().psic_instances.create(runstep=self.step_E1_RS)
        self.make_complete_non_reused(step_E1_RSIC, [self.raw_symDS], [self.raw_symDS])
        if bp == "first_cable": return

        # First RunStep completed.
        self.make_complete_non_reused(self.step_E1_RS, [self.raw_symDS], [self.doublet_symDS])
        if bp == "first_step_complete": return

        # Second RunStep associated.
        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        if bp == "second_step": return

        # Sub-pipeline for step 2 - reset step_E2_RS.
        self.step_E2_RS.delete()
        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run, reused=None)
        self.complete_RSICs(self.step_E2_RS, [self.triplet_symDS, self.singlet_symDS], 
                                             [self.D1_in_symDS, self.singlet_symDS])
        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
        if bp == "sub_pipeline": return

        # Complete sub-Pipeline.
        self.step_D1_RS = self.step_D1.pipelinestep_instances.create(run=self.pD_run)
        self.complete_RSICs(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], 
                                             [self.D1_in_symDS, self.singlet_symDS])
        self.make_complete_non_reused(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], [self.C1_in_symDS])
        pD_ROC = self.pD.outcables.first().poc_instances.create(run=self.pD_run)
        self.make_complete_non_reused(pD_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
        if bp == "sub_pipeline_complete": return

        # Third RunStep associated.
        self.step_E3_RS = self.step_E3.pipelinestep_instances.create(run=self.pE_run)
        if bp == "third_step": return

        # Third RunStep completed.
        self.complete_RSICs(self.step_E3_RS, [self.C1_in_symDS, self.doublet_symDS], 
                                             [self.C1_in_symDS, self.C2_in_symDS])
        self.make_complete_non_reused(self.step_E3_RS, [self.C1_in_symDS, self.C2_in_symDS],
                                                       [self.singlet_symDS, self.raw_symDS, self.raw_symDS])
        if bp == "third_step_complete": return

        # Outcables associated.
        roc1 = self.pE.outcables.get(output_idx=1).poc_instances.create(run=self.pE_run)
        self.make_complete_non_reused(roc1, [self.C1_in_symDS], [self.doublet_symDS])
        if bp == "first_outcable": return

        roc2 = self.pE.outcables.get(output_idx=2).poc_instances.create(run=self.pE_run)
        self.make_complete_non_reused(roc2, [self.singlet_symDS], [self.singlet_symDS])
        roc3 = self.pE.outcables.get(output_idx=3).poc_instances.create(run=self.pE_run)
        self.make_complete_non_reused(roc3, [self.C3_out_symDS], [self.C3_out_symDS])

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
                                re.escape('{} "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'no log should have been generated'.format("RunOutputCable", cable1)),
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

class RunSICTests(ArchiveTestSetup):

    def test_RSIC_many_execlogs(self):
        run = self.pE.pipeline_instances.create(user=self.myUser)
        runstep = self.pE.steps.first().pipelinestep_instances.create(run=run)
        cable = self.pE.steps.first().cables_in.first()
        rsic = cable.psic_instances.create(runstep=runstep)
        rsic.reused = False
        for i in range(2):
            rsic.log.create(invoking_record=rsic,
                            start_time=timezone.now(),
                            end_time=timezone.now())
        self.assertRaisesRegexp(ValidationError,
                'RunSIC "{}" has {} ExecLogs but should have only one'.
                        format(rsic, 2),
                rsic.clean)

    def step_through_runsic_creation(self, bp):
        """
        Helper function to step through creating an RSIC, breaking at a
        certain point (see the code).
        """
        self.step_E3_RS = self.step_E3.pipelinestep_instances.create(run=self.pE_run)
        if bp == "runstep": return 
        
        self.E11_32_RSIC = self.E11_32.psic_instances.create(runstep=self.step_E3_RS)
        if bp == "rsic_created": return

        self.make_complete_non_reused(self.E11_32_RSIC, [self.doublet_symDS], [self.C2_in_symDS])
        if bp == "rsic_completed": return

        self.E21_31_RSIC = self.E21_31.psic_instances.create(runstep=self.step_E3_RS)
        self.make_complete_non_reused(self.E21_31_RSIC, [self.C1_in_symDS], [self.C1_in_symDS])
        self.make_complete_non_reused(self.step_E3_RS, [self.C1_in_symDS, self.C2_in_symDS],
                                                  [self.C1_out_symDS, self.C2_out_symDS, self.C3_out_symDS])
        if bp == "runstep_completed": return

    def test_RunSIC_clean_wrong_pipelinestep(self):
        """
        A RunSIC whose PipelineStepInputCable does not belong to its
        RunStep's PipelineStep, is not clean.
        """
        self.step_through_runsic_creation("runstep")
        rsic = self.E01_21.psic_instances.create(runstep=self.step_E3_RS)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('PSIC "{}" does not belong to PipelineStep "{}"'
                                          .format(self.E01_21, self.step_E3)),
                                rsic.clean)

    def test_RunSIC_clean_unset_reused(self):
        """
        A RunSIC whose PipelineStepInputCable and RunStep are
        consistent, but which has not set reused yet, is clean.
        """
        self.step_through_runsic_creation("rsic_created")
        self.assertIsNone(self.E11_32_RSIC.clean())

    def test_RunSIC_clean_unset_reused_with_data(self):
        """
        A RunSIC which has not decided whether to reuse an ExecRecord,
        but which has associated data, is not clean.
        """
        self.step_through_runsic_creation("rsic_created")
        self.doublet_DS.created_by = self.E11_32_RSIC
        self.doublet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSIC "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'no Datasets should be associated'.format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_unset_reused_with_execrecord(self):
        """
        A RunSIC which has not decided whether to reuse an ExecRecord,
        but which has one associated, is not clean.
        """
        self.step_through_runsic_creation("rsic_created")
        self.E11_32_RSIC.reused = None

        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_runstep = self.step_E3.pipelinestep_instances.create(run=other_run)
        other_rsic = self.E11_32.psic_instances.create(runstep=other_runstep)
        self.make_complete_non_reused(other_rsic, [self.doublet_symDS], [self.C2_in_symDS])
        self.E11_32_RSIC.execrecord = other_rsic.execrecord

        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSIC "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'execrecord should not be set yet'.format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_reused_with_data(self):
        """
        A RunSIC which has reused an existing ExecRecord, but has an
        associated Dataset, is not clean.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = True
        self.doublet_DS.created_by = self.E11_32_RSIC
        self.doublet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSIC "{}" reused an ExecRecord and should not have generated any Datasets'
                                          .format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_reused_bad_execrecord(self):
        """
        A RunSIC whose ExecRecord is not clean, is not itself clean.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = True
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.C1_in.execrecordouts_referencing.add(ero)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Input "{}" is not the one fed by the PSIC of ExecRecord "{}"'
                                          .format(self.C1_in, self.E11_32_RSIC.execrecord)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_reused_incompatible_execrecord(self):
        """
        A RunSIC which is reusing an ExecRecord for an incompatible
        PipelineStepInputCable is not clean.
        """
        self.step_through_runsic_creation("rsic_complete")
        self.E11_32_RSIC.reused = True

        # Create an incompatible RunSIC.
        runstep = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        runsic = self.E02_22.psic_instances.create(runstep=runstep)
        self.make_complete_non_reused(runsic, [self.singlet_symDS], [self.singlet_symDS])
        self.E11_32_RSIC.execrecord = runsic.execrecord
        self.assertRaisesRegexp(ValidationError,
                                re.escape('PSIC of RunSIC "{}" is incompatible with that of its ExecRecord'
                                          .format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_reused_execrecord_wrong_object(self):
        """
        A RunSIC's ExecRecord must be for a PipelineStepInputCable and
        not some other pipeline component (reused case).
        """
        self.step_through_runsic_creation("runstep_completed")
        self.E11_32_RSIC.reused = True

        self.E11_32_RSIC.execrecord = self.step_E3_RS.execrecord
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord of RunSIC "{}" does not represent a PSIC'
                                          .format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)
        # Check of propagation:
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord of RunSIC "{}" does not represent a PSIC'
                                          .format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.complete_clean)

    def test_RunSIC_clean_reused_psic_keeps_output_no_data(self):
        """
        A RunSIC reusing an ExecRecord, whose PipelineStepInputCable
        keeps its output should, have data in its ExecRecordOut.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = True
        self.E11_32.keep_output = True
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSIC "{}" keeps its output; ExecRecordOut "{}" should reference existent '
                                          'data'.format(self.E11_32_RSIC, ero)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_reused_psic_keeps_output_with_data(self):
        """
        A RunSIC reusing an ExecRecord, whose PipelineStepInputCable
        keeps its output, should have data in its ExecRecordOut.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = True
        self.E11_32.keep_output = True
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.E11_32_output_DS.symbolicdataset = ero.symbolicdataset
        self.E11_32_output_DS.save()
        self.assertIsNone(self.E11_32_RSIC.clean())

    def test_RunSIC_clean_reused_complete_RSIC(self):
        """
        A RunSIC reusing an ExecRecord, whose PipelineStepInputCable
        keeps its output, having data in its ExecRecordOut, is complete
        and clean.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = True
        self.E11_32.keep_output = True
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.E11_32_output_DS.symbolicdataset = ero.symbolicdataset
        self.E11_32_output_DS.save()
        self.assertTrue(self.E11_32_RSIC.is_complete())
        self.assertIsNone(self.E11_32_RSIC.complete_clean())

    def test_RunSIC_complete_reused_no_execrecord(self):
        """
        A RunSIC reusing an ExecRecord, which doesn't have one
        associated, is not complete.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = True
        self.E11_32.keep_output = True
        self.E11_32_RSIC.execrecord = None
        self.assertFalse(self.E11_32_RSIC.is_complete())
        self.assertRaisesRegexp(ValidationError, 
                                re.escape('{} "{}" is not complete'.format("RunSIC", self.E11_32_RSIC)),
                                self.E11_32_RSIC.complete_clean)

    def test_RunSIC_clean_not_reused_no_execrecord(self):
        """
        A RunSIC which has decieded not to reuse an ExecRecord, but
        which doesn't have one yet, is clean.
        """
        self.step_through_runsic_creation("rsic_created")
        self.E11_32_RSIC.reused = False
        self.assertIsNone(self.E11_32_RSIC.clean())

    def test_RunSIC_clean_not_reused_bad_execrecord(self):
        """
        A RunSIC which is not reusing an ExecRecord, but has a bad
        ExecRecord associated, is not clean.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.C1_in.execrecordouts_referencing.add(ero)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Input "{}" is not the one fed by the PSIC of ExecRecord "{}"'
                                          .format(self.C1_in, self.E11_32_RSIC.execrecord)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_not_reused_incompatible_execrecord(self):
        """
        A RunSIC which is not reusing an ExecRecord, and has an
        ExecRecord for an incompatible PipelineStepInputCable, is
        not clean.
        """
        self.step_through_runsic_creation("rsic_complete")
        self.E11_32_RSIC.reused = False

        # Create an incompatible RunSIC.
        runstep = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        runsic = self.E02_22.psic_instances.create(runstep=runstep)
        self.make_complete_non_reused(runsic, [self.singlet_symDS], [self.singlet_symDS])
        self.E11_32_RSIC.execrecord = runsic.execrecord
        self.assertRaisesRegexp(ValidationError,
                                re.escape('PSIC of RunSIC "{}" is incompatible with that of its ExecRecord'
                                          .format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_not_reused_execrecord_wrong_object(self):
        """
        A RunSIC's ExecRecord must be for a PipelineStepInputCable and
        not some other pipeline component (non-reused case).
        """
        self.step_through_runsic_creation("runstep_completed")
        self.E11_32_RSIC.reused = False

        self.E11_32_RSIC.execrecord = self.step_E3_RS.execrecord
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord of RunSIC "{}" does not represent a PSIC'
                                          .format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)
        # Check of propagation:
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord of RunSIC "{}" does not represent a PSIC'
                                          .format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.complete_clean)

    def test_RunSIC_clean_not_reused_psic_discards_output_no_data(self):
        """
        A RunSIC not reusing an ExecRecord, whose PipelineStepInputCable
        does not keep its output, should not have data in its ExecRecordOut.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.assertIsNone(self.E11_32_RSIC.clean())

    def test_RunSIC_clean_not_reused_psic_discards_output_with_data(self):
        """
        A RunSIC not reusing an ExecRecord, whose PipelineStepInputCable
        does not keep its output, should not have data in its ExecRecordOut.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.E11_32_RSIC.output.add(self.E11_32_output_DS)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSIC "{}" does not keep its output but a dataset was registered'
                                          .format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_not_reused_psic_keeps_output_no_data(self):
        """
        A RunSIC not reusing an ExecRecord, whose PipelineStepInputCable
        keeps its output, should have data in its ExecRecordOut.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.E11_32.keep_output = True
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSIC "{}" keeps its output; ExecRecordOut "{}" should reference existent '
                                          'data'.format(self.E11_32_RSIC, ero)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_not_reused_psic_keeps_output_with_data(self):
        """
        A RunSIC not reusing an ExecRecord, whose PipelineStepInputCable
        keeps its output, should have data in its ExecRecordOut.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.E11_32.keep_output = True
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.E11_32_output_DS.symbolicdataset = ero.symbolicdataset
        self.E11_32_output_DS.save()
        self.E11_32_RSIC.output.add(self.E11_32_output_DS)
        self.assertIsNone(self.E11_32_RSIC.clean())

    def test_RunSIC_clean_not_reused_nontrivial_no_data(self):
        """
        A RunSIC which is not reused, non-trivial, and is for a
        PipelineStepInputCable which keeps its output, must have
        produced data.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.E11_32.keep_output = True
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.E11_32_output_DS.symbolicdataset = ero.symbolicdataset
        self.E11_32_output_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSIC "{}" was not reused, trivial, or deleted; it should have produced '
                                          'data'.format(self.E11_32_RSIC)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_not_reused_nontrivial_wrong_data(self):
        """
        A RunSIC which is nontrivial, is not reusing an ExecRecord, and
        is for a PipelineStepInputCable which keeps its output, must
        have produced the same Dataset as is recorded in its
        ExecRecordOut.  
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.E11_32.keep_output = True

        # Associate different datasets to RSIC and associated ERO.
        self.doublet_DS.created_by = self.E11_32_RSIC
        self.doublet_DS.save()
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.E11_32_output_DS.symbolicdataset = ero.symbolicdataset
        self.E11_32_output_DS.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Dataset "{}" was produced by RunSIC "{}" but is not in an ERO of '
                                          'ExecRecord "{}"'.format(self.doublet_DS, self.E11_32_RSIC,
                                                                   self.E11_32_RSIC.execrecord)),
                                self.E11_32_RSIC.clean)
    
    def test_RunSIC_clean_not_reused_nontrivial_correct_data(self):
        """
        A RunSIC which is nontrivial, is not reusing an ExecRecord, and
        is for a PipelineStepInputCable which keeps its output, must
        have produced the same Dataset as is recorded in its
        ExecRecordOut.  
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.E11_32.keep_output = True
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        self.E11_32_output_DS.created_by = self.E11_32_RSIC
        self.E11_32_output_DS.symbolicdataset = ero.symbolicdataset
        self.E11_32_output_DS.save()
        self.assertIsNone(self.E11_32_RSIC.clean())

    def test_RunSIC_complete_not_reused(self):
        """
        A RunSIC which is nontrivial, is not reusing an ExecRecord, and
        is for a PipelineStepInputCable which keeps its output, which
        has produced the same Dataset as is recorded in its
        ExecRecordOut, is clean and complete.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.E11_32.keep_output = True
        self.E11_32_output_DS.created_by = self.E11_32_RSIC
        self.E11_32_output_DS.save()
        self.E11_32_output_DS.symbolicdataset = self.E11_32_RSIC.execrecord.execrecordouts.first().symbolicdataset
        self.E11_32_output_DS.save()
        self.assertTrue(self.E11_32_RSIC.is_complete())
        self.assertIsNone(self.E11_32_RSIC.complete_clean())

    def test_RunSIC_incomplete_not_reused(self):
        """
        A RunSIC which is not reusing an ExecRecord, but which does not
        have an ExecRecord, is not complete.  
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False
        self.E11_32_RSIC.execrecord = None
        self.assertFalse(self.E11_32_RSIC.is_complete())
        self.assertRaisesRegexp(ValidationError,
                                re.escape('{} "{}" is not complete'.format("RunSIC", self.E11_32_RSIC)),
                                self.E11_32_RSIC.complete_clean)

class RunOutputCableTests(ArchiveTestSetup):

    def test_ROC_many_execlogs(self):
        run = self.pE.pipeline_instances.create(user=self.myUser)
        run_output_cable = self.E31_42.poc_instances.create(run=run)
        run_output_cable.reused = False
        for i in range(2):
            run_output_cable.log.create(invoking_record=run_output_cable,
                                        start_time=timezone.now(),
                                        end_time=timezone.now())
        self.assertRaisesRegexp(ValidationError,
                'RunOutputCable "{}" has {} ExecLogs but should have only one'.
                        format(run_output_cable, 2),
                run_output_cable.clean)

    def step_through_roc_creation(self, bp):
        """Break at an intermediate stage of ROC creation."""
        self.E31_42_ROC = self.E31_42.poc_instances.create(run=self.pE_run)
        self.E21_41_ROC = self.E21_41.poc_instances.create(run=self.pE_run)
        if bp == "roc_created": return

        self.make_complete_non_reused(self.E31_42_ROC, [self.singlet_symDS], [self.singlet_symDS])
        if bp == "trivial_roc_completed": return

        self.make_complete_non_reused(self.E21_41_ROC, [self.C1_in_symDS], [self.doublet_symDS])
        self.doublet_DS.created_by = self.E21_41_ROC
        if bp == "custom_roc_completed": return

        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        self.pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
        self.D11_21_ROC = self.D11_21.poc_instances.create(run=self.pD_run)
        # Define some custom wiring for D11_21: swap the first two columns.
        pin1, pin2, _ = (m for m in self.triplet_cdt.members.all())
        self.D11_21.custom_outwires.create(source_pin=pin1, dest_pin=pin2)
        self.D11_21.custom_outwires.create(source_pin=pin2, dest_pin=pin1)
        if bp == "subrun": return

        self.make_complete_non_reused(self.D11_21_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
        if bp == "subrun_complete": return

    def test_ROC_clean_correct_parent_run(self):
        """PipelineOutputCable belongs to parent Run's Pipeline.

        A RunOutputCable's PipelineOutputCable must belong to the
        Pipeline of its parent Run.
        """
        self.step_through_roc_creation("roc_created")
        self.assertIsNone(self.E31_42_ROC.clean())

    def test_ROC_clean_wrong_parent_run(self):
        """PipelineOutputCable is for Pipeline not of parent Run.

        A RunOutputCable's PipelineOutputCable must belong to the
        Pipeline of its parent Run.
        """
        self.step_through_roc_creation("roc_created")
        pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        self.E31_42_ROC.run = pD_run
        self.assertRaisesRegexp(ValidationError, 
                                re.escape('POC "{}" does not belong to Pipeline "{}"'.format(self.E31_42, self.pD)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_unset_reused_with_data(self):
        """Reused is not set but data is associated. 
        
        A RunOutputCable which has not decided whether to reuse an
        ExecRecord should not have generated any data.
        """
        self.step_through_roc_creation("roc_created")
        self.C1_out_DS.created_by = self.E31_42_ROC
        self.C1_out_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'no Datasets should be associated'.format(self.E31_42_ROC)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_unset_reused_with_execrecord(self):
        """Reused is not set but an ExecRecord is associated.

        A RunOutputCable which has not decided whether to reuse an
        ExecRecord should not have one associated.
        """
        self.step_through_roc_creation("roc_created")

        # Create a compatible ExecRecord to associate.
        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_roc = self.E31_42.poc_instances.create(run=other_run)
        self.make_complete_non_reused(other_roc, [self.C1_out_symDS], [self.C1_out_symDS])
        self.E31_42_ROC.execrecord = other_roc.execrecord

        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" has not decided whether or not to reuse an ExecRecord; '
                                          'execrecord should not be set yet'.format(self.E31_42_ROC)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_reused_with_data(self):
        """Reused is True but data is associated.

        A RunOutputCable which is reusing an ExecRecord should not have
        generated any Datasets.
        """
        self.step_through_roc_creation("roc_created")
        self.E31_42_ROC.reused = True
        self.singlet_DS.created_by = self.E31_42_ROC
        self.singlet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" reused an ExecRecord and should not have generated any '
                                          'Datasets'.format(self.E31_42_ROC)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_not_reused_trivial_no_data(self):
        """Reused is False, cable is trivial, no data associated.

        A RunOutputCable which is not reusing an ExecRecord, but which
        is trivial, should not have generated any Datasets.
        """
        self.step_through_roc_creation("roc_created")
        self.E31_42_ROC.reused = False
        self.assertIsNone(self.E31_42_ROC.clean())

    def test_ROC_clean_not_reused_trivial_with_data(self):
        """Reused is False, cable is trivial, data associated.

        A RunOutputCable which is not reusing an ExecRecord, but which
        is trivial, should not have generated any Datasets.
        """
        self.step_through_roc_creation("roc_created")
        self.E31_42_ROC.reused = False
        self.E31_42_ROC.log.create(invoking_record=self.E31_42_ROC,
                                   start_time=timezone.now(),
                                   end_time=timezone.now())
        self.singlet_DS.created_by = self.E31_42_ROC
        self.singlet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" is trivial and should not have generated any Datasets'
                                          .format(self.E31_42_ROC)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_not_reused_nontrivial_with_data(self):
        """Non-trivial cable, good data attached.

        A RunOutputCable which is non-trivial, is not reusing an
        ExecRecord, and has data attached, is clean.
        """
        self.step_through_roc_creation("custom_roc_completed")
        self.assertIsNone(self.E21_41_ROC.clean())

    def test_ROC_clean_not_reused_nontrivial_multiple_datasets(self):
        """Non-trivial cable, multiple datasets attached.

        A RunOutputCable which is non-trivial, is not reusing an
        ExecRecord, should generate at most one Dataset.
        """
        self.step_through_roc_creation("custom_roc_completed")
        self.E1_out_DS.created_by = self.E21_41_ROC
        self.E1_out_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" should generate at most one Dataset'
                                          .format(self.E21_41_ROC)),
                                self.E21_41_ROC.clean)
        
    def test_ROC_clean_not_reused_nontrivial_bad_data(self):
        """Propagation: bad data attached to RunOutputCable.

        A RunOutputCable which produced a bad Dataset is not clean.
        """
        self.step_through_roc_creation("custom_roc_completed")
        old_checksum = self.doublet_DS.symbolicdataset.MD5_checksum
        self.doublet_DS.symbolicdataset.MD5_checksum = "foo"
        self.doublet_DS.symbolicdataset.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('File integrity of "{}" lost. Current checksum "{}" does not equal expected '
                                          'checksum "{}"'.format(self.doublet_DS, old_checksum, "foo")),
                                self.E21_41_ROC.clean)

    def test_ROC_clean_not_reused_incomplete_execrecord(self):
        """Propagation: ExecRecord is not complete and clean.

        A trivial RunOutputCable which has an ExecRecord without the
        appropriate ExecRecordIns is not clean.
        """
        self.step_through_roc_creation("trivial_roc_completed")
        self.E31_42_ROC.execrecord.execrecordins.first().delete()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Input to ExecRecord "{}" is not quenched'
                                          .format(self.E31_42_ROC.execrecord)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_incompatible_execrecord(self):
        """RunOutputCable has ExecRecord for wrong PipelineOuputCable.

        The ExecRecord of a RunOutputCable must correspond to the same
        PipelineOutputCable that the RunOutputCable corresponds to.
        """
        self.step_through_roc_creation("custom_roc_completed")
        self.E31_42_ROC.execrecord = self.E21_41_ROC.execrecord
        error_msg = "POC of RunOutputCable .* is incompatible with that of its ExecRecord"
        self.assertRaisesRegexp(ValidationError, 
                                re.escape('POC of RunOutputCable "{}" is incompatible with that of its ExecRecord'
                                          .format(self.E31_42_ROC)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_wrong_object_execrecord(self):
        """RunOutputCable has ExecRecord for PipelineStep.

        A RunOutputCable's ExecRecord must be for a PipelineOutputCable.
        """
        self.step_through_roc_creation("custom_roc_completed")
        runstep = self.step_E1.pipelinestep_instances.create(run=self.pE_run)
        self.complete_RSICs(runstep, [self.raw_symDS], [self.raw_symDS])
        self.make_complete_non_reused(runstep, [self.raw_symDS], [self.doublet_symDS])
        self.E31_42_ROC.execrecord = runstep.execrecord
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord of RunOutputCable "{}" does not represent a POC'
                                          .format(self.E31_42_ROC)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_deleted_output_no_data(self):
        """RunOutputCable with output marked for deletion has no data.

        A RunOutputCable from a subrun, where the PipelineStep has
        marked the relevant output for deletion, should not have
        any associated Datasets."""
        self.step_through_roc_creation("subrun")
        self.step_E2.outputs_to_delete.add(self.pD.outputs.get(dataset_name="D1_out"))
        self.assertIsNone(self.D11_21_ROC.clean())

    def test_ROC_clean_deleted_output_with_data(self):
        """RunOutputCable with output marked for deletion has data.

        A RunOutputCable from a subrun, where the PipelineStep has
        marked the relevant output for deletion, should not have
        any associated Datasets."""

        self.step_through_roc_creation("subrun_complete")
        self.step_E2.outputs_to_delete.add(self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(ValidationError,
                                re.escape('{} "{}" does not keep its output but a dataset was registered'
                                          .format("RunOutputCable", self.D11_21_ROC)),
                                self.D11_21_ROC.clean)

    def test_ROC_clean_kept_output_no_data(self):
        """RunOutputCable which should keep its output has no data.

        A RunOutputCable from a subrun, where the PipelineStep has
        not marked the relevant output for deletion, should have a
        Dataset in its ExecRecordOut.
        """
        self.step_through_roc_creation("subrun_complete")
        self.triplet_3_rows_DS.created_by = self.D11_21_ROC
        self.triplet_3_rows_DS.save()
        self.C1_in_DS.delete()
        ero = self.D11_21_ROC.execrecord.execrecordouts.first()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecordOut "{}" should reference existent data'.format(ero)),
                                self.D11_21_ROC.clean)

    def test_ROC_clean_kept_output_reused_no_data(self):
        """Reused RunOutputCable should produce no data.

        A RunOutputCable from a subrun, where the PipelineStep has not
        marked the relevant output for deletion, should have no data if
        it is reusing an ExecRecord.
        """
        self.step_through_roc_creation("subrun")
        self.D11_21_ROC.reused = True
        self.assertFalse(self.D11_21_ROC.output.exists())
        self.assertIsNone(self.D11_21_ROC.clean())

    def test_ROC_clean_kept_output_trivial_no_data(self):
        """Non-reused, trivial RunOutputCable should produce no data.

        A RunOutputCable from a subrun where the PipelineStep has not
        marked the relevant output for deletion, and which is not
        reusing an ExecRecord, should still have no data associated if
        it is a trivial cable.
        """
        self.step_through_roc_creation("subrun")
        self.D11_21.custom_outwires.all().delete()
        self.assertFalse(self.D11_21_ROC.output.exists())
        self.assertIsNone(self.D11_21_ROC.clean())

    def test_ROC_clean_kept_output_nontrivial_no_data(self):
        """Non-reused, nontrivial RunOutputCable with no data.

        A nontrivial RunOutputCable from a subrun which is not reusing
        an ExecRecord, where the PipelineStep has not marked the output
        for deletion, should produce data.
        """
        self.step_through_roc_creation("subrun_complete")
        self.assertFalse(self.D11_21_ROC.output.exists())
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" was not reused, trivial, or deleted; it should have '
                                          'produced data'.format(self.D11_21_ROC)),
                                self.D11_21_ROC.clean)

    def test_ROC_clean_wrong_data(self):
        """Non-reused, nontrival RunOutputCable with the wrong Dataset.

        A RunOutputCable with a Dataset different from that in its
        ExecRecordOut is not clean.
        """
        self.step_through_roc_creation("subrun")
        self.D11_21_ROC.output.add(self.triplet_3_rows_DS)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Dataset "{}" was produced by RunOutputCable "{}" but is not in an ERO of '
                                          'ExecRecord "{}"'.format(self.triplet_3_rows_DS, self.D11_21_ROC, 
                                                                   self.D11_21_ROC.execrecord)),
                                self.D11_21_ROC.clean)

    def test_ROC_clean_correct_data(self):
        """Non-reused, nontrivial RunOutputCable with correct Dataset.

        A RunOutputCable with the same Dataset in its ExecRecordOut as
        in its output, is clean.
        """
        self.step_through_roc_creation("subrun_complete")
        self.D11_21_ROC.output.add(self.C1_in_DS)
        self.assertIsNone(self.D11_21_ROC.clean())

    def test_ROC_clean_trivial_no_data(self):
        """Trivial top-level cable, no data associated.

        A trivial RunOutputCable not for a subrun, which has no output
        Dataset associated, is clean.
        """
        self.step_through_roc_creation("trivial_roc_completed")
        self.assertIsNone(self.E31_31_ROC.clean())

    def test_ROC_clean_trivial_with_data(self):
        """Trivial top-level cable with associated data.

        A trivial RunOutputCable not for a subrun, which has an output
        Dataset associated, is not clean.
        """
        self.step_through_roc_creation("trivial_roc_completed")
        self.singlet_DS.created_by = self.E31_42_ROC
        self.singlet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" is trivial and should not have generated any Datasets'
                                          .format(self.E31_42_ROC)),
                                self.E31_42_ROC.clean)

    def test_ROC_clean_trivial_no_data(self):
        """Trivial top-level cable with no associated data.

        A trivial RunOutputCable not for a subrun, which has no output
        Dataset associated, is clean.
        """
        self.step_through_roc_creation("trivial_roc_completed")
        self.assertFalse(self.E31_42_ROC.has_data())
        self.assertIsNone(self.E31_42_ROC.clean())

    def test_ROC_clean_nontrivial_good_data(self):
        """Nontrivial top-level cable with correct associated data.

        A RunOutputCable with custom wires, which has associated output
        data matching its ExecRecordOut, is clean.
        """
        self.step_through_roc_creation("custom_roc_completed")
        self.E21_41_ROC.output.add(self.doublet_DS)
        self.assertTrue(self.E21_41_ROC.has_data())
        self.assertIsNone(self.E21_41_ROC.clean())

    def test_ROC_clean_nontrivial_no_data(self):
        """Nontrivial top-level cable with no data associated.

        A nontrivial, non-reused RunOutputCable not for a subrun must
        have produced output data, otherwise it is not clean.
        """
        self.step_through_roc_creation("custom_roc_completed")
        self.doublet_DS.created_by = None
        self.doublet_DS.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunOutputCable "{}" was not reused, trivial, or deleted; it should have '
                                          'produced data'.format(self.E21_41_ROC)),
                                self.E21_41_ROC.clean)

    def test_ROC_subrun_complete(self):
        """Clean and complete RunOutputCable for subrun.

        A RunOutputCable for a subrun where the output of the
        sub-pipeline kept, which has the correct associated Dataset,
        is clean and complete.
        """
        self.step_through_roc_creation("subrun")
        self.D11_21_ROC.output.add(self.C1_in_DS)
        self.assertTrue(self.D11_21_ROC.output.exists())
        self.assertTrue(self.D11_21_ROC.is_complete())
        self.assertIsNone(self.D11_21_ROC.complete_clean())

    def test_ROC_subrun_no_execrecord(self):
        """RunOutputCable with no ExecRecord.

        A nontrivial RunOutputCable for a subrun which has no ExecRecord
        is not complete yet.
        """
        self.step_through_roc_creation("subrun")
        self.D11_21_ROC.execrecord = None
        self.assertFalse(self.D11_21_ROC.is_complete())
        self.assertRaisesRegexp(ValidationError,
                                re.escape('{} "{}" is not complete'.format("RunOutputCable", self.D11_21_ROC)),
                                self.D11_21_ROC.complete_clean)

class DatasetTests(librarian.tests.LibrarianTestSetup):

    def test_Dataset_check_MD5(self):
        old_md5 = "7dc85e11b5c02e434af5bd3b3da9938e"
        new_md5 = "d41d8cd98f00b204e9800998ecf8427e"

        # MD5 is now stored in symbolic dataset - even after the dataset was deleted
        self.assertEqual(self.raw_DS.compute_md5(), old_md5)

        # Initially, no change to the raw dataset has occured, so the md5 check will pass
        self.assertEqual(self.raw_DS.clean(), None)

        # The contents of the file are changed, disrupting file integrity
        self.raw_DS.dataset_file.close()
        self.raw_DS.dataset_file.open(mode='w')
        self.raw_DS.dataset_file.close()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('File integrity of "{}" lost. Current checksum "{}" does not equal expected '
                                           'checksum "{}"'.format(self.raw_DS, new_md5, old_md5)),
                                self.raw_DS.clean)
