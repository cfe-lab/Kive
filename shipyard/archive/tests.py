"""
Shipyard archive application unit tests.
"""

from django.test import TestCase
from django.core.files import File
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType

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
import sandbox.execute

from method.tests import samplecode_path
import librarian.tests
# TODO: Put this someplace better, maybe shipyard/testing_utils.py?
import sandbox.tests_rm
from file_access_utils import compute_md5


# Note that these tests use the exact same setup as librarian.


class ArchiveTestSetup(librarian.tests.LibrarianTestSetup, sandbox.tests_rm.UtilityMethods):
    def setUp(self):
        librarian.tests.LibrarianTestSetup.setUp(self)
        sandbox.tests_rm.UtilityMethods.setUp(self)
        self.pE_run = self.pE.pipeline_instances.create(user=self.myUser)

    def tearDown(self):
        super(ArchiveTestSetup, self).tearDown()
        sandbox.tests_rm.clean_files()

    def make_complete_non_reused(self, record, input_SDs, output_SDs):
        """
        Helper function to do everything necessary to make a RunStep, 
        RunOutputCable, or RunStepInputCable complete, when it has not
        reused an ExecRecord (ie. make a new ExecRecord).

        """
        self.make_execlog_and_mark_non_reused_runatomic(record)

        curr_execlog = record.log.first()
        execrecord = ExecRecord.create(curr_execlog, record.component, input_SDs, output_SDs)
        record.execrecord = execrecord
        record.save()

    def make_execlog_and_mark_non_reused_runatomic(self, record):
        """Attaches a good ExecLog to a RunAtomic."""
        record_type = record.__class__.__name__
        record.reused = False
        record.save()

        execlog = ExecLog(record=record, invoking_record=record, start_time=timezone.now(), end_time=timezone.now())
        execlog.save()
        if record_type == "RunStep":
            MethodOutput(execlog=execlog, return_code=0).save()

    def make_complete_reused(self, record, input_SDs, output_SDs, other_parent):
        """
        Helper function to do everything necessary to make a RunStep, 
        RunOutputCable, or RunStepInputCable complete, when it _has_
        reused an ExecRecord (ie. make an ExecRecord for it to resue).
        """
        record_type = record.__class__.__name__

        new_record = record.__class__.create(record.component, other_parent)
    
        execlog = ExecLog(record=new_record, invoking_record=new_record, start_time=timezone.now(), 
                          end_time=timezone.now())
        execlog.save()
        if record_type == "RunStep":
            MethodOutput(execlog=execlog, return_code=0).save()
        execrecord = ExecRecord.create(execlog, record.component, input_SDs, output_SDs)

        record.execrecord = execrecord
        record.reused = True
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
        self.raw_symDS.integrity_checks.create(execlog=self.E03_11_RSIC.log.first())
        if bp == "first_rsic": return

        self.make_complete_non_reused(self.step_E1_RS, [self.raw_symDS], [self.doublet_symDS])
        step1_in_ccl = self.doublet_symDS.content_checks.first()
        step1_in_ccl.execlog = self.step_E1_RS.log.first()
        step1_in_ccl.save()
        self.doublet_DS.created_by = self.step_E1_RS
        self.doublet_DS.save()
        if bp == "first_runstep_complete": return

        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        if bp == "second_runstep": return

        self.complete_RSICs(self.step_E2_RS, [self.triplet_symDS, self.singlet_symDS],
                                        [self.D1_in_symDS, self.singlet_symDS])
        self.E01_21_RSIC = self.step_E2_RS.RSICs.filter(PSIC=self.E01_21).first()
        self.E02_22_RSIC = self.step_E2_RS.RSICs.filter(PSIC=self.E02_22).first()

        D1_in_ccl = self.D1_in_symDS.content_checks.first()
        D1_in_ccl.execlog = self.E01_21_RSIC.log.first()
        D1_in_ccl.save()

        self.singlet_symDS.integrity_checks.create(execlog=self.E02_22_RSIC.log.first())
        if bp == "second_runstep_complete": return

        # Associate and complete sub-Pipeline.
        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
        self.step_D1_RS = self.step_D1.pipelinestep_instances.create(run=self.pD_run)
        self.complete_RSICs(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS],
                                        [self.D1_in_symDS, self.singlet_symDS])
        self.D01_11_RSIC = self.step_D1_RS.RSICs.filter(PSIC=self.D01_11).first()
        self.D02_12_RSIC = self.step_D1_RS.RSICs.filter(PSIC=self.D02_12).first()
        self.D1_in_symDS.integrity_checks.create(execlog=self.D01_11_RSIC.log.first())
        self.singlet_symDS.integrity_checks.create(execlog=self.D02_12_RSIC.log.first())

        self.make_complete_non_reused(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], [self.C1_in_symDS])
        C1_ccl = self.C1_in_symDS.content_checks.first()
        C1_ccl.execlog = self.step_D1_RS.log.first()
        C1_ccl.save()
        self.C1_in_DS.created_by = self.step_D1_RS
        self.C1_in_DS.save()

        pD_ROC = self.pD.outcables.first().poc_instances.create(run=self.pD_run)
        self.make_complete_non_reused(pD_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
        self.C1_in_symDS.integrity_checks.create(execlog=pD_ROC.log.first())

        if bp == "sub_pipeline": return

    def step_through_run_creation(self, bp):
        """
        Helper function to step through creation of a Run. bp is a
        breakpoint - these are defined throughout (see the code).
        """
        # Changed May 14, 2014 to add CCLs/ICLs where appropriate.
        # Empty Runs.
        self.pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        if bp == "empty_runs": return

        # First RunStep associated.
        self.step_E1_RS = self.step_E1.pipelinestep_instances.create(run=self.pE_run)
        if bp == "first_step": return

        # First RunSIC associated and completed.
        step_E1_RSIC = self.step_E1.cables_in.first().psic_instances.create(runstep=self.step_E1_RS)
        if bp == "first_cable_created": return

        self.make_complete_non_reused(step_E1_RSIC, [self.raw_symDS], [self.raw_symDS])
        icl = self.raw_symDS.integrity_checks.create(execlog=step_E1_RSIC.log.first())
        icl.start()
        icl.stop()
        icl.save()
        if bp == "first_cable": return

        # First RunStep completed.
        self.make_complete_non_reused(self.step_E1_RS, [self.raw_symDS], [self.doublet_symDS])
        step1_in_ccl = self.doublet_symDS.content_checks.first()
        step1_in_ccl.execlog = self.step_E1_RS.log.first()
        step1_in_ccl.save()
        self.doublet_DS.created_by = self.step_E1_RS
        self.doublet_DS.save()
        if bp == "first_step_complete": return

        # Second RunStep associated.
        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        if bp == "second_step": return

        # Sub-pipeline for step 2 - reset step_E2_RS.
        self.step_E2_RS.delete()
        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run, reused=None)
        self.complete_RSICs(self.step_E2_RS, [self.triplet_symDS, self.singlet_symDS],
                                             [self.D1_in_symDS, self.singlet_symDS])

        self.E01_21_RSIC = self.step_E2_RS.RSICs.filter(PSIC=self.E01_21).first()
        self.E02_22_RSIC = self.step_E2_RS.RSICs.filter(PSIC=self.E02_22).first()

        D1_in_ccl = self.D1_in_symDS.content_checks.first()
        D1_in_ccl.execlog = self.E01_21_RSIC.log.first()
        D1_in_ccl.save()

        icl = self.singlet_symDS.integrity_checks.create(execlog=self.E02_22_RSIC.log.first())
        icl.start()
        icl.stop()
        icl.save()

        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
        if bp == "sub_pipeline": return

        # Complete sub-Pipeline.
        self.step_D1_RS = self.step_D1.pipelinestep_instances.create(run=self.pD_run)
        self.complete_RSICs(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS],
                                             [self.D1_in_symDS, self.singlet_symDS])

        self.D01_11_RSIC = self.step_D1_RS.RSICs.filter(PSIC=self.D01_11).first()
        self.D02_12_RSIC = self.step_D1_RS.RSICs.filter(PSIC=self.D02_12).first()
        icl = self.D1_in_symDS.integrity_checks.create(execlog=self.D01_11_RSIC.log.first())
        icl.start()
        icl.stop()
        icl.save()
        icl = self.singlet_symDS.integrity_checks.create(execlog=self.D02_12_RSIC.log.first())
        icl.start()
        icl.stop()
        icl.save()

        self.make_complete_non_reused(self.step_D1_RS, [self.D1_in_symDS, self.singlet_symDS], [self.C1_in_symDS])
        C1_ccl = self.C1_in_symDS.content_checks.first()
        C1_ccl.execlog = self.step_D1_RS.log.first()
        C1_ccl.save()
        self.C1_in_DS.created_by = self.step_D1_RS
        self.C1_in_DS.save()

        pD_ROC = self.pD.outcables.first().poc_instances.create(run=self.pD_run)
        self.make_complete_non_reused(pD_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
        icl = self.C1_in_symDS.integrity_checks.create(execlog=pD_ROC.log.first())
        icl.start()
        icl.stop()
        icl.save()
        if bp == "sub_pipeline_complete": return

        # Third RunStep associated.
        self.step_E3_RS = self.step_E3.pipelinestep_instances.create(run=self.pE_run)
        if bp == "third_step": return

        # Third RunStep completed.
        self.complete_RSICs(self.step_E3_RS, [self.C1_in_symDS, self.doublet_symDS],
                                             [self.C1_in_symDS, self.C2_in_symDS])

        self.E21_31_RSIC = self.step_E3_RS.RSICs.filter(PSIC=self.E21_31).first()
        self.E11_32_RSIC = self.step_E3_RS.RSICs.filter(PSIC=self.E11_32).first()
        icl = self.C1_in_symDS.integrity_checks.create(execlog=self.E21_31_RSIC.log.first())
        icl.start()
        icl.stop()
        icl.save()

        # C2_in_symDS was created here so we associate its CCL with cable
        # E11_32.
        C2_in_ccl = self.C2_in_symDS.content_checks.first()
        C2_in_ccl.execlog = self.E11_32_RSIC.log.first()
        C2_in_ccl.save()

        if bp == "third_step_cables_done": return

        step3_outs = [self.C1_out_symDS, self.C2_out_symDS, self.C3_out_symDS]
        self.make_complete_non_reused(self.step_E3_RS, [self.C1_in_symDS, self.C2_in_symDS], step3_outs)
        # All of these were first created here, so associate the CCL of C1_out_symDS to step_E3_RS.
        # The others are raw and don't have CCLs.
        C1_out_ccl = self.C1_out_symDS.content_checks.first()
        C1_out_ccl.execlog = self.step_E3_RS.log.first()
        C1_out_ccl.save()

        if bp == "third_step_complete": return

        # Outcables associated.
        roc1 = self.pE.outcables.get(output_idx=1).poc_instances.create(run=self.pE_run)
        self.make_complete_non_reused(roc1, [self.C1_in_symDS], [self.E1_out_symDS])
        # This was first created here, so associate the CCL appropriately.
        E1_out_ccl = self.E1_out_symDS.content_checks.first()
        E1_out_ccl.execlog = roc1.log.first()
        E1_out_ccl.save()
        self.E1_out_DS.created_by = roc1
        self.E1_out_DS.save()

        if bp == "first_outcable": return

        roc2 = self.pE.outcables.get(output_idx=2).poc_instances.create(run=self.pE_run)
        self.make_complete_non_reused(roc2, [self.C1_out_symDS], [self.C1_out_symDS])
        roc3 = self.pE.outcables.get(output_idx=3).poc_instances.create(run=self.pE_run)
        self.make_complete_non_reused(roc3, [self.C3_out_symDS], [self.C3_out_symDS])

        # roc2 and roc3 are trivial cables, so we associate integrity checks with C1_out_symDS
        # and C3_out_symDS.
        icl = self.C1_out_symDS.integrity_checks.create(execlog=roc2.log.first())
        icl.start()
        icl.stop()
        icl.save()
        icl = self.C3_out_symDS.integrity_checks.create(execlog=roc3.log.first())
        icl.start()
        icl.stop()
        icl.save()

        if bp == "outcables_done": return

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
        # C2_in_symDS is created by this cable so associate a CCL appropriately.
        C2_ccl = self.C2_in_symDS.content_checks.first()
        C2_ccl.execlog = self.E11_32_RSIC.log.first()
        C2_ccl.save()
        if bp == "rsic_completed": return

        self.E21_31_RSIC = self.E21_31.psic_instances.create(runstep=self.step_E3_RS)
        self.make_complete_non_reused(self.E21_31_RSIC, [self.C1_in_symDS], [self.C1_in_symDS])
        # C1_in_symDS is not created by this RSIC, so associate an ICL.
        self.C1_in_symDS.integrity_checks.create(execlog=self.E21_31_RSIC.log.first())
        self.make_complete_non_reused(self.step_E3_RS, [self.C1_in_symDS, self.C2_in_symDS],
                                                  [self.C1_out_symDS, self.C2_out_symDS, self.C3_out_symDS])
        # Associate the CCL of C1_out_symDS with step_E3_RS.
        C1_out_ccl = self.C1_out_symDS.content_checks.first()
        C1_out_ccl.execlog = self.step_E3_RS.log.first()
        C1_out_ccl.save()
        if bp == "runstep_completed": return

    def step_through_roc_creation(self, bp):
        """Break at an intermediate stage of ROC creation."""
        self.E31_42_ROC = self.E31_42.poc_instances.create(run=self.pE_run)
        self.E21_41_ROC = self.E21_41.poc_instances.create(run=self.pE_run)
        if bp == "roc_created": return

        self.make_complete_non_reused(self.E31_42_ROC, [self.singlet_symDS], [self.singlet_symDS])
        if bp == "trivial_roc_completed": return

        self.make_complete_non_reused(self.E21_41_ROC, [self.C1_in_symDS], [self.doublet_symDS])
        self.doublet_DS.created_by = self.E21_41_ROC
        self.doublet_DS.save()
        if bp == "custom_roc_completed": return

        self.step_E2_RS = self.step_E2.pipelinestep_instances.create(run=self.pE_run)
        self.step_E2_RS.start()
        self.pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        self.pD_run.parent_runstep = self.step_E2_RS
        self.pD_run.save()
        self.D11_21_ROC = self.D11_21.poc_instances.create(run=self.pD_run)
        self.D11_21_ROC.start()
        # Define some custom wiring for D11_21: swap the first two columns.
        pin1, pin2, _ = (m for m in self.triplet_cdt.members.all())
        self.D11_21.custom_outwires.create(source_pin=pin1, dest_pin=pin2)
        self.D11_21.custom_outwires.create(source_pin=pin2, dest_pin=pin1)
        if bp == "subrun": return

        self.make_complete_non_reused(self.D11_21_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
        self.C1_in_DS.created_by = self.D11_21_ROC
        self.C1_in_DS.save()
        self.C1_in_symDS.content_checks.create(execlog=self.D11_21_ROC.log.first(), start_time=timezone.now(),
                                               end_time=timezone.now())
        self.D11_21_ROC.stop()
        if bp == "subrun_complete": return

    def run_pipelines_recovering_reused_step(self):
        """
        Setting up and running two pipelines, where the second one reuses and then recovers a step from the first.
        """
        p_one = self.make_first_pipeline("p_one", "two no-ops")
        self.create_linear_pipeline(p_one, [self.method_noop, self.method_noop], "p_one_in", "p_one_out")
        p_one.create_outputs()
        p_one.save()
        # Mark the output of step 1 as not retained.
        p_one.steps.get(step_num=1).add_deletion(self.method_noop.outputs.first())

        p_two = self.make_first_pipeline("p_two", "one no-op then one trivial")
        self.create_linear_pipeline(p_two, [self.method_noop, self.method_trivial], "p_two_in", "p_two_out")
        p_two.create_outputs()
        p_two.save()
        # We also delete the output of step 1 so that it reuses the existing ER we'll have
        # create for p_one.
        p_two.steps.get(step_num=1).add_deletion(self.method_noop.outputs.first())

        # Set up a words dataset.
        self.make_words_symDS()

        self.sandbox_one = sandbox.execute.Sandbox(self.user_bob, p_one, [self.symds_words])
        self.sandbox_one.execute_pipeline()

        self.sandbox_two = sandbox.execute.Sandbox(self.user_bob, p_two, [self.symds_words])
        self.sandbox_two.execute_pipeline()


class RunAtomicTests(ArchiveTestSetup, sandbox.tests_rm.UtilityMethods):
    """Tests of functionality shared by all RunAtomics."""

    def setUp(self):
        ArchiveTestSetup.setUp(self)
        sandbox.tests_rm.UtilityMethods.setUp(self)

    def test_clean_execlogs_invoked_logs_cleaned(self):
        """Test that _clean_execlogs properly calls clean on its invoked logs."""
        self.step_through_run_creation("outcables_done")

        # For every RunAtomic invoked during this run, break each of its invoked_logs and see if it appears.
        runatomics = (list(RunStep.objects.filter(
            pipelinestep__content_type=ContentType.objects.get_for_model(Method))) +
                      list(RunSIC.objects.all()) +
                      list(RunOutputCable.objects.all()))

        for runatomic in runatomics:
            # Skip RunAtomics that are not part of this Run.
            if runatomic.get_top_level_run() != self.pE_run:
                continue

            for invoked_log in runatomic.invoked_logs.all():
                original_el_start_time = invoked_log.start_time
                invoked_log.start_time = None
                invoked_log.save()
                self.assertRaisesRegexp(
                    ValidationError,
                    'Stopwatch "{}" does not have a start time but it has an end time'.format(invoked_log),
                    runatomic.clean)
                invoked_log.start_time = original_el_start_time
                invoked_log.save()

    def test_clean_execlogs_invoked_logs_ICLs_CCLs_cleaned(self):
        """Test that ICLs and CCLs are appropriately cleaned in _clean_execlogs."""
        self.step_through_run_creation("outcables_done")

        # For every RunAtomic invoked during this run, break each of its ICLs/CCLs and see if it appears.
        runatomics = (list(RunStep.objects.filter(
            pipelinestep__content_type=ContentType.objects.get_for_model(Method))) +
                      list(RunSIC.objects.all()) +
                      list(RunOutputCable.objects.all()))

        for runatomic in runatomics:
            # Skip RunAtomics that are not part of this Run.
            if runatomic.get_top_level_run() != self.pE_run:
                continue

            for invoked_log in runatomic.invoked_logs.all():
                for checklog in (list(invoked_log.integrity_checks.all()) + list(invoked_log.content_checks.all())):
                    original_checklog_start_time = checklog.start_time
                    checklog.start_time = None
                    checklog.save()
                    self.assertRaisesRegexp(
                        ValidationError,
                        'Stopwatch "{}" does not have a start time but it has an end time'.format(checklog),
                        runatomic.clean)
                    checklog.start_time = original_checklog_start_time
                    checklog.save()

    def test_clean_execlogs_log_not_among_invoked_logs(self):
        """A RunAtomic's log should be among its invoked_logs if any invoked_logs exist."""
        self.step_through_run_creation("outcables_done")

        # Imagine that step 3 invokes step 1 but not itself.  Note that this would break the Run overall
        # but we're only looking to check for errors local to a single RunAtomic.
        step_3_el = self.step_E3_RS.log.first()
        step_1_el = self.step_E1_RS.log.first()

        step_1_el.invoking_record = self.step_E3_RS
        step_1_el.save()

        step_3_el.invoking_record = self.pE_run.runoutputcables.get(pipelineoutputcable=self.E21_41)
        step_3_el.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecLog of {} "{}" is not included with its invoked ExecLogs'.format(
                                    self.step_E3_RS.__class__.__name__, self.step_E3_RS
                                )),
                                self.step_E3_RS.clean)

    def test_clean_execlogs_log_set_before_invoked_ExecLogs_complete(self):
        """A RunAtomic's log should not be set before all invoked_logs are complete."""
        self.step_through_run_creation("third_step_complete")

        # Imagine that step 3 invokes step 1 and itself.  Note that this would break the Run overall
        # but we're only looking to check for errors local to a single RunAtomic.
        step_1_el = self.step_E1_RS.log.first()
        step_1_el.invoking_record = self.step_E3_RS
        step_1_el.save()

        # Make step_1_el incomplete.
        step_1_el.end_time = None
        step_1_el.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecLog of {} "{}" is set before all invoked ExecLogs are complete'.format(
                                    self.step_E3_RS.__class__.__name__, self.step_E3_RS
                                )),
                                self.step_E3_RS.clean)

    def test_clean_execlogs_log_set_before_invoked_ExecLogs_finish_checks(self):
        """A RunAtomic's log should not be set before all invoked_logs finish their checks."""
        self.step_through_run_creation("third_step_complete")

        # Imagine that step 3 invokes step 1 and itself.  Note that this would break the Run overall
        # but we're only looking to check for errors local to a single RunAtomic.
        step_1_el = self.step_E1_RS.log.first()
        step_1_el.invoking_record = self.step_E3_RS
        step_1_el.save()

        # Remove step_1_el's ContentCheckLog.
        step_1_el.content_checks.first().delete()

        self.assertRaisesRegexp(
            ValidationError,
            re.escape('Invoked ExecLogs preceding log of {} "{}" did not successfully pass all of their checks'.format(
                self.step_E3_RS.__class__.__name__, self.step_E3_RS
            )),
            self.step_E3_RS.clean)

    def test_clean_execlogs_runatomic_invokes_previous_runatomic(self):
        """Testing clean on a RunAtomic which invoked a previous RunAtomic in the correct fashion."""
        self.step_through_run_creation("third_step_complete")

        # Imagine that step 3 invokes step 1 and itself.  Note that this would break the Run overall
        # but we're only looking to check for errors local to a single RunAtomic.
        step_1_el = self.step_E1_RS.log.first()
        step_1_el.invoking_record = self.step_E3_RS
        step_1_el.save()

        self.assertIsNone(self.step_E3_RS.clean())

    def test_clean_execlogs_runatomic_invoked_by_subsequent_runatomic(self):
        """
        Testing clean on a RunAtomic whose ExecLog was invoked by a subsequent RunAtomic.
        """
        # Run two pipelines, where the second reuses parts from the first.
        self.run_pipelines_recovering_reused_step()

        # The ExecLog of the first RunStep in sandbox_two's run should have been invoked by
        # the cable feeding step 2.
        run_two_step_one = self.sandbox_two.run.runsteps.get(pipelinestep__step_num=1)
        run_two_step_two = self.sandbox_two.run.runsteps.get(pipelinestep__step_num=2)
        run_two_step_two_input_cable = run_two_step_two.RSICs.first()

        self.assertEquals(run_two_step_one.log.first().invoking_record, run_two_step_two_input_cable)
        self.assertIsNone(run_two_step_one.clean())
        self.assertIsNone(run_two_step_two.clean())

    def test_clean_undecided_reused_invoked_logs_exist(self):
        """A RunAtomic that has not decided on reuse should not have any invoked logs."""
        self.step_through_run_creation("third_step_cables_done")
        step_one_el = self.step_E1_RS.log.first()
        step_one_el.invoking_record = self.step_E3_RS
        step_one_el.save()

        self.assertRaisesRegexp(
            ValidationError,
            re.escape('{} "{}" has not decided whether or not to reuse an ExecRecord'
                      "; no steps or cables should have been invoked".format("RunStep", self.step_E3_RS)),
            self.step_E3_RS.clean
        )

    def test_clean_reused_invoked_logs_exist(self):
        """A RunAtomic that reuses an ExecRecord should not have any invoked logs."""
        self.step_through_runstep_creation("first_runstep_complete")
        self.step_E1_RS.reused = True
        for curr_output in self.step_E1_RS.outputs.all():
            curr_output.created_by = None
            curr_output.save()

        self.assertRaisesRegexp(
            ValidationError,
            re.escape('{} "{}" reused an ExecRecord; no steps or cables should have been invoked'.format(
                "RunStep", self.step_E1_RS)),
            self.step_E1_RS.clean
        )

    # May 28, 2014: just introduced _clean_not_reused to RunAtomic.
    def test_clean_not_reused_runatomic_log_invoked_elsewhere(self):
        """Testing clean on a RunAtomic whose log was invoked elsewhere by a subsequent RunAtomic."""
        self.step_through_run_creation("third_step_complete")

        # Imagine that step 3 invokes step 1 and itself.  Note that this would break the Run overall
        # but we're only looking to check for errors local to a single RunAtomic.
        step_1_el = self.step_E1_RS.log.first()
        step_1_el.invoking_record = self.step_E3_RS
        step_1_el.save()

        self.assertRaisesRegexp(
            ValidationError,
            re.escape('{} "{}" is not reused and has not completed its own ExecLog but does have an ExecRecord'.format(
                "RunStep", self.step_E1_RS
            )),
            self.step_E1_RS.clean)


class RunStepTests(ArchiveTestSetup):

    def test_RunStep_many_execlogs(self):
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

        # Make this RunSIC incomplete by removing the ICL.
        icl_to_remove = self.raw_symDS.integrity_checks.filter(execlog=self.E03_11_RSIC.log.first()).first()
        icl_to_remove.execlog = None
        icl_to_remove.save()

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

    def test_RunStep_input_unquenched_with_log(self):
        """
        A RunStep with unquenched input cables, but which has an
        associated ExecLog, is not clean.
        """
        self.step_through_runstep_creation("first_runstep_complete")
        self.E03_11_RSIC.delete()
        self.step_E1_RS.execrecord = None
        self.step_E1_RS.reused = None
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunStep "{}" inputs not quenched; no log should have been generated'
                                          .format(self.step_E1_RS)),
                                self.step_E1_RS.clean)

    def test_RunStep_input_unquenched_invokes_other_RunAtomics(self):
        """
        A RunStep with unquenched input cables should not have any invoked_logs.
        """
        # This is a broken setup: step 1 can't have been invoked by step 3.
        # Still, we're just checking step 3 here.
        self.step_through_run_creation("third_step")

        step_one_el = self.step_E1_RS.log.first()
        step_one_el.invoking_record = self.step_E3_RS
        step_one_el.save()

        self.assertRaisesRegexp(
            ValidationError,
            re.escape('RunStep "{}" inputs not quenched; no other steps or cables should have been invoked'
                      .format(self.step_E3_RS)),
            self.step_E3_RS.clean)

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
        # May 14, 2014: fixed this test to reflect the way things work now.
        self.step_through_runstep_creation("first_rsic")

        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        self.make_complete_reused(self.step_E1_RS, [self.raw_symDS], [self.doublet_symDS], other_run)

        step1_in_ccl = self.doublet_symDS.content_checks.first()
        step1_in_ccl.execlog = self.step_E1_RS.log.first()
        step1_in_ccl.save()

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
        self.assertIsNone(self.step_E2_RS.clean())

    def test_RunStep_clean_bad_child_run(self):
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
        self.step_through_runstep_creation("first_runstep_complete")
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

    ####
    # keeps_output tests added March 26, 2014 -- RL
    def test_RunStep_keeps_output_true(self):
        """
        A RunStep with retained output.
        """
        self.step_through_runstep_creation("first_runstep")
        self.assertTrue(self.step_E1_RS.keeps_output(self.A1_out))

    def test_RunStep_keeps_output_false(self):
        """
        A RunStep with deleted output.
        """
        self.step_through_runstep_creation("first_runstep")
        self.step_E1.add_deletion(self.A1_out)
        self.assertFalse(self.step_E1_RS.keeps_output(self.A1_out))

    def test_RunStep_keeps_output_multiple_outputs(self):
        """
        A RunStep with several outputs, none deleted.
        """
        # This is copied from RunTests.step_through_run_creation.

        # Third RunStep associated.
        self.step_E3_RS = self.step_E3.pipelinestep_instances.create(run=self.pE_run)

        self.assertTrue(self.step_E3_RS.keeps_output(self.C1_out))
        self.assertTrue(self.step_E3_RS.keeps_output(self.C2_rawout))
        self.assertTrue(self.step_E3_RS.keeps_output(self.C3_rawout))

    def test_RunStep_keeps_output_multiple_outputs_some_deleted(self):
        """
        A RunStep with several outputs, some deleted.
        """
        # This is copied from RunTests.step_through_run_creation.

        # Third RunStep associated.
        self.step_E3_RS = self.step_E3.pipelinestep_instances.create(run=self.pE_run)
        self.step_E3.add_deletion(self.C1_out)
        self.step_E3.add_deletion(self.C3_rawout)

        self.assertFalse(self.step_E3_RS.keeps_output(self.C1_out))
        # The deletions shouldn't affect C2_rawout.
        self.assertTrue(self.step_E3_RS.keeps_output(self.C2_rawout))
        self.assertFalse(self.step_E3_RS.keeps_output(self.C3_rawout))


class RunTests(ArchiveTestSetup):

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
        self.step_through_runsic_creation("rsic_created")
        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_runstep = self.step_E3.pipelinestep_instances.create(run=other_run)
        self.make_complete_reused(self.E11_32_RSIC, [self.doublet_symDS], [self.C2_in_symDS], other_runstep)

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
        self.step_through_runsic_creation("rsic_created")

        # Create an incompatible RunSIC.
        runsic = self.E21_31.psic_instances.create(runstep=self.step_E3_RS)
        self.make_complete_non_reused(runsic, [self.C1_in_symDS], [self.C1_in_symDS])

        run = self.pE.pipeline_instances.create(user=self.myUser)
        runstep = self.step_E3.pipelinestep_instances.create(run=run)
        self.make_complete_reused(self.E11_32_RSIC, [self.doublet_symDS], [self.C2_in_symDS], runstep)

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
        self.step_through_runsic_creation("rsic_created")
        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_runstep = self.step_E3.pipelinestep_instances.create(run=other_run)

        self.make_complete_reused(self.E11_32_RSIC, [self.doublet_symDS], [self.C2_in_symDS], other_runstep)
        self.E21_31_RSIC = self.E21_31.psic_instances.create(runstep=self.step_E3_RS)
        self.make_complete_non_reused(self.E21_31_RSIC, [self.C1_in_symDS], [self.C1_in_symDS])
        self.make_complete_non_reused(self.step_E3_RS, [self.C1_in_symDS, self.C2_in_symDS],
                                                  [self.C1_out_symDS, self.C2_out_symDS, self.C3_out_symDS])

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
        keeps its output, should have data in its ExecRecordOut.
        """
        self.step_through_runsic_creation("rsic_created")
        run = self.pE.pipeline_instances.create(user=self.myUser)
        runstep = self.step_E3.pipelinestep_instances.create(run=run)
        self.E11_32.keep_output = True
        self.E11_32.save()
        self.make_complete_reused(self.E11_32_RSIC, [self.doublet_symDS], [self.C2_in_symDS], runstep)
        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()

        self.assertTrue(self.E11_32_RSIC.keeps_output())
        # Removed May 12, 2014: a reused RunSIC has no log.
        # self.assertListEqual(self.E11_32_RSIC.log.first().missing_outputs(), [])
        self.assertFalse(ero.has_data())
        self.assertRaisesRegexp(ValidationError,
                                re.escape('RunSIC "{}" keeps its output; ExecRecordOut "{}" should reference existent '
                                          'data'.format(self.E11_32_RSIC, ero)),
                                self.E11_32_RSIC.clean)

    def test_RunSIC_clean_reused_psic_keeps_output_with_data(self):
        """
        A RunSIC reusing an ExecRecord, whose PipelineStepInputCable
        keeps its output, should have data in its ExecRecordOut.
        """
        self.step_through_runsic_creation("rsic_created")

        # Make another RSIC which is reused by E11_32_RSIC.
        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_run.save()
        other_RS = self.step_E3.pipelinestep_instances.create(run=other_run)

        self.make_complete_reused(self.E11_32_RSIC, [self.doublet_symDS], [self.C2_in_symDS], other_RS)

        ero = self.E11_32_RSIC.execrecord.execrecordouts.first()
        # self.E11_32_output_DS.symbolicdataset = ero.symbolicdataset
        # self.E11_32_output_DS.save()
        self.assertIsNone(self.E11_32_RSIC.clean())

    def test_RunSIC_clean_reused_complete_RSIC(self):
        """
        A RunSIC reusing an ExecRecord, whose PipelineStepInputCable
        keeps its output, having data in its ExecRecordOut, is complete
        and clean.
        """
        self.step_through_runsic_creation("rsic_created")
        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_runstep = self.step_E3.pipelinestep_instances.create(run=other_run)

        self.make_complete_reused(self.E11_32_RSIC, [self.doublet_symDS], [self.C2_in_symDS], other_runstep)
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
        self.step_through_runsic_creation("rsic_created")

        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_RS = self.step_E3.pipelinestep_instances.create(run=other_run)
        self.make_complete_reused(self.E11_32_RSIC, [self.doublet_symDS], [self.C2_in_symDS],
                                  other_RS)

        self.E11_32.keep_output = True
        self.E11_32_RSIC.execrecord = None

        self.assertFalse(self.E11_32_RSIC.is_complete())
        self.assertRaisesRegexp(ValidationError, 
                                re.escape('{} "{}" is not complete'.format("RunSIC", self.E11_32_RSIC)),
                                self.E11_32_RSIC.complete_clean)

    def test_RunSIC_clean_not_reused_no_execrecord(self):
        """
        A RunSIC which has decided not to reuse an ExecRecord, but
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

        # Swap the output of E11_32_RSIC (i.e. C2_in_symDS) for E11_32_output_symDS,
        # which has data.
        self.E11_32_RSIC.reused = False
        self.E11_32.keep_output = True
        self.E11_32_output_DS.created_by = self.E11_32_RSIC
        self.E11_32_output_DS.save()

        ero_to_change = self.E11_32_RSIC.execrecord.execrecordouts.first()
        ero_to_change.symbolicdataset = self.E11_32_output_symDS
        ero_to_change.save()

        # Point the CCL of E11_32_output_symDS to the ExecLog of E11_32_RSIC.
        ccl = self.E11_32_output_symDS.content_checks.first()
        ccl.execlog = self.E11_32_RSIC.log.first()
        ccl.save()

        self.assertTrue(self.E11_32_RSIC.is_complete())
        self.assertIsNone(self.E11_32_RSIC.complete_clean())

    def test_RunSIC_incomplete_not_reused(self):
        """
        A RunSIC which is not reusing an ExecRecord, but which does not
        have an ExecRecord, is not complete.  
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32_RSIC.reused = False

        # May 14, 2014: we now make it incomplete by removing the CCL, not by
        # removing the ExecRecord.
        C2_ccl = self.C2_in_symDS.content_checks.first()
        C2_ccl.execlog = None
        C2_ccl.save()

        self.assertFalse(self.E11_32_RSIC.is_complete())
        self.assertRaisesRegexp(ValidationError,
                                re.escape('{} "{}" is not complete'.format("RunSIC", self.E11_32_RSIC)),
                                self.E11_32_RSIC.complete_clean)

    ####
    # keeps_output tests added March 26, 2014 -- RL.
    def test_RunSIC_keeps_output_trivial(self):
        """
        A trivial RunSIC should have keeps_output() return False regardless of the keep_output setting.
        """
        self.step_through_runsic_creation("runstep_completed")
        self.E21_31.keep_output = True
        self.assertFalse(self.E11_32_RSIC.keeps_output())
        self.E21_31.keep_output = False
        self.assertFalse(self.E11_32_RSIC.keeps_output())

    def test_RunSIC_keeps_output_true(self):
        """
        A RunSIC that keeps its output should have keeps_output() return True.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32.keep_output = True
        self.assertTrue(self.E11_32_RSIC.keeps_output())

    def test_RunSIC_keeps_output_false(self):
        """
        A RunSIC that discards its output should have keeps_output() return False.
        """
        self.step_through_runsic_creation("rsic_completed")
        self.E11_32.keep_output = False
        self.assertFalse(self.E11_32_RSIC.keeps_output())


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

        self.assertFalse(self.E21_41_ROC.reused)
        self.assertFalse(self.E21_41_ROC.component.is_trivial())
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

        self.assertFalse(self.D11_21_ROC.keeps_output())
        self.assertTrue(self.D11_21_ROC.output.exists())
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

        # May 12, 2014: this caused the test to fail.  We just want the ERO to not have
        # data.
        # self.triplet_3_rows_DS.created_by = self.D11_21_ROC
        # self.triplet_3_rows_DS.save()

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
        self.step_through_roc_creation("subrun")
        self.make_complete_non_reused(self.D11_21_ROC, [self.C1_in_symDS], [self.C1_in_symDS])

        self.assertTrue(self.D11_21_ROC.keeps_output())
        self.assertListEqual(self.D11_21_ROC.log.first().missing_outputs(), [])
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
        self.make_complete_non_reused(self.D11_21_ROC, [self.C1_in_symDS], [self.C1_in_symDS])
        self.triplet_3_rows_DS.created_by = self.D11_21_ROC
        self.triplet_3_rows_DS.save()

        self.assertFalse(self.D11_21_ROC.component.is_trivial())
        self.assertFalse(self.D11_21_ROC.reused)
        self.assertNotEqual(self.triplet_3_rows_DS, self.D11_21_ROC.execrecord.execrecordouts.first().symbolicdataset)

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
        self.step_through_roc_creation("subrun_complete")
        self.C1_in_DS.created_by = self.D11_21_ROC
        self.C1_in_DS.save()

        self.assertIsNone(self.D11_21_ROC.clean())
        self.assertIsNotNone(self.D11_21_ROC.execrecord)
        self.assertFalse(self.D11_21_ROC.reused)

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

    ####
    # keeps_output tests added March 26, 2014 -- RL.
    def test_ROC_keeps_output_top_level_trivial(self):
        """
        A top-level trivial RunSIC should have keeps_output() return False.
        """
        self.step_through_roc_creation("trivial_roc_completed")
        self.assertFalse(self.E31_42_ROC.keeps_output())

    def test_ROC_keeps_output_top_level_custom(self):
        """
        A top-level custom RunSIC should have keeps_output() return True.
        """
        self.step_through_roc_creation("custom_roc_completed")
        self.assertTrue(self.E21_41_ROC.keeps_output())

    def test_ROC_keeps_output_top_level_trivial_incomplete(self):
        """
        A top-level trivial incomplete RunSIC should have keeps_output() return False.
        """
        self.step_through_roc_creation("roc_created")
        self.assertFalse(self.E31_42_ROC.keeps_output())

    def test_ROC_keeps_output_top_level_custom_incomplete(self):
        """
        A top-level custom incomplete RunSIC should have keeps_output() return True.
        """
        self.step_through_roc_creation("roc_created")
        self.assertTrue(self.E21_41_ROC.keeps_output())

    def test_ROC_keeps_output_subrun_trivial_true(self):
        """
        A trivial POC of a sub-run that doesn't discard its output should have keeps_output() return False.
        """
        self.step_through_roc_creation("subrun_complete")
        self.D11_21.custom_outwires.all().delete()
        self.assertFalse(self.D11_21_ROC.keeps_output())

    def test_ROC_keeps_output_subrun_custom_true(self):
        """
        A custom POC of a sub-run that doesn't discard its output should have keeps_output() return True.
        """
        self.step_through_roc_creation("subrun_complete")
        self.assertTrue(self.D11_21_ROC.keeps_output())

    def test_ROC_keeps_output_subrun_custom_false(self):
        """
        A custom POC of a sub-run that does discard its output should have keeps_output() return False.
        """
        self.step_through_roc_creation("subrun_complete")
        self.step_E2.add_deletion(self.D1_out)
        self.assertFalse(self.D11_21_ROC.keeps_output())

    def test_ROC_keeps_output_incomplete_subrun_trivial_true(self):
        """
        A trivial POC of an incomplete sub-run that doesn't discard its output should have keeps_output() return False.
        """
        self.step_through_roc_creation("subrun")
        self.D11_21.custom_outwires.all().delete()
        self.assertFalse(self.D11_21_ROC.keeps_output())

    def test_ROC_keeps_output_incomplete_subrun_custom_true(self):
        """
        A custom cable of an incomplete sub-run that doesn't discard its output should have keeps_output() return True.
        """
        self.step_through_roc_creation("subrun")
        self.assertTrue(self.D11_21_ROC.keeps_output())

    def test_ROC_keeps_output_incomplete_subrun_custom_false(self):
        """
        A custom cable of an incomplete sub-run that does discard its output should have keeps_output() return False.
        """
        self.step_through_roc_creation("subrun")
        self.step_E2.add_deletion(self.D1_out)
        self.assertFalse(self.D11_21_ROC.keeps_output())


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


# Added March 26, 2014, as it's not working if I put this in
# Stopwatch.tests.
class StopwatchTests(ArchiveTestSetup):

    # Note that ArchiveTestSetup creates self.pE_run, which is a
    # Stopwatch, in its setUp.  We'll use this as our Stopwatch.

    def test_clean_neither_set(self):
        """
        Neither start nor end time is set.  Stopwatch should be clean.
        """
        self.assertIsNone(self.pE_run.clean())

    def test_clean_start_set_end_unset(self):
        """
        start_time set, end_time not set.  This is fine.
        """
        self.pE_run.start()
        self.assertIsNone(self.pE_run.clean())

    def test_clean_start_set_end_set(self):
        """
        start_time set, end_time set afterwards.  This is fine.
        """
        self.pE_run.start()
        self.pE_run.stop()
        self.assertIsNone(self.pE_run.clean())

    def test_clean_start_unset_end_set(self):
        """
        end_time set and start_time unset.  This is not coherent.
        """
        self.pE_run.end_time = timezone.now()
        self.assertRaisesRegexp(
            ValidationError,
            re.escape('Stopwatch "{}" does not have a start time but it has an end time'.format(self.pE_run)),
            self.pE_run.clean
        )

    def test_clean_end_before_start(self):
        """
        end_time is before and start_time.  This is not coherent.
        """
        self.pE_run.end_time = timezone.now()
        self.pE_run.start_time = timezone.now()
        self.assertRaisesRegexp(
            ValidationError,
            re.escape('Stopwatch "{}" start time is later than its end time'.format(self.pE_run)),
            self.pE_run.clean
        )

    def test_has_started_true(self):
        """
        start_time is set.
        """
        self.pE_run.start_time = timezone.now()
        self.assertTrue(self.pE_run.has_started())

    def test_has_started_false(self):
        """
        start_time is unset.
        """
        self.assertFalse(self.pE_run.has_started())

    def test_has_ended_true(self):
        """
        end_time is set.
        """
        self.pE_run.start_time = timezone.now()
        self.pE_run.end_time = timezone.now()
        self.assertTrue(self.pE_run.has_ended())

    def test_has_ended_false(self):
        """
        end_time is unset.
        """
        # First, the neither-set case.
        self.assertFalse(self.pE_run.has_ended())

        # Now, the started-but-not-stopped case
        self.pE_run.start_time = timezone.now()
        self.assertFalse(self.pE_run.has_ended())

    def test_start(self):
        """
        start() sets start_time.
        """
        self.assertFalse(self.pE_run.has_started())
        self.pE_run.start()
        self.assertTrue(self.pE_run.has_started())

    def test_stop(self):
        """
        stop() sets end_time.
        """
        self.assertFalse(self.pE_run.has_ended())
        self.pE_run.start()
        self.assertFalse(self.pE_run.has_ended())
        self.pE_run.stop()
        self.assertTrue(self.pE_run.has_ended())


class ExecLogTests(ArchiveTestSetup):
    def test_delete_exec_log(self):
        """Can delete an ExecLog."""
        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=self.pE_run)
        execlog = step_E1_RS.log.create(invoking_record=step_E1_RS)
        self.assertIsNone(execlog.delete())

    def test_clean_record_not_RunAtomic(self):
        """record of ExecLog should be a RunAtomic."""
        self.step_through_run_creation("outcables_done")
        # Retrieve an ExecLog and point it at a subrun.

        for el in ExecLog.objects.all():
            original_record = el.record
            el.record = self.step_E2_RS
            self.assertRaisesRegexp(ValidationError,
                                    'ExecLog "{}" does not correspond to a Method or cable'.format(el),
                                    el.clean)
            el.record = original_record
            el.save()

    def test_clean_record_and_invoked_records_different_Run(self):
        """ExecLog's record and invoked_records should belong to the same top-level Run."""
        self.step_through_runstep_creation("first_rsic")

        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        self.make_complete_reused(self.step_E1_RS, [self.raw_symDS], [self.doublet_symDS], other_run)

        # Now step_E1_RS is marked as reused, and there is a dummy record of a RunStep belonging to
        # other_run.  Let's retrieve that ExecLog and mis-wire it so that its record belongs to self.pE_run
        # while its invoking record remains other_run and that should trigger an error in ExecLog.clean().
        el_to_mess_up = other_run.runsteps.get(pipelinestep__step_num=1).log.first()

        el_to_mess_up.record = self.step_E1_RS

        self.assertRaisesRegexp(
            ValidationError,
            'ExecLog "{}" belongs to a different Run than its invoking RunStep/RSIC/ROC'.format(el_to_mess_up),
            el_to_mess_up.clean)

    def test_clean_invoking_record_precedes_record_different_coords(self):
        """An ExecLog should not have been invoked before its own record."""
        self.step_through_run_creation("outcables_done")
        # Get all ExecLogs and change their invoking record to things preceding it in the run.

        for el in ExecLog.objects.all():
            # Skip if this ExecLog doesn't have top-level run pE_run.
            if el.record.get_top_level_run() is not self.pE_run:
                continue

            # Skip if this ExecLog has coordinates (1,).
            if el.record.get_coordinates() == (1,):
                continue

            # Change invoking_record to step 1.
            original_invoking_record = el.invoking_record
            el.invoking_record = self.step_E1_RS
            self.assertRaisesRegexp(
                ValidationError,
                'ExecLog "{}" is invoked earlier than the RunStep/RSIC/ROC it belongs to'.format(el),
                el.clean)
            el.invoking_record = original_invoking_record
            el.save()

    def test_clean_invoking_record_precedes_record_RSIC_of_RS(self):
        """The ExecLog of a RunStep should not be invoked by its own RSICs."""
        self.step_through_run_creation("outcables_done")

        # Retrieve all ExecLogs of steps.
        step_ELs = ExecLog.objects.filter(content_type=ContentType.objects.get_for_model(RunStep))

        for el in step_ELs:
            original_invoking_record = el.invoking_record
            for rsic in el.record.RSICs.all():
                el.invoking_record = rsic
                self.assertRaisesRegexp(
                    ValidationError,
                    'ExecLog "{}" is invoked earlier than the RunStep it belongs to'.format(el),
                    el.clean)
            el.invoking_record = original_invoking_record
            el.save()

    def test_clean_invoking_record_precedes_record_anything_precedes_ROC(self):
        """The ExecLog of a RunOutputCable should not be invoked by anything before it in its run."""
        self.step_through_run_creation("outcables_done")

        # Retrieve all ExecLogs of ROCs.
        ROC_ELs = ExecLog.objects.filter(content_type=ContentType.objects.get_for_model(RunOutputCable))

        for el in ROC_ELs:
            # Get its containing run.
            containing_run = el.record.run

            original_invoking_record = el.invoking_record

            # Change its invoking record to all steps and RSICs prior to it.
            for step in containing_run.runsteps.all():

                # Only use this step if it isn't a sub-pipeline.
                if not step.has_subrun():
                    el.invoking_record = step
                    self.assertRaisesRegexp(
                        ValidationError,
                        'ExecLog "{}" is invoked earlier than the ROC it belongs to'.format(el),
                        el.clean)

                for rsic in step.RSICs.all():
                    el.invoking_record = rsic
                    self.assertRaisesRegexp(
                        ValidationError,
                        'ExecLog "{}" is invoked earlier than the ROC it belongs to'.format(el),
                        el.clean)
            el.invoking_record = original_invoking_record
            el.save()

    def test_clean_good_ExecLog(self):
        """
        Test that clean doesn't barf on good ExecLogs.
        """
        self.step_through_run_creation("outcables_done")
        for el in ExecLog.objects.all():
            self.assertIsNone(el.clean())

    def test_clean_good_ExecLog_invoked_later(self):
        """
        ExecLogs can be invoked later in the same pipeline than their own record.

        Note that this case causes the containing Run and such to be inconsistent,
        but we're only checking ExecLog right now.
        """
        self.step_through_run_creation("outcables_done")
        el_to_mess_with = self.step_E1_RS.log.first()

        el_to_mess_with.invoking_record = self.step_E3_RS

        self.assertIsNone(el_to_mess_with.clean())


class GetCoordinatesTests(ArchiveTestSetup, sandbox.tests_rm.UtilityMethods):
    """Tests of the get_coordinates functions of all Run and RunAtomic classes."""

    def setUp(self):
        """Set up our testing environment."""
        ArchiveTestSetup.setUp(self)
        # These are Rosemary's methods for defining pipelines.
        sandbox.tests_rm.UtilityMethods.setUp(self)

    def _setup_deep_nested_run(self):
        """Set up a pipeline with sub-sub-pipelines to test recursion."""
        # Everything in this pipeline will be a no-op, so all can be linked together
        # without remorse.
        p_basic = self.make_first_pipeline("p_basic", "innermost pipeline")
        self.create_linear_pipeline(p_basic, [self.method_noop, self.method_noop], "basic_in", "basic_out")
        p_basic.create_outputs()
        p_basic.save()

        p_sub = self.make_first_pipeline("p_sub", "second-level pipeline")
        self.create_linear_pipeline(p_sub, [p_basic, p_basic], "sub_in", "sub_out")
        p_sub.create_outputs()
        p_sub.save()

        p_top = self.make_first_pipeline("p_top", "top-level pipeline")
        self.create_linear_pipeline(p_top, [p_sub, p_sub, p_sub], "top_in", "top_out")
        p_top.create_outputs()
        p_top.save()

        # Set up a dataset with words in it called self.symds_words.
        self.make_words_symDS()

        run_sandbox = sandbox.execute.Sandbox(self.user_bob, p_top, [self.symds_words])
        run_sandbox.execute_pipeline()

    def test_get_coordinates_top_level_run(self):
        """Coordinates of a top-level run should be an empty tuple."""
        self.step_through_run_creation("outcables_done")
        top_level_runs = Run.objects.filter(parent_runstep=None)
        for run in top_level_runs:
            self.assertEquals(run.get_coordinates(), ())

    def test_get_coordinates_subrun(self):
        """Coordinates of a sub-run should match that of their parent runstep."""
        self.step_through_run_creation("outcables_done")
        # pD_run is the second step of its containing top-level run.
        self.assertEquals(self.pD_run.get_coordinates(), (2,))
        self.assertEquals(self.pD_run.get_coordinates(), self.step_E2_RS.get_coordinates())

    def test_get_coordinates_nested_runs(self):
        """Test get_coordinates for a deeper-nested sub-run."""
        self._setup_deep_nested_run()

        top_level_run = Run.objects.get(pipeline__family__name="p_top")

        self.assertEquals(top_level_run.get_coordinates(), ())

        # Check all second-level and third-level runs.
        for step in top_level_run.runsteps.all():
            first_lvl_step_num = step.pipelinestep.step_num
            subrun = step.child_run

            self.assertEquals(subrun.get_coordinates(), (first_lvl_step_num,))

            for substep in subrun.runsteps.all():
                second_lvl_step_num = substep.pipelinestep.step_num
                basic_run = substep.child_run
                self.assertEqual(basic_run.get_coordinates(), (first_lvl_step_num, second_lvl_step_num))

    def test_get_coordinates_top_level_step(self):
        """Coordinates of a top-level step should be a one-entry tuple with its step number as the entry."""
        self.step_through_run_creation("outcables_done")

        top_level_steps = []
        for runstep in RunStep.objects.all():
            if runstep.run.parent_runstep == None:
                top_level_steps.append(runstep)

        for top_level_step in top_level_steps:
            self.assertEquals(top_level_step.get_coordinates(),
                              (top_level_step.pipelinestep.step_num,))

    def test_get_coordinates_subrun_step(self):
        """Coordinates of a subrun step should be a tuple lexicographically giving its location."""
        self.step_through_run_creation("outcables_done")

        # step_D1_RS (as defined by Eric) is at position (2,1).
        self.assertEquals(self.step_D1_RS.get_coordinates(), (2,1))

    def test_get_coordinates_nested_runstep(self):
        """Test get_coordinates for deeper-nested RunSteps."""
        self._setup_deep_nested_run()

        top_level_run = Run.objects.get(pipeline__family__name="p_top")

        # Check all RunSteps of the top-level run and also their child and grandchild runs.
        for step in top_level_run.runsteps.all():
            first_lvl_step_num = step.pipelinestep.step_num
            self.assertEquals(step.get_coordinates(), (first_lvl_step_num,))

            subrun = step.child_run
            for substep in subrun.runsteps.all():
                second_lvl_step_num = substep.pipelinestep.step_num
                self.assertEqual(substep.get_coordinates(), (first_lvl_step_num, second_lvl_step_num))

                basic_run = substep.child_run
                for basic_step in basic_run.runsteps.all():
                    third_lvl_step_num = basic_step.pipelinestep.step_num
                    self.assertEqual(basic_step.get_coordinates(),
                                     (first_lvl_step_num, second_lvl_step_num, third_lvl_step_num))

    def test_get_coordinates_top_level_rsic(self):
        """Coordinates of top-level RSICs should be one-entry tuples matching their parent RSs."""
        self.step_through_run_creation("outcables_done")

        for runstep in RunStep.objects.all():
            if runstep.run.parent_runstep == None:
                # Examine the input cables.
                for rsic in runstep.RSICs.all():
                    self.assertEquals(rsic.get_coordinates(), (runstep.pipelinestep.step_num,))
                    self.assertEquals(rsic.get_coordinates(), runstep.get_coordinates())

    def test_get_coordinates_subrun_rsic(self):
        """Coordinates of sub-run RSICs should match that of their parent runstep."""
        self.step_through_run_creation("outcables_done")

        # step_D1_RS (as defined by Eric) is at position (2,1).
        for rsic in self.step_D1_RS.RSICs.all():
            self.assertEquals(rsic.get_coordinates(), (2,1))
            self.assertEquals(rsic.get_coordinates(), self.step_D1_RS.get_coordinates())

    def test_get_coordinates_nested_rsic(self):
        """Test get_coordinates for deeper-nested RSICs."""
        self._setup_deep_nested_run()

        top_level_run = Run.objects.get(pipeline__family__name="p_top")

        # Check all RunSteps of the top-level run and also their child and grandchild runs.
        for step in top_level_run.runsteps.all():
            first_lvl_step_num = step.pipelinestep.step_num

            for rsic in step.RSICs.all():
                self.assertEquals(rsic.get_coordinates(), (first_lvl_step_num,))

            subrun = step.child_run
            for substep in subrun.runsteps.all():
                second_lvl_step_num = substep.pipelinestep.step_num

                for subrsic in substep.RSICs.all():
                    self.assertEqual(subrsic.get_coordinates(), (first_lvl_step_num, second_lvl_step_num))

                basic_run = substep.child_run
                for basic_step in basic_run.runsteps.all():
                    third_lvl_step_num = basic_step.pipelinestep.step_num

                    for basic_rsic in basic_step.RSICs.all():
                        self.assertEqual(basic_rsic.get_coordinates(),
                                         (first_lvl_step_num, second_lvl_step_num, third_lvl_step_num))

    def test_get_coordinates_top_level_roc(self):
        """Coordinates of top-level ROCs should be empty tuples."""
        self.step_through_run_creation("outcables_done")

        for roc in RunOutputCable.objects.all():
            if roc.run.parent_runstep == None:
                # Examine the cable.
                self.assertEquals(roc.get_coordinates(), ())

    def test_get_coordinates_subrun_roc(self):
        """Coordinates of a subrun ROC should be the same as its parent run."""
        self.step_through_run_creation("outcables_done")

        # The second step is a sub-run.
        for roc in self.pD_run.runoutputcables.all():
            self.assertEquals(roc.get_coordinates(), (2,))

    def test_get_coordinates_nested_roc(self):
        """Test get_coordinates for deeper-nested sub-ROCs."""
        self._setup_deep_nested_run()

        top_level_run = Run.objects.get(pipeline__family__name="p_top")

        for roc in top_level_run.runoutputcables.all():
            self.assertEquals(roc.get_coordinates(), ())

        # Check all second-level and third-level runs.
        for step in top_level_run.runsteps.all():
            first_lvl_step_num = step.pipelinestep.step_num
            subrun = step.child_run

            for subroc in subrun.runoutputcables.all():
                self.assertEquals(subroc.get_coordinates(), (first_lvl_step_num,))

            for substep in subrun.runsteps.all():
                second_lvl_step_num = substep.pipelinestep.step_num
                basic_run = substep.child_run

                for basic_roc in basic_run.runoutputcables.all():
                    self.assertEqual(basic_roc.get_coordinates(), (first_lvl_step_num, second_lvl_step_num))


class IsCompleteTests(ArchiveTestSetup):
    """Tests the is_complete functions of Run, RunAtomic, ExecLog."""

    def test_execlog_good_cases(self):
        """
        Testing that all ExecLogs are clean after a (simulated) good run.
        """
        self.step_through_run_creation("outcables_done")

        for el in ExecLog.objects.all():
            if el.record.get_top_level_run == self.pE_run:
                self.assertTrue(el.is_complete())

    def test_execlog_has_not_ended_yet(self):
        """
        Test on ExecLogs where has_ended() is False.
        """
        self.step_through_run_creation("outcables_done")

        # Artificially change the logs' end_time to None.
        for el in ExecLog.objects.all():
            if el.record.get_top_level_run == self.pE_run:
                orig_end_time = el.end_time
                el.end_time = None
                self.assertFalse(el.is_complete())
                el.end_time = orig_end_time

    def test_execlog_of_runstep_has_no_methodoutput(self):
        """Test on ExecLogs for RunSteps that have no MethodOutput."""
        self.step_through_run_creation("outcables_done")

        for el in ExecLog.objects.all():
            # Skip any RunCables.
            if type(el.record) != RunStep:
                continue

            if el.record.get_top_level_run == self.pE_run:
                orig_methodoutput = el.methodoutput
                el.methodoutput = None
                self.assertFalse(el.is_complete())
                el.methodoutput = orig_methodoutput

    def test_runatomic_successful_run(self):
        """
        Quick test of good cases coming out of a (simulated) good run.
        """
        self.step_through_run_creation("outcables_done")

        runatomics = (list(RunStep.objects.filter(
            pipelinestep__content_type=ContentType.objects.get_for_model(Method))) +
                      list(RunSIC.objects.all()) +
                      list(RunOutputCable.objects.all()))

        for runatomic in runatomics:
            # Skip RunAtomics that are not part of this Run.
            if runatomic.get_top_level_run() != self.pE_run:
                continue

            self.assertTrue(runatomic.is_complete())

    def test_runatomic_successful_no_execrecord(self):
        """Testing of a RunAtomic (RunSIC) that is successful but has no ExecRecord yet."""
        self.step_through_run_creation("first_cable_created")

        incomplete_cable = self.step_E1_RS.RSICs.get(PSIC=self.step_E1.cables_in.first())

        self.make_execlog_and_mark_non_reused_runatomic(incomplete_cable)
        self.assertFalse(incomplete_cable.is_complete())

    def test_runatomic_successful_has_execrecord_reused(self):
        """Testing of a RunAtomic which has an ExecRecord and is reused (so is done)"""
        self.step_through_run_creation("first_cable_created")

        incomplete_cable = self.step_E1_RS.RSICs.get(PSIC=self.step_E1.cables_in.first())

        # Create another run.
        other_run = self.pE.pipeline_instances.create(user=self.myUser)
        other_step1 = self.step_E1.pipelinestep_instances.create(run=other_run)
        self.make_complete_reused(incomplete_cable, [self.raw_symDS], [self.raw_symDS], other_step1)
        other_cable = other_step1.RSICs.first()
        icl = self.raw_symDS.integrity_checks.create(execlog=other_cable.log.first())
        icl.start()
        icl.stop()
        icl.save()

        self.assertTrue(incomplete_cable.is_complete())

    def test_runatomic_successful_checks_not_passed(self):
        """Testing of a RunAtomic (RunSIC) that is successful but has no ExecRecord yet."""
        self.step_through_run_creation("first_cable_created")

        incomplete_cable = self.step_E1_RS.RSICs.get(PSIC=self.step_E1.cables_in.first())
        self.make_complete_non_reused(incomplete_cable, [self.raw_symDS], [self.raw_symDS])

        self.assertFalse(incomplete_cable.is_complete())

    def test_runatomic_successful_checks_passed(self):
        """Testing of a RunAtomic (RunSIC) that is successful and all checks pass."""
        self.step_through_run_creation("first_cable_created")

        incomplete_cable = self.step_E1_RS.RSICs.get(PSIC=self.step_E1.cables_in.first())
        self.make_complete_non_reused(incomplete_cable, [self.raw_symDS], [self.raw_symDS])

        icl = self.raw_symDS.integrity_checks.create(execlog=incomplete_cable.log.first())
        icl.start()
        icl.stop()
        icl.save()

        self.assertTrue(incomplete_cable.is_complete())

    # FIXME continue from here.  Need some tests of RunAtomics that fail at
    # - recovery (one of the invoked_logs failed or has a failed data check)

    def test_runatomic_unsuccessful_failed_execlog(self):
        """Testing of a RunAtomic (RunStep) which fails at the ExecLog stage."""
        self.step_through_run_creation("first_step_complete")

        step_1_log = self.step_E1_RS.log.first()

        ccl_to_wipe = step_1_log.content_checks.first()
        ccl_to_wipe.execlog = None
        ccl_to_wipe.save()

        mo_to_change = step_1_log.methodoutput
        mo_to_change.return_code = 1
        mo_to_change.save()

        self.assertTrue(self.step_E1_RS.is_complete())

    def test_runatomic_unsuccessful_failed_content_check(self):
        """Testing of a RunAtomic (RunStep) which failed at the content check stage."""
        self.step_through_run_creation("first_step_complete")

        step_1_log = self.step_E1_RS.log.first()

        ccl_to_fail = step_1_log.content_checks.first()
        ccl_to_fail.add_bad_header()

        self.assertTrue(self.step_E1_RS.is_complete())

    def test_runatomic_unsuccessful_failed_integrity_check(self):
        """Testing of a RunAtomic (RunSIC) which failed at the integrity check stage."""
        self.step_through_run_creation("first_cable_created")
        step_E1_RSIC = self.step_E1_RS.RSICs.first()
        self.make_complete_non_reused(step_E1_RSIC, [self.raw_symDS], [self.raw_symDS])

        # Make a bad ICL.
        conflicting_datafile = tempfile.NamedTemporaryFile(delete=False)
        conflicting_datafile.write("THIS IS A FAILURE")
        conflicting_datafile.close()

        # The output of this first cable is self.raw_symDS.  This creates a bad ICL.
        self.raw_symDS.check_integrity(conflicting_datafile.name, self.pE_run.user, step_E1_RSIC.log.first())
        self.assertTrue(step_E1_RSIC.is_complete())

    def test_runatomic_unsuccessful_failed_invoked_log(self):
        """Testing of a RunAtomic which has a failed invoked_log and never gets to its own execution."""
        # Run two pipelines, the second of which reuses parts of the first, but the method has been
        # screwed with in between.
        p_one = self.make_first_pipeline("p_one", "two no-ops")
        self.create_linear_pipeline(p_one, [self.method_noop, self.method_noop], "p_one_in", "p_one_out")
        p_one.create_outputs()
        p_one.save()
        # Mark the output of step 1 as not retained.
        p_one.steps.get(step_num=1).add_deletion(self.method_noop.outputs.first())

        # Set up a words dataset.
        self.make_words_symDS()

        self.sandbox_one = sandbox.execute.Sandbox(self.user_bob, p_one, [self.symds_words])
        self.sandbox_one.execute_pipeline()

        # Oops!  Between runs, self.method_noop gets screwed with.
        with tempfile.TemporaryFile() as f:
            f.write("#!/bin/bash\n exit 1")
            self.coderev_noop.content_file=File(f)
            self.coderev_noop.save()

        p_two = self.make_first_pipeline("p_two", "one no-op then one trivial")
        self.create_linear_pipeline(p_two, [self.method_noop, self.method_trivial], "p_two_in", "p_two_out")
        p_two.create_outputs()
        p_two.save()
        # We also delete the output of step 1 so that it reuses the existing ER we'll have
        # create for p_one.
        p_two.steps.get(step_num=1).add_deletion(self.method_noop.outputs.first())

        self.sandbox_two = sandbox.execute.Sandbox(self.user_bob, p_two, [self.symds_words])
        self.sandbox_two.execute_pipeline()

        # In the second run: the cable feeding the second step should have tried to invoke the log of step 1 and
        # failed.
        run2_step1 = self.sandbox_one.run.runsteps.get(pipelinestep__step_num=1)
        run2_step2 = self.sandbox_two.run.runsteps.get(pipelinestep__step_num=2)
        run2_step2_cable = run2_step2.RSICs.first()

        self.assertIsNone(run2_step2_cable.log.first())
        self.assertEquals(run2_step2_cable.invoked_logs.count(), 1)
        self.assertEquals(run2_step2_cable.invoked_logs.first(), run2_step1.log.first())

        self.assertFalse(run2_step1.log.is_successful())
        self.assertTrue(run2_step2_cable.is_complete())