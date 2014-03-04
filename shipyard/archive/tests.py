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

# Note that these tests use the exact same setup as librarian.

class RunStepTests(librarian.tests.LibrarianTestSetup):

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


    def test_RunStep_clean(self):
        """Check coherence tests for RunStep at all stages of its creation."""
        # Create some infrastructure for our RunSteps.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)

        # Bad case: RS has a PS that does not belong to the pipeline.
        step_D1_RS = self.step_D1.pipelinestep_instances.create(run=pE_run)
        self.assertRaisesRegexp(
            ValidationError,
            "PipelineStep .* of RunStep .* does not belong to Pipeline .*",
            step_D1_RS.clean)

        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=pE_run)

        # Bad case: step E1 should not have a child_run defined.
        pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        pD_run.parent_runstep = step_E1_RS
        pD_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "PipelineStep of RunStep .* is not a Pipeline but a child run exists",
            step_E1_RS.clean)

        # Moving on....
        pD_run.parent_runstep = None
        pD_run.save()

        # Good case: no RSICs.
        self.assertEquals(step_E1_RS.clean(), None)

        # Bad case (propagation): define an RSIC that is not complete.
        E03_11_RSIC = self.E03_11.psic_instances.create(runstep=step_E1_RS)
        E03_11_EL = E03_11_RSIC.log.create()
        E03_11_EL.start_time = timezone.now()
        time.sleep(1)
        E03_11_EL.end_time = timezone.now()
        E03_11_EL.save()
        E03_11_ER = ExecRecord(generator=E03_11_EL)
        E03_11_ER.save()
        E03_11_ER.execrecordins.create(generic_input=self.E3_rawin,
                                       symbolicdataset=self.raw_symDS)
        E03_11_ER.execrecordouts.create(generic_output=self.A1_rawin,
                                        symbolicdataset=self.raw_symDS)

        E03_11_RSIC.reused = False
        E03_11_RSIC.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunSIC .* has no ExecRecord",
            step_E1_RS.clean)

        # Good propagation case: RSIC is complete.
        E03_11_RSIC.execrecord = E03_11_ER
        E03_11_RSIC.save()
        self.assertEquals(step_E1_RS.clean(), None)

        # Bad case: cables not quenched, but there is an associated dataset.
        E03_11_RSIC.delete()
        self.doublet_DS.created_by = step_E1_RS
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* inputs not quenched; no data should have been generated",
            step_E1_RS.clean)
        # Reset....
        self.doublet_DS.created_by = None
        self.doublet_DS.save()

        # Bad case: cables not quenched, but reused is set.
        step_E1_RS.reused = False
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* inputs not quenched; reused and execrecord should not be set",
            step_E1_RS.clean)

        # Bad case: cables not quenched, but execrecord is set
        step_E1_RS.reused = None
        # Define ER for mA
        mA_RS = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        mA_ER = self.ER_from_record(mA_RS)
        mA_ER_in = mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                              generic_input=self.A1_rawin)
        mA_ER_out = mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                                generic_output=self.A1_out)
        step_E1_RS.execrecord = mA_ER
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* inputs not quenched; reused and execrecord should not be set",
            step_E1_RS.clean)

        # Reset....
        step_E1_RS.execrecord = None

        # Bad case: PS is a Pipeline, PS has child_run set, but cables are not
        # quenched.
        step_E2_RS = self.step_E2.pipelinestep_instances.create(run=pE_run)
        pD_run.parent_runstep = step_E2_RS
        pD_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* inputs not quenched; child_run should not be set",
            step_E2_RS.clean)

        # Reset....
        pD_run.parent_runstep = None
        pD_run.save()
        E03_11_RSIC = self.E03_11.psic_instances.create(
            runstep=step_E1_RS,
            reused=False,
            execrecord=E03_11_ER)
        self.assertEquals(step_E1_RS.clean(), None)

        # Quench cables for step E2 as well.
        E01_21_RSIC = self.E01_21.psic_instances.create(
            runstep = step_E2_RS, reused=False)
        E01_21_ER = self.ER_from_record(E01_21_RSIC)
        E01_21_ER.execrecordins.create(generic_input=self.E1_in,
                                       symbolicdataset=self.triplet_symDS)
        E01_21_ER.execrecordouts.create(generic_output=self.D1_in,
                                        symbolicdataset=self.D1_in_symDS)
        E01_21_RSIC.execrecord = E01_21_ER
        E01_21_RSIC.save()

        E02_22_RSIC = self.E02_22.psic_instances.create(
            runstep=step_E2_RS, reused=False)
        E02_22_ER = self.ER_from_record(E02_22_RSIC)
        E02_22_ER.execrecordins.create(generic_input=self.E2_in,
                                       symbolicdataset=self.singlet_symDS)
        E02_22_ER.execrecordouts.create(generic_output=self.D2_in,
                                        symbolicdataset=self.singlet_symDS)
        E02_22_RSIC.execrecord = E02_22_ER
        E02_22_RSIC.save()
        self.assertEquals(step_E2_RS.clean(), None)

        
        # Bad case: reused is not set, but there is an associated dataset.
        self.doublet_DS.created_by = step_E1_RS
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has not decided whether or not to reuse an ExecRecord; no data should have been generated",
            step_E1_RS.clean)
        # Reset....
        self.doublet_DS.created_by = None
        self.doublet_DS.save()

        # Bad case: reused not set, but ER is.
        step_E1_RS.execrecord = mA_ER
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has not decided whether or not to reuse an ExecRecord; execrecord should not be set",
            step_E1_RS.clean)

        # Jan 14: not necessary anymore, since ExecRecords can no longer point to pipelines. -Rosemary
        pD_ER = self.ER_from_record(step_D1_RS)
        #step_E2_RS.execrecord = pD_ER
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "RunStep .* has not decided whether or not to reuse an ExecRecord; execrecord should not be set",
        #    step_E2_RS.clean)
        ## Proceeding....
        step_E1_RS.execrecord = None
        step_E2_RS.execrecord = None
        step_E2_RS.reused = None

        # Jan 14: this exception doesn't appear in the code anymore. Is this no longer
        # a restriction? -Rosemary
        # Bad case: PS is a Pipeline, reused is not set, child_run is set.
        #pD_run.parent_runstep = step_E2_RS
        #pD_run.save()
        #step_E2_RS.save()
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "RunStep .* has not decided whether or not to reuse an ExecRecord; child_run should not be set",
        #    step_E2_RS.clean)
        ## Proceeding....
        #pD_run.parent_runstep = None
        #pD_run.save()

        # Bad case: reused = True, there is data associated to this RS.
        step_E1_RS.reused = True
        self.doublet_DS.created_by = step_E1_RS
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* reused an ExecRecord and should not have generated any Datasets",
            step_E1_RS.clean)

        # Jan 14: This exception doesn't appear in the code anymore. Replaced with a test for
        # being a sub-pipeline and being reused (which can't happen?).
        # Bad case: reused = True and child_run is set.
        step_E2_RS.reused = True
        #pD_run.parent_runstep = step_E2_RS
        #pD_run.save()
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "RunStep .* reused an ExecRecord and should not have a child run",
        #    step_E2_RS.clean)
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* represents a sub-pipeline so reused should not be set",
            step_E2_RS.clean)

        # Reset....
        step_E2_RS.reused = None
        self.doublet_DS.created_by = None
        self.doublet_DS.save()
        pD_run.parent_runstep = None
        pD_run.save()

        # Good case: reused = True and ER is not.
        self.assertEquals(step_E1_RS.clean(), None)
        self.assertEquals(step_E2_RS.clean(), None)

        # Create ExecLogs with associated MethodOutputs for RunSteps.
        step_E1_EL = step_E1_RS.log.create()
        step_E1_EL.start_time = timezone.now()
        time.sleep(1)
        step_E1_EL.end_time = timezone.now()
        step_E1_EL.save()

        step_E2_EL = step_E2_RS.log.create()
        step_E1_EL.start_time = timezone.now()
        time.sleep(1)
        step_E1_EL.end_time = timezone.now()
        step_E1_EL.save()

        step_E1_MO = MethodOutput(execlog=step_E1_EL, return_code = 0)
        step_E2_MO = MethodOutput(execlog=step_E2_EL, return_code = 0)
        step_E1_MO.save()
        step_E2_MO.save()
        step_E1_EL.methodoutput = step_E1_MO
        step_E2_EL.methodoutput = step_E2_MO
        
        # Bad propagation case: reused = False and associated data is not clean.
        step_E1_RS.reused = False
        self.doublet_DS.created_by = step_E1_RS
        self.doublet_symDS.MD5_checksum = "foo"
        self.doublet_DS.save()
        self.doublet_symDS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "File integrity of .* lost.  Current checksum .* does not equal expected checksum .*",
            step_E1_RS.clean)

        # Good propagation case for E1: reused = False, associated data is clean.
        self.doublet_symDS.MD5_checksum = self.doublet_DS.compute_md5()
        self.doublet_DS.save()
        self.doublet_symDS.save()
        self.assertEquals(step_E1_RS.clean(), None)

        # Jan 15: This scenario is no longer good, since ExecLogs are now only 
        # associated with atomic transformations, but step_E2 is a sub-pipeline.
        # Replaced with a test that sub-pipelines can't have ExecLogs, nor can
        # they set reused (why the second one?). -Rosemary
        # Good propagation case for E2: reused = False and no child run is set.
        step_E2_RS.reused = False
        #self.assertEquals(step_E2_RS.clean(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* represents a sub-pipeline so no log should be associated",
            step_E2_RS.clean)

        # Remove the log, should still fail because reused is set.
        step_E2_RS.log = step_E2_RS.log.none()
        step_E2_RS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* represents a sub-pipeline so reused should not be set",
            step_E2_RS.clean)

        # Good case: sub-pipeline with no log and unset reused.
        step_E2_RS.reused = None
        step_E2_RS.save()
        self.assertEquals(step_E2_RS.clean(), None)

        # Good case: child run is set and clean.
        pD_run.parent_runstep = step_E2_RS
        pD_run.save()
        self.assertEquals(step_E2_RS.clean(), None)

        # Jan 15: No longer relevant, since Runs can't have ExecRecords anymore. -Rosemary
        # Bad propagation case: child run is set but not clean.
        #pD_run.execrecord = pD_ER
        #pD_run.save()
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "Run .* has not decided whether or not to reuse an ER yet, so execrecord should not be set",
        #    step_E2_RS.clean)
        ## Reset....
        #pD_run.execrecord = None
        #pD_run.save()

        # Bad case: child run is set and clean, but there is data
        # associated with the RunStep.
        self.C1_in_DS.created_by = step_E2_RS
        self.C1_in_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* represents a sub-pipeline and should not have generated any data",
            step_E2_RS.clean)
        # Reset....
        self.C1_in_DS.created_by = None
        self.C1_in_DS.save()

        # Bad case: child run is set and clean, but ER is also set.
        pD_run.execrecord = None
        pD_run.save()
        step_E2_RS.execrecord = pD_ER
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* represents a sub-pipeline so execrecord should not be set",
            step_E2_RS.clean)

        # Reset....
        step_E2_RS.execrecord = None

        # From here on, we do tests where execrecord (or child_run.execrecord) is set.
        # Bad propagation case: execrecord is not complete.
        mA_ER_in.delete()
        step_E1_RS.execrecord = mA_ER
        self.assertRaisesRegexp(
            ValidationError,
            "Input\(s\) to ExecRecord .* are not quenched",
            step_E1_RS.clean)

        # Jan 15: No longer relevant, since step_E2 is a sub-pipeline so it can't have
        # an ExecRecord. -Rosemary
        #pD_run.reused = True
        #pD_run.execrecord = pD_ER
        #pD_run.save()
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "Input\(s\) to ExecRecord .* are not quenched",
        #    step_E2_RS.clean)

        # Reset....
        #pD_run.parent_runstep = None
        #pD_run.save()
        
        # Good propagation cases: ERs are complete, outputs are not deleted, all
        # associated data belongs to an ERO of this ER (i.e. this case proceeds
        # to the end).
        mA_ER_in = mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                              generic_input=self.A1_rawin)
        pD_ER_in = pD_ER.execrecordins.create(symbolicdataset=self.D1_in_symDS,
                                              generic_input=self.D1_in)
        pD_ER_in = pD_ER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.D2_in)
        pD_ER_out = pD_ER.execrecordouts.create(
            symbolicdataset=self.C1_in_symDS,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertEquals(step_E1_RS.clean(), None)
        self.assertEquals(step_E2_RS.clean(), None)
        
        # Bad case: ER points to the wrong transformation.
        # Jan 15: TODO The data types are checked first, making this exception hard to reach.
        # To test this, I have to make two methods with the same inputs and outputs.
        #step_E1_RS.execrecord = pD_ER
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "RunStep .* points to transformation .* but corresponding ER does not",
        #    step_E1_RS.clean)

        # Jan 15: No longer relevant, since step_E2 is a sub-pipeline so it can't have
        # an ExecRecord. -Rosemary
        #step_E2_RS.execrecord = mA_ER
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "RunStep .* points to transformation .* but corresponding ER does not",
        #    step_E2_RS.clean)
        #step_E2_RS.execrecord = pD_ER
        #step_E2_RS.save()

        # Reset....
        #step_E1_RS.execrecord = mA_ER
        #step_E1_RS.save()

        # Bad case: step E1's output is marked for deletion, and step
        # is not reused, but there is an associated Dataset.
        self.step_E1.outputs_to_delete.add(
            self.mA.outputs.get(dataset_name="A1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "Output .* of RunStep .* is deleted; no data should be associated",
            step_E1_RS.clean)

        # Bad case: output not deleted, but ERO has no existent data.
        self.step_E1.outputs_to_delete.remove(
            self.mA.outputs.get(dataset_name="A1_out"))
        self.step_E1.save()
        empty_symDS = SymbolicDataset()
        empty_symDS.save()
        structure = DatasetStructure(symbolicdataset=empty_symDS, compounddatatype=self.doublet_cdt)
        structure.save()
        empty_symDS.structure = structure
        mA_ER_out.symbolicdataset = empty_symDS
        mA_ER_out.save()

        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut .* of RunStep .* should reference existent data",
            step_E1_RS.clean)
        # Reset....
        mA_ER_out.symbolicdataset = self.doublet_symDS
        mA_ER_out.save()

        # Jan 15: No longer relevant, since step_E2 is a sub-pipeline so it can't have
        # an ExecRecord. -Rosemary
        #pD_ER_out.symbolicdataset = empty_symDS
        #pD_ER_out.save()
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "ExecRecordOut .* of RunStep .* should reference existent data",
        #    step_E2_RS.clean)
        # Reset....
        #pD_ER_out.symbolicdataset = self.C1_in_symDS
        #pD_ER_out.save()

        # Bad case: ER is not reused, output was not deleted, but no Dataset is associated.
        # Jan 15: This exception doesn't seem to be in the code anymore. -Rosemary
        #self.doublet_DS.created_by = None
        #self.doublet_DS.save()
        #self.assertRaisesRegexp(
        #    ValidationError,
        #    "RunStep .* did not reuse an ExecRecord, had no child run, and output .* was not deleted; a corresponding Dataset should be associated",
        #    step_E1_RS.clean)
        ## Reset....
        #self.doublet_DS.created_by = step_E1_RS
        #self.doublet_DS.save()

        # Bad case: there is an associated dataset that does not belong to any ERO
        # of this ER.
        self.triplet_DS.created_by = step_E1_RS
        self.triplet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* generated Dataset .* but it is not in its ExecRecord",
            step_E1_RS.clean)
        # Reset....
        self.triplet_DS.created_by = None
        self.triplet_DS.save()
        
        # Let's check some of the above cases for step E2 when it's *not* reused.
        # Sadly this means we have to define a whole sub-run for pD.
        # Jan 15: Actually E2 can never be reused, because it is a sub-pipeline.
        # But we still need the sub-run for some of the below tests.
        pD_run.parent_runstep = step_E2_RS
        pD_run.reused = False
        
        step_D1_RS.run = pD_run
        step_D1_RS.reused = False
        D01_11_RSIC = self.D01_11.psic_instances.create(runstep=step_D1_RS,
                                                        reused=False)
        D01_11_ER = self.ER_from_record(D01_11_RSIC)
        D01_11_ER.execrecordins.create(symbolicdataset=self.D1_in_symDS,
                                       generic_input=self.D1_in)
        D01_11_ER.execrecordouts.create(symbolicdataset=self.D1_in_symDS,
                                        generic_output=self.B1_in)
        D01_11_RSIC.execrecord = D01_11_ER
        D01_11_RSIC.save()
        
        D02_12_RSIC = self.D02_12.psic_instances.create(runstep=step_D1_RS,
                                                        reused=False)
        D02_12_ER = self.ER_from_record(D02_12_RSIC)
        D02_12_ER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                       generic_input=self.D2_in)
        D02_12_ER.execrecordouts.create(symbolicdataset=self.singlet_symDS,
                                        generic_output=self.B2_in)
        D02_12_RSIC.execrecord = D02_12_ER
        D02_12_RSIC.save()

        # Method mB is step step_D1 of pipeline pD.
        step_D1_RS.log = step_D1_RS.log.none()
        mB_ER = self.ER_from_record(step_D1_RS)
        mB_ER.execrecordins.create(symbolicdataset=self.D1_in_symDS,
                                   generic_input=self.B1_in)
        mB_ER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                   generic_input=self.B2_in)
        mB_ER.execrecordouts.create(
            symbolicdataset=self.C1_in_symDS,
            generic_output=self.mB.outputs.get(dataset_name="B1_out"))
        self.C1_in_DS.created_by = step_D1_RS
        self.C1_in_DS.save()
        step_D1_RS.execrecord = mB_ER
        step_D1_RS.save()
        
        D11_21_ROC = self.D11_21.poc_instances.create(run=pD_run,
                                                      reused=False)
        D11_21_ER = self.ER_from_record(D11_21_ROC)
        D11_21_ER.execrecordins.create(
            symbolicdataset=self.C1_in_symDS,
            generic_input=self.mB.outputs.get(dataset_name="B1_out"))
        D11_21_ER.execrecordouts.create(
            symbolicdataset=self.C1_in_symDS,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))
        D11_21_ROC.execrecord = D11_21_ER
        D11_21_ROC.save()

        # pD_ER was already defined above.
        pD_run.runsteps.add(step_D1_RS)
        pD_run.save()

        # None of the bad cases really work in this setting, because other checks
        # will catch them all!
        self.assertEquals(step_E2_RS.clean(), None)

        # Finally, check is_complete and complete_clean.
        self.assertEquals(step_E1_RS.is_complete(), True)
        self.assertEquals(step_E1_RS.complete_clean(), None)

        step_E1_RS.execrecord = None
        self.assertEquals(step_E1_RS.is_complete(), False)
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* is not complete",
            step_E1_RS.complete_clean)

        # Propagation check on complete_clean:
        step_E1_RS.reused = None
        step_E1_ER = self.ER_from_record(step_E1_RS)
        step_E1_RS.execrecord = step_E1_ER
        step_E1_RS.log = step_E1_RS.log.none()
        step_E1_RS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has not decided whether or not to reuse an ExecRecord; no data should have been generated",
            step_E1_RS.complete_clean)

        step_E2_RS.child_run = pD_run
        step_E2_RS.save()
        self.assertEquals(step_E2_RS.is_complete(), True)
        self.assertEquals(step_E2_RS.complete_clean(), None)

class RunTests(librarian.tests.LibrarianTestSetup):

    def make_complete_non_reused(self, record, input_SDs, output_SDs):
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

    def complete_RSICs(self, runstep, input_SDs, output_SDs):
        """
        Helper function to create and complete all the RunSIC's needed for
        a given RunStep. input_SDs and output_SDs are lists of the input and
        output symbolic datasets for each cable, in order.
        """
        for i, cable in enumerate(runstep.pipelinestep.cables_in.order_by("dest__dataset_idx")):
            rsic = cable.psic_instances.create(runstep=runstep)
            self.make_complete_non_reused(rsic, [input_SDs[i]], [output_SDs[i]])

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
