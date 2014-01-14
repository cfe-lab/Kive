"""
Shipyard archive application unit tests.
"""

from django.test import TestCase
from django.core.files import File
from django.core.exceptions import ValidationError

import os
from librarian.models import *
from archive.models import *
import librarian.tests

# Note that these tests use the exact same setup as librarian.

class RunStepTests(librarian.tests.LibrarianTestSetup):

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

        # Moving on....
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
        E03_11_ER = self.E03_11.execrecords.create()
        E03_11_ER.execrecordins.create(generic_input=self.E3_rawin,
                                       symbolicdataset=self.raw_symDS)
        E03_11_ER.execrecordouts.create(generic_output=self.A1_rawin,
                                        symbolicdataset=self.raw_symDS)

        E03_11_RSIC.reused = False
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
        mA_ER = self.mA.execrecords.create()
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
        E01_21_ER = self.E01_21.execrecords.create()
        E01_21_ER.execrecordins.create(generic_input=self.E1_in,
                                       symbolicdataset=self.triplet_symDS)
        E01_21_ER.execrecordouts.create(generic_output=self.D1_in,
                                        symbolicdataset=self.D1_in_symDS)
        E01_21_RSIC.execrecord = E01_21_ER
        E01_21_RSIC.save()

        E02_22_RSIC = self.E02_22.psic_instances.create(
            runstep=step_E2_RS, reused=False)
        E02_22_ER = self.E02_22.execrecords.create()
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
        pD_ER = self.pD.execrecords.create()
        step_E2_RS.execrecord = pD_ER
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has not decided whether or not to reuse an ExecRecord; execrecord should not be set",
            step_E2_RS.clean)
        # Proceeding....
        step_E1_RS.execrecord = None
        step_E2_RS.execrecord = None

        # Bad case: PS is a Pipeline, reused is not set, child_run is set.
        pD_run.parent_runstep = step_E2_RS
        pD_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has not decided whether or not to reuse an ExecRecord; child_run should not be set",
            step_E2_RS.clean)
        # Proceeding....
        pD_run.parent_runstep = None
        pD_run.save()

        # Bad case: reused = True, there is data associated to this RS.
        step_E1_RS.reused = True
        self.doublet_DS.created_by = step_E1_RS
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* reused an ExecRecord and should not have generated any data",
            step_E1_RS.clean)

        # Bad case: reused = True and child_run is set.
        step_E2_RS.reused = True
        pD_run.parent_runstep = step_E2_RS
        pD_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* reused an ExecRecord and should not have a child run",
            step_E2_RS.clean)

        # Reset....
        self.doublet_DS.created_by = None
        self.doublet_DS.save()
        pD_run.parent_runstep = None
        pD_run.save()

        # Good case: reused = True and ER is not.
        self.assertEquals(step_E1_RS.clean(), None)
        self.assertEquals(step_E2_RS.clean(), None)
        
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

        # Good propagation case for E2: reused = False and no child run is set.
        step_E2_RS.reused = False
        self.assertEquals(step_E2_RS.clean(), None)

        # Good case: child run is set and clean.
        pD_run.parent_runstep = step_E2_RS
        pD_run.save()
        self.assertEquals(step_E2_RS.clean(), None)

        # Bad propagation case: child run is set but not clean.
        pD_run.execrecord = pD_ER
        pD_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not decided whether or not to reuse an ER yet, so execrecord should not be set",
            step_E2_RS.clean)
        # Reset....
        pD_run.execrecord = None
        pD_run.save()

        # Bad case: child run is set and clean, but there is data
        # associated with the RunStep.
        self.C1_in_DS.created_by = step_E2_RS
        self.C1_in_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has a child run so should not have generated any data",
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
            "RunStep .* has a child run so execrecord should not be set",
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

        pD_run.reused = True
        pD_run.execrecord = pD_ER
        pD_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Input\(s\) to ExecRecord .* are not quenched",
            step_E2_RS.clean)

        # Reset....
        pD_run.parent_runstep = None
        pD_run.save()
        step_E2_RS.reused = True
        step_E2_RS.execrecord = pD_ER
        step_E2_RS.save()
        
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
        step_E1_RS.execrecord = pD_ER
        step_E2_RS.execrecord = mA_ER
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* points to transformation .* but corresponding ER does not",
            step_E1_RS.clean)
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* points to transformation .* but corresponding ER does not",
            step_E2_RS.clean)
        # Reset....
        step_E1_RS.execrecord = mA_ER
        step_E1_RS.save()
        step_E2_RS.execrecord = pD_ER
        step_E2_RS.save()

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
        empty_symDS = SymbolicDataset()
        empty_symDS.save()
        mA_ER_out.symbolicdataset = empty_symDS
        mA_ER_out.save()
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut .* of RunStep .* should reference existent data",
            step_E1_RS.clean)
        # Reset....
        mA_ER_out.symbolicdataset = self.doublet_symDS
        mA_ER_out.save()

        pD_ER_out.symbolicdataset = empty_symDS
        pD_ER_out.save()
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut .* of RunStep .* should reference existent data",
            step_E2_RS.clean)
        # Reset....
        pD_ER_out.symbolicdataset = self.C1_in_symDS
        pD_ER_out.save()

        # Bad case: ER is not reused, output was not deleted, but no Dataset is associated.
        self.doublet_DS.created_by = None
        self.doublet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* did not reuse an ExecRecord, had no child run, and output .* was not deleted; a corresponding Dataset should be associated",
            step_E1_RS.clean)
        # Reset....
        self.doublet_DS.created_by = step_E1_RS
        self.doublet_DS.save()

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
        step_E2_RS.reused = False
        step_E2_RS.execrecord = None

        pD_run.parent_runstep = step_E2_RS
        pD_run.reused = False
        
        step_D1_RS.run = pD_run
        step_D1_RS.reused = False
        D01_11_RSIC = self.D01_11.psic_instances.create(runstep=step_D1_RS,
                                                        reused=False)
        D01_11_ER = self.D01_11.execrecords.create()
        D01_11_ER.execrecordins.create(symbolicdataset=self.D1_in_symDS,
                                       generic_input=self.D1_in)
        D01_11_ER.execrecordouts.create(symbolicdataset=self.D1_in_symDS,
                                        generic_output=self.B1_in)
        D01_11_RSIC.execrecord = D01_11_ER
        D01_11_RSIC.save()
        
        D02_12_RSIC = self.D02_12.psic_instances.create(runstep=step_D1_RS,
                                                        reused=False)
        D02_12_ER = self.D02_12.execrecords.create()
        D02_12_ER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                       generic_input=self.D2_in)
        D02_12_ER.execrecordouts.create(symbolicdataset=self.singlet_symDS,
                                        generic_output=self.B2_in)
        D02_12_RSIC.execrecord = D02_12_ER
        D02_12_RSIC.save()

        mB_ER = self.mB.execrecords.create()
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
        D11_21_ER = self.D11_21.execrecords.create()
        D11_21_ER.execrecordins.create(
            symbolicdataset=self.C1_in_symDS,
            generic_input=self.mB.outputs.get(dataset_name="B1_out"))
        D11_21_ER.execrecordouts.create(
            symbolicdataset=self.C1_in_symDS,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))
        D11_21_ROC.execrecord = D11_21_ER
        D11_21_ROC.save()

        # pD_ER was already defined above.
        pD_run.execrecord = pD_ER
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
            "RunStep .* has no ExecRecord",
            step_E1_RS.complete_clean)

        # Propagation check on complete_clean:
        step_E1_RS.reused = None
        step_E1_RS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has not decided whether or not to reuse an ExecRecord; no data should have been generated",
            step_E1_RS.complete_clean)

        self.assertEquals(step_E2_RS.is_complete(), True)
        self.assertEquals(step_E2_RS.complete_clean(), None)

        pD_run.execrecord = None
        pD_run.save()
        self.assertEquals(step_E2_RS.is_complete(), False)
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has no ExecRecord",
            step_E2_RS.complete_clean)


class RunTests(librarian.tests.LibrarianTestSetup):

    def test_Run_clean_early(self):
        """Check coherence of a Run at all stages of its creation before reused is set."""
        # Create a top-level run.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        # Good case: nothing has happened yet.
        self.assertEquals(pE_run.clean(), None)

        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=pE_run)

        # Bad case: parent_runstep is set, but pipeline is not consistent with it.
        pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        pD_run.parent_runstep = step_E1_RS
        self.assertRaisesRegexp(
            ValidationError,
            "Pipeline of Run .* is not consistent with its parent RunStep",
            pD_run.clean)

        # Good case: parent_runstep is set, and pipeline is consistent with it.
        step_E2_RS = self.step_E2.pipelinestep_instances.create(run=pE_run)
        pD_run.parent_runstep = step_E2_RS
        self.assertEquals(pD_run.clean(), None)
        # Reset....
        pD_run.delete()
        step_E2_RS.delete()

        # Bad case: reused == None, but there is a RS associated to the Run.
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not decided whether or not to reuse an ER yet, so there should be no associated RunSteps",
            pE_run.clean)
        # Reset....
        step_E1_RS.delete()

        # Bad case: reused == None, but there is an ROC associated.
        E33_43_ROC = self.E33_43.poc_instances.create(run=pE_run)
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not decided whether or not to reuse an ER yet, so there should be no associated RunOutputCables",
            pE_run.clean)
        # Reset....
        E33_43_ROC.delete()

        # Bad case: reused == None, but there is an execrecord associated.
        pE_ER = self.pE.execrecords.create()
        pE_run.execrecord = pE_ER
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not decided whether or not to reuse an ER yet, so execrecord should not be set",
            pE_run.clean)
        # Reset....
        pE_run.execrecord = None
        pE_run.save()
        pE_ER.delete()


    def test_Run_clean_reused(self):
        """Check coherence of a Run after it has decided to reuse an ExecRecord."""
        # Create a top-level run.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        pE_run.reused = True

        # Good case: no RS or ROC associated, no ER yet.
        self.assertEquals(pE_run.clean(), None)

        # Bad case: RS associated.
        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=pE_run)
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* reuses an ER, so there should be no associated RunSteps",
            pE_run.clean)
        # Reset....
        step_E1_RS.delete()

        # Bad case: ROC associated.
        E33_43_ROC = self.E33_43.poc_instances.create(run=pE_run)
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* reuses an ER, so there should be no associated RunOutputCables",
            pE_run.clean)
        # Reset....
        E33_43_ROC.delete()

        # Set an ER.
        pE_ER = self.pE.execrecords.create()
        pE_run.execrecord = pE_ER
        pE_run.save()

        # Bad propagation case: execrecord is not complete.
        self.assertRaisesRegexp(
            ValidationError,
            "Input\(s\) to ExecRecord .* are not quenched",
            pE_run.clean)

        # Proceed:
        pE_ER_in1 = pE_ER.execrecordins.create(
            generic_input=self.E1_in,
            symbolicdataset=self.triplet_symDS)
        pE_ER_in2 = pE_ER.execrecordins.create(
            generic_input=self.E2_in,
            symbolicdataset=self.singlet_symDS)
        pE_ER_in3 = pE_ER.execrecordins.create(
            generic_input=self.E3_rawin,
            symbolicdataset=self.raw_symDS)
        
        pE_ER_out1 = pE_ER.execrecordouts.create(
            generic_output=self.E1_out,
            symbolicdataset=self.E1_out_symDS)
        pE_ER_out2 = pE_ER.execrecordouts.create(
            generic_output=self.E2_out,
            symbolicdataset=self.C1_out_symDS)
        pE_ER_out3 = pE_ER.execrecordouts.create(
            generic_output=self.E3_rawout,
            symbolicdataset=self.C3_out_symDS)

        # Bad case: ER and pipeline are not consistent.
        pE_run.pipeline = self.pD
        pE_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* points to pipeline .* but corresponding ER does not",
            pE_run.clean)

        # Now test is_complete, complete_clean.
        self.assertEquals(pE_run.is_complete(), True)
        # Propagation bad case: clean fails.
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* points to pipeline .* but corresponding ER does not",
            pE_run.complete_clean)
        
        # Good case: the run is complete and clean.
        pE_run.pipeline = self.pE
        pE_run.save()
        self.assertEquals(pE_run.is_complete(), True)
        self.assertEquals(pE_run.complete_clean(), None)

        # Bad case: the run is incomplete but clean.
        pE_run.execrecord = None
        self.assertEquals(pE_run.is_complete(), False)
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has no ExecRecord",
            pE_run.complete_clean)


        
    def test_Run_clean_not_reused(self):
        """Check coherence of a Run after it has decided not to reuse an ExecRecord."""
        # Create a top-level run.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        pE_run.reused = False

        # Good case: no RS or ROC associated, no ER yet.
        self.assertEquals(pE_run.clean(), None)

        # Good case: first RS is associated and incomplete, nothing else is.
        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=pE_run)
        self.assertEquals(pE_run.clean(), None)

        # Bad case: second RS is associated before first is complete.
        step_E2_RS = self.step_E2.pipelinestep_instances.create(run=pE_run)
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* has no ExecRecord",
            pE_run.clean)

        # Bad case: second RS is associated, first one is not.
        step_E1_RS.delete()
        self.assertRaisesRegexp(
            ValidationError,
            "RunSteps of Run .* are not consecutively numbered starting from 1",
            pE_run.clean)

        # Reset....
        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=pE_run)
        step_E2_RS.delete()

        # Bad case: first RS is not clean, no others associated.
        step_E1_RS.reused = False
        step_E1_RS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* inputs not quenched; reused and execrecord should not be set",
            pE_run.clean)
        # Reset....
        step_E1_RS.reused = None
        step_E1_RS.save()
        
        # Good case: first RS is complete, nothing else associated.
        step_E1_RS.reused = False
        E03_11_RSIC = self.E03_11.psic_instances.create(runstep=step_E1_RS,
                                                        reused=False)
        E03_11_ER = self.E03_11.execrecords.create()
        E03_11_ER.execrecordins.create(generic_input=self.E3_rawin,
                                       symbolicdataset=self.raw_symDS)
        E03_11_ER.execrecordouts.create(generic_output=self.A1_rawin,
                                        symbolicdataset=self.raw_symDS)
        E03_11_RSIC.execrecord = E03_11_ER
        E03_11_RSIC.save()
        self.doublet_DS.created_by = step_E1_RS
        self.doublet_DS.save()
        mA_ER = self.mA.execrecords.create()
        mA_ER_in = mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                              generic_input=self.A1_rawin)
        mA_ER_out = mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                                generic_output=self.A1_out)
        step_E1_RS.execrecord = mA_ER
        step_E1_RS.save()
        self.assertEquals(pE_run.clean(), None)

        # Good case: first RS is complete, second one is associated and clean.
        step_E2_RS = self.step_E2.pipelinestep_instances.create(run=pE_run)
        self.assertEquals(pE_run.clean(), None)

        # Bad case: first RS is complete, second one is unclean.
        step_E2_RS.reused = False
        step_E2_RS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunStep .* inputs not quenched; reused and execrecord should not be set",
            pE_run.clean)
        # Reset....
        step_E2_RS.reused = None
        step_E2_RS.save()

        # Bad case: there aren't enough RSs and an ROC is associated.
        E31_42_ROC = self.E31_42.poc_instances.create(run=pE_run)
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not completed all of its RunSteps, so there should be no associated RunOutputCables",
            pE_run.clean)
        # Reset....
        E31_42_ROC.delete()

        # Bad case: not enough RSs and an ER is associated.
        pE_ER = self.pE.execrecords.create()
        pE_run.execrecord = pE_ER
        pE_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not completed all of its RunSteps, so execrecord should not be set",
            pE_run.clean)
        # Reset....
        pE_run.execrecord = None
        pE_run.save()
        # NOTE: can't delete this before setting pE_run.execrecord =
        # None, or else it will cascade deletions and break stuff
        # afterwards!
        pE_ER.delete()
        
        # Bad case: enough RSs, but last one is not complete.
        # To save time, let's just reuse the second step.
        E01_21_RSIC = self.E01_21.psic_instances.create(
            runstep=step_E2_RS, reused=True)
        E01_21_ER = self.E01_21.execrecords.create()
        E01_21_ER.execrecordins.create(generic_input=self.E1_in,
                                       symbolicdataset=self.triplet_symDS)
        E01_21_ER.execrecordouts.create(generic_output=self.D1_in,
                                        symbolicdataset=self.D1_in_symDS)
        E01_21_RSIC.execrecord = E01_21_ER
        E01_21_RSIC.save()

        E02_22_RSIC = self.E02_22.psic_instances.create(
            runstep=step_E2_RS, reused=True)
        E02_22_ER = self.E02_22.execrecords.create()
        E02_22_ER.execrecordins.create(generic_input=self.E2_in,
                                       symbolicdataset=self.singlet_symDS)
        E02_22_ER.execrecordouts.create(generic_output=self.D2_in,
                                        symbolicdataset=self.singlet_symDS)
        E02_22_RSIC.execrecord = E02_22_ER
        E02_22_RSIC.save()
        step_E2_RS.reused = True
        pD_ER = self.pD.execrecords.create()
        pD_ER_in = pD_ER.execrecordins.create(symbolicdataset=self.D1_in_symDS,
                                              generic_input=self.D1_in)
        pD_ER_in = pD_ER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.D2_in)
        pD_ER_out = pD_ER.execrecordouts.create(
            symbolicdataset=self.C1_in_symDS,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))
        step_E2_RS.execrecord = pD_ER
        step_E2_RS.save()
        
        # Good case: two complete RSs, no last one.
        self.assertEquals(pE_run.clean(), None)

        # Create an incomplete last RS.
        step_E3_RS = self.step_E3.pipelinestep_instances.create(run=pE_run)

        # Good case: two complete RSs, clean but incomplete last one.
        self.assertEquals(pE_run.clean(), None)

        # Repeat the last two bad cases.
        # Bad case: enough RSs but last is incomplete; ROC associated.
        E31_42_ROC = self.E31_42.poc_instances.create(run=pE_run)
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not completed all of its RunSteps, so there should be no associated RunOutputCables",
            pE_run.clean)
        # Reset....
        E31_42_ROC.delete()

        # Bad case: enough RSs but last is incomplete; ER is associated.
        pE_ER = self.pE.execrecords.create()
        pE_run.execrecord = pE_ER
        pE_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not completed all of its RunSteps, so execrecord should not be set",
            pE_run.clean)
        # Reset....
        pE_run.execrecord = None
        pE_run.save()
        pE_ER.delete()

        # Proceed: complete the last RunStep.
        E21_31_RSIC = self.E21_31.psic_instances.create(
            runstep=step_E3_RS, reused=False)
        E21_31_ER = self.E21_31.execrecords.create()
        E21_31_ER.execrecordins.create(
            generic_input=self.D1_out, symbolicdataset=self.C1_in_symDS)
        E21_31_ER.execrecordouts.create(
            generic_output=self.C1_in, symbolicdataset=self.C1_in_symDS)
        E21_31_RSIC.execrecord = E21_31_ER
        E21_31_RSIC.save()
        
        E11_32_RSIC = self.E11_32.psic_instances.create(
            runstep=step_E3_RS, reused=False)
        E11_32_ER = self.E11_32.execrecords.create()
        E11_32_ER.execrecordins.create(
            generic_input=self.A1_out, symbolicdataset=self.doublet_symDS)
        E11_32_ER.execrecordouts.create(
            generic_output=self.C2_in, symbolicdataset=self.C2_in_symDS)
        E11_32_RSIC.execrecord = E11_32_ER
        E11_32_RSIC.save()
        
        step_E3_RS.reused = False
        
        mC_ER = self.mC.execrecords.create()
        mC_ER_in1 = mC_ER.execrecordins.create(
            generic_input=self.C1_in,
            symbolicdataset=self.C1_in_symDS)
        mC_ER_in2 = mC_ER.execrecordins.create(
            generic_input=self.C2_in,
            symbolicdataset=self.C2_in_symDS)
        mC_ER_out1 = mC_ER.execrecordouts.create(
            generic_output=self.C1_out,
            symbolicdataset=self.C1_out_symDS)
        mC_ER_out2 = mC_ER.execrecordouts.create(
            generic_output=self.C2_rawout,
            symbolicdataset=self.C2_out_symDS)
        mC_ER_out3 = mC_ER.execrecordouts.create(
            generic_output=self.C3_rawout,
            symbolicdataset=self.C3_out_symDS)
        self.C1_out_DS.created_by = step_E3_RS
        self.C1_out_DS.save()
        self.C2_out_DS.created_by = step_E3_RS
        self.C2_out_DS.save()
        self.C3_out_DS.created_by = step_E3_RS
        self.C3_out_DS.save()

        step_E3_RS.execrecord = mC_ER
        step_E3_RS.save()

        # Good case: all RSs complete, no outcables set.
        self.assertEquals(pE_run.clean(), None)

        # Bad propagation case: bad ROC associated.
        E21_41_ROC = self.E21_41.poc_instances.create(run=pE_run)
        E21_41_ER = self.E21_41.execrecords.create()
        E21_41_ER.execrecordins.create(
            generic_input=self.D1_out,
            symbolicdataset=self.C1_in_symDS)
        E21_41_ER.execrecordouts.create(
            generic_output=self.E1_out,
            symbolicdataset=self.E1_out_symDS)
        E21_41_ROC.execrecord = E21_41_ER
        E21_41_ROC.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* has not decided whether or not to reuse an ExecRecord; execrecord should not be set yet",
            pE_run.clean)

        # Proceed....
        E21_41_ROC.reused = False
        self.E1_out_DS.created_by = E21_41_ROC
        self.E1_out_DS.save()
        E21_41_ROC.save()
        # Good propagation case: one good ROC associated.
        self.assertEquals(pE_run.clean(), None)

        # Bad case: ER is set, only one output cable is set and done.
        pE_ER = self.pE.execrecords.create()
        pE_run.execrecord = pE_ER
        pE_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not completed all of its RunOutputCables, so execrecord should not be set",
            pE_run.clean)
        # Reset....
        pE_run.execrecord = None
        pE_run.save()

        # Good case: one output cable is done, the other two are clean
        # but incomplete.
        E31_42_ROC = self.E31_42.poc_instances.create(run=pE_run)
        E33_43_ROC = self.E33_43.poc_instances.create(run=pE_run)
        self.assertEquals(pE_run.clean(), None)

        # Bad case: only one output cable is done, but all 3 are set,
        # but ER is set.
        pE_run.execrecord = pE_ER
        pE_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has not completed all of its RunOutputCables, so execrecord should not be set",
            pE_run.clean)
        # Reset....
        pE_run.execrecord = None
        pE_run.save()

        # Good case: all 3 output cables are done, ER not set.
        E31_42_ROC.reused = False
        E31_42_ER = self.E31_42.execrecords.create()
        E31_42_ER.execrecordins.create(
            generic_input=self.C1_out, symbolicdataset=self.C1_out_symDS)
        E31_42_ER.execrecordouts.create(
            generic_output=self.E2_out, symbolicdataset=self.C1_out_symDS)
        self.C1_out_DS.created_by = E31_42_ROC
        E31_42_ROC.execrecord = E31_42_ER
        E31_42_ROC.save()
        
        E33_43_ROC.reused = False
        E33_43_ER = self.E33_43.execrecords.create()
        E33_43_ER.execrecordins.create(
            generic_input=self.C3_rawout, symbolicdataset=self.C3_out_symDS)
        E33_43_ER.execrecordouts.create(
            generic_output=self.E3_rawout, symbolicdataset=self.C3_out_symDS)
        self.C3_out_DS.created_by = E33_43_ROC
        E33_43_ROC.execrecord = E33_43_ER
        E33_43_ROC.save()
        self.assertEquals(pE_run.clean(), None)

        # Propagation bad case: ER set but not complete.
        pE_run.execrecord = pE_ER
        self.assertRaisesRegexp(
            ValidationError,
            "Input\(s\) to ExecRecord .* are not quenched",
            pE_run.clean)

        # Propagation good case: ER is complete.
        pE_ER_in1 = pE_ER.execrecordins.create(
            generic_input=self.E1_in,
            symbolicdataset=self.triplet_symDS)
        pE_ER_in2 = pE_ER.execrecordins.create(
            generic_input=self.E2_in,
            symbolicdataset=self.singlet_symDS)
        pE_ER_in3 = pE_ER.execrecordins.create(
            generic_input=self.E3_rawin,
            symbolicdataset=self.raw_symDS)
        
        pE_ER_out1 = pE_ER.execrecordouts.create(
            generic_output=self.E1_out,
            symbolicdataset=self.E1_out_symDS)
        pE_ER_out2 = pE_ER.execrecordouts.create(
            generic_output=self.E2_out,
            symbolicdataset=self.C1_out_symDS)
        pE_ER_out3 = pE_ER.execrecordouts.create(
            generic_output=self.E3_rawout,
            symbolicdataset=self.C3_out_symDS)
        self.assertEquals(pE_run.clean(), None)

        # Bad case: ER and pipeline are not consistent.
        pE_run.execrecord = mA_ER
        pE_run.save()
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* points to pipeline .* but corresponding ER does not",
            pE_run.clean)
        # Reset....
        pE_run.execrecord = pE_ER
        pE_run.save()

        # Bad case: ERO has a symbolic dataset different from that
        # of the corresponding ROC.
        pE_ER_out2.symbolicdataset = self.singlet_3rows_symDS
        pE_ER_out2.save()
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut .* of Run .* does not match the corresponding RunOutputCable",
            pE_run.clean)

        # Before we reset, check is_complete and complete_clean.
        self.assertEquals(pE_run.is_complete(), True)
        # Propagation bad case: pE_run is not clean.
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut .* of Run .* does not match the corresponding RunOutputCable",
            pE_run.complete_clean)
        
        # Reset....
        pE_ER_out2.symbolicdataset = self.C1_out_symDS
        pE_ER_out2.save()
        self.assertEquals(pE_run.is_complete(), True)
        self.assertEquals(pE_run.complete_clean(), None)

        pE_run.execrecord = None
        pE_run.save()
        self.assertEquals(pE_run.is_complete(), False)
        self.assertRaisesRegexp(
            ValidationError,
            "Run .* has no ExecRecord",
            pE_run.complete_clean)

class RunSICTests(librarian.tests.LibrarianTestSetup):

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
        E11_32_ER = self.E11_32.execrecords.create()
        source = E11_32_ER.execrecordins.create(
            generic_input=self.mA.outputs.get(dataset_name="A1_out"),
            symbolicdataset=self.doublet_symDS)
        dest = E11_32_ER.execrecordouts.create(
            generic_output=self.C2_in,
            symbolicdataset=self.C2_in_symDS)
        E11_32_RSIC.execrecord = E11_32_ER
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
        step_E3_RS = self.step_E3.pipelinestep_instances.create(
            run=pE_run)
        E11_32_RSIC = self.E11_32.psic_instances.create(runstep=step_E3_RS)
        E11_32_RSIC.reused = True

        E11_32_ER = self.E11_32.execrecords.create()
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
        E02_22_ER = self.E02_22.execrecords.create()
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
        mA_ER = self.mA.execrecords.create()
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

        E11_32_ER = self.E11_32.execrecords.create()
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
        E02_22_ER = self.E02_22.execrecords.create()
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
        mA_ER = self.mA.execrecords.create()
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
            "RunSIC .* does not keep its output; no data should be produced",
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

    def test_ROC_clean(self):
        """Checks coherence of a RunOutputCable at all stages of its creation."""
        # Define a run for pE so that this ROC has something to belong to.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)

        # Create a ROC for one of the POCs.
        E31_42_ROC = self.E31_42.poc_instances.create(run=pE_run)

        # Good case: POC belongs to the parent run's Pipeline.
        self.assertEquals(E31_42_ROC.clean(), None)

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
        E31_42_ER = self.E31_42.execrecords.create()
        E31_42_ROC.execrecord = E31_42_ER
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* has not decided whether or not to reuse an ExecRecord; execrecord should not be set yet",
            E31_42_ROC.clean)
        # Reset....
        E31_42_ROC.execrecord = None

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
        # Create ER for mA.
        mA_ER = self.mA.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)

        # Create an execrecord for another of the POCs.
        E21_41_ER = self.E21_41.execrecords.create()
        empty_sd_source = SymbolicDataset()
        empty_sd_source.save()
        empty_sd_dest = SymbolicDataset()
        empty_sd_dest.save()
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
        D11_21_ER = self.D11_21.execrecords.create()
        empty_symDS = SymbolicDataset()
        empty_symDS.save()
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
        other_triplet_3_rows_symDS = SymbolicDataset()
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
            dataset=other_triplet_3_rows_DS,
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
        self.assertEquals(E21_41_ROC.clean(), None)

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
