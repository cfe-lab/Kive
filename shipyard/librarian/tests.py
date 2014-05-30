"""
Shipyard models pertaining to the librarian app.
"""

from django.test import TestCase
from django.core.exceptions import ValidationError
from django.core.files import File
from django.utils import timezone
from django.contrib.auth.models import User

import random
import tempfile
import os.path
import time

import file_access_utils
from constants import datatypes

from archive.models import *
from pipeline.models import *
from method.models import *
from librarian.models import *
from metadata.models import *

import metadata.tests
from method.tests import samplecode_path

class LibrarianTestSetup(metadata.tests.MetadataTestSetup):
    """
    Set up a database state for unit testing the librarian app.

    This extends PipelineTestSetup, which itself extended
    other stuff (follow the chain).
    """
    def setUp(self):
        """Set up default database state for librarian unit testing."""
        # Methods, CR/CRR/CRDs, DTs/CDTs, and Pipelines are set up by
        # calling this.
        super(LibrarianTestSetup, self).setUp()
        
        ####
        # This is the big pipeline Eric developed that was originally
        # used in copperfish/tests.py.
        
        # CRs and CRRs
        self.generic_cr = CodeResource(
            name="genericCR", description="Just a CR",
            filename="generic_script.py")
        self.generic_cr.save()
        self.generic_crRev = CodeResourceRevision(
            coderesource=self.generic_cr, revision_name="v1", revision_number=1,
            revision_desc="desc")
        with open(os.path.join(samplecode_path, "generic_script.py"), "rb") as f:
            self.generic_crRev.content_file.save("generic_script.py", File(f))
        self.generic_crRev.save()
        
        # Method family, methods, and their input/outputs
        self.mf = MethodFamily(name="method_family",description="Holds methods A/B/C"); self.mf.save()
        self.mA = Method(revision_name="mA_name",revision_desc="A_desc",family = self.mf,driver = self.generic_crRev); self.mA.save()
        self.A1_rawin = self.mA.create_input(dataset_name="A1_rawin", dataset_idx=1)
        self.A1_out = self.mA.create_output(compounddatatype=self.doublet_cdt,dataset_name="A1_out",dataset_idx=1)

        self.mB = Method(revision_name="mB_name",revision_desc="B_desc",family = self.mf,driver = self.generic_crRev); self.mB.save()
        self.B1_in = self.mB.create_input(compounddatatype=self.doublet_cdt,dataset_name="B1_in",dataset_idx=1)
        self.B2_in = self.mB.create_input(compounddatatype=self.singlet_cdt,dataset_name="B2_in",dataset_idx=2)
        self.B1_out = self.mB.create_output(compounddatatype=self.triplet_cdt,dataset_name="B1_out",dataset_idx=1,max_row=5)

        self.mC = Method(revision_name="mC_name",revision_desc="C_desc",family = self.mf,driver = self.generic_crRev); self.mC.save()
        self.C1_in = self.mC.create_input(compounddatatype=self.triplet_cdt,dataset_name="C1_in",dataset_idx=1)
        self.C2_in = self.mC.create_input(compounddatatype=self.doublet_cdt,dataset_name="C2_in",dataset_idx=2)
        self.C1_out = self.mC.create_output(compounddatatype=self.singlet_cdt,dataset_name="C1_out",dataset_idx=1)
        self.C2_rawout = self.mC.create_output(dataset_name="C2_rawout",dataset_idx=2)
        self.C3_rawout = self.mC.create_output(dataset_name="C3_rawout",dataset_idx=3)

        # Pipeline family, pipelines, and their input/outputs
        self.pf = PipelineFamily(name="Pipeline_family", description="PF desc"); self.pf.save()
        self.pD = Pipeline(family=self.pf, revision_name="pD_name",revision_desc="D"); self.pD.save()
        self.D1_in = self.pD.create_input(compounddatatype=self.doublet_cdt,dataset_name="D1_in",dataset_idx=1)
        self.D2_in = self.pD.create_input(compounddatatype=self.singlet_cdt,dataset_name="D2_in",dataset_idx=2)
        self.pE = Pipeline(family=self.pf, revision_name="pE_name",revision_desc="E"); self.pE.save()
        self.E1_in = self.pE.create_input(compounddatatype=self.triplet_cdt,dataset_name="E1_in",dataset_idx=1)
        self.E2_in = self.pE.create_input(compounddatatype=self.singlet_cdt,dataset_name="E2_in",dataset_idx=2,min_row=10)
        self.E3_rawin = self.pE.create_input(dataset_name="E3_rawin",dataset_idx=3)

        # Pipeline steps
        self.step_D1 = self.pD.steps.create(transformation=self.mB,step_num=1)
        self.step_E1 = self.pE.steps.create(transformation=self.mA,step_num=1)
        self.step_E2 = self.pE.steps.create(transformation=self.pD,step_num=2)
        self.step_E3 = self.pE.steps.create(transformation=self.mC,step_num=3)

        # Pipeline cables and outcables
        self.D01_11 = self.step_D1.cables_in.create(dest=self.B1_in,source_step=0,source=self.D1_in)
        self.D02_12 = self.step_D1.cables_in.create(dest=self.B2_in,source_step=0,source=self.D2_in)
        self.D11_21 = self.pD.outcables.create(output_name="D1_out",output_idx=1,output_cdt=self.triplet_cdt,source_step=1,source=self.B1_out)
        self.pD.create_outputs()
        self.D1_out = self.pD.outputs.get(dataset_name="D1_out")

        self.E03_11 = self.step_E1.cables_in.create(dest=self.A1_rawin,source_step=0,source=self.E3_rawin)
        self.E01_21 = self.step_E2.cables_in.create(dest=self.D1_in,source_step=0,source=self.E1_in)
        self.E02_22 = self.step_E2.cables_in.create(dest=self.D2_in,source_step=0,source=self.E2_in)
        self.E11_32 = self.step_E3.cables_in.create(dest=self.C2_in,source_step=1,source=self.A1_out)
        self.E21_31 = self.step_E3.cables_in.create(dest=self.C1_in,source_step=2,source=self.step_E2.transformation.outputs.get(dataset_name="D1_out"))
        self.E21_41 = self.pE.outcables.create(output_name="E1_out",output_idx=1,output_cdt=self.doublet_cdt,source_step=2,source=self.step_E2.transformation.outputs.get(dataset_name="D1_out"))
        self.E31_42 = self.pE.outcables.create(output_name="E2_out",output_idx=2,output_cdt=self.singlet_cdt,source_step=3,source=self.C1_out)
        self.E33_43 = self.pE.outcables.create(output_name="E3_rawout",output_idx=3,output_cdt=None,source_step=3,source=self.C3_rawout)
        self.pE.create_outputs()
        self.E1_out = self.pE.outputs.get(dataset_name="E1_out")
        self.E2_out = self.pE.outputs.get(dataset_name="E2_out")
        self.E3_rawout = self.pE.outputs.get(dataset_name="E3_rawout")

        # Custom wiring/outwiring
        self.E01_21_wire1 = self.E01_21.custom_wires.create(source_pin=self.triplet_cdt.members.all()[0],dest_pin=self.doublet_cdt.members.all()[1])
        self.E01_21_wire2 = self.E01_21.custom_wires.create(source_pin=self.triplet_cdt.members.all()[2],dest_pin=self.doublet_cdt.members.all()[0])
        self.E11_32_wire1 = self.E11_32.custom_wires.create(source_pin=self.doublet_cdt.members.all()[0],dest_pin=self.doublet_cdt.members.all()[1])
        self.E11_32_wire2 = self.E11_32.custom_wires.create(source_pin=self.doublet_cdt.members.all()[1],dest_pin=self.doublet_cdt.members.all()[0])
        self.E21_41_wire1 = self.E21_41.custom_outwires.create(source_pin=self.triplet_cdt.members.all()[1],dest_pin=self.doublet_cdt.members.all()[1])
        self.E21_41_wire2 = self.E21_41.custom_outwires.create(source_pin=self.triplet_cdt.members.all()[2],dest_pin=self.doublet_cdt.members.all()[0])
        self.pE.clean()

        # Runs for the pipelines.
        self.pD_run = self.pD.pipeline_instances.create(user=self.myUser)
        self.pD_run.save()
        self.pE_run = self.pE.pipeline_instances.create(user=self.myUser)
        self.pE_run.save()

        # November 7, 2013: use a helper function (defined in
        # librarian.models) to define our SymDSs and DSs.
            
        # Define singlet, doublet, triplet, and raw uploaded datasets
        self.triplet_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "step_0_triplet.csv"),
            self.triplet_cdt, make_dataset=True, user=self.myUser,
            name="triplet", description="lol")
        self.triplet_symDS_structure = self.triplet_symDS.structure
        self.triplet_DS = self.triplet_symDS.dataset
        
        self.doublet_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "doublet_cdt.csv"),
            self.doublet_cdt, user=self.myUser,
            name="doublet", description="lol")
        self.doublet_symDS_structure = self.doublet_symDS.structure
        self.doublet_DS = self.doublet_symDS.dataset

        self.singlet_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "singlet_cdt_large.csv"),
            self.singlet_cdt, user=self.myUser, name="singlet",
            description="lol")
        self.singlet_symDS_structure = self.singlet_symDS.structure
        self.singlet_DS = self.singlet_symDS.dataset
        
        # October 1, 2013: this is the same as the old singlet_symDS.
        self.singlet_3rows_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "step_0_singlet.csv"),
            self.singlet_cdt, user=self.myUser, name="singlet",
            description="lol")
        self.singlet_3rows_symDS_structure = self.singlet_3rows_symDS.structure
        self.singlet_3rows_DS = self.singlet_3rows_symDS.dataset

        self.raw_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "step_0_raw.fasta"),
            cdt=None, user=self.myUser, name="raw", description="lol")
        self.raw_DS = self.raw_symDS.dataset

        # Added September 30, 2013: symbolic dataset that results from E01_21.
        # November 7, 2013: created a file that this SD actually represented,
        # even though it isn't in the database.
        self.D1_in_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "doublet_remuxed_from_triplet.csv"),
            cdt=self.doublet_cdt, make_dataset=False)
        self.D1_in_symDS_structure = self.D1_in_symDS.structure
        
        self.C1_in_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "C1_in_triplet.csv"),
            self.triplet_cdt, user=self.myUser, name="C1_in_triplet",
            description="triplet 3 rows")
        self.C1_in_symDS_structure = self.C1_in_symDS.structure
        self.C1_in_DS = self.C1_in_symDS.dataset
        
        # November 7, 2013: compute the MD5 checksum from the data file,
        # which is the same as below.
        self.C2_in_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "E11_32_output.csv"),
            self.doublet_cdt, make_dataset=False)
        self.C2_in_symDS_structure = self.C2_in_symDS.structure

        # October 16: an alternative to C2_in_symDS, which has existent data.
        self.E11_32_output_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "E11_32_output.csv"),
            self.doublet_cdt, user=self.myUser,
            name="E11_32 output doublet",
            description="result of E11_32 fed by doublet_cdt.csv")
        self.E11_32_output_symDS_structure = self.E11_32_output_symDS.structure
        self.E11_32_output_DS = self.E11_32_output_symDS.dataset
        
        self.C1_out_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "step_0_singlet.csv"),
            self.singlet_cdt, user=self.myUser, name="raw",
            description="lol")
        self.C1_out_symDS_structure = self.C1_out_symDS.structure
        self.C1_out_DS = self.C1_out_symDS.dataset
        
        self.C2_out_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "step_0_raw.fasta"),
            cdt=None, user=self.myUser, name="raw", description="lol")
        self.C2_out_DS = self.C2_out_symDS.dataset

        self.C3_out_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "step_0_raw.fasta"),
            cdt=None, user=self.myUser, name="raw", description="lol")
        self.C3_out_DS = self.C3_out_symDS.dataset
        
        self.triplet_3_rows_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"),
            self.triplet_cdt, user=self.myUser, name="triplet",
            description="lol")
        self.triplet_3_rows_symDS_structure = self.triplet_3_rows_symDS.structure
        self.triplet_3_rows_DS = self.triplet_3_rows_symDS.dataset
        
        # October 9, 2013: added as the result of cable E21_41.
        self.E1_out_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "doublet_remuxed_from_t3r.csv"),
            self.doublet_cdt, user=self.myUser, name="E1_out",
            description="doublet remuxed from triplet")
        self.E1_out_symDS_structure = self.E1_out_symDS.structure
        self.E1_out_DS = self.E1_out_symDS.dataset
        
        # October 15, 2013: SymbolicDatasets that go into and come out
        # of cable E01_21 and E21_41.
        self.DNA_triplet_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "DNA_triplet.csv"),
                                                           self.DNA_triplet_cdt, user=self.myUser, name="DNA_triplet", 
                                                           description="DNA triplet data")
        self.DNA_triplet_symDS_structure = self.DNA_triplet_symDS.structure
        self.DNA_triplet_DS = self.DNA_triplet_symDS.dataset

        self.E01_21_DNA_doublet_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "E01_21_DNA_doublet.csv"),
            self.DNA_doublet_cdt,
            user=self.myUser, name="E01_21_DNA_doublet",
            description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E01_21")
        self.E01_21_DNA_doublet_symDS_structure = self.E01_21_DNA_doublet_symDS.structure
        self.E01_21_DNA_doublet_DS = self.E01_21_DNA_doublet_symDS.dataset
        
        self.E21_41_DNA_doublet_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "E21_41_DNA_doublet.csv"),
            self.DNA_doublet_cdt,
            user=self.myUser, name="E21_41_DNA_doublet",
            description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E21_41")
        self.E21_41_DNA_doublet_symDS_structure = self.E21_41_DNA_doublet_symDS.structure
        self.E21_41_DNA_doublet_DS = self.E21_41_DNA_doublet_symDS.dataset

        # Some ExecRecords, some failed, others not.
        i = 0
        for step in PipelineStep.objects.all():
            if step.is_subpipeline: continue
            run = step.pipeline.pipeline_instances.create(user=self.myUser); run.save()
            runstep = RunStep(pipelinestep=step, run=run, reused=False); runstep.save()
            execlog = ExecLog.create(runstep, runstep)
            execlog.methodoutput.return_code = i%2; execlog.methodoutput.save()
            execrecord = ExecRecord(generator=execlog); execrecord.save()
            for step_input in step.transformation.inputs.all():
                sd = SymbolicDataset.objects.filter(structure__compounddatatype=step_input.compounddatatype)[0]
                execrecord.execrecordins.create(symbolicdataset=sd, generic_input=step_input)
            runstep.execrecord = execrecord; runstep.save()
            i += 1

    def ER_from_record(self, record):
        """
        Helper function to create an ExecRecord from an Run, RunStep, or 
        RunOutputCable (record), by creating a throwaway ExecLog.
        """
        myEL = ExecLog(record=record, invoking_record=record)
        myEL.start_time = timezone.now()
        time.sleep(1)
        myEL.end_time = timezone.now()
        myEL.save()
        if record.__class__.__name__ == "RunStep":
            output = MethodOutput(execlog=myEL, return_code = 0)
            output.save()
            myEL.methodoutput = output
            myEL.save()
        myER = ExecRecord(generator=myEL)
        myER.save()
        return(myER)

    def ER_from_PSIC(self, run, PS, PSIC):
        """
        Helper function to create an ExecRecord associated to a
        PipelineStepInputCable, for a particular run and pipeline step.
        """
        myRS = run.runsteps.create(pipelinestep=PS)
        myRSIC = PSIC.psic_instances.create(runstep=myRS)
        return self.ER_from_record(myRSIC)

class SymbolicDatasetTests(LibrarianTestSetup):

    def setUp(self):
        super(SymbolicDatasetTests, self).setUp()

        rows = 10
        seqlen = 10

        self.data = ""
        for i in range(rows):
            seq = "".join([random.choice("ATCG") for j in range(seqlen)])
            self.data += "patient{},{}\n".format(i, seq)
        self.header = "header,sequence"

        self.datatype_str = Datatype.objects.get(pk=datatypes.STR_PK)
        self.datatype_dna = Datatype(name="DNA", description="sequences of ATCG")
        self.datatype_dna.clean()
        self.datatype_dna.save()
        self.datatype_dna.restricts.add(self.datatype_str)
        self.datatype_dna.complete_clean()
        self.cdt_record = CompoundDatatype()
        self.cdt_record.save()
        self.cdt_record.members.create(datatype=self.datatype_str, 
            column_name="header", column_idx=1)
        self.cdt_record.members.create(datatype=self.datatype_dna,
            column_name="sequence", column_idx=2)
        self.cdt_record.clean()

        self.data_file = tempfile.NamedTemporaryFile(delete=False)
        self.data_file.write(self.header + "\n" + self.data)
        self.file_path = self.data_file.name
        self.data_file.close()

        self.dsname = "good data"
        self.dsdesc = "some headers and sequences"
        self.sym_dataset = SymbolicDataset.create_SD(file_path = self.file_path,
                cdt = self.cdt_record, make_dataset = True, user = self.myUser,
                name = self.dsname, description = self.dsdesc)
    
    def test_is_raw(self):
        self.assertEqual(self.triplet_symDS.is_raw(), False)
        self.assertEqual(self.raw_symDS.is_raw(), True)

    def test_forgot_header(self):
        """
        Symbolic dataset creation with a CDT fails when the header is left off
        the data file.
        """
        # Write the data with no header.
        data_file = tempfile.NamedTemporaryFile(delete=False)
        data_file.write(self.data)
        data_file.close()

        # Try to create a symbolic dataset.
        self.assertRaisesRegexp(ValueError,
                                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                                          .format(data_file.name, self.cdt_record)),
                                lambda : SymbolicDataset.create_SD(file_path=data_file.name, cdt=self.cdt_record,
                                                                   user=self.myUser, name="lab data", 
                                                                   description = "patient sequences"))
        os.remove(data_file.name)

    def test_empty_file(self):
        """
        SymbolicDataset creation fails if the file passed is empty.
        """
        data_file = tempfile.NamedTemporaryFile(delete=False)
        file_path = data_file.name
        data_file.close()

        self.assertRaisesRegexp(ValueError,
                                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                                          .format(file_path, self.cdt_record)),
                                lambda : SymbolicDataset.create_SD(file_path=data_file.name, cdt=self.cdt_record,
                                                                   user=self.myUser, name="missing data", 
                                                                   description="oops!"))

    def test_too_many_columns(self):
        """
        Symbolic dataset creation fails if the data file has too many
        columns.
        """
        data_file = tempfile.NamedTemporaryFile(delete=False)
        header = "header,sequence,extra"
        data = "foo,bar,baz"
        data_file.write(header + "\n" + data)
        file_path = data_file.name
        data_file.close()

        self.assertRaisesRegexp(ValueError,
                                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                                          .format(data_file.name, self.cdt_record)),
                                lambda : SymbolicDataset.create_SD(file_path=data_file.name, cdt=self.cdt_record,
                                                                   user=self.myUser, name="bad data", 
                                                                   description="too many columns"))
        os.remove(data_file.name)

    def test_dataset_created(self):
        """
        Test coherence of the Dataset created alongsite a SymbolicDataset.
        """
        data_file = tempfile.NamedTemporaryFile(delete=False)
        data_file.write(self.header + "\n" + self.data)
        file_path = data_file.name
        data_file.close()

        dsname = "good data"
        dsdesc = "some headers and sequences"
        sym_dataset = SymbolicDataset.create_SD(file_path = data_file.name,
                cdt = self.cdt_record, make_dataset = True, user = self.myUser,
                name = dsname, description = dsdesc)
        dataset = sym_dataset.dataset
        self.assertEqual(dataset.clean(), None)
        self.assertEqual(dataset.user, self.myUser)
        self.assertEqual(dataset.name, dsname)
        self.assertEqual(dataset.description, dsdesc)
        self.assertEqual(dataset.date_created.date(), timezone.now().date())
        self.assertEqual(dataset.date_created < timezone.now(), True)
        self.assertEqual(dataset.symbolicdataset, sym_dataset)
        self.assertEqual(dataset.created_by, None)
        self.assertEqual(os.path.basename(dataset.dataset_file.path), os.path.basename(file_path))

    def test_dataset_created(self):
        """
        Test coherence of the Dataset created alongsite a SymbolicDataset.
        """
        dataset = self.sym_dataset.dataset
        self.assertEqual(dataset.clean(), None)
        self.assertEqual(dataset.user, self.myUser)
        self.assertEqual(dataset.name, self.dsname)
        self.assertEqual(dataset.description, self.dsdesc)
        self.assertEqual(dataset.date_created.date(), timezone.now().date())
        self.assertEqual(dataset.date_created < timezone.now(), True)
        self.assertEqual(dataset.symbolicdataset, self.sym_dataset)
        self.assertEqual(dataset.created_by, None)
        self.assertEqual(os.path.basename(dataset.dataset_file.path), os.path.basename(self.file_path))

    def test_symds_creation(self):
        """
        Test coherence of newly created SymbolicDataset.
        """
        self.assertEqual(self.sym_dataset.clean(), None)
        self.assertEqual(self.sym_dataset.has_data(), True)
        self.assertEqual(self.sym_dataset.is_raw(), False)

class DatasetStructureTests(LibrarianTestSetup):

    def test_num_rows(self):
        self.assertEqual(self.triplet_3_rows_symDS.num_rows(), 3)
        self.assertEqual(self.triplet_3_rows_symDS.structure.num_rows, 3)

class ExecRecordTests(LibrarianTestSetup):
    def test_delete_execrecord(self):
        """Delete an ExecRecord."""
        runstep = RunStep(pipelinestep=self.step_D1, run=self.pD_run)
        runstep.save()
        execlog = ExecLog(record=runstep, invoking_record=runstep)
        execlog.save()
        execrecord = ExecRecord(generator=execlog)
        execrecord.save()

        self.assertIsNone(ExecRecord.objects.first().delete())

    def test_ER_links_POC_so_ERI_must_link_TO_that_POC_gets_output_from(self):
        # ER links POC: ERI must link to the TO that the POC gets output from
        myROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        myER = self.ER_from_record(myROC)
        myERI_bad = myER.execrecordins.create(
            symbolicdataset = self.singlet_symDS,
            generic_input = self.C1_out)

        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO that feeds the parent ExecRecord POC",
            myERI_bad.clean)

    def test_ER_links_PSIC_so_ERI_must_link_TX_that_PSIC_is_fed_by(self):
        # ER links PSIC: ERI must link to the TO/TI that the PSIC is fed by
        myER = self.ER_from_PSIC(self.pE_run, self.step_E3, self.E11_32)
        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            myERI_bad.clean)
        
        yourER = self.ER_from_PSIC(self.pE_run, self.step_E2, self.E02_22)
        yourERI_bad = yourER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                                  generic_input=self.D2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            yourERI_bad.clean)

    def test_ER_doesnt_link_cable_so_ERI_mustnt_link_TO(self):
        # ER's EL doesn't refer to a RSIC or ROC (So, RunStep): ERI must refer to a TI
        myRS = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        myER = self.ER_from_record(myRS)
        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" must refer to a TI of the Method of the parent ExecRecord",
            myERI_bad.clean)

    def test_general_transf_returns_correct_method(self):
        """
        Test if ExecRecord.general_transf() returns the method of the PipelineStep
        it was defined with.
        """
        myRS = self.pD_run.runsteps.create(pipelinestep=self.step_D1)
        myER = self.ER_from_record(myRS)
        self.assertEqual(myER.general_transf(), self.step_D1.transformation)

    def test_ER_links_sub_pipelinemethod_so_ERI_must_link_TI_belonging_to_transformation(self):
        # ER is a method - ERI must refer to TI of that transformation
        # The transformation of step_D1 is method mB, which has input B1_in.
        myRS = self.pD_run.runsteps.create(pipelinestep=self.step_D1)
        myER = self.ER_from_record(myRS)
        myERI_good = myER.execrecordins.create(
            symbolicdataset=self.D1_in_symDS,
            generic_input=self.B1_in)

        self.assertEqual(myERI_good.clean(), None)
        
        myERI_bad = myER.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.mB.outputs.all()[0])
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" must refer to a TI of the Method of the parent ExecRecord",
            myERI_bad.clean)

    def test_ERI_dataset_must_match_rawunraw_state_of_generic_input_it_was_fed_into(self):
        # ERI has a dataset: it's raw/unraw state must match the raw/unraw state of the generic_input it was fed into
        # Method mC is step step_E3 of pipeline pE, and method mA is step step_E1 of pipeline pE.
        myRS_C = self.pE_run.runsteps.create(pipelinestep=self.step_E3)
        myER_C = self.ER_from_record(myRS_C)

        myERI_unraw_unraw = myER_C.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.C1_in)
        self.assertEqual(myERI_unraw_unraw.clean(), None)

        myERI_raw_unraw_BAD = myER_C.execrecordins.create(
            symbolicdataset=self.raw_symDS,
            generic_input=self.C2_in)
        self.assertRaisesRegexp(ValidationError,"Dataset \".*\" cannot feed source \".*\"",myERI_raw_unraw_BAD.clean)
        myERI_raw_unraw_BAD.delete()

        myRS_A = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        myER_A = self.ER_from_record(myRS_A)
        myERI_unraw_raw_BAD = myER_A.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.A1_rawin)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" cannot feed source \".*\"",
            myERI_unraw_raw_BAD.clean)
        myERI_unraw_raw_BAD.delete()
    
        myERI_raw_raw = myER_A.execrecordins.create(
            symbolicdataset=self.raw_symDS,
            generic_input=self.A1_rawin)
        self.assertEqual(myERI_raw_raw.clean(), None)

    def test_ER_links_POC_ERI_links_TO_which_constrains_input_dataset_CDT(self):
        # ERI links with a TO (For a POC leading from source TO), the input dataset CDT is constrained by the source TO
        myROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        myER = self.ER_from_record(myROC)

        # We annotate that triplet was fed from D1_out into E21_41
        myERI_wrong_CDT = myER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not a restriction of the required CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        # Right CDT but wrong number of rows (It needs < 5, we have 10)
        myERI_too_many_rows = myER.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "SymbolicDataset \".*\" has too many rows to have come from TransformationOutput \".*\"",
            myERI_too_many_rows.clean)

    def test_ER_links_pipelinestep_ERI_links_TI_which_constrains_input_CDT(self):
        # The transformation input of its PipelineStep constrains the dataset when the ER links with a method
        # Method mC is step step_E3 of pipeline pE.
        myROC = self.pE_run.runsteps.create(pipelinestep=self.step_E3)
        myER = self.ER_from_record(myROC)
        myERI_wrong_CDT = myER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.C2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not a restriction of the required CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        myERI_right_CDT = myER.execrecordins.create(
            symbolicdataset=self.doublet_symDS, generic_input=self.C2_in)
        self.assertEqual(myERI_right_CDT.clean(), None)

    def test_ER_links_with_POC_ERO_TO_must_belong_to_same_pipeline_as_ER_POC(self):
        # If the parent ER is linked with a POC, the ERO TO must belong to that pipeline

        # E31_42 belongs to pipeline E
        myROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E31_42)
        myER = self.ER_from_record(myROC)

        # This ERO has a TO that belongs to this pipeline
        myERO_good = myER.execrecordouts.create(
            symbolicdataset=self.singlet_symDS,
            generic_output=self.pE.outputs.get(dataset_name="E2_out"))
        self.assertEqual(myERO_good.clean(), None)
        myERO_good.delete()

        # This ERO has a TO that does NOT belong to this pipeline
        myERO_bad = myER.execrecordouts.create(
            symbolicdataset=self.triplet_3_rows_symDS,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut \".*\" does not belong to the same pipeline as its parent ExecRecord POC",
            myERO_bad.clean)

    def test_ER_links_with_POC_and_POC_output_name_must_match_pipeline_TO_name(self):
        # The TO must have the same name as the POC which supposedly created it

        # Make ER for POC E21_41 which defines pipeline E's TO "E1_out"
        myROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        myER = self.ER_from_record(myROC)

        # Define ERO with a TO that is part of pipeline E but with the wrong name from the POC
        myERO_bad = myER.execrecordouts.create(
            symbolicdataset=self.triplet_3_rows_symDS,
            generic_output=self.pE.outputs.get(dataset_name="E2_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut \".*\" does not represent the same output as its parent ExecRecord POC",
            myERO_bad.clean)

    def test_ER_if_dataset_is_undeleted_it_must_be_coherent_with_output(self):
        # 1) If the data is raw, the ERO output TO must also be raw
        # Method mC is step step_E3 of pipeline pE.
        myRS = self.pE_run.runsteps.create(pipelinestep=self.step_E3)
        myER = self.ER_from_record(myRS)

        myERO_rawDS_rawTO = myER.execrecordouts.create(
            symbolicdataset=self.raw_symDS, generic_output=self.C3_rawout)
        self.assertEqual(myERO_rawDS_rawTO.clean(), None)
        myERO_rawDS_rawTO.delete()

        myERO_rawDS_nonrawTO = myER.execrecordouts.create(
            symbolicdataset=self.raw_symDS, generic_output=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "SymbolicDataset \".*\" cannot have come from output \".*\"",
            myERO_rawDS_nonrawTO.clean)
        myERO_rawDS_nonrawTO.delete()

        myERO_DS_rawTO = myER.execrecordouts.create(
            symbolicdataset=self.singlet_symDS, generic_output=self.C3_rawout)
        self.assertRaisesRegexp(
            ValidationError,
            "SymbolicDataset \".*\" cannot have come from output \".*\"",
            myERO_DS_rawTO.clean)
        myERO_DS_rawTO.delete()

        myERO_DS_TO = myER.execrecordouts.create(
            symbolicdataset=self.singlet_symDS, generic_output=self.C1_out)
        self.assertEqual(myERO_DS_TO.clean(), None)
        myERO_DS_TO.delete()
        
        # 2) SymbolicDataset must have the same CDT of the producing TO
        myERO_invalid_CDT = myER.execrecordouts.create(
            symbolicdataset=self.triplet_symDS, generic_output=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "SymbolicDataset \".*\" cannot have come from output \".*\"",
            myERO_DS_rawTO.clean)
        myERO_invalid_CDT.delete()

        # Dataset must have num rows within the row constraints of the producing TO
        # Method mB is step step_D1 of pipeline pD.
        myRS = self.pD_run.runsteps.create(pipelinestep=self.step_D1)
        myER_2 = self.ER_from_record(myRS)
        myERO_too_many_rows = myER_2.execrecordouts.create(
            symbolicdataset=self.triplet_symDS, generic_output=self.B1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "SymbolicDataset \".*\" was produced by TransformationOutput \".*\" but has too many rows",
            myERO_too_many_rows.clean)
        myERO_too_many_rows.delete()

    def test_ERI_associated_Dataset_must_be_restriction_of_input_CDT(self):
        """If the ERI has a real non-raw Dataset associated to it, the Dataset must have a CDT that is a restriction of the input it feeds."""
        # Method mC is step step_E3 of pipeline pE.
        mC_RS = self.pE_run.runsteps.create(pipelinestep=self.step_E3)
        mC_ER = self.ER_from_record(mC_RS)
        mC_ER_in_1 = mC_ER.execrecordins.create(
            generic_input=self.C1_in,
            symbolicdataset=self.C1_in_symDS)

        # Good case: input SymbolicDataset has the CDT of
        # generic_input.
        self.assertEqual(mC_ER_in_1.clean(), None)

        # Good case: input SymbolicDataset has an identical CDT of
        # generic_input.
        other_CDT = CompoundDatatype()
        other_CDT.save()

        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="a", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="b", column_idx=2)
        col3 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="c", column_idx=3)

        self.C1_in_symDS.structure.compounddatatype = other_CDT
        self.assertEqual(mC_ER_in_1.clean(), None)

        # Good case: proper restriction.
        col1.datatype = self.DNA_dt
        col2.datatype = self.RNA_dt
        self.assertEqual(mC_ER_in_1.clean(), None)

        # Bad case: a type that is not a restriction at all.
        self.C1_in_symDS.structure.compounddatatype = self.doublet_cdt
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not a restriction of the required CDT",
            mC_ER_in_1.clean)
        
    def test_ERO_CDT_restrictions_Method(self):
        """ERO CDT restriction tests for the ER of a Method."""
        # Method mA is step step_E1 of pipeline pE.
        mA_RS = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        mA_ER = self.ER_from_record(mA_RS)
        mA_ERO = mA_ER.execrecordouts.create(
            generic_output=self.A1_out,
            symbolicdataset=self.doublet_symDS)

        # Good case: output SymbolicDataset has the CDT of
        # generic_output.
        self.assertEqual(mA_ERO.clean(), None)

        # Bad case: output SymbolicDataset has an identical CDT.
        other_CDT = CompoundDatatype()
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="x", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="y", column_idx=2)
        
        self.doublet_symDS.structure.compounddatatype = other_CDT
        self.doublet_symDS.structure.save()

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not the CDT of the TransformationOutput .* of the generating Method",
            mA_ERO.clean)

        # Bad case: output SymbolicDataset has another CDT altogether.
        mA_ERO.symbolicdataset=self.triplet_symDS

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not the CDT of the TransformationOutput .* of the generating Method",
            mA_ERO.clean)

    def test_ERO_CDT_restrictions_POC(self):
        """ERO CDT restriction tests for the ER of a POC."""
        ####
        outcable_ROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        outcable_ER = self.ER_from_record(outcable_ROC)
        outcable_ERO = outcable_ER.execrecordouts.create(
            generic_output=self.E1_out,
            symbolicdataset=self.E1_out_symDS)

        # Good case: output SymbolicDataset has the CDT of generic_output.
        self.assertEqual(outcable_ERO.clean(), None)

        # Good case: output SymbolicDataset has an identical CDT.
        other_CDT = CompoundDatatype()
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="x", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="y", column_idx=2)
        
        self.E1_out_symDS.structure.compounddatatype = other_CDT
        self.E1_out_symDS.structure.save()
        self.assertEqual(outcable_ERO.clean(), None)

        # Bad case: output SymbolicDataset has a CDT that is a restriction of
        # generic_output.
        col1.datatype = self.DNA_dt
        col1.save()
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            outcable_ERO.clean)

        # Bad case: output SymbolicDataset has another CDT altogether.
        outcable_ERO.symbolicdataset = self.singlet_symDS

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            outcable_ERO.clean)

    def test_ERO_CDT_restrictions_PSIC(self):
        """ERO CDT restriction tests for the ER of a PSIC."""
        ####
        cable_ER = self.ER_from_PSIC(self.pE_run, self.step_E3, self.E11_32)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.C2_in,
            symbolicdataset=self.doublet_symDS)

        # Good case: output Dataset has the CDT of generic_output.
        self.assertEqual(cable_ERO.clean(), None)

        # Good case: output Dataset has an identical CDT.
        other_CDT = CompoundDatatype()
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="x", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="y", column_idx=2)
        
        self.doublet_symDS.structure.compounddatatype = other_CDT
        self.doublet_symDS.structure.save()
        self.assertEqual(cable_ERO.clean(), None)

        # Good case: output Dataset has a CDT that is a restriction of
        # generic_output.
        col1.datatype = self.DNA_dt
        col1.save()
        self.assertEqual(cable_ERO.clean(), None)

        # Bad case: output Dataset has another CDT altogether.
        cable_ERO.symbolicdataset = self.singlet_symDS

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not a restriction of the CDT of the fed TransformationInput .*",
            cable_ERO.clean)

    def test_ER_trivial_PSICs_have_same_SD_on_both_sides(self):
        """ERs representing trivial PSICs must have the same SymbolicDataset on both sides."""
        cable_ER = self.ER_from_PSIC(self.pE_run, self.step_E2, self.E02_22)
        cable_ERI = cable_ER.execrecordins.create(
            generic_input=self.E2_in,
            symbolicdataset = self.singlet_symDS)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.D2_in,
            symbolicdataset = self.singlet_symDS)

        # Good case: SDs on either side of this trivial cable match.
        self.assertEqual(cable_ER.clean(), None)

        # Bad case: SDs don't match.
        cable_ERO.symbolicdataset = self.C1_out_symDS
        cable_ERO.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord "{}" represents a trivial cable but its input and output do not '
                                          'match'.format(cable_ER)),
                                cable_ER.clean)

    def test_ER_trivial_POCs_have_same_SD_on_both_sides(self):
        """ERs representing trivial POCs must have the same SymbolicDataset on both sides."""
        # E31_42 belongs to pipeline E
        outcable_ROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E31_42)
        outcable_ER = self.ER_from_record(outcable_ROC)
        outcable_ERI = outcable_ER.execrecordins.create(
            generic_input=self.C1_out,
            symbolicdataset = self.C1_out_symDS)
        outcable_ERO = outcable_ER.execrecordouts.create(
            generic_output=self.E2_out,
            symbolicdataset = self.C1_out_symDS)

        # Good case: SDs on either side of this trivial POC match.
        self.assertEqual(outcable_ER.clean(), None)

        # Bad case: SDs don't match.
        outcable_ERO.symbolicdataset = self.singlet_symDS
        outcable_ERO.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord "{}" represents a trivial cable but its input and output do not '
                                          'match'.format(outcable_ER)),
                                outcable_ER.clean)
        

    def test_ER_Datasets_passing_through_non_trivial_POCs(self):
        """Test that the Datatypes of Datasets passing through POCs are properly preserved."""
        outcable_ROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        outcable_ER = self.ER_from_record(outcable_ROC)
        outcable_ERI = outcable_ER.execrecordins.create(generic_input=self.D1_out, symbolicdataset=self.C1_in_symDS)
        outcable_ERO = outcable_ER.execrecordouts.create(generic_output=self.E1_out, symbolicdataset=self.E1_out_symDS)

        # Good case: the Datatypes are exactly those needed.
        self.assertEqual(outcable_ER.clean(), None)

        # Good case: same as above, but with CDTs that are restrictions.
        D1_out_structure = self.D1_out.structure.all()[0]
        E1_out_structure = self.E1_out.structure.all()[0]
        D1_out_structure.compounddatatype = self.DNA_triplet_cdt
        D1_out_structure.save()
        E1_out_structure.compounddatatype = self.DNA_doublet_cdt
        E1_out_structure.save()
        
        outcable_ERI.symbolicdataset = self.DNA_triplet_symDS
        outcable_ERI.save()
        outcable_ERO.symbolicdataset = self.E21_41_DNA_doublet_symDS
        outcable_ERO.save()
        self.assertIsNone(outcable_ER.clean())

        # Bad case: cable does some casting.
        output_col1 = (self.E21_41_DNA_doublet_symDS.structure.compounddatatype.members.get(column_idx=1))
        output_col1.datatype = self.string_dt
        output_col1.save()

        source_datatype = outcable_ERI.symbolicdataset.structure.compounddatatype.members.get(column_idx=1).datatype
        dest_datatype = output_col1.datatype
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord "{}" represents a cable, but the Datatype of its destination '
                                          'column, "{}", does not match the Datatype of its source column, "{}"'
                                          .format(outcable_ER, dest_datatype, source_datatype)),
                                outcable_ER.clean)
        
    def test_ER_Datasets_passing_through_non_trivial_PSICs(self):
        """Test that the Datatypes of Datasets passing through PSICs are properly preserved."""
        cable_ER = self.ER_from_PSIC(self.pE_run, self.step_E2, self.E01_21)
        cable_ERI = cable_ER.execrecordins.create(
            generic_input=self.E1_in,
            symbolicdataset=self.triplet_symDS)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.D1_in,
            symbolicdataset=self.D1_in_symDS)

        # Good case: the Datatypes are exactly those needed.
        self.assertEqual(cable_ER.clean(), None)

        # Good case: same as above, but with CDTs that are restrictions.
        in_structure = self.E1_in.structure.all()[0]
        out_structure = self.D1_in.structure.all()[0]
        in_structure.compounddatatype = self.DNA_triplet_cdt
        in_structure.save()
        out_structure.compounddatatype = self.DNA_doublet_cdt
        out_structure.save()
        
        cable_ERI.symbolicdataset = self.DNA_triplet_symDS
        cable_ERI.save()
        cable_ERO.symbolicdataset = self.E01_21_DNA_doublet_symDS
        cable_ERO.save()
        self.assertEqual(cable_ER.clean(), None)

        # Bad case: cable does some casting.
        output_col1 = (self.E01_21_DNA_doublet_symDS.structure.
                       compounddatatype.members.get(column_idx=1))
        output_col1.datatype = self.string_dt
        output_col1.save()
        source_datatype = cable_ERI.symbolicdataset.structure.compounddatatype.members.get(column_idx=1).datatype
        dest_datatype = output_col1.datatype

        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord "{}" represents a cable, but the Datatype of its destination '
                                          'column, "{}", does not match the Datatype of its source column, "{}"'
                                          .format(cable_ER, dest_datatype, source_datatype)),
            cable_ER.clean)

    def test_execrecord_new_never_failed(self):
        """An ExecRecord with one good RunStep has never failed."""
        pipeline = Pipeline.objects.first()
        user = User.objects.first()
        self.assertIsNotNone(pipeline)
        for run in pipeline.pipeline_instances.all():
            run.delete()
        run = archive.models.Run(pipeline=pipeline, user=user); run.save()
        runstep = run.runsteps.create(pipelinestep=pipeline.steps.first(), run=run)
        execlog = archive.models.ExecLog(record=runstep, invoking_record=runstep); execlog.save()
        execrecord = librarian.models.ExecRecord(generator=execlog); execrecord.save()
        runstep.execrecord = execrecord
        runstep.save()

        self.assertEqual(execrecord.archive_runstep_related.count(), 1)
        self.assertFalse(execrecord.has_ever_failed())

    def test_execrecord_multiple_runsteps_never_failed(self):
        """An ExecRecord with >1 good RunStep has never failed."""
        pipeline = Pipeline.objects.first()
        user = User.objects.first()
        self.assertIsNotNone(pipeline)
        self.assertIsNotNone(user)

        for run in pipeline.pipeline_instances.all():
            run.delete()

        for i in range(2):
            run = archive.models.Run(pipeline=pipeline, user=user); run.save()
            runstep = run.runsteps.create(pipelinestep=pipeline.steps.first(), run=run)
            execlog = archive.models.ExecLog(record=runstep, invoking_record=runstep); execlog.save()
            if i == 0:
                execrecord = librarian.models.ExecRecord(generator=execlog); execrecord.save()
            runstep.execrecord = execrecord
            runstep.save()

        self.assertEqual(execrecord.archive_runstep_related.count(), 2)
        self.assertFalse(execrecord.has_ever_failed())

    def test_execrecord_one_failed_runstep(self):
        """An ExecRecord with one bad RunStep has failed."""
        pipeline = Pipeline.objects.first()
        user = User.objects.first()
        self.assertIsNotNone(pipeline)
        for run in pipeline.pipeline_instances.all():
            run.delete()
        run = archive.models.Run(pipeline=pipeline, user=user); run.save()
        runstep = run.runsteps.create(pipelinestep=pipeline.steps.first(), run=run)
        execlog = archive.models.ExecLog(record=runstep, invoking_record=runstep); execlog.save()
        archive.models.MethodOutput(execlog=execlog, return_code=1).save()
        execrecord = librarian.models.ExecRecord(generator=execlog); execrecord.save()
        runstep.execrecord = execrecord
        runstep.save()

        self.assertFalse(runstep.successful_execution())
        self.assertEqual(execrecord.archive_runstep_related.count(), 1)
        self.assertTrue(execrecord.has_ever_failed())

    def test_execrecord_multiple_good_one_failed_runstep(self):
        """An ExecRecord with one bad RunStep, and some good ones, has failed."""
        pipeline = Pipeline.objects.first()
        user = User.objects.first()
        self.assertIsNotNone(pipeline)
        for run in pipeline.pipeline_instances.all():
            run.delete()

        for i in range(2):
            run = archive.models.Run(pipeline=pipeline, user=user); run.save()
            runstep = run.runsteps.create(pipelinestep=pipeline.steps.first(), run=run)
            execlog = archive.models.ExecLog(record=runstep, invoking_record=runstep); execlog.save()
            if i == 1:
                archive.models.MethodOutput(execlog=execlog, return_code=1).save()
            else:
                execrecord = librarian.models.ExecRecord(generator=execlog); execrecord.save()
            runstep.execrecord = execrecord
            runstep.save()

        self.assertEqual(execrecord.archive_runstep_related.count(), 2)
        self.assertEqual(execrecord.archive_runstep_related.first().successful_execution(), True)
        self.assertEqual(execrecord.archive_runstep_related.last().successful_execution(), False)
        self.assertTrue(execrecord.has_ever_failed())

class FindCompatibleERTests(LibrarianTestSetup):

    def test_find_compatible_ER_never_failed(self):
        """Should be able to find a compatible ExecRecord which never failed."""
        execrecord = None
        for e in librarian.models.ExecRecord.objects.all():
            if not e.has_ever_failed():
                execrecord = e
                break
        self.assertIsNotNone(execrecord)
        input_SDs = [eri.symbolicdataset for eri in execrecord.execrecordins.all()]
        runstep = execrecord.archive_runstep_related.first()
        runstep.reused = False
        runstep.save()
        method = runstep.pipelinestep.transformation
        self.assertFalse(execrecord.has_ever_failed())
        self.assertEqual(method.find_compatible_ER(input_SDs), execrecord)

    def test_find_compatible_ER_failed(self):
        """Should not find a compatible ExecRecord which failed."""
        execrecord = None
        for e in librarian.models.ExecRecord.objects.all():
            if e.has_ever_failed():
                execrecord = e
                break
        self.assertIsNotNone(execrecord)
        input_SDs = [eri.symbolicdataset for eri in execrecord.execrecordins.all()]
        runstep = execrecord.archive_runstep_related.first()
        runstep.reused = False
        runstep.save()
        method = runstep.pipelinestep.transformation
        self.assertTrue(execrecord.has_ever_failed())
        self.assertIsNone(method.find_compatible_ER(input_SDs))
