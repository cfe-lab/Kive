"""
Shipyard models pertaining to the librarian app.
"""

import os
import random
import re
import tempfile
import time

from django.core.exceptions import ValidationError
from django.core.files import File
from django.utils import timezone
from django.contrib.auth.models import User
from django.test import TestCase

from archive.models import ExecLog, MethodOutput, Run, RunStep
from constants import datatypes
from librarian.models import SymbolicDataset, ExecRecord
from metadata.models import Datatype, CompoundDatatype
import metadata.tests
from method.models import CodeResource, CodeResourceRevision, Method, \
    MethodFamily
from method.tests import samplecode_path
from pipeline.models import Pipeline, PipelineFamily, PipelineStep
import logging


def create_librarian_test_environment(case):
    """
    Set up default state for Librarian unit testing.

    This sets up the environment as in the Metadata tests, and then augments with
    Methods, CR/CRR/CRDs, and DT/CDTs.  Note that these are *not* the same
    as those set up in the Method testing.
    """
    # This sets up some DTs and CDTs.
    metadata.tests.create_metadata_test_environment(case)

    ####
    # This is the big pipeline Eric developed that was originally
    # used in copperfish/tests.py.

    # CRs and CRRs
    case.generic_cr = CodeResource(
        name="genericCR", description="Just a CR",
        filename="generic_script.py")
    case.generic_cr.save()
    case.generic_crRev = CodeResourceRevision(
        coderesource=case.generic_cr, revision_name="v1", revision_desc="desc")
    with open(os.path.join(samplecode_path, "generic_script.py"), "rb") as f:
        case.generic_crRev.content_file.save("generic_script.py", File(f))
    case.generic_crRev.save()

    # Method family, methods, and their input/outputs
    case.mf = MethodFamily(name="method_family",description="Holds methods A/B/C"); case.mf.save()
    case.mA = Method(revision_name="mA_name", revision_desc="A_desc", family = case.mf, driver =
            case.generic_crRev)
    case.mA.save()
    case.A1_rawin = case.mA.create_input(dataset_name="A1_rawin", dataset_idx=1)
    case.A1_out = case.mA.create_output(compounddatatype=case.doublet_cdt,dataset_name="A1_out",dataset_idx=1)

    case.mB = Method(revision_name="mB_name", revision_desc="B_desc", family=case.mf, driver=case.generic_crRev)
    case.mB.save()
    case.B1_in = case.mB.create_input(compounddatatype=case.doublet_cdt,dataset_name="B1_in",dataset_idx=1)
    case.B2_in = case.mB.create_input(compounddatatype=case.singlet_cdt,dataset_name="B2_in",dataset_idx=2)
    case.B1_out = case.mB.create_output(compounddatatype=case.triplet_cdt,dataset_name="B1_out",dataset_idx=1,max_row=5)

    case.mC = Method(revision_name="mC_name", revision_desc="C_desc", family=case.mf, driver=case.generic_crRev)
    case.mC.save()
    case.C1_in = case.mC.create_input(compounddatatype=case.triplet_cdt,dataset_name="C1_in",dataset_idx=1)
    case.C2_in = case.mC.create_input(compounddatatype=case.doublet_cdt,dataset_name="C2_in",dataset_idx=2)
    case.C1_out = case.mC.create_output(compounddatatype=case.singlet_cdt,dataset_name="C1_out",dataset_idx=1)
    case.C2_rawout = case.mC.create_output(dataset_name="C2_rawout",dataset_idx=2)
    case.C3_rawout = case.mC.create_output(dataset_name="C3_rawout",dataset_idx=3)

    # Pipeline family, pipelines, and their input/outputs
    case.pf = PipelineFamily(name="Pipeline_family", description="PF desc"); case.pf.save()
    case.pD = Pipeline(family=case.pf, revision_name="pD_name", revision_desc="D")
    case.pD.save()
    case.D1_in = case.pD.create_input(compounddatatype=case.doublet_cdt,dataset_name="D1_in",dataset_idx=1)
    case.D2_in = case.pD.create_input(compounddatatype=case.singlet_cdt,dataset_name="D2_in",dataset_idx=2)
    case.pE = Pipeline(family=case.pf, revision_name="pE_name", revision_desc="E")
    case.pE.save()
    case.E1_in = case.pE.create_input(compounddatatype=case.triplet_cdt,dataset_name="E1_in",dataset_idx=1)
    case.E2_in = case.pE.create_input(compounddatatype=case.singlet_cdt,dataset_name="E2_in",dataset_idx=2,min_row=10)
    case.E3_rawin = case.pE.create_input(dataset_name="E3_rawin",dataset_idx=3)

    # Pipeline steps
    case.step_D1 = case.pD.steps.create(transformation=case.mB,step_num=1)
    case.step_E1 = case.pE.steps.create(transformation=case.mA,step_num=1)
    case.step_E2 = case.pE.steps.create(transformation=case.pD,step_num=2)
    case.step_E3 = case.pE.steps.create(transformation=case.mC,step_num=3)

    # Pipeline cables and outcables
    case.D01_11 = case.step_D1.cables_in.create(dest=case.B1_in,source_step=0,source=case.D1_in)
    case.D02_12 = case.step_D1.cables_in.create(dest=case.B2_in,source_step=0,source=case.D2_in)
    case.D11_21 = case.pD.outcables.create(output_name="D1_out",output_idx=1,output_cdt=case.triplet_cdt,source_step=1,source=case.B1_out)
    case.pD.create_outputs()
    case.D1_out = case.pD.outputs.get(dataset_name="D1_out")

    case.E03_11 = case.step_E1.cables_in.create(dest=case.A1_rawin,source_step=0,source=case.E3_rawin)
    case.E01_21 = case.step_E2.cables_in.create(dest=case.D1_in,source_step=0,source=case.E1_in)
    case.E02_22 = case.step_E2.cables_in.create(dest=case.D2_in,source_step=0,source=case.E2_in)
    case.E11_32 = case.step_E3.cables_in.create(dest=case.C2_in,source_step=1,source=case.A1_out)
    case.E21_31 = case.step_E3.cables_in.create(dest=case.C1_in,source_step=2,source=case.step_E2.transformation.outputs.get(dataset_name="D1_out"))
    case.E21_41 = case.pE.outcables.create(output_name="E1_out",output_idx=1,output_cdt=case.doublet_cdt,source_step=2,source=case.step_E2.transformation.outputs.get(dataset_name="D1_out"))
    case.E31_42 = case.pE.outcables.create(output_name="E2_out",output_idx=2,output_cdt=case.singlet_cdt,source_step=3,source=case.C1_out)
    case.E33_43 = case.pE.outcables.create(output_name="E3_rawout",output_idx=3,output_cdt=None,source_step=3,source=case.C3_rawout)
    case.pE.create_outputs()
    case.E1_out = case.pE.outputs.get(dataset_name="E1_out")
    case.E2_out = case.pE.outputs.get(dataset_name="E2_out")
    case.E3_rawout = case.pE.outputs.get(dataset_name="E3_rawout")

    # Custom wiring/outwiring
    case.E01_21_wire1 = case.E01_21.custom_wires.create(
        source_pin=case.triplet_cdt.members.get(column_idx=1),
        dest_pin=case.doublet_cdt.members.get(column_idx=2)
    )
    case.E01_21_wire2 = case.E01_21.custom_wires.create(
        source_pin=case.triplet_cdt.members.get(column_idx=3),
        dest_pin=case.doublet_cdt.members.get(column_idx=1)
    )
    case.E11_32_wire1 = case.E11_32.custom_wires.create(
        source_pin=case.doublet_cdt.members.get(column_idx=1),
        dest_pin=case.doublet_cdt.members.get(column_idx=2)
    )
    case.E11_32_wire2 = case.E11_32.custom_wires.create(
        source_pin=case.doublet_cdt.members.get(column_idx=2),
        dest_pin=case.doublet_cdt.members.get(column_idx=1)
    )
    case.E21_41_wire1 = case.E21_41.custom_wires.create(
        source_pin=case.triplet_cdt.members.get(column_idx=2),
        dest_pin=case.doublet_cdt.members.get(column_idx=2)
    )
    case.E21_41_wire2 = case.E21_41.custom_wires.create(
        source_pin=case.triplet_cdt.members.get(column_idx=3),
        dest_pin=case.doublet_cdt.members.get(column_idx=1)
    )
    case.pE.clean()

    # Runs for the pipelines.
    case.pD_run = case.pD.pipeline_instances.create(user=case.myUser)
    case.pD_run.save()
    case.pE_run = case.pE.pipeline_instances.create(user=case.myUser)
    case.pE_run.save()

    # November 7, 2013: use a helper function (defined in
    # librarian.models) to define our SymDSs and DSs.

    # Define singlet, doublet, triplet, and raw uploaded datasets
    case.triplet_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "step_0_triplet.csv"),
        case.triplet_cdt, make_dataset=True, user=case.myUser,
        name="triplet", description="lol")
    case.triplet_symDS_structure = case.triplet_symDS.structure
    case.triplet_DS = case.triplet_symDS.dataset

    case.doublet_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "doublet_cdt.csv"),
        case.doublet_cdt, user=case.myUser,
        name="doublet", description="lol")
    case.doublet_symDS_structure = case.doublet_symDS.structure
    case.doublet_DS = case.doublet_symDS.dataset

    case.singlet_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "singlet_cdt_large.csv"),
        case.singlet_cdt, user=case.myUser, name="singlet",
        description="lol")
    case.singlet_symDS_structure = case.singlet_symDS.structure
    case.singlet_DS = case.singlet_symDS.dataset

    # October 1, 2013: this is the same as the old singlet_symDS.
    case.singlet_3rows_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "step_0_singlet.csv"),
        case.singlet_cdt, user=case.myUser, name="singlet",
        description="lol")
    case.singlet_3rows_symDS_structure = case.singlet_3rows_symDS.structure
    case.singlet_3rows_DS = case.singlet_3rows_symDS.dataset

    case.raw_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "step_0_raw.fasta"),
        cdt=None, user=case.myUser, name="raw", description="lol")
    case.raw_DS = case.raw_symDS.dataset

    # Added September 30, 2013: symbolic dataset that results from E01_21.
    # November 7, 2013: created a file that this SD actually represented,
    # even though it isn't in the database.
    case.D1_in_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "doublet_remuxed_from_triplet.csv"),
        cdt=case.doublet_cdt, make_dataset=False)
    case.D1_in_symDS_structure = case.D1_in_symDS.structure

    case.C1_in_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "C1_in_triplet.csv"),
        case.triplet_cdt, user=case.myUser, name="C1_in_triplet",
        description="triplet 3 rows")
    case.C1_in_symDS_structure = case.C1_in_symDS.structure
    case.C1_in_DS = case.C1_in_symDS.dataset

    # November 7, 2013: compute the MD5 checksum from the data file,
    # which is the same as below.
    case.C2_in_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "E11_32_output.csv"),
        case.doublet_cdt, make_dataset=False)
    case.C2_in_symDS_structure = case.C2_in_symDS.structure

    # October 16: an alternative to C2_in_symDS, which has existent data.
    case.E11_32_output_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "E11_32_output.csv"),
        case.doublet_cdt, user=case.myUser,
        name="E11_32 output doublet",
        description="result of E11_32 fed by doublet_cdt.csv")
    case.E11_32_output_symDS_structure = case.E11_32_output_symDS.structure
    case.E11_32_output_DS = case.E11_32_output_symDS.dataset

    case.C1_out_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "step_0_singlet.csv"),
        case.singlet_cdt, user=case.myUser, name="raw",
        description="lol")
    case.C1_out_symDS_structure = case.C1_out_symDS.structure
    case.C1_out_DS = case.C1_out_symDS.dataset

    case.C2_out_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "step_0_raw.fasta"),
        cdt=None, user=case.myUser, name="raw", description="lol")
    case.C2_out_DS = case.C2_out_symDS.dataset

    case.C3_out_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "step_0_raw.fasta"),
        cdt=None, user=case.myUser, name="raw", description="lol")
    case.C3_out_DS = case.C3_out_symDS.dataset

    case.triplet_3_rows_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"),
        case.triplet_cdt, user=case.myUser, name="triplet",
        description="lol")
    case.triplet_3_rows_symDS_structure = case.triplet_3_rows_symDS.structure
    case.triplet_3_rows_DS = case.triplet_3_rows_symDS.dataset

    # October 9, 2013: added as the result of cable E21_41.
    case.E1_out_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "doublet_remuxed_from_t3r.csv"),
        case.doublet_cdt, user=case.myUser, name="E1_out",
        description="doublet remuxed from triplet")
    case.E1_out_symDS_structure = case.E1_out_symDS.structure
    case.E1_out_DS = case.E1_out_symDS.dataset

    # October 15, 2013: SymbolicDatasets that go into and come out
    # of cable E01_21 and E21_41.
    case.DNA_triplet_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "DNA_triplet.csv"),
                                                       case.DNA_triplet_cdt, user=case.myUser, name="DNA_triplet",
                                                       description="DNA triplet data")
    case.DNA_triplet_symDS_structure = case.DNA_triplet_symDS.structure
    case.DNA_triplet_DS = case.DNA_triplet_symDS.dataset

    case.E01_21_DNA_doublet_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "E01_21_DNA_doublet.csv"),
        case.DNA_doublet_cdt,
        user=case.myUser, name="E01_21_DNA_doublet",
        description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E01_21")
    case.E01_21_DNA_doublet_symDS_structure = case.E01_21_DNA_doublet_symDS.structure
    case.E01_21_DNA_doublet_DS = case.E01_21_DNA_doublet_symDS.dataset

    case.E21_41_DNA_doublet_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "E21_41_DNA_doublet.csv"),
        case.DNA_doublet_cdt,
        user=case.myUser, name="E21_41_DNA_doublet",
        description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E21_41")
    case.E21_41_DNA_doublet_symDS_structure = case.E21_41_DNA_doublet_symDS.structure
    case.E21_41_DNA_doublet_DS = case.E21_41_DNA_doublet_symDS.dataset

    # Some ExecRecords, some failed, others not.
    i = 0
    for step in PipelineStep.objects.all():
        if step.is_subpipeline: continue
        run = step.pipeline.pipeline_instances.create(user=case.myUser); run.save()
        runstep = RunStep(pipelinestep=step, run=run, reused=False); runstep.save()
        execlog = ExecLog.create(runstep, runstep)
        execlog.methodoutput.return_code = i%2; execlog.methodoutput.save()
        execrecord = ExecRecord(generator=execlog); execrecord.save()
        for step_input in step.transformation.inputs.all():
            sd = SymbolicDataset.objects.filter(structure__compounddatatype=step_input.compounddatatype)[0]
            execrecord.execrecordins.create(symbolicdataset=sd, generic_input=step_input)
        runstep.execrecord = execrecord; runstep.save()
        i += 1


def ER_from_record(record):
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


def ER_from_PSIC(run, PS, PSIC):
    """
    Helper function to create an ExecRecord associated to a
    PipelineStepInputCable, for a particular run and pipeline step.
    """
    myRS = run.runsteps.create(pipelinestep=PS)
    myRSIC = PSIC.psic_instances.create(runstep=myRS)
    return ER_from_record(myRSIC)


class LibrarianTestCase(TestCase):
    """
    Set up a database state for unit testing the librarian app.

    This extends PipelineTestCase, which itself extended
    other stuff (follow the chain).
    """
    def setUp(self):
        """Set up default database state for librarian unit testing."""
        create_librarian_test_environment(self)

    def tearDown(self):
        metadata.tests.clean_up_all_files()


class SymbolicDatasetTests(LibrarianTestCase):

    def setUp(self):
        super(SymbolicDatasetTests, self).setUp()

        # Turn off logging, so the test output isn't polluted.
        logging.getLogger('SymbolicDataset').setLevel(logging.CRITICAL)
        logging.getLogger('CompoundDatatype').setLevel(logging.CRITICAL)
        
        rows = 10
        seqlen = 10

        self.data = ""
        for i in range(rows):
            seq = "".join([random.choice("ATCG") for _ in range(seqlen)])
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

    def tearDown(self):
        super(SymbolicDatasetTests, self).tearDown()
        os.remove(self.file_path)
    
    def test_is_raw(self):
        self.assertEqual(self.triplet_symDS.is_raw(), False)
        self.assertEqual(self.raw_symDS.is_raw(), True)

    def test_forgot_header(self):
        """
        Symbolic dataset creation with a CDT fails when the header is left off
        the data file.
        """
        # Write the data with no header.
        data_file = tempfile.NamedTemporaryFile()
        data_file.write(self.data)

        # Try to create a symbolic dataset.
        self.assertRaisesRegexp(ValueError,
                                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                                          .format(data_file.name, self.cdt_record)),
                                lambda : SymbolicDataset.create_SD(file_path=data_file.name, cdt=self.cdt_record,
                                                                   user=self.myUser, name="lab data", 
                                                                   description = "patient sequences"))
        data_file.close()

    def test_empty_file(self):
        """
        SymbolicDataset creation fails if the file passed is empty.
        """
        data_file = tempfile.NamedTemporaryFile()
        file_path = data_file.name

        self.assertRaisesRegexp(ValueError,
                                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                                          .format(file_path, self.cdt_record)),
                                lambda : SymbolicDataset.create_SD(file_path=data_file.name, cdt=self.cdt_record,
                                                                   user=self.myUser, name="missing data", 
                                                                   description="oops!"))
        data_file.close()

    def test_too_many_columns(self):
        """
        Symbolic dataset creation fails if the data file has too many
        columns.
        """
        with tempfile.NamedTemporaryFile() as data_file:
            data_file.write("""\
header,sequence,extra
foo,bar,baz
""")
            data_file.flush()
            file_path = data_file.name

            self.assertRaisesRegexp(
                ValueError,
                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                          .format(file_path, self.cdt_record)),
                lambda : SymbolicDataset.create_SD(file_path=file_path,
                                                   cdt=self.cdt_record,
                                                   user=self.myUser, name="bad data", 
                                                   description="too many columns"))

    def test_right_columns(self):
        """
        Symbolic dataset creation fails if the data file has too many
        columns.
        """
        with tempfile.NamedTemporaryFile() as data_file:
            data_file.write("""\
header,sequence
foo,bar
""")
            data_file.flush()
            file_path = data_file.name

            SymbolicDataset.create_SD(file_path=file_path,
                                      cdt=self.cdt_record,
                                      user=self.myUser,
                                      name="good data", 
                                      description="right columns")

    def test_invalid_integer_field(self):
        """
        Symbolic dataset creation fails if the data file has too many
        columns.
        """
        compound_datatype = CompoundDatatype()
        compound_datatype.save()
        compound_datatype.members.create(datatype=self.STR, 
                                         column_name="name",
                                         column_idx=1)
        compound_datatype.members.create(datatype=self.INT,
                                         column_name="count",
                                         column_idx=2)
        compound_datatype.clean()

        with tempfile.NamedTemporaryFile() as data_file:
            data_file.write("""\
name,count
Bob,tw3nty
""")
            data_file.flush()
            file_path = data_file.name

            self.assertRaisesRegexp(
                ValueError,
                re.escape('The entry at row 1, column 2 of file "{}" did not pass the constraints of Datatype "integer"'
                          .format(file_path)),
                lambda : SymbolicDataset.create_SD(file_path=file_path,
                                                   cdt=compound_datatype,
                                                   user=self.myUser,
                                                   name="bad data",
                                                   description="bad integer field"))

    def test_dataset_created(self):
        """
        Test coherence of the Dataset created alongsite a SymbolicDataset.
        """
        data_file = tempfile.NamedTemporaryFile()
        data_file.write(self.header + "\n" + self.data)
        data_file.seek(0)
        file_path = data_file.name

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
        data_file.close()

    def test_dataset_bulk_created(self):
        """
        Test coherence of the Dataset created alongsite a SymbolicDataset.
        """
        bulk_dataset_csv = tempfile.NamedTemporaryFile(suffix="csv")
        bulk_dataset_csv.write("Name,Description,File")
        dsname = "tempdataset"
        dsdesc = "some headers and sequences"
        file_paths = []
        data_files = []
        for i in range(2):
            data_files.append(tempfile.NamedTemporaryFile())
            data_files[-1].write(self.header + "\n" + self.data)
            file_path = data_files[-1].name
            file_paths.extend([file_path])
            bulk_dataset_csv.write("\n" + dsname+str(i) + "," + dsdesc+str(i) + "," + file_path)

        sym_datasets = SymbolicDataset.create_SD_bulk(csv_file_path=bulk_dataset_csv.name, check=True,
                                                      cdt=self.cdt_record, make_dataset=True, user=self.myUser)
        for f in data_files:
            f.close()
        bulk_dataset_csv.close()
        for i, sym_dataset in enumerate(sym_datasets):

            dataset = sym_dataset.dataset
            self.assertEqual(dataset.clean(), None)
            self.assertEqual(dataset.user, self.myUser)
            self.assertEqual(dataset.name, dsname+str(i))
            self.assertEqual(dataset.description, dsdesc+str(i))
            self.assertEqual(dataset.date_created.date(), timezone.now().date())
            self.assertEqual(dataset.date_created < timezone.now(), True)
            self.assertEqual(dataset.symbolicdataset, sym_dataset)
            self.assertEqual(dataset.created_by, None)
            self.assertEqual(os.path.basename(dataset.dataset_file.path), os.path.basename(file_paths[i]))

    def test_dataset_created2(self):
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


class DatasetStructureTests(LibrarianTestCase):

    def test_num_rows(self):
        self.assertEqual(self.triplet_3_rows_symDS.num_rows(), 3)
        self.assertEqual(self.triplet_3_rows_symDS.structure.num_rows, 3)


class ExecRecordTests(LibrarianTestCase):
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
        myER = ER_from_record(myROC)
        myERI_bad = myER.execrecordins.create(
            symbolicdataset = self.singlet_symDS,
            generic_input = self.C1_out)

        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO that feeds the parent ExecRecord POC",
            myERI_bad.clean)

    def test_ER_links_PSIC_so_ERI_must_link_TX_that_PSIC_is_fed_by(self):
        # ER links PSIC: ERI must link to the TO/TI that the PSIC is fed by
        myER = ER_from_PSIC(self.pE_run, self.step_E3, self.E11_32)
        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            myERI_bad.clean)
        
        yourER = ER_from_PSIC(self.pE_run, self.step_E2, self.E02_22)
        yourERI_bad = yourER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                                  generic_input=self.D2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            yourERI_bad.clean)

    def test_ER_doesnt_link_cable_so_ERI_mustnt_link_TO(self):
        # ER's EL doesn't refer to a RSIC or ROC (So, RunStep): ERI must refer to a TI
        myRS = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        myER = ER_from_record(myRS)
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
        myER = ER_from_record(myRS)
        self.assertEqual(myER.general_transf(), self.step_D1.transformation)

    def test_ER_links_sub_pipelinemethod_so_ERI_must_link_TI_belonging_to_transformation(self):
        # ER is a method - ERI must refer to TI of that transformation
        # The transformation of step_D1 is method mB, which has input B1_in.
        myRS = self.pD_run.runsteps.create(pipelinestep=self.step_D1)
        myER = ER_from_record(myRS)
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
        myER_C = ER_from_record(myRS_C)

        myERI_unraw_unraw = myER_C.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.C1_in)
        self.assertEqual(myERI_unraw_unraw.clean(), None)

        myERI_raw_unraw_BAD = myER_C.execrecordins.create(
            symbolicdataset=self.raw_symDS,
            generic_input=self.C2_in)
        self.assertRaisesRegexp(
            ValidationError,
            r'SymbolicDataset ".*" \(raw\) cannot feed source ".*" \(non-raw\)',
            myERI_raw_unraw_BAD.clean)
        myERI_raw_unraw_BAD.delete()

        myRS_A = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        myER_A = ER_from_record(myRS_A)
        myERI_unraw_raw_BAD = myER_A.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.A1_rawin)
        self.assertRaisesRegexp(
            ValidationError,
            r'SymbolicDataset ".*" \(non-raw\) cannot feed source ".*" \(raw\)',
            myERI_unraw_raw_BAD.clean)
        myERI_unraw_raw_BAD.delete()
    
        myERI_raw_raw = myER_A.execrecordins.create(
            symbolicdataset=self.raw_symDS,
            generic_input=self.A1_rawin)
        self.assertEqual(myERI_raw_raw.clean(), None)

    def test_ER_links_POC_ERI_links_TO_which_constrains_input_dataset_CDT(self):
        # ERI links with a TO (For a POC leading from source TO), the input dataset CDT is constrained by the source TO
        myROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        myER = ER_from_record(myROC)

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
        myER = ER_from_record(myROC)
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
        myER = ER_from_record(myROC)

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
        myER = ER_from_record(myROC)

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
        myER = ER_from_record(myRS)

        myERO_rawDS_rawTO = myER.execrecordouts.create(
            symbolicdataset=self.raw_symDS, generic_output=self.C3_rawout)
        self.assertEqual(myERO_rawDS_rawTO.clean(), None)
        myERO_rawDS_rawTO.delete()

        myERO_rawDS_nonrawTO = myER.execrecordouts.create(
            symbolicdataset=self.raw_symDS, generic_output=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            r'SymbolicDataset ".*" \(raw\) cannot have come from output ".*" \(non-raw\)',
            myERO_rawDS_nonrawTO.clean)
        myERO_rawDS_nonrawTO.delete()

        myERO_DS_rawTO = myER.execrecordouts.create(
            symbolicdataset=self.singlet_symDS, generic_output=self.C3_rawout)
        self.assertRaisesRegexp(
            ValidationError,
            r'SymbolicDataset ".*" \(non-raw\) cannot have come from output ".*" \(raw\)',
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
            'CDT of SymbolicDataset ".*" is not the CDT of the TransformationOutput ".*" of the generating Method',
            myERO_invalid_CDT.clean)
        myERO_invalid_CDT.delete()

        # Dataset must have num rows within the row constraints of the producing TO
        # Method mB is step step_D1 of pipeline pD.
        myRS = self.pD_run.runsteps.create(pipelinestep=self.step_D1)
        myER_2 = ER_from_record(myRS)
        myERO_too_many_rows = myER_2.execrecordouts.create(
            symbolicdataset=self.triplet_symDS, generic_output=self.B1_out)
        self.assertRaisesRegexp(
            ValidationError,
            'SymbolicDataset ".*" was produced by TransformationOutput ".*" but has too many rows',
            myERO_too_many_rows.clean)
        myERO_too_many_rows.delete()

    def test_ERI_associated_Dataset_must_be_restriction_of_input_CDT(self):
        """If the ERI has a real non-raw Dataset associated to it, the Dataset must have a CDT that is a restriction of the input it feeds."""
        # Method mC is step step_E3 of pipeline pE.
        mC_RS = self.pE_run.runsteps.create(pipelinestep=self.step_E3)
        mC_ER = ER_from_record(mC_RS)
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
        other_CDT.members.create(datatype=self.string_dt,
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
        mA_ER = ER_from_record(mA_RS)
        mA_ERO = mA_ER.execrecordouts.create(
            generic_output=self.A1_out,
            symbolicdataset=self.doublet_symDS)

        # Good case: output SymbolicDataset has the CDT of
        # generic_output.
        self.assertEqual(mA_ERO.clean(), None)

        # Bad case: output SymbolicDataset has an identical CDT.
        other_CDT = CompoundDatatype()
        other_CDT.save()
        other_CDT.members.create(datatype=self.string_dt,
                                 column_name="x", column_idx=1)
        other_CDT.members.create(datatype=self.string_dt,
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
        outcable_ER = ER_from_record(outcable_ROC)
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
        other_CDT.members.create(datatype=self.string_dt,
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
        cable_ER = ER_from_PSIC(self.pE_run, self.step_E3, self.E11_32)
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
        other_CDT.members.create(datatype=self.string_dt,
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
        cable_ER = ER_from_PSIC(self.pE_run, self.step_E2, self.E02_22)
        cable_ER.execrecordins.create(
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
        outcable_ER = ER_from_record(outcable_ROC)
        outcable_ER.execrecordins.create(
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
        outcable_ER = ER_from_record(outcable_ROC)
        outcable_ERI = outcable_ER.execrecordins.create(generic_input=self.D1_out, symbolicdataset=self.C1_in_symDS)
        outcable_ERO = outcable_ER.execrecordouts.create(generic_output=self.E1_out, symbolicdataset=self.E1_out_symDS)

        # Good case: the Datatypes are exactly those needed.
        self.assertEqual(outcable_ER.clean(), None)

        # Good case: same as above, but with CDTs that are restrictions.
        D1_out_structure = self.D1_out.structure
        E1_out_structure = self.E1_out.structure
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
        cable_ER = ER_from_PSIC(self.pE_run, self.step_E2, self.E01_21)
        cable_ERI = cable_ER.execrecordins.create(
            generic_input=self.E1_in,
            symbolicdataset=self.triplet_symDS)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.D1_in,
            symbolicdataset=self.D1_in_symDS)

        # Good case: the Datatypes are exactly those needed.
        self.assertEqual(cable_ER.clean(), None)

        # Good case: same as above, but with CDTs that are restrictions.
        in_structure = self.E1_in.structure
        out_structure = self.D1_in.structure
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
        run = Run(pipeline=pipeline, user=user); run.save()
        runstep = run.runsteps.create(pipelinestep=pipeline.steps.first(), run=run)
        execlog = ExecLog(record=runstep, invoking_record=runstep); execlog.save()
        execrecord = ExecRecord(generator=execlog); execrecord.save()
        runstep.execrecord = execrecord
        runstep.save()

        self.assertEqual(execrecord.used_by_components.count(), 1)
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
            run = Run(pipeline=pipeline, user=user); run.save()
            runstep = run.runsteps.create(pipelinestep=pipeline.steps.first(), run=run)
            execlog = ExecLog(record=runstep, invoking_record=runstep); execlog.save()
            if i == 0:
                execrecord = ExecRecord(generator=execlog); execrecord.save()
            runstep.execrecord = execrecord
            runstep.save()

        self.assertEqual(execrecord.used_by_components.count(), 2)
        self.assertFalse(execrecord.has_ever_failed())

    def test_execrecord_one_failed_runstep(self):
        """An ExecRecord with one bad RunStep has failed."""
        pipeline = Pipeline.objects.first()
        user = User.objects.first()
        self.assertIsNotNone(pipeline)
        for run in pipeline.pipeline_instances.all():
            run.delete()
        run = Run(pipeline=pipeline, user=user); run.save()
        runstep = run.runsteps.create(pipelinestep=pipeline.steps.first(), run=run)
        execlog = ExecLog(record=runstep, invoking_record=runstep); execlog.save()
        MethodOutput(execlog=execlog, return_code=1).save()
        execrecord = ExecRecord(generator=execlog); execrecord.save()
        runstep.execrecord = execrecord
        runstep.save()

        self.assertFalse(runstep.successful_execution())
        self.assertEqual(execrecord.used_by_components.count(), 1)
        self.assertTrue(execrecord.has_ever_failed())

    def test_execrecord_multiple_good_one_failed_runstep(self):
        """An ExecRecord with one bad RunStep, and some good ones, has failed."""
        pipeline = Pipeline.objects.first()
        user = User.objects.first()
        self.assertIsNotNone(pipeline)
        for run in pipeline.pipeline_instances.all():
            run.delete()

        for i in range(2):
            run = Run(pipeline=pipeline, user=user); run.save()
            runstep = run.runsteps.create(pipelinestep=pipeline.steps.first(), run=run)
            execlog = ExecLog(record=runstep, invoking_record=runstep); execlog.save()
            if i == 1:
                MethodOutput(execlog=execlog, return_code=1).save()
            else:
                execrecord = ExecRecord(generator=execlog); execrecord.save()
            runstep.execrecord = execrecord
            runstep.save()

        self.assertEqual(execrecord.used_by_components.count(), 2)
        self.assertEqual(execrecord.used_by_components.first().definite.successful_execution(), True)
        self.assertEqual(execrecord.used_by_components.last().definite.successful_execution(), False)
        self.assertTrue(execrecord.has_ever_failed())


class FindCompatibleERTests(LibrarianTestCase):

    def test_find_compatible_ER_never_failed(self):
        """Should be able to find a compatible ExecRecord which never failed."""
        execrecord = None
        for e in ExecRecord.objects.all():
            if not e.has_ever_failed():
                execrecord = e
                break
        self.assertIsNotNone(execrecord)
        input_SDs_decorated = [(eri.generic_input.definite.dataset_idx, eri.symbolicdataset)
                               for eri in execrecord.execrecordins.all()]
        input_SDs_decorated.sort()
        input_SDs = [entry[1] for entry in input_SDs_decorated]
        runstep = execrecord.used_by_components.first().definite
        runstep.reused = False
        runstep.save()
        method = runstep.pipelinestep.transformation.method
        self.assertFalse(execrecord.has_ever_failed())
        self.assertIn(execrecord, method.find_compatible_ERs(input_SDs))

    def test_find_compatible_ER_failed(self):
        """Should also find a compatible ExecRecord which failed."""
        execrecord = None
        for e in ExecRecord.objects.all():
            if e.has_ever_failed():
                execrecord = e
                break
        self.assertIsNotNone(execrecord)
        input_SDs_decorated = [(eri.generic_input.definite.dataset_idx, eri.symbolicdataset)
                               for eri in execrecord.execrecordins.all()]
        input_SDs_decorated.sort()
        input_SDs = [entry[1] for entry in input_SDs_decorated]
        runstep = execrecord.used_by_components.first().definite
        runstep.reused = False
        runstep.save()
        method = runstep.pipelinestep.transformation.method
        self.assertTrue(execrecord.has_ever_failed())
        self.assertIn(execrecord, method.find_compatible_ERs(input_SDs))

    def test_find_compatible_ER_skips_nulls(self):
        """
        Incomplete run steps don't break search for compatible ExecRecords.
        """
        # Find an ExecRecord that has never failed
        execrecord = None
        for execrecord in ExecRecord.objects.all():
            if not execrecord.has_ever_failed():
                break
        input_SDs_decorated = [(eri.generic_input.definite.dataset_idx, eri.symbolicdataset)
                               for eri in execrecord.execrecordins.all()]
        input_SDs_decorated.sort()
        input_SDs = [entry[1] for entry in input_SDs_decorated]

        # Create a method with two run steps, the first one is incomplete.
        method = Method()
        method.family = MethodFamily.objects.first()
        method.driver = CodeResourceRevision.objects.first()
        method.save()
        
        pipeline_step1 = PipelineStep()
        pipeline_step1.pipeline = Pipeline.objects.first()
        pipeline_step1.transformation = method
        pipeline_step1.step_num = 99
        pipeline_step1.save()
        
        pipeline_step2 = PipelineStep()
        pipeline_step2.pipeline = Pipeline.objects.first()
        pipeline_step2.transformation = method
        pipeline_step2.step_num = 100
        pipeline_step2.save()
        
        # Incomplete: no exec record
        run_step1 = RunStep()
        run_step1.run = Run.objects.first()
        run_step1.pipelinestep = pipeline_step1
        run_step1.reused = False
        run_step1.save()
        
        # Complete: has an exec record
        run_step2 = RunStep()
        run_step2.run = Run.objects.first()
        run_step2.pipelinestep = pipeline_step2
        run_step2.reused = False
        run_step2.execrecord = execrecord
        run_step2.save()
        
        self.assertIn(execrecord, method.find_compatible_ERs(input_SDs))
