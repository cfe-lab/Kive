"""
Shipyard models pertaining to the librarian app.
"""

import os
import random
import re
import tempfile
import time
import logging

from django.core.exceptions import ValidationError
from django.utils import timezone
from django.contrib.auth.models import User
from django.test import TestCase
from django.core.urlresolvers import reverse, resolve
from django.core.files import File

from rest_framework.test import force_authenticate, APIRequestFactory
from rest_framework import status

from archive.models import ExecLog, MethodOutput, Run, RunStep
from constants import datatypes
from librarian.models import Dataset, ExecRecord
from metadata.models import Datatype, CompoundDatatype, kive_user, everyone_group
from method.models import CodeResource, CodeResourceRevision, Method, \
    MethodFamily
from pipeline.models import Pipeline, PipelineFamily
from librarian.serializers import DatasetSerializer

import file_access_utils
import kive.testing_utils as tools
from kive.tests import BaseTestCases, DuckContext


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
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        """Set up default database state for librarian unit testing."""
        tools.create_librarian_test_environment(self)

    def tearDown(self):
        tools.clean_up_all_files()


class DatasetTests(LibrarianTestCase):

    def setUp(self):
        super(DatasetTests, self).setUp()

        # Turn off logging, so the test output isn't polluted.
        logging.getLogger('Dataset').setLevel(logging.CRITICAL)
        logging.getLogger('CompoundDatatype').setLevel(logging.CRITICAL)
        
        rows = 10
        seqlen = 10

        self.data = ""
        for i in range(rows):
            seq = "".join([random.choice("ATCG") for _ in range(seqlen)])
            self.data += "patient{},{}\n".format(i, seq)
        self.header = "header,sequence"

        self.datatype_str = Datatype.objects.get(pk=datatypes.STR_PK)
        self.datatype_dna = Datatype(name="DNA", description="sequences of ATCG",
                                     user=self.myUser)
        self.datatype_dna.clean()
        self.datatype_dna.save()
        self.datatype_dna.restricts.add(self.datatype_str)
        self.datatype_dna.complete_clean()
        self.cdt_record = CompoundDatatype(user=self.myUser)
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
        self.dataset = Dataset.create_dataset(file_path=self.file_path, user=self.myUser,
                                              cdt=self.cdt_record, keep_file=True, name=self.dsname,
                                              description=self.dsdesc)

    def tearDown(self):
        super(DatasetTests, self).tearDown()
        os.remove(self.file_path)

    def test_filehandle(self):
        """
        Test that you can pass a filehandle to create_dataset() to make a dataset.
        """
        import datetime
        dt = datetime.datetime.now()
        # Turn off logging, so the test output isn't polluted.
        logging.getLogger('Dataset').setLevel(logging.CRITICAL)
        logging.getLogger('CompoundDatatype').setLevel(logging.CRITICAL)

        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        tmpfile.file.write("Random stuff")
        tmpfile.file.flush()  # flush python buffer to os buffer
        os.fsync(tmpfile.file.fileno())  # flush os buffer to disk
        tmpfile.file.seek(0)  # go to beginning of file before calculating expected md5

        expected_md5 = file_access_utils.compute_md5(tmpfile)

        raw_datatype = None  # raw compound datatype
        name = "Test file handle" + str(dt.microsecond)
        desc = "Test create dataset with file handle"
        dataset = Dataset.create_dataset(file_path=None, user=self.myUser, cdt=raw_datatype,
                                         keep_file=True, name=name, description=desc, check=True,
                                         file_handle=tmpfile)

        tmpfile.close()
        os.remove(tmpfile.name)

        self.assertIsNotNone(Dataset.objects.filter(name=name).get(),
                             msg="Can't find Dataset in DB for name=" + name)

        actual_md5 = Dataset.objects.filter(id=dataset.id).get().MD5_checksum
        self.assertEqual(actual_md5, expected_md5,
                         msg="Checksum for Dataset ({}) file does not match expected ({})".format(
                             actual_md5,
                             expected_md5
                         ))

    def test_is_raw(self):
        self.assertEqual(self.triplet_dataset.is_raw(), False)
        self.assertEqual(self.raw_dataset.is_raw(), True)

    def test_forgot_header(self):
        """
        Dataset creation with a CDT fails when the header is left off
        the data file.
        """
        # Write the data with no header.
        data_file = tempfile.NamedTemporaryFile()
        data_file.write(self.data)

        # Try to create a dataset.
        self.assertRaisesRegexp(ValueError,
                                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                                          .format(data_file.name, self.cdt_record)),
                                lambda: Dataset.create_dataset(file_path=data_file.name,
                                                               user=self.myUser, cdt=self.cdt_record,
                                                               name="lab data", description="patient sequences"))
        data_file.close()

    def test_empty_file(self):
        """
        Dataset creation fails if the file passed is empty.
        """
        data_file = tempfile.NamedTemporaryFile()
        file_path = data_file.name

        self.assertRaisesRegexp(ValueError,
                                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                                          .format(file_path, self.cdt_record)),
                                lambda: Dataset.create_dataset(file_path=data_file.name,
                                                               user=self.myUser, cdt=self.cdt_record,
                                                               name="missing data", description="oops!"))
        data_file.close()

    def test_too_many_columns(self):
        """
        Dataset creation fails if the data file has too many
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
                lambda: Dataset.create_dataset(file_path=file_path, user=self.myUser,
                                               cdt=self.cdt_record, name="bad data",
                                               description="too many columns")
            )

    def test_right_columns(self):
        """
        Dataset creation fails if the data file has too many
        columns.
        """
        with tempfile.NamedTemporaryFile() as data_file:
            data_file.write("""\
header,sequence
foo,bar
""")
            data_file.flush()
            file_path = data_file.name

            Dataset.create_dataset(file_path=file_path, user=self.myUser, cdt=self.cdt_record,
                                   description="right columns", name="good data")

    def test_invalid_integer_field(self):
        """
        Dataset creation fails if the data file has too many
        columns.
        """
        compound_datatype = CompoundDatatype(user=self.myUser)
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
                lambda: Dataset.create_dataset(file_path=file_path, user=self.myUser, cdt=compound_datatype,
                                               name="bad data", description="bad integer field"))

    def test_dataset_creation(self):
        """
        Test coherence of a freshly created Dataset.
        """
        self.assertEqual(self.dataset.clean(), None)
        self.assertEqual(self.dataset.has_data(), True)
        self.assertEqual(self.dataset.is_raw(), False)

        self.assertEqual(self.dataset.user, self.myUser)
        self.assertEqual(self.dataset.name, self.dsname)
        self.assertEqual(self.dataset.description, self.dsdesc)
        self.assertEqual(self.dataset.date_created.date(), timezone.now().date())
        self.assertEqual(self.dataset.date_created < timezone.now(), True)
        self.assertEqual(self.dataset.file_source, None)
        self.assertEqual(os.path.basename(self.dataset.dataset_file.path), os.path.basename(self.file_path))
        self.data_file.close()

    def test_dataset_bulk_created(self):
        """
        Test coherence of the Dataset created alongsite a Dataset.
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

        datasets = Dataset.create_dataset_bulk(csv_file_path=bulk_dataset_csv.name,
                                               user=self.myUser, cdt=self.cdt_record, keep_files=True,
                                               check=True)
        for f in data_files:
            f.close()
        bulk_dataset_csv.close()
        for i, dataset in enumerate(datasets):

            self.assertEqual(dataset.clean(), None)
            self.assertEqual(dataset.user, self.myUser)
            self.assertEqual(dataset.name, dsname+str(i))
            self.assertEqual(dataset.description, dsdesc+str(i))
            self.assertEqual(dataset.date_created.date(), timezone.now().date())
            self.assertEqual(dataset.date_created < timezone.now(), True)
            self.assertEqual(dataset.file_source, None)
            self.assertEqual(os.path.basename(dataset.dataset_file.path), os.path.basename(file_paths[i]))


class DatasetStructureTests(LibrarianTestCase):

    def test_num_rows(self):
        self.assertEqual(self.triplet_3_rows_dataset.num_rows(), 3)
        self.assertEqual(self.triplet_3_rows_dataset.structure.num_rows, 3)


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
            dataset = self.singlet_dataset,
            generic_input = self.C1_out)

        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO that feeds the parent ExecRecord POC",
            myERI_bad.clean)

    def test_ER_links_PSIC_so_ERI_must_link_TX_that_PSIC_is_fed_by(self):
        # ER links PSIC: ERI must link to the TO/TI that the PSIC is fed by
        myER = ER_from_PSIC(self.pE_run, self.step_E3, self.E11_32)
        myERI_bad = myER.execrecordins.create(dataset=self.singlet_dataset,
                                              generic_input=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            myERI_bad.clean)
        
        yourER = ER_from_PSIC(self.pE_run, self.step_E2, self.E02_22)
        yourERI_bad = yourER.execrecordins.create(dataset=self.singlet_dataset,
                                                  generic_input=self.D2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            yourERI_bad.clean)

    def test_ER_doesnt_link_cable_so_ERI_mustnt_link_TO(self):
        # ER's EL doesn't refer to a RSIC or ROC (So, RunStep): ERI must refer to a TI
        myRS = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        myER = ER_from_record(myRS)
        myERI_bad = myER.execrecordins.create(dataset=self.singlet_dataset,
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
            dataset=self.D1_in_dataset,
            generic_input=self.B1_in)

        self.assertEqual(myERI_good.clean(), None)
        
        myERI_bad = myER.execrecordins.create(
            dataset=self.triplet_dataset,
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
            dataset=self.triplet_dataset,
            generic_input=self.C1_in)
        self.assertEqual(myERI_unraw_unraw.clean(), None)

        myERI_raw_unraw_BAD = myER_C.execrecordins.create(
            dataset=self.raw_dataset,
            generic_input=self.C2_in)
        self.assertRaisesRegexp(
            ValidationError,
            r'Dataset ".*" \(raw\) cannot feed source ".*" \(non-raw\)',
            myERI_raw_unraw_BAD.clean)
        myERI_raw_unraw_BAD.delete()

        myRS_A = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        myER_A = ER_from_record(myRS_A)
        myERI_unraw_raw_BAD = myER_A.execrecordins.create(
            dataset=self.triplet_dataset,
            generic_input=self.A1_rawin)
        self.assertRaisesRegexp(
            ValidationError,
            r'Dataset ".*" \(non-raw\) cannot feed source ".*" \(raw\)',
            myERI_unraw_raw_BAD.clean)
        myERI_unraw_raw_BAD.delete()
    
        myERI_raw_raw = myER_A.execrecordins.create(
            dataset=self.raw_dataset,
            generic_input=self.A1_rawin)
        self.assertEqual(myERI_raw_raw.clean(), None)

    def test_ER_links_POC_ERI_links_TO_which_constrains_input_dataset_CDT(self):
        # ERI links with a TO (For a POC leading from source TO), the input dataset CDT is constrained by the source TO
        myROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        myER = ER_from_record(myROC)

        # We annotate that triplet was fed from D1_out into E21_41
        myERI_wrong_CDT = myER.execrecordins.create(
            dataset=self.singlet_dataset,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not a restriction of the required CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        # Right CDT but wrong number of rows (It needs < 5, we have 10)
        myERI_too_many_rows = myER.execrecordins.create(
            dataset=self.triplet_dataset,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" has too many rows to have come from TransformationOutput \".*\"",
            myERI_too_many_rows.clean)

    def test_ER_links_pipelinestep_ERI_links_TI_which_constrains_input_CDT(self):
        # The transformation input of its PipelineStep constrains the dataset when the ER links with a method
        # Method mC is step step_E3 of pipeline pE.
        myROC = self.pE_run.runsteps.create(pipelinestep=self.step_E3)
        myER = ER_from_record(myROC)
        myERI_wrong_CDT = myER.execrecordins.create(
            dataset=self.singlet_dataset,
            generic_input=self.C2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not a restriction of the required CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        myERI_right_CDT = myER.execrecordins.create(
            dataset=self.doublet_dataset, generic_input=self.C2_in)
        self.assertEqual(myERI_right_CDT.clean(), None)

    def test_ER_links_with_POC_ERO_TO_must_belong_to_same_pipeline_as_ER_POC(self):
        # If the parent ER is linked with a POC, the ERO TO must belong to that pipeline

        # E31_42 belongs to pipeline E
        myROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E31_42)
        myER = ER_from_record(myROC)

        # This ERO has a TO that belongs to this pipeline
        myERO_good = myER.execrecordouts.create(
            dataset=self.singlet_dataset,
            generic_output=self.pE.outputs.get(dataset_name="E2_out"))
        self.assertEqual(myERO_good.clean(), None)
        myERO_good.delete()

        # This ERO has a TO that does NOT belong to this pipeline
        myERO_bad = myER.execrecordouts.create(
            dataset=self.triplet_3_rows_dataset,
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
            dataset=self.triplet_3_rows_dataset,
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
            dataset=self.raw_dataset, generic_output=self.C3_rawout)
        self.assertEqual(myERO_rawDS_rawTO.clean(), None)
        myERO_rawDS_rawTO.delete()

        myERO_rawDS_nonrawTO = myER.execrecordouts.create(
            dataset=self.raw_dataset, generic_output=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            r'Dataset ".*" \(raw\) cannot have come from output ".*" \(non-raw\)',
            myERO_rawDS_nonrawTO.clean)
        myERO_rawDS_nonrawTO.delete()

        myERO_DS_rawTO = myER.execrecordouts.create(
            dataset=self.singlet_dataset, generic_output=self.C3_rawout)
        self.assertRaisesRegexp(
            ValidationError,
            r'Dataset ".*" \(non-raw\) cannot have come from output ".*" \(raw\)',
            myERO_DS_rawTO.clean)
        myERO_DS_rawTO.delete()

        myERO_DS_TO = myER.execrecordouts.create(
            dataset=self.singlet_dataset, generic_output=self.C1_out)
        self.assertEqual(myERO_DS_TO.clean(), None)
        myERO_DS_TO.delete()
        
        # 2) Dataset must have the same CDT of the producing TO
        myERO_invalid_CDT = myER.execrecordouts.create(
            dataset=self.triplet_dataset, generic_output=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            'CDT of Dataset ".*" is not the CDT of the TransformationOutput ".*" of the generating Method',
            myERO_invalid_CDT.clean)
        myERO_invalid_CDT.delete()

        # Dataset must have num rows within the row constraints of the producing TO
        # Method mB is step step_D1 of pipeline pD.
        myRS = self.pD_run.runsteps.create(pipelinestep=self.step_D1)
        myER_2 = ER_from_record(myRS)
        myERO_too_many_rows = myER_2.execrecordouts.create(
            dataset=self.triplet_dataset, generic_output=self.B1_out)
        self.assertRaisesRegexp(
            ValidationError,
            'Dataset ".*" was produced by TransformationOutput ".*" but has too many rows',
            myERO_too_many_rows.clean)
        myERO_too_many_rows.delete()

    def test_ERI_associated_Dataset_must_be_restriction_of_input_CDT(self):
        """If the ERI has a real non-raw Dataset associated to it, the Dataset must have a CDT that is a restriction of the input it feeds."""
        # Method mC is step step_E3 of pipeline pE.
        mC_RS = self.pE_run.runsteps.create(pipelinestep=self.step_E3)
        mC_ER = ER_from_record(mC_RS)
        mC_ER_in_1 = mC_ER.execrecordins.create(
            generic_input=self.C1_in,
            dataset=self.C1_in_dataset)

        # Good case: input Dataset has the CDT of
        # generic_input.
        self.assertEqual(mC_ER_in_1.clean(), None)

        # Good case: input Dataset has an identical CDT of
        # generic_input.
        other_CDT = CompoundDatatype(user=self.myUser)
        other_CDT.save()

        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="a", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="b", column_idx=2)
        other_CDT.members.create(datatype=self.string_dt,
                                 column_name="c", column_idx=3)

        self.C1_in_dataset.structure.compounddatatype = other_CDT
        self.assertEqual(mC_ER_in_1.clean(), None)

        # Good case: proper restriction.
        col1.datatype = self.DNA_dt
        col2.datatype = self.RNA_dt
        self.assertEqual(mC_ER_in_1.clean(), None)

        # Bad case: a type that is not a restriction at all.
        self.C1_in_dataset.structure.compounddatatype = self.doublet_cdt
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not a restriction of the required CDT",
            mC_ER_in_1.clean)
        
    def test_ERO_CDT_restrictions_Method(self):
        """ERO CDT restriction tests for the ER of a Method."""
        # Method mA is step step_E1 of pipeline pE.
        mA_RS = self.pE_run.runsteps.create(pipelinestep=self.step_E1)
        mA_ER = ER_from_record(mA_RS)
        mA_ERO = mA_ER.execrecordouts.create(
            generic_output=self.A1_out,
            dataset=self.doublet_dataset)

        # Good case: output Dataset has the CDT of
        # generic_output.
        self.assertEqual(mA_ERO.clean(), None)

        # Bad case: output Dataset has an identical CDT.
        other_CDT = CompoundDatatype(user=self.myUser)
        other_CDT.save()
        other_CDT.members.create(datatype=self.string_dt,
                                 column_name="x", column_idx=1)
        other_CDT.members.create(datatype=self.string_dt,
                                 column_name="y", column_idx=2)
        
        self.doublet_dataset.structure.compounddatatype = other_CDT
        self.doublet_dataset.structure.save()

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not the CDT of the TransformationOutput .* of the generating Method",
            mA_ERO.clean)

        # Bad case: output Dataset has another CDT altogether.
        mA_ERO.dataset=self.triplet_dataset

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not the CDT of the TransformationOutput .* of the generating Method",
            mA_ERO.clean)

    def test_ERO_CDT_restrictions_POC(self):
        """ERO CDT restriction tests for the ER of a POC."""
        ####
        outcable_ROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        outcable_ER = ER_from_record(outcable_ROC)
        outcable_ERO = outcable_ER.execrecordouts.create(
            generic_output=self.E1_out,
            dataset=self.E1_out_dataset)

        # Good case: output Dataset has the CDT of generic_output.
        self.assertEqual(outcable_ERO.clean(), None)

        # Good case: output Dataset has an identical CDT.
        other_CDT = CompoundDatatype(user=self.myUser)
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="x", column_idx=1)
        other_CDT.members.create(datatype=self.string_dt,
                                 column_name="y", column_idx=2)
        
        self.E1_out_dataset.structure.compounddatatype = other_CDT
        self.E1_out_dataset.structure.save()
        self.assertEqual(outcable_ERO.clean(), None)

        # Bad case: output Dataset has a CDT that is a restriction of
        # generic_output.
        col1.datatype = self.DNA_dt
        col1.save()
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            outcable_ERO.clean)

        # Bad case: output Dataset has another CDT altogether.
        outcable_ERO.dataset = self.singlet_dataset

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            outcable_ERO.clean)

    def test_ERO_CDT_restrictions_PSIC(self):
        """ERO CDT restriction tests for the ER of a PSIC."""
        ####
        cable_ER = ER_from_PSIC(self.pE_run, self.step_E3, self.E11_32)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.C2_in,
            dataset=self.doublet_dataset)

        # Good case: output Dataset has the CDT of generic_output.
        self.assertEqual(cable_ERO.clean(), None)

        # Good case: output Dataset has an identical CDT.
        other_CDT = CompoundDatatype(user=self.myUser)
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="x", column_idx=1)
        other_CDT.members.create(datatype=self.string_dt,
                                 column_name="y", column_idx=2)
        
        self.doublet_dataset.structure.compounddatatype = other_CDT
        self.doublet_dataset.structure.save()
        self.assertEqual(cable_ERO.clean(), None)

        # Good case: output Dataset has a CDT that is a restriction of
        # generic_output.
        col1.datatype = self.DNA_dt
        col1.save()
        self.assertEqual(cable_ERO.clean(), None)

        # Bad case: output Dataset has another CDT altogether.
        cable_ERO.dataset = self.singlet_dataset

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not a restriction of the CDT of the fed TransformationInput .*",
            cable_ERO.clean)

    def test_ER_trivial_PSICs_have_same_dataset_on_both_sides(self):
        """ERs representing trivial PSICs must have the same Dataset on both sides."""
        cable_ER = ER_from_PSIC(self.pE_run, self.step_E2, self.E02_22)
        cable_ER.execrecordins.create(
            generic_input=self.E2_in,
            dataset = self.singlet_dataset)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.D2_in,
            dataset = self.singlet_dataset)

        # Good case: datasets on either side of this trivial cable match.
        self.assertEqual(cable_ER.clean(), None)

        # Bad case: datasets don't match.
        cable_ERO.dataset = self.C1_out_dataset
        cable_ERO.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord "{}" represents a trivial cable but its input and output do not '
                                          'match'.format(cable_ER)),
                                cable_ER.clean)

    def test_ER_trivial_POCs_have_same_dataset_on_both_sides(self):
        """ERs representing trivial POCs must have the same Dataset on both sides."""
        # E31_42 belongs to pipeline E
        outcable_ROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E31_42)
        outcable_ER = ER_from_record(outcable_ROC)
        outcable_ER.execrecordins.create(
            generic_input=self.C1_out,
            dataset=self.C1_out_dataset)
        outcable_ERO = outcable_ER.execrecordouts.create(
            generic_output=self.E2_out,
            dataset=self.C1_out_dataset)

        # Good case: datasets on either side of this trivial POC match.
        self.assertEqual(outcable_ER.clean(), None)

        # Bad case: datasets don't match.
        outcable_ERO.dataset = self.singlet_dataset
        outcable_ERO.save()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('ExecRecord "{}" represents a trivial cable but its input and output do not '
                                          'match'.format(outcable_ER)),
                                outcable_ER.clean)
        

    def test_ER_Datasets_passing_through_non_trivial_POCs(self):
        """Test that the Datatypes of Datasets passing through POCs are properly preserved."""
        outcable_ROC = self.pE_run.runoutputcables.create(pipelineoutputcable=self.E21_41)
        outcable_ER = ER_from_record(outcable_ROC)
        outcable_ERI = outcable_ER.execrecordins.create(generic_input=self.D1_out, dataset=self.C1_in_dataset)
        outcable_ERO = outcable_ER.execrecordouts.create(generic_output=self.E1_out, dataset=self.E1_out_dataset)

        # Good case: the Datatypes are exactly those needed.
        self.assertEqual(outcable_ER.clean(), None)

        # Good case: same as above, but with CDTs that are restrictions.
        D1_out_structure = self.D1_out.structure
        E1_out_structure = self.E1_out.structure
        D1_out_structure.compounddatatype = self.DNA_triplet_cdt
        D1_out_structure.save()
        E1_out_structure.compounddatatype = self.DNA_doublet_cdt
        E1_out_structure.save()
        
        outcable_ERI.dataset = self.DNA_triplet_dataset
        outcable_ERI.save()
        outcable_ERO.dataset = self.E21_41_DNA_doublet_dataset
        outcable_ERO.save()
        self.assertIsNone(outcable_ER.clean())

        # Bad case: cable does some casting.
        output_col1 = (self.E21_41_DNA_doublet_dataset.structure.compounddatatype.members.get(column_idx=1))
        output_col1.datatype = self.string_dt
        output_col1.save()

        source_datatype = outcable_ERI.dataset.structure.compounddatatype.members.get(column_idx=1).datatype
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
            dataset=self.triplet_dataset)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.D1_in,
            dataset=self.D1_in_dataset)

        # Good case: the Datatypes are exactly those needed.
        self.assertEqual(cable_ER.clean(), None)

        # Good case: same as above, but with CDTs that are restrictions.
        in_structure = self.E1_in.structure
        out_structure = self.D1_in.structure
        in_structure.compounddatatype = self.DNA_triplet_cdt
        in_structure.save()
        out_structure.compounddatatype = self.DNA_doublet_cdt
        out_structure.save()
        
        cable_ERI.dataset = self.DNA_triplet_dataset
        cable_ERI.save()
        cable_ERO.dataset = self.E01_21_DNA_doublet_dataset
        cable_ERO.save()
        self.assertEqual(cable_ER.clean(), None)

        # Bad case: cable does some casting.
        output_col1 = (self.E01_21_DNA_doublet_dataset.structure.
                       compounddatatype.members.get(column_idx=1))
        output_col1.datatype = self.string_dt
        output_col1.save()
        source_datatype = cable_ERI.dataset.structure.compounddatatype.members.get(column_idx=1).datatype
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
        input_datasets_decorated = [(eri.generic_input.definite.dataset_idx, eri.dataset)
                               for eri in execrecord.execrecordins.all()]
        input_datasets_decorated.sort()
        input_datasets = [entry[1] for entry in input_datasets_decorated]
        runstep = execrecord.used_by_components.first().definite
        runstep.reused = False
        runstep.save()
        method = runstep.pipelinestep.transformation.method
        self.assertFalse(execrecord.has_ever_failed())
        self.assertIn(execrecord, method.find_compatible_ERs(input_datasets, runstep))

    def test_find_compatible_ER_failed(self):
        """Should also find a compatible ExecRecord which failed."""
        execrecord = None
        for e in ExecRecord.objects.all():
            if e.has_ever_failed():
                execrecord = e
                break
        self.assertIsNotNone(execrecord)
        input_datasets_decorated = [(eri.generic_input.definite.dataset_idx, eri.dataset)
                               for eri in execrecord.execrecordins.all()]
        input_datasets_decorated.sort()
        input_datasets = [entry[1] for entry in input_datasets_decorated]
        runstep = execrecord.used_by_components.first().definite
        runstep.reused = False
        runstep.save()
        method = runstep.pipelinestep.transformation.method
        self.assertTrue(execrecord.has_ever_failed())
        self.assertIn(execrecord, method.find_compatible_ERs(input_datasets, runstep))

    def test_find_compatible_ER_skips_nulls(self):
        """
        Incomplete run steps don't break search for compatible ExecRecords.
        """
        # Find an ExecRecord that has never failed
        execrecord = None
        for execrecord in ExecRecord.objects.all():
            if not execrecord.has_ever_failed():
                break
        input_datasets_decorated = [(eri.generic_input.definite.dataset_idx, eri.dataset)
                               for eri in execrecord.execrecordins.all()]
        input_datasets_decorated.sort()
        input_datasets = [entry[1] for entry in input_datasets_decorated]

        method = execrecord.general_transf()
        pipeline = execrecord.generating_run.pipeline
        ps = pipeline.steps.get(transformation=method)

        # Create two RunSteps using this method.  First, an incomplete one.
        run1 = Run(user=self.myUser, pipeline=pipeline, name="First incomplete run",
                   description="Be patient!")
        run1.save()
        run1.start()
        run1.runsteps.create(pipelinestep=ps)

        # Second, one that is looking for an ExecRecord.
        run2 = Run(user=self.myUser, pipeline=pipeline, name="Second run in progress",
                   description="Impatient!")
        run2.save()
        run2.start()
        rs2 = run2.runsteps.create(pipelinestep=ps)

        self.assertIn(execrecord, method.find_compatible_ERs(input_datasets, rs2))


class RemovalTests(TestCase):
    fixtures = ["removal"]

    def setUp(self):
        self.remover = User.objects.get(username="Rem Over")
        self.noop_plf = PipelineFamily.objects.get(name="Nucleotide Sequence Noop")
        self.noop_pl = self.noop_plf.members.get(revision_name="v1")
        self.first_run = self.noop_pl.pipeline_instances.order_by("start_time").first()
        self.second_run = self.noop_pl.pipeline_instances.order_by("start_time").last()
        self.input_DS = Dataset.objects.get(name="Removal test data")
        self.nuc_seq_noop_mf = MethodFamily.objects.get(name="Noop (nucleotide sequence)")
        self.nuc_seq_noop = self.nuc_seq_noop_mf.members.get(revision_name="v1")
        self.p_nested_plf = PipelineFamily.objects.get(name="Nested pipeline")
        self.p_nested = self.p_nested_plf.members.get(revision_name="v1")
        self.noop_cr = CodeResource.objects.get(name="Noop")
        self.noop_crr = self.noop_cr.revisions.get(revision_name="1")
        self.pass_through_cr = CodeResource.objects.get(name="Pass Through")
        self.pass_through_crr = self.pass_through_cr.revisions.get(revision_name="1")
        self.nuc_seq = Datatype.objects.get(name="Nucleotide sequence")
        self.one_col_nuc_seq = self.nuc_seq.CDTMs.get(column_name="sequence", column_idx=1).compounddatatype

        self.two_step_noop_plf = PipelineFamily.objects.get(name="Nucleotide Sequence two-step Noop")
        self.two_step_noop_pl = self.two_step_noop_plf.members.get(revision_name="v1")
        self.two_step_input_dataset = Dataset.objects.get(name="Removal test data for a two-step Pipeline")

        # Datasets and ExecRecords produced by the first run.
        self.produced_data = set()
        self.execrecords = set()
        for runstep in self.first_run.runsteps.all():
            self.produced_data.update(runstep.outputs.all())
            self.execrecords.add(runstep.execrecord)
            for rsic in runstep.RSICs.all():
                self.produced_data.update(rsic.outputs.all())
                self.execrecords.add(rsic.execrecord)
        for roc in self.first_run.runoutputcables.all():
            self.produced_data.update(roc.outputs.all())
            self.execrecords.add(roc.execrecord)

        self.step_log = self.first_run.runsteps.first().log

        self.two_step_run = self.two_step_noop_pl.pipeline_instances.first()
        self.two_step_intermediate_data = self.two_step_run.runsteps.get(
            pipelinestep__step_num=1).outputs.first()
        self.two_step_output_data = self.two_step_run.runsteps.get(
            pipelinestep__step_num=2).outputs.first()
        self.two_step_execrecords = set()
        for runstep in self.two_step_run.runsteps.all():
            self.two_step_execrecords.add(runstep.execrecord)
            for rsic in runstep.RSICs.all():
                self.two_step_execrecords.add(rsic.execrecord)
        for roc in self.two_step_run.runoutputcables.all():
            self.two_step_execrecords.add(roc.execrecord)

    def tearDown(self):
        tools.clean_up_all_files()

    def removal_plan_tester(self, obj_to_remove, datasets=None, ERs=None, runs=None, pipelines=None, pfs=None,
                            methods=None, mfs=None, CDTs=None, DTs=None, CRRs=None, CRs=None):
        removal_plan = obj_to_remove.build_removal_plan()
        self.assertSetEqual(removal_plan["Datasets"], set(datasets) if datasets is not None else set())
        self.assertSetEqual(removal_plan["ExecRecords"], set(ERs) if ERs is not None else set())
        self.assertSetEqual(removal_plan["Runs"], set(runs) if runs is not None else set())
        self.assertSetEqual(removal_plan["Pipelines"], set(pipelines) if pipelines is not None else set())
        self.assertSetEqual(removal_plan["PipelineFamilies"], set(pfs) if pfs is not None else set())
        self.assertSetEqual(removal_plan["Methods"], set(methods) if methods is not None else set())
        self.assertSetEqual(removal_plan["MethodFamilies"], set(mfs) if mfs is not None else set())
        self.assertSetEqual(removal_plan["CompoundDatatypes"], set(CDTs) if CDTs is not None else set())
        self.assertSetEqual(removal_plan["Datatypes"], set(DTs) if DTs is not None else set())
        self.assertSetEqual(removal_plan["CodeResourceRevisions"], set(CRRs) if CRRs is not None else set())
        self.assertSetEqual(removal_plan["CodeResources"], set(CRs) if CRs is not None else set())

    def test_run_build_removal_plan(self):
        """Removing a Run should remove all intermediate/output data and ExecRecords, and all Runs that reused it."""
        self.removal_plan_tester(self.first_run, datasets=self.produced_data, ERs=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_reused_run_build_removal_plan(self):
        """Removing a reused Run should leave reused data/ExecRecords alone."""
        self.removal_plan_tester(self.second_run, runs={self.second_run})

    def test_input_data_build_removal_plan(self):
        """Removing input data to a Run should remove any Run started from it."""
        all_data = self.produced_data
        all_data.add(self.input_DS)

        self.removal_plan_tester(self.input_DS, datasets=all_data, ERs=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_produced_data_build_removal_plan(self):
        """Removing data produced by the Run should have the same effect as removing the Run itself."""
        produced_dataset = list(self.produced_data)[0]

        self.removal_plan_tester(produced_dataset, datasets=self.produced_data, ERs=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_step_ER_build_removal_plan(self):
        """Removing the ExecRecord of the first RunStep should be like removing the whole Run."""
        first_step_ER = self.first_run.runsteps.get(pipelinestep__step_num=1).execrecord

        self.removal_plan_tester(first_step_ER, datasets=self.produced_data, ERs=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_rsic_ER_build_removal_plan(self):
        """Removing the ExecRecord of a RunSIC should be like removing the whole Run."""
        first_RSIC_ER = self.first_run.runsteps.get(pipelinestep__step_num=1).RSICs.first().execrecord

        self.removal_plan_tester(first_RSIC_ER, datasets=self.produced_data, ERs=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_roc_ER_build_removal_plan(self):
        """Removing the ExecRecord of a RunOutputCable should be like removing the whole Run."""
        first_ROC_ER = self.first_run.runoutputcables.first().execrecord

        self.removal_plan_tester(first_ROC_ER, datasets=self.produced_data, ERs=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_pipeline_build_removal_plan(self):
        """Removing a Pipeline."""
        self.removal_plan_tester(self.noop_pl, datasets=self.produced_data, ERs=self.execrecords,
                                 runs={self.first_run, self.second_run}, pipelines={self.noop_pl, self.p_nested})

    def test_nested_pipeline_build_removal_plan(self):
        """Removing a nested Pipeline."""
        self.removal_plan_tester(self.p_nested, pipelines={self.p_nested})

    def test_pipelinefamily_build_removal_plan(self):
        """Removing a PipelineFamily removes everything that goes along with it."""
        self.removal_plan_tester(self.noop_plf, datasets=self.produced_data, ERs=self.execrecords,
                                 runs={self.first_run, self.second_run}, pipelines={self.noop_pl, self.p_nested},
                                 pfs={self.noop_plf})

    def test_method_build_removal_plan(self):
        """Removing a Method removes all Pipelines containing it and all of the associated stuff."""
        self.removal_plan_tester(self.nuc_seq_noop, datasets=self.produced_data.union(
            {self.two_step_intermediate_data, self.two_step_output_data}),
                                 ERs=self.execrecords.union(self.two_step_execrecords),
                                 runs={self.first_run, self.second_run, self.two_step_run},
                                 pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
                                 methods={self.nuc_seq_noop})

    def test_methodfamily_build_removal_plan(self):
        """Removing a MethodFamily."""
        self.removal_plan_tester(self.nuc_seq_noop_mf, datasets=self.produced_data.union(
            {self.two_step_intermediate_data, self.two_step_output_data}),
                                 ERs=self.execrecords.union(self.two_step_execrecords),
                                 runs={self.first_run, self.second_run, self.two_step_run},
                                 pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
                                 methods={self.nuc_seq_noop}, mfs={self.nuc_seq_noop_mf})

    def test_crr_build_removal_plan(self):
        """Removing a CodeResourceRevision."""
        self.removal_plan_tester(self.noop_crr, datasets=self.produced_data.union(
            {self.two_step_intermediate_data, self.two_step_output_data}),
                                 ERs=self.execrecords.union(self.two_step_execrecords),
                                 runs={self.first_run, self.second_run, self.two_step_run},
                                 pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
                                 methods={self.nuc_seq_noop}, CRRs={self.noop_crr, self.pass_through_crr})

    def test_crr_nodep_build_removal_plan(self):
        """Removing a CodeResourceRevision that is dependent on another leaves the other alone."""
        self.removal_plan_tester(self.pass_through_crr, CRRs={self.pass_through_crr})

    def test_cr_build_removal_plan(self):
        """Removing a CodeResource removes its revisions."""
        self.removal_plan_tester(self.noop_cr, datasets=self.produced_data.union(
            {self.two_step_intermediate_data, self.two_step_output_data}),
                                 ERs=self.execrecords.union(self.two_step_execrecords),
                                 runs={self.first_run, self.second_run, self.two_step_run},
                                 pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
                                 methods={self.nuc_seq_noop}, CRRs={self.noop_crr, self.pass_through_crr},
                                 CRs={self.noop_cr})

    def test_cdt_build_removal_plan(self):
        """Removing a CompoundDatatype."""
        all_data = self.produced_data.union(
            {
                self.input_DS,
                self.two_step_input_dataset,
                self.two_step_intermediate_data,
                self.two_step_output_data
            }
        )
        self.removal_plan_tester(self.one_col_nuc_seq, datasets=all_data,
                                 ERs=self.execrecords.union(self.two_step_execrecords),
                                 runs={self.first_run, self.second_run, self.two_step_run},
                                 pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
                                 methods={self.nuc_seq_noop}, CDTs={self.one_col_nuc_seq})

    def test_dt_build_removal_plan(self):
        """Removing a Datatype."""
        all_data = self.produced_data.union(
            {
                self.input_DS,
                self.two_step_input_dataset,
                self.two_step_intermediate_data,
                self.two_step_output_data
            }
        )
        self.removal_plan_tester(self.nuc_seq, datasets=all_data, ERs=self.execrecords.union(self.two_step_execrecords),
                                 runs={self.first_run, self.second_run, self.two_step_run},
                                 pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
                                 methods={self.nuc_seq_noop}, CDTs={self.one_col_nuc_seq}, DTs={self.nuc_seq})

    def remove_tester(self, obj_to_remove):
        removal_plan = obj_to_remove.build_removal_plan()

        dataset_pks = [x.pk for x in removal_plan["Datasets"]]
        ER_pks = [x.pk for x in removal_plan["ExecRecords"]]
        run_pks = [x.pk for x in removal_plan["Runs"]]
        pipeline_pks = [x.pk for x in removal_plan["Pipelines"]]
        pf_pks = [x.pk for x in removal_plan["PipelineFamilies"]]
        method_pks = [x.pk for x in removal_plan["Methods"]]
        mf_pks = [x.pk for x in removal_plan["MethodFamilies"]]
        CDT_pks = [x.pk for x in removal_plan["CompoundDatatypes"]]
        DT_pks = [x.pk for x in removal_plan["Datatypes"]]
        CRR_pks = [x.pk for x in removal_plan["CodeResourceRevisions"]]
        CR_pks = [x.pk for x in removal_plan["CodeResources"]]

        obj_to_remove.remove()
        self.assertFalse(Dataset.objects.filter(pk__in=dataset_pks).exists())
        self.assertFalse(ExecRecord.objects.filter(pk__in=ER_pks).exists())
        self.assertFalse(Run.objects.filter(pk__in=run_pks).exists())
        self.assertFalse(Pipeline.objects.filter(pk__in=pipeline_pks).exists())
        self.assertFalse(PipelineFamily.objects.filter(pk__in=pf_pks).exists())
        self.assertFalse(Method.objects.filter(pk__in=method_pks).exists())
        self.assertFalse(MethodFamily.objects.filter(pk__in=mf_pks).exists())
        self.assertFalse(CompoundDatatype.objects.filter(pk__in=CDT_pks).exists())
        self.assertFalse(Datatype.objects.filter(pk__in=DT_pks).exists())
        self.assertFalse(CodeResourceRevision.objects.filter(pk__in=CRR_pks).exists())
        self.assertFalse(CodeResource.objects.filter(pk__in=CR_pks).exists())

    def test_pipeline_remove(self):
        """
        Removing a Pipeline should remove all Runs created from it.
        """
        self.remove_tester(self.noop_pl)

    def test_nested_pipeline_build_removal_plan(self):
        """Removing a nested Pipeline."""
        self.remove_tester(self.p_nested)

    def test_pipelinefamily_remove(self):
        """Removing a PipelineFamily should remove all Pipelines in it."""
        self.remove_tester(self.noop_plf)

    def test_method_remove(self):
        """Removing a Method should remove the Pipelines containing it."""

        self.remove_tester(self.nuc_seq_noop)

    def test_methodfamily_remove(self):
        """Removing a MethodFamily should remove the Methods in it."""
        self.remove_tester(self.nuc_seq_noop_mf)

    def test_crr_remove(self):
        """Removing a CodeResourceRevision should remove the Methods using it, and its dependencies."""
        self.remove_tester(self.noop_crr)

    def test_crr_nodep_remove(self):
        """Removing a CodeResourceRevision that is dependent on another leaves the other alone."""
        self.remove_tester(self.pass_through_crr)

    def test_cr_remove(self):
        """Removing a CodeResource should remove the CodeResourceRevisions using it."""
        self.remove_tester(self.noop_cr)

    def test_cdt_remove(self):
        """Removing a CDT should remove the Methods/Pipelines/Datasets using it."""
        self.remove_tester(self.one_col_nuc_seq)

    def test_datatype_remove(self):
        """Removing a Datatype should remove the CDTs that use it."""
        self.remove_tester(self.nuc_seq)

    def test_dataset_remove(self):
        """Removing a Dataset should remove anything that touches it."""
        self.remove_tester(self.input_DS)

    def test_run_remove(self):
        """Removing a Run."""
        self.remove_tester(self.first_run)

    def test_reused_run_remove(self):
        """Removing a reused Run."""
        self.remove_tester(self.second_run)

    def test_produced_data_remove(self):
        """Removing data produced by the Run should have the same effect as removing the Run itself."""
        produced_dataset = list(self.produced_data)[0]
        self.remove_tester(produced_dataset)

    def test_step_ER_remove(self):
        """Removing the ExecRecord of the first RunStep should be like removing the whole Run."""
        first_step_ER = self.first_run.runsteps.get(pipelinestep__step_num=1).execrecord
        self.remove_tester(first_step_ER)

    def test_rsic_ER_remove(self):
        """Removing the ExecRecord of a RunSIC should be like removing the whole Run."""
        first_RSIC_ER = self.first_run.runsteps.get(pipelinestep__step_num=1).RSICs.first().execrecord
        self.remove_tester(first_RSIC_ER)

    def test_roc_ER_remove(self):
        """Removing the ExecRecord of a RunOutputCable should be like removing the whole Run."""
        first_ROC_ER = self.first_run.runoutputcables.first().execrecord
        self.remove_tester(first_ROC_ER)

    def dataset_redaction_plan_tester(self, dataset_to_redact, datasets=None, output_logs=None, error_logs=None,
                                      return_codes=None):
        redaction_plan = dataset_to_redact.build_redaction_plan()

        # The following ExecRecords should also be in the redaction plan.
        redaction_plan_execrecords = set()
        dataset_set = datasets or set()
        for dataset in dataset_set:
            for eri in dataset.execrecordins.all():
                redaction_plan_execrecords.add(eri.execrecord)

        self.assertSetEqual(redaction_plan["Datasets"], set(datasets) if datasets is not None else set())
        self.assertSetEqual(redaction_plan["OutputLogs"], set(output_logs) if output_logs is not None else set())
        self.assertSetEqual(redaction_plan["ErrorLogs"], set(error_logs) if error_logs is not None else set())
        self.assertSetEqual(redaction_plan["ReturnCodes"], set(return_codes) if return_codes is not None else set())
        self.assertSetEqual(redaction_plan["ExecRecords"], redaction_plan_execrecords)

    def dataset_redaction_tester(self, dataset_to_redact):
        redaction_plan = dataset_to_redact.build_redaction_plan()
        dataset_to_redact.redact()
        self.redaction_tester_helper(redaction_plan)

    def redaction_tester_helper(self, redaction_plan):
        # Check that all of the objects in the plan, and the RunComponents/ExecRecords that
        # reference them, got redacted.
        for dataset in redaction_plan["Datasets"]:
            reloaded_dataset = Dataset.objects.get(pk=dataset.pk)
            self.assertTrue(reloaded_dataset.is_redacted())

        execlogs_affected = redaction_plan["OutputLogs"].union(
            redaction_plan["ErrorLogs"]).union(redaction_plan["ReturnCodes"])
        for log in execlogs_affected:
            reloaded_log = ExecLog.objects.get(pk=log.pk)
            if log in redaction_plan["OutputLogs"]:
                self.assertTrue(reloaded_log.methodoutput.is_output_redacted())
            if log in redaction_plan["ErrorLogs"]:
                self.assertTrue(reloaded_log.methodoutput.is_error_redacted())
            if log in redaction_plan["ReturnCodes"]:
                self.assertTrue(reloaded_log.methodoutput.is_code_redacted())

            self.assertTrue(reloaded_log.is_redacted())
            self.assertTrue(reloaded_log.record.is_redacted())
            if reloaded_log.generated_execrecord():
                self.assertTrue(reloaded_log.execrecord.is_redacted())

        for er in redaction_plan["ExecRecords"]:
            self.assertTrue(er.is_redacted())
            for rc in er.used_by_components.all():
                self.assertTrue(rc.is_redacted())

    def log_redaction_plan_tester(self, log_to_redact, output_log=True, error_log=True, return_code=True):
        output_already_redacted = log_to_redact.methodoutput.is_output_redacted()
        error_already_redacted = log_to_redact.methodoutput.is_error_redacted()
        code_already_redacted = log_to_redact.methodoutput.is_code_redacted()

        redaction_plan = log_to_redact.build_redaction_plan(output_log=output_log, error_log=error_log,
                                                            return_code=return_code)

        self.assertSetEqual(redaction_plan["Datasets"], set())
        self.assertSetEqual(redaction_plan["ExecRecords"], set())
        self.assertSetEqual(redaction_plan["OutputLogs"],
                            {log_to_redact} if output_log and not output_already_redacted else set())
        self.assertSetEqual(redaction_plan["ErrorLogs"],
                            {log_to_redact} if error_log and not error_already_redacted else set())
        self.assertSetEqual(redaction_plan["ReturnCodes"],
                            {log_to_redact} if return_code and not code_already_redacted else set())

    def log_redaction_tester(self, log_to_redact, output_log=True, error_log=True, return_code=True):
        redaction_plan = log_to_redact.build_redaction_plan(output_log, error_log, return_code)

        if output_log:
            log_to_redact.methodoutput.redact_output_log()
        if error_log:
            log_to_redact.methodoutput.redact_error_log()
        if return_code:
            log_to_redact.methodoutput.redact_return_code()

        self.redaction_tester_helper(redaction_plan)

    def test_input_dataset_build_redaction_plan(self):
        """Test redaction of the input dataset to a Run."""
        logs_to_redact = {self.step_log}

        self.dataset_redaction_plan_tester(
            self.input_DS,
            datasets=self.produced_data.union({self.input_DS}),
            output_logs=logs_to_redact,
            error_logs=logs_to_redact,
            return_codes=logs_to_redact
        )

    def test_input_dataset_redact(self):
        self.dataset_redaction_tester(self.input_DS)

    def test_dataset_redact_idempotent(self):
        """Redacting an already-redacted Dataset should give an empty redaction plan."""
        self.input_DS.redact()
        # All of the parameters to this function are None, indicating nothing gets redacted.
        self.dataset_redaction_plan_tester(self.input_DS)

    def test_produced_dataset_build_redaction_plan(self):
        """Redacting produced data."""
        # The run we're dealing with has a single step, and that's the only produced data.
        produced_dataset = list(self.produced_data)[0]

        self.dataset_redaction_plan_tester(
            produced_dataset,
            datasets=self.produced_data
        )

    def test_produced_dataset_redact(self):
        produced_dataset = list(self.produced_data)[0]
        self.dataset_redaction_tester(produced_dataset)

    def test_intermediate_dataset_build_redaction_plan(self):
        """Redacting a Dataset from the middle of a Run only redacts the stuff following it."""
        logs_to_redact = {self.two_step_run.runsteps.get(pipelinestep__step_num=2).log}

        self.dataset_redaction_plan_tester(
            self.two_step_intermediate_data,
            datasets={self.two_step_intermediate_data, self.two_step_output_data},
            output_logs=logs_to_redact,
            error_logs=logs_to_redact,
            return_codes=logs_to_redact
        )

    def test_intermediate_dataset_redact(self):
        self.dataset_redaction_tester(self.two_step_intermediate_data)

    def test_step_log_build_redaction_plan_remove_all(self):
        # There's only one step in self.first_run.
        self.log_redaction_plan_tester(
            self.step_log, True, True, True
        )

    def test_step_log_redact_all(self):
        self.log_redaction_tester(
            self.step_log, True, True, True
        )

    def test_step_log_build_redaction_plan_redact_output_log(self):
        self.log_redaction_plan_tester(
            self.step_log, output_log=True, error_log=False, return_code=False
        )

    def test_step_log_redact_output_log(self):
        self.log_redaction_tester(
            self.step_log, output_log=True, error_log=False, return_code=False
        )

    def test_step_log_build_redaction_plan_redact_error_log(self):
        self.log_redaction_plan_tester(
            self.step_log, output_log=False, error_log=True, return_code=False
        )

    def test_step_log_redact_error_log(self):
        self.log_redaction_tester(
            self.step_log, output_log=False, error_log=True, return_code=False
        )

    def test_step_log_build_redaction_plan_redact_return_code(self):
        self.log_redaction_plan_tester(
            self.step_log, output_log=False, error_log=False, return_code=True
        )

    def test_step_log_redact_return_code(self):
        self.log_redaction_tester(
            self.step_log, output_log=False, error_log=False, return_code=True
        )

    def test_step_log_build_redaction_plan_redact_partially_redacted(self):
        """Redacting something that's been partially redacted should take that into account."""
        self.step_log.methodoutput.redact_output_log()
        self.log_redaction_plan_tester(
            self.step_log, output_log=True, error_log=True, return_code=True
        )


class DatasetWithFileTests(TestCase):

    def setUp(self):
        tools.create_librarian_test_environment(self)

    def tearDown(self):
        tools.clean_up_all_files()

    def test_Dataset_check_MD5(self):
        old_md5 = "7dc85e11b5c02e434af5bd3b3da9938e"
        new_md5 = "d41d8cd98f00b204e9800998ecf8427e"

        self.assertEqual(self.raw_dataset.compute_md5(), old_md5)

        # Initially, no change to the raw dataset has occured, so the md5 check will pass
        self.assertEqual(self.raw_dataset.clean(), None)

        # The contents of the file are changed, disrupting file integrity
        self.raw_dataset.dataset_file.close()
        self.raw_dataset.dataset_file.open(mode='w')
        self.raw_dataset.dataset_file.close()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('File integrity of "{}" lost. Current checksum "{}" does not equal expected '
                                          'checksum "{}"'.format(self.raw_dataset, new_md5, old_md5)),
                                self.raw_dataset.clean)

    def test_Dataset_filename_MD5_clash(self):
        ds1, ds2 = Dataset.objects.all()[:2]
        ds1.name = ds2.name
        ds1.MD5_checksum = ds2.MD5_checksum
        ds1.save()
        msg = "A Dataset with that name and MD5 already exists"
        self.assertRaisesRegexp(ValidationError, msg, ds1.validate_unique)


class DatasetApiTests(BaseTestCases.ApiTestCase):

    def setUp(self):
        super(DatasetApiTests, self).setUp()
        num_cols = 12

        self.list_path = reverse("dataset-list")
        # This should equal librarian.ajax.DatasetViewSet.as_view({"get": "list"}).
        self.list_view, _, _ = resolve(self.list_path)

        with tempfile.TemporaryFile() as f:
            data = ','.join(map(str, range(num_cols)))
            f.write(data)
            f.seek(0)
            self.test_dataset = Dataset.create_dataset(
                file_path=None,
                user=self.kive_user,
                users_allowed=None,
                groups_allowed=None,
                cdt=None,
                keep_file=True,
                name="Test dataset",
                description="Test data for a test that tests test data",
                file_source=None,
                check=True,
                file_handle=f
            )
            self.test_dataset_path = "{}{}/".format(self.list_path,
                                                    self.test_dataset.pk)
            self.n_preexisting_datasets = 1

        self.detail_pk = self.test_dataset.pk
        self.detail_path = reverse("dataset-detail",
                                   kwargs={'pk': self.detail_pk})
        self.redaction_path = reverse("dataset-redaction-plan",
                                      kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("dataset-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.redaction_view, _, _ = resolve(self.redaction_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def tearDown(self):
        for d in Dataset.objects.all():
            d.dataset_file.delete()

    def test_dataset_list(self, expected_entries=0):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)

        force_authenticate(request, user=self.kive_user)
        resp = self.list_view(request).data

        self.assertEquals(len(resp), expected_entries + self.n_preexisting_datasets)
        self.assertEquals(resp[-1]['description'],
                          "Test data for a test that tests test data")

    def test_dataset_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(
            response.data['description'],
            "Test data for a test that tests test data")

    def test_dataset_add(self):
        """
        Test adding a Dataset via the API.

        Each dataset must have unique content.
        """
        num_cols = 12
        num_files = 2
        FROM_FILE_END = 2

        with tempfile.TemporaryFile() as f:
            data = ','.join(map(str, range(num_cols)))
            f.write(data)
            for i in xrange(num_files):
                f.seek(0, FROM_FILE_END)
                f.write('data file {}\n'.format(i))
                f.seek(0)
                request = self.factory.post(
                    self.list_path,
                    {
                        'name': "My cool file %d" % i,
                        'description': 'A really cool file',
                        # No CompoundDatatype -- this is raw.
                        'dataset_file': f
                    }
                )

                force_authenticate(request, user=self.kive_user)
                resp = self.list_view(request).render().data

                self.assertIsNone(resp.get('errors'))
                self.assertEquals(resp['name'], "My cool file %d" % i)

        self.test_dataset_list(expected_entries=num_files)

    def test_dataset_add_duplicate(self):
        """
        Test adding a duplicate Dataset via the API.

        Each dataset must have unique content.
        """
        num_cols = 12

        with tempfile.TemporaryFile() as f:
            data = ','.join(map(str, range(num_cols)))
            f.write(data)
            f.seek(0)

            # First, we add this file and it works.
            request = self.factory.post(
                self.list_path,
                {
                    'name': "Original",
                    'description': 'Totes unique',
                    # No CompoundDatatype -- this is raw.
                    'dataset_file': f
                }
            )
            force_authenticate(request, user=self.kive_user)
            self.list_view(request).render()

            # Now we add the same file again.
            request = self.factory.post(
                self.list_path,
                {
                    'name': "CarbonCopy",
                    'description': "Maybe not so unique",
                    'dataset_file': f
                }
            )
            force_authenticate(request, user=self.kive_user)
            resp = self.list_view(request).render().data

        self.assertEqual({'dataset_file': [u'The submitted file is empty.']},
                         resp)

    def test_dataset_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEquals(response.data['Datasets'], 1)
        self.assertEquals(response.data['CompoundDatatypes'], 0)

    def test_dataset_removal(self):
        start_count = Dataset.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Method.objects.all().count()
        self.assertEquals(end_count, start_count - 1)

    def test_dataset_redaction_plan(self):
        request = self.factory.get(self.redaction_path)
        force_authenticate(request, user=self.kive_user)
        response = self.redaction_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['Datasets'], 1)
        self.assertEquals(response.data['OutputLogs'], 0)

    def test_dataset_redaction(self):

        request = self.factory.patch(self.detail_path,
                                     {'is_redacted': "true"})
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        dataset = Dataset.objects.get(pk=self.detail_pk)
        self.assertTrue(dataset.is_redacted())


class DatasetSerializerTests(TestCase):
    """
    Tests of DatasetSerializer.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        self.factory = APIRequestFactory()
        self.list_path = reverse("dataset-list")

        # This defines a user named "john" which is now accessible as self.myUser.
        tools.create_metadata_test_environment(self)
        self.kive_user = kive_user()
        self.duck_context = DuckContext()

        num_cols = 12
        self.raw_file_contents = ','.join(map(str, range(num_cols)))

        # A CompoundDatatype that belongs to the Kive user.
        self.kive_CDT = CompoundDatatype(user=self.kive_user)
        self.kive_CDT.save()
        self.kive_CDT.members.create(
            datatype=self.string_dt,
            column_name="col1",
            column_idx=1
        )
        self.kive_CDT.full_clean()

        self.kive_file_contents = """col1
foo
bar
baz
"""

        self.data_to_serialize = {
            "name": "SerializedData",
            "description": "Dataset for testing deserialization",
            "users_allowed": [],
            "groups_allowed": []
        }

    def test_validate(self):
        """
        Test validating a new Dataset.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_with_users_allowed(self):
        """
        Test validating a new Dataset with users allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)
            self.data_to_serialize["users_allowed"].append(self.myUser.username)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_with_groups_allowed(self):
        """
        Test validating a new Dataset with groups allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)
            self.data_to_serialize["groups_allowed"].append(everyone_group().name)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_with_CDT(self):
        """
        Test validating a Dataset with a CDT.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.kive_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)
            self.data_to_serialize["compounddatatype"] = self.kive_CDT.pk

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_ineligible_CDT(self):
        """
        Test validating a Dataset with a CDT that the user doesn't have access to.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.kive_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)
            self.data_to_serialize["compounddatatype"] = self.kive_CDT.pk

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=DuckContext(self.myUser)
            )
            self.assertFalse(ds.is_valid())
            self.assertEquals(len(ds.errors["compounddatatype"]), 1)

    def test_create(self):
        """
        Test creating a Dataset.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            # Probe the Dataset to make sure everything looks fine.
            self.assertEquals(dataset.name, self.data_to_serialize["name"])
            self.assertEquals(dataset.description, self.data_to_serialize["description"])
            self.assertIsNone(dataset.compounddatatype)
            self.assertEquals(dataset.user, self.kive_user)

    def test_create_with_CDT(self):
        """
        Test creating a Dataset with a CDT.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.kive_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)
            self.data_to_serialize["compounddatatype"] = self.kive_CDT.pk

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            # Probe to make sure the CDT got set correctly.
            self.assertEquals(dataset.compounddatatype, self.kive_CDT)

    def test_create_with_users_allowed(self):
        """
        Test validating a new Dataset with users allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)
            self.data_to_serialize["users_allowed"].append(self.myUser.username)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            self.assertListEqual(list(dataset.users_allowed.all()),
                                 [self.myUser])

    def test_create_with_groups_allowed(self):
        """
        Test validating a new Dataset with groups allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f)
            self.data_to_serialize["groups_allowed"].append(everyone_group().name)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            self.assertListEqual(list(dataset.groups_allowed.all()),
                                 [everyone_group()])