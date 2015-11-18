import csv
import os
import random
import shutil
import subprocess
import tempfile
import logging
import hashlib

from django.contrib.auth.models import User
from django.core.files import File
from django.db import transaction
from django.db.models import Count

from constants import datatypes
import file_access_utils
from librarian.models import Dataset, ExecRecord
from metadata.models import BasicConstraint, CompoundDatatype, Datatype, everyone_group
from method.models import CodeResource, CodeResourceRevision, Method, MethodFamily
from pipeline.models import Pipeline, PipelineFamily, PipelineStep
from archive.models import RunStep, ExecLog, MethodOutput
from datachecking.models import VerificationLog
from portal.models import StagedFile
import sandbox.execute


samplecode_path = "../samplecode"


# This is copied from
# http://stackoverflow.com/questions/2023608/check-what-files-are-open-in-python
def get_open_fds():
    """
    Return the number of open file descriptors for the current process.

    Warning: will only work on UNIX-like operating systems.
    """
    pid = os.getpid()
    procs = subprocess.check_output(
        ["lsof", '-w', '-Ff', "-p", str(pid)])

    nprocs = len(filter(lambda s: s and s[0] == 'f' and s[1:].isdigit(),
                        procs.split('\n')))
    return nprocs


# For tracking whether we're leaking file descriptors.
fd_count_logger = logging.getLogger("method.tests")


def fd_count(msg):
    fd_count_logger.debug("{}: {}".format(msg, get_open_fds()))


def create_metadata_test_environment(case):
    """Setup default database state from which to perform unit testing."""
    # Define a user.  This was previously in librarian/tests_queuedrunGETRIDOFTHIS,
    # but we put it here now so all tests can use it.
    case.myUser = User.objects.create_user('john',
                                           'lennon@thebeatles.com',
                                           'johnpassword')
    case.myUser.save()
    case.ringoUser = User.objects.create_user('ringo',
                                              'starr@thebeatles.com',
                                              'ringopassword')
    case.ringoUser.save()
    case.myUser.groups.add(everyone_group())
    case.myUser.save()

    # Load up the builtin Datatypes.
    case.STR = Datatype.objects.get(pk=datatypes.STR_PK)
    case.FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
    case.INT = Datatype.objects.get(pk=datatypes.INT_PK)
    case.BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

    # Many tests use case.string_dt as a name for case.STR.
    case.string_dt = case.STR

    # Create Datatype "DNANucSeq" with a regexp basic constraint.
    case.DNA_dt = Datatype(
        name="DNANucSeq",
        description="String consisting of ACGTacgt",
        user=case.myUser)
    case.DNA_dt.save()
    # DNA_dt is a restricted type of string
    case.DNA_dt.restricts.add(case.string_dt)
    case.DNA_dt.grant_everyone_access()
    case.DNA_dt.basic_constraints.create(
        ruletype=BasicConstraint.REGEXP,
        rule="^[ACGTacgt]*$")
    case.DNA_dt.save()

    # Similarly, create Datatype "RNANucSeq".
    case.RNA_dt = Datatype(
        name="RNANucSeq",
        description="String consisting of ACGUacgu",
        user=case.myUser)
    case.RNA_dt.save()
    # RNA_dt is a restricted type of string
    case.RNA_dt.restricts.add(case.string_dt)
    case.RNA_dt.grant_everyone_access()
    case.RNA_dt.basic_constraints.create(
        ruletype=BasicConstraint.REGEXP,
        rule="^[ACGUacgu]*$")
    case.RNA_dt.save()

    # Define a new CDT with a bunch of different member
    case.basic_cdt = CompoundDatatype(user=case.myUser)
    case.basic_cdt.save()
    case.basic_cdt.grant_everyone_access()
    case.basic_cdt.save()

    case.basic_cdt.members.create(
        datatype=case.string_dt,
        column_name='label',
        column_idx=1)
    case.basic_cdt.members.create(
        datatype=case.INT,
        column_name='integer',
        column_idx=2)
    case.basic_cdt.members.create(
        datatype=case.FLOAT,
        column_name='float',
        column_idx=3)
    case.basic_cdt.members.create(
        datatype=case.BOOL,
        column_name='bool',
        column_idx=4)
    case.basic_cdt.members.create(
        datatype=case.RNA_dt,
        column_name="rna",
        column_idx=5)
    case.basic_cdt.full_clean()
    case.basic_cdt.save()

    # Define a new CDT that is only accessible to two users
    shared_cdt = CompoundDatatype(user=case.myUser)
    shared_cdt.save()
    shared_cdt.users_allowed.add(case.ringoUser)
    shared_cdt.save()

    shared_cdt.members.create(
        datatype=case.string_dt,
        column_name='label',
        column_idx=1)

    # Define test_cdt as containing 3 members:
    # (label, PBMCseq, PLAseq) as (string,DNA,RNA)
    case.test_cdt = CompoundDatatype(user=case.myUser)
    case.test_cdt.save()
    case.test_cdt.grant_everyone_access()
    case.test_cdt.save()
    case.test_cdt.members.create(
        datatype=case.string_dt,
        column_name="label",
        column_idx=1)
    case.test_cdt.members.create(
        datatype=case.DNA_dt,
        column_name="PBMCseq",
        column_idx=2)
    case.test_cdt.members.create(
        datatype=case.RNA_dt,
        column_name="PLAseq",
        column_idx=3)
    case.test_cdt.full_clean()
    case.test_cdt.save()

    # Define DNAinput_cdt (1 member)
    case.DNAinput_cdt = CompoundDatatype(user=case.myUser)
    case.DNAinput_cdt.save()
    case.DNAinput_cdt.members.create(
        datatype=case.DNA_dt,
        column_name="SeqToComplement",
        column_idx=1)
    case.DNAinput_cdt.grant_everyone_access()
    case.DNAinput_cdt.full_clean()
    case.DNAinput_cdt.save()

    # Define DNAoutput_cdt (1 member)
    case.DNAoutput_cdt = CompoundDatatype(user=case.myUser)
    case.DNAoutput_cdt.save()
    case.DNAoutput_cdt.members.create(
        datatype=case.DNA_dt,
        column_name="ComplementedSeq",
        column_idx=1)
    case.DNAoutput_cdt.grant_everyone_access()
    case.DNAoutput_cdt.full_clean()
    case.DNAoutput_cdt.save()

    # Define RNAinput_cdt (1 column)
    case.RNAinput_cdt = CompoundDatatype(user=case.myUser)
    case.RNAinput_cdt.save()
    case.RNAinput_cdt.members.create(
        datatype=case.RNA_dt,
        column_name="SeqToComplement",
        column_idx=1)
    case.RNAinput_cdt.grant_everyone_access()
    case.RNAinput_cdt.full_clean()
    case.RNAinput_cdt.save()

    # Define RNAoutput_cdt (1 column)
    case.RNAoutput_cdt = CompoundDatatype(user=case.myUser)
    case.RNAoutput_cdt.save()
    case.RNAoutput_cdt.members.create(
        datatype=case.RNA_dt,
        column_name="ComplementedSeq",
        column_idx=1)
    case.RNAoutput_cdt.grant_everyone_access()
    case.RNAoutput_cdt.full_clean()
    case.RNAoutput_cdt.save()

    ####
    # Everything above this point is used in metadata.tests.
    # This next bit is used in method.tests.

    # Define "tuple" CDT containing (x,y): members x and y exist at index 1 and 2
    case.tuple_cdt = CompoundDatatype(user=case.myUser)
    case.tuple_cdt.save()
    case.tuple_cdt.members.create(datatype=case.string_dt, column_name="x", column_idx=1)
    case.tuple_cdt.members.create(datatype=case.string_dt, column_name="y", column_idx=2)
    case.tuple_cdt.grant_everyone_access()

    # Define "singlet" CDT containing CDT member (a) and "triplet" CDT with members (a,b,c)
    case.singlet_cdt = CompoundDatatype(user=case.myUser)
    case.singlet_cdt.save()
    case.singlet_cdt.members.create(
        datatype=case.string_dt, column_name="k", column_idx=1)
    case.singlet_cdt.grant_everyone_access()

    case.triplet_cdt = CompoundDatatype(user=case.myUser)
    case.triplet_cdt.save()
    case.triplet_cdt.members.create(datatype=case.string_dt, column_name="a", column_idx=1)
    case.triplet_cdt.members.create(datatype=case.string_dt, column_name="b", column_idx=2)
    case.triplet_cdt.members.create(datatype=case.string_dt, column_name="c", column_idx=3)
    case.triplet_cdt.grant_everyone_access()

    ####
    # This next bit is used for pipeline.tests.

    # Define CDT "triplet_squares_cdt" with 3 members for use as an input/output
    case.triplet_squares_cdt = CompoundDatatype(user=case.myUser)
    case.triplet_squares_cdt.save()
    case.triplet_squares_cdt.members.create(datatype=case.string_dt, column_name="a^2", column_idx=1)
    case.triplet_squares_cdt.members.create(datatype=case.string_dt, column_name="b^2", column_idx=2)
    case.triplet_squares_cdt.members.create(datatype=case.string_dt, column_name="c^2", column_idx=3)
    case.triplet_squares_cdt.grant_everyone_access()

    # A CDT with mixed Datatypes
    case.mix_triplet_cdt = CompoundDatatype(user=case.myUser)
    case.mix_triplet_cdt.save()
    case.mix_triplet_cdt.members.create(datatype=case.string_dt, column_name="StrCol1", column_idx=1)
    case.mix_triplet_cdt.members.create(datatype=case.DNA_dt, column_name="DNACol2", column_idx=2)
    case.mix_triplet_cdt.members.create(datatype=case.string_dt, column_name="StrCol3", column_idx=3)
    case.mix_triplet_cdt.grant_everyone_access()

    # Define CDT "doublet_cdt" same as tuple: x, y
    case.doublet_cdt = case.tuple_cdt

    ####
    # Stuff from this point on is used in librarian and archive
    # testing.

    # October 15: more CDTs.
    case.DNA_triplet_cdt = CompoundDatatype(user=case.myUser)
    case.DNA_triplet_cdt.save()
    case.DNA_triplet_cdt.members.create(datatype=case.DNA_dt, column_name="a", column_idx=1)
    case.DNA_triplet_cdt.members.create(datatype=case.DNA_dt, column_name="b", column_idx=2)
    case.DNA_triplet_cdt.members.create(datatype=case.DNA_dt, column_name="c", column_idx=3)
    case.DNA_triplet_cdt.grant_everyone_access()

    case.DNA_doublet_cdt = CompoundDatatype(user=case.myUser)
    case.DNA_doublet_cdt.save()
    case.DNA_doublet_cdt.members.create(datatype=case.DNA_dt, column_name="x", column_idx=1)
    case.DNA_doublet_cdt.members.create(datatype=case.DNA_dt, column_name="y", column_idx=2)
    case.DNA_doublet_cdt.grant_everyone_access()


def load_metadata_test_environment(case):
    case.myUser = User.objects.get(username='john')
    case.ringoUser = User.objects.get(username='ringo')

    case.STR = Datatype.objects.get(pk=datatypes.STR_PK)
    case.FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
    case.INT = Datatype.objects.get(pk=datatypes.INT_PK)
    case.BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)
    case.string_dt = case.STR

    case.DNA_dt = Datatype.objects.get(name="DNANucSeq")
    case.RNA_dt = Datatype.objects.get(name="RNANucSeq")

    case.basic_cdt = CompoundDatatype.objects.get(members__column_name='rna')
    counted = CompoundDatatype.objects.annotate(num_members=Count('members'))
    case.shared_cdt = counted.get(members__column_name='label',
                                  num_members=1)
    case.test_cdt = CompoundDatatype.objects.get(members__column_name='PBMCseq')
    case.DNAinput_cdt = CompoundDatatype.objects.get(
        members__datatype=case.DNA_dt,
        members__column_name='SeqToComplement')
    case.DNAoutput_cdt = CompoundDatatype.objects.get(
        members__datatype=case.DNA_dt,
        members__column_name='ComplementedSeq')
    case.RNAinput_cdt = CompoundDatatype.objects.get(
        members__datatype=case.RNA_dt,
        members__column_name='SeqToComplement')
    case.RNAoutput_cdt = CompoundDatatype.objects.get(
        members__datatype=case.RNA_dt,
        members__column_name='ComplementedSeq')
    case.tuple_cdt = CompoundDatatype.objects.get(
        members__datatype=case.string_dt,
        members__column_name='x')
    case.singlet_cdt = CompoundDatatype.objects.get(members__column_name='k')
    case.triplet_cdt = CompoundDatatype.objects.get(
        members__datatype=case.string_dt,
        members__column_name='a')
    case.triplet_squares_cdt = CompoundDatatype.objects.get(
        members__column_name='a^2')
    case.mix_triplet_cdt = CompoundDatatype.objects.get(
        members__column_name='StrCol1')
    case.doublet_cdt = case.tuple_cdt
    case.DNA_triplet_cdt = CompoundDatatype.objects.get(
        members__datatype=case.DNA_dt,
        members__column_name='a')
    case.DNA_doublet_cdt = CompoundDatatype.objects.get(
        members__datatype=case.DNA_dt,
        members__column_name='x')


def clean_up_all_files():
    """
    Delete all files that have been put into the database as FileFields.
    """
    for crr in CodeResourceRevision.objects.all():
        # Remember that this can be empty.
        # if crr.content_file != None:
        #     crr.content_file.delete()
        # Weirdly, if crr.content_file == None,
        # it still entered the above.  This seems to be a bug
        # in Django!
        if crr.coderesource.filename != "":
            crr.content_file.close()
            crr.content_file.delete()

        crr.delete()

    # Also clear all datasets.  This was previously in librarian.tests
    # but we move it here.
    for dataset in Dataset.objects.all():
        dataset.dataset_file.close()
        dataset.dataset_file.delete()
        dataset.delete()

    for mo in MethodOutput.objects.all():
        mo.output_log.close()
        mo.output_log.delete()
        mo.error_log.close()
        mo.error_log.delete()
        mo.delete()

    for vl in VerificationLog.objects.all():
        vl.output_log.close()
        vl.output_log.delete()
        vl.error_log.close()
        vl.error_log.delete()
        vl.delete()

    for sf in StagedFile.objects.all():
        sf.uploaded_file.close()
        sf.uploaded_file.delete()
        sf.delete()


def create_eric_martin_test_environment(case):
    """
    Set up the original test state Eric Martin designed.

    This sets up the environment as in the Metadata tests, and then augments with
    Methods, CR/CRR/CRDs, and DT/CDTs.  Note that these are *not* the same
    as those set up in the Method testing.
    """
    create_metadata_test_environment(case)

    ####
    # This is the big pipeline Eric developed that was originally
    # used in copperfish/tests.
    # CRs and CRRs:
    case.generic_cr = CodeResource(
        name="genericCR", description="Just a CR",
        filename="generic_script.py", user=case.myUser)
    case.generic_cr.save()
    case.generic_cr.grant_everyone_access()
    with open(os.path.join(samplecode_path, "generic_script.py"), "rb") as f:
        case.generic_crRev = CodeResourceRevision(
            coderesource=case.generic_cr,
            revision_name="v1",
            revision_desc="desc",
            user=case.myUser,
            content_file=File(f)
        )
        case.generic_crRev.clean()
        case.generic_crRev.save()
    case.generic_crRev.grant_everyone_access()

    # Method family, methods, and their input/outputs
    case.mf = MethodFamily(name="method_family",
                           description="Holds methods A/B/C",
                           user=case.myUser)
    case.mf.save()
    case.mf.grant_everyone_access()
    case.mA = Method(revision_name="mA_name", revision_desc="A_desc", family=case.mf, driver=case.generic_crRev,
                     user=case.myUser)
    case.mA.save()
    case.mA.grant_everyone_access()
    case.A1_rawin = case.mA.create_input(dataset_name="A1_rawin", dataset_idx=1)
    case.A1_out = case.mA.create_output(compounddatatype=case.doublet_cdt,
                                        dataset_name="A1_out",
                                        dataset_idx=1)

    case.mB = Method(revision_name="mB_name", revision_desc="B_desc", family=case.mf, driver=case.generic_crRev,
                     user=case.myUser)
    case.mB.save()
    case.mB.grant_everyone_access()
    case.B1_in = case.mB.create_input(compounddatatype=case.doublet_cdt,
                                      dataset_name="B1_in",
                                      dataset_idx=1)
    case.B2_in = case.mB.create_input(compounddatatype=case.singlet_cdt,
                                      dataset_name="B2_in",
                                      dataset_idx=2)
    case.B1_out = case.mB.create_output(compounddatatype=case.triplet_cdt,
                                        dataset_name="B1_out",
                                        dataset_idx=1,
                                        max_row=5)

    case.mC = Method(revision_name="mC_name", revision_desc="C_desc", family=case.mf, driver=case.generic_crRev,
                     user=case.myUser)
    case.mC.save()
    case.mC.grant_everyone_access()
    case.C1_in = case.mC.create_input(compounddatatype=case.triplet_cdt,
                                      dataset_name="C1_in",
                                      dataset_idx=1)
    case.C2_in = case.mC.create_input(compounddatatype=case.doublet_cdt,
                                      dataset_name="C2_in",
                                      dataset_idx=2)
    case.C1_out = case.mC.create_output(compounddatatype=case.singlet_cdt,
                                        dataset_name="C1_out",
                                        dataset_idx=1)
    case.C2_rawout = case.mC.create_output(dataset_name="C2_rawout",
                                           dataset_idx=2)
    case.C3_rawout = case.mC.create_output(dataset_name="C3_rawout",
                                           dataset_idx=3)

    # Pipeline family, pipelines, and their input/outputs
    case.pf = PipelineFamily(name="Pipeline_family", description="PF desc", user=case.myUser)
    case.pf.save()
    case.pf.grant_everyone_access()
    case.pD = Pipeline(family=case.pf, revision_name="pD_name", revision_desc="D", user=case.myUser)
    case.pD.save()
    case.pD.grant_everyone_access()
    case.D1_in = case.pD.create_input(compounddatatype=case.doublet_cdt,
                                      dataset_name="D1_in",
                                      dataset_idx=1)
    case.D2_in = case.pD.create_input(compounddatatype=case.singlet_cdt,
                                      dataset_name="D2_in",
                                      dataset_idx=2)
    case.pE = Pipeline(family=case.pf, revision_name="pE_name", revision_desc="E", user=case.myUser)
    case.pE.save()
    case.pE.grant_everyone_access()
    case.E1_in = case.pE.create_input(compounddatatype=case.triplet_cdt,
                                      dataset_name="E1_in",
                                      dataset_idx=1)
    case.E2_in = case.pE.create_input(compounddatatype=case.singlet_cdt,
                                      dataset_name="E2_in",
                                      dataset_idx=2,
                                      min_row=10)
    case.E3_rawin = case.pE.create_input(dataset_name="E3_rawin",
                                         dataset_idx=3)

    # Pipeline steps
    case.step_D1 = case.pD.steps.create(transformation=case.mB,
                                        step_num=1)
    case.step_E1 = case.pE.steps.create(transformation=case.mA,
                                        step_num=1)
    case.step_E2 = case.pE.steps.create(transformation=case.pD,
                                        step_num=2)
    case.step_E3 = case.pE.steps.create(transformation=case.mC,
                                        step_num=3)

    # Pipeline cables and outcables
    case.D01_11 = case.step_D1.cables_in.create(dest=case.B1_in,
                                                source_step=0,
                                                source=case.D1_in)
    case.D02_12 = case.step_D1.cables_in.create(dest=case.B2_in,
                                                source_step=0,
                                                source=case.D2_in)
    case.D11_21 = case.pD.outcables.create(output_name="D1_out",
                                           output_idx=1,
                                           output_cdt=case.triplet_cdt,
                                           source_step=1,
                                           source=case.B1_out)
    case.pD.create_outputs()
    case.D1_out = case.pD.outputs.get(dataset_name="D1_out")

    case.E03_11 = case.step_E1.cables_in.create(dest=case.A1_rawin,
                                                source_step=0,
                                                source=case.E3_rawin)
    case.E01_21 = case.step_E2.cables_in.create(dest=case.D1_in,
                                                source_step=0,
                                                source=case.E1_in)
    case.E02_22 = case.step_E2.cables_in.create(dest=case.D2_in,
                                                source_step=0,
                                                source=case.E2_in)
    case.E11_32 = case.step_E3.cables_in.create(dest=case.C2_in,
                                                source_step=1,
                                                source=case.A1_out)
    case.E21_31 = case.step_E3.cables_in.create(
        dest=case.C1_in,
        source_step=2,
        source=case.step_E2.transformation.outputs.get(dataset_name="D1_out"))
    case.E21_41 = case.pE.outcables.create(
        output_name="E1_out",
        output_idx=1,
        output_cdt=case.doublet_cdt,
        source_step=2,
        source=case.step_E2.transformation.outputs.get(dataset_name="D1_out"))
    case.E31_42 = case.pE.outcables.create(output_name="E2_out",
                                           output_idx=2,
                                           output_cdt=case.singlet_cdt,
                                           source_step=3,
                                           source=case.C1_out)
    case.E33_43 = case.pE.outcables.create(output_name="E3_rawout",
                                           output_idx=3,
                                           output_cdt=None,
                                           source_step=3,
                                           source=case.C3_rawout)
    case.pE.create_outputs()
    case.E1_out = case.pE.outputs.get(dataset_name="E1_out")
    case.E2_out = case.pE.outputs.get(dataset_name="E2_out")
    case.E3_rawout = case.pE.outputs.get(dataset_name="E3_rawout")

    # Custom wiring/outwiring
    case.E01_21_wire1 = case.E01_21.custom_wires.create(
        source_pin=case.triplet_cdt.members.get(column_idx=1), dest_pin=case.doublet_cdt.members.get(column_idx=2))
    case.E01_21_wire2 = case.E01_21.custom_wires.create(
        source_pin=case.triplet_cdt.members.get(column_idx=3), dest_pin=case.doublet_cdt.members.get(column_idx=1))
    case.E11_32_wire1 = case.E11_32.custom_wires.create(
        source_pin=case.doublet_cdt.members.get(column_idx=1), dest_pin=case.doublet_cdt.members.get(column_idx=2))
    case.E11_32_wire2 = case.E11_32.custom_wires.create(
        source_pin=case.doublet_cdt.members.get(column_idx=2), dest_pin=case.doublet_cdt.members.get(column_idx=1))
    case.E21_41_wire1 = case.E21_41.custom_wires.create(
        source_pin=case.triplet_cdt.members.get(column_idx=2), dest_pin=case.doublet_cdt.members.get(column_idx=2))
    case.E21_41_wire2 = case.E21_41.custom_wires.create(
        source_pin=case.triplet_cdt.members.get(column_idx=3), dest_pin=case.doublet_cdt.members.get(column_idx=1))
    case.pE.clean()

    # Runs for the pipelines.
    case.pD_run = case.pD.pipeline_instances.create(user=case.myUser,
                                                    name='pD_run')
    case.pD_run.save()
    case.pD_run.grant_everyone_access()
    case.pE_run = case.pE.pipeline_instances.create(user=case.myUser,
                                                    name='pE_run')
    case.pE_run.save()
    case.pE_run.grant_everyone_access()

    # November 7, 2013: use a helper function (defined in
    # librarian.models) to define our Datasets.

    # Define singlet, doublet, triplet, and raw uploaded datasets
    case.triplet_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "step_0_triplet.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.triplet_cdt,
        keep_file=True,
        name="triplet",
        description="lol"
    )
    case.triplet_dataset_structure = case.triplet_dataset.structure

    case.doublet_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "doublet_cdt.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.doublet_cdt,
        name="doublet",
        description="lol"
    )
    case.doublet_dataset_structure = case.doublet_dataset.structure

    case.singlet_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "singlet_cdt_large.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.singlet_cdt,
        name="singlet",
        description="lol"
    )
    case.singlet_dataset_structure = case.singlet_dataset.structure

    case.singlet_3rows_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "step_0_singlet.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.singlet_cdt,
        name="singlet",
        description="lol"
    )
    case.singlet_3rows_dataset_structure = case.singlet_3rows_dataset.structure

    case.raw_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "step_0_raw.fasta"),
        user=case.myUser,
        groups_allowed=[everyone_group()],
        cdt=None,
        name="raw_DS",
        description="lol"
    )

    # Added September 30, 2013: dataset that results from E01_21.
    # November 7, 2013: created a file that this Dataset actually represented,
    # even though it isn't in the database.
    case.D1_in_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "doublet_remuxed_from_triplet.csv"),
        user=case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.doublet_cdt,
        keep_file=False
    )
    case.D1_in_dataset_structure = case.D1_in_dataset.structure

    case.C1_in_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "C1_in_triplet.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.triplet_cdt,
        name="C1_in_triplet",
        description="triplet 3 rows"
    )
    case.C1_in_dataset_structure = case.C1_in_dataset.structure

    # November 7, 2013: compute the MD5 checksum from the data file,
    # which is the same as below.
    case.C2_in_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "E11_32_output.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.doublet_cdt,
        keep_file=False
    )
    case.C2_in_dataset_structure = case.C2_in_dataset.structure

    # October 16: an alternative to C2_in_dataset, which has existent data.
    case.E11_32_output_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "E11_32_output.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.doublet_cdt,
        name="E11_32 output doublet",
        description="result of E11_32 fed by doublet_cdt.csv"
    )
    case.E11_32_output_dataset_structure = case.E11_32_output_dataset.structure

    case.C1_out_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "step_0_singlet.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.singlet_cdt,
        name="raw",
        description="lol"
    )
    case.C1_out_dataset_structure = case.C1_out_dataset.structure

    case.C2_out_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "step_0_raw.fasta"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=None,
        name="C2_out",
        description="lol"
    )

    case.C3_out_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "step_0_raw.fasta"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=None,
        name="C3_out",
        description="lol"
    )

    case.triplet_3_rows_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.triplet_cdt,
        name="triplet",
        description="lol"
    )
    case.triplet_3_rows_dataset_structure = case.triplet_3_rows_dataset.structure

    # October 9, 2013: added as the result of cable E21_41.
    case.E1_out_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "doublet_remuxed_from_t3r.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.doublet_cdt,
        name="E1_out",
        description="doublet remuxed from triplet"
    )
    case.E1_out_dataset_structure = case.E1_out_dataset.structure

    # October 15, 2013: Datasets that go into and come out
    # of cable E01_21 and E21_41.
    case.DNA_triplet_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "DNA_triplet.csv"),
        case.myUser,
        groups_allowed=[everyone_group()],
        cdt=case.DNA_triplet_cdt,
        name="DNA_triplet",
        description="DNA triplet data"
    )
    case.DNA_triplet_dataset_structure = case.DNA_triplet_dataset.structure

    case.E01_21_DNA_doublet_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "E01_21_DNA_doublet.csv"),
        case.myUser, groups_allowed=[everyone_group()],
        cdt=case.DNA_doublet_cdt,
        name="E01_21_DNA_doublet",
        description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E01_21"
    )
    case.E01_21_DNA_doublet_dataset_structure = case.E01_21_DNA_doublet_dataset.structure

    case.E21_41_DNA_doublet_dataset = Dataset.create_dataset(
        os.path.join(samplecode_path, "E21_41_DNA_doublet.csv"),
        case.myUser, groups_allowed=[everyone_group()],
        cdt=case.DNA_doublet_cdt,
        name="E21_41_DNA_doublet",
        description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E21_41"
    )
    case.E21_41_DNA_doublet_dataset_structure = case.E21_41_DNA_doublet_dataset.structure


def load_eric_martin_test_environment(case):
    load_metadata_test_environment(case)

    case.generic_cr = CodeResource.objects.get(name="genericCR")
    case.generic_crRev = case.generic_cr.revisions.get()

    case.mf = MethodFamily.objects.get(name="method_family")
    case.mA = Method.objects.get(revision_name="mA_name")
    case.A1_rawin = case.mA.inputs.get()
    case.A1_out = case.mA.outputs.get()

    case.mB = Method.objects.get(revision_name="mB_name")
    case.B1_in = case.mB.inputs.get(dataset_name="B1_in")
    case.B2_in = case.mB.inputs.get(dataset_name="B2_in")
    case.B1_out = case.mB.outputs.get()

    case.mC = Method.objects.get(revision_name="mC_name")
    case.C1_in = case.mC.inputs.get(dataset_name="C1_in")
    case.C2_in = case.mC.inputs.get(dataset_name="C2_in")
    case.C1_out = case.mC.outputs.get(dataset_name="C1_out")
    case.C2_rawout = case.mC.outputs.get(dataset_name="C2_rawout")
    case.C3_rawout = case.mC.outputs.get(dataset_name="C3_rawout")

    case.pf = PipelineFamily.objects.get(name="Pipeline_family")
    case.pD = Pipeline.objects.get(revision_name="pD_name")
    case.D1_in = case.pD.inputs.get(dataset_name="D1_in")
    case.D2_in = case.pD.inputs.get(dataset_name="D2_in")
    case.pE = Pipeline.objects.get(revision_name="pE_name")
    case.E1_in = case.pE.inputs.get(dataset_name="E1_in")
    case.E2_in = case.pE.inputs.get(dataset_name="E2_in")
    case.E3_rawin = case.pE.inputs.get(dataset_name="E3_rawin")

    case.step_D1 = case.pD.steps.get(step_num=1)
    case.step_E1 = case.pE.steps.get(step_num=1)
    case.step_E2 = case.pE.steps.get(step_num=2)
    case.step_E3 = case.pE.steps.get(step_num=3)

    case.D01_11 = case.step_D1.cables_in.get(dest=case.B1_in)
    case.D02_12 = case.step_D1.cables_in.get(dest=case.B2_in)
    case.D11_21 = case.pD.outcables.get(output_name="D1_out")
    case.D1_out = case.pD.outputs.get(dataset_name="D1_out")

    case.E03_11 = case.step_E1.cables_in.get(dest=case.A1_rawin)
    case.E01_21 = case.step_E2.cables_in.get(dest=case.D1_in)
    case.E02_22 = case.step_E2.cables_in.get(dest=case.D2_in)
    case.E11_32 = case.step_E3.cables_in.get(dest=case.C2_in)
    case.E21_31 = case.step_E3.cables_in.get(dest=case.C1_in)
    case.E21_41 = case.pE.outcables.get(output_name="E1_out")
    case.E31_42 = case.pE.outcables.get(output_name="E2_out")
    case.E33_43 = case.pE.outcables.get(output_name="E3_rawout")
    case.E1_out = case.pE.outputs.get(dataset_name="E1_out")
    case.E2_out = case.pE.outputs.get(dataset_name="E2_out")
    case.E3_rawout = case.pE.outputs.get(dataset_name="E3_rawout")

    case.E01_21_wire1 = case.E01_21.custom_wires.get(
        source_pin=case.triplet_cdt.members.get(column_idx=1))
    case.E01_21_wire2 = case.E01_21.custom_wires.get(
        source_pin=case.triplet_cdt.members.get(column_idx=3))
    case.E11_32_wire1 = case.E11_32.custom_wires.get(
        source_pin=case.doublet_cdt.members.get(column_idx=1))
    case.E11_32_wire2 = case.E11_32.custom_wires.get(
        source_pin=case.doublet_cdt.members.get(column_idx=2))
    case.E21_41_wire1 = case.E21_41.custom_wires.get(
        source_pin=case.triplet_cdt.members.get(column_idx=2))
    case.E21_41_wire2 = case.E21_41.custom_wires.get(
        source_pin=case.triplet_cdt.members.get(column_idx=3))

    case.pD_run = case.pD.pipeline_instances.get(name='pD_run')
    case.pE_run = case.pE.pipeline_instances.get(name='pE_run')

    case.triplet_dataset = Dataset.objects.get(
        dataset__name="triplet",
        dataset__dataset_file__endswith="step_0_triplet.csv")
    case.triplet_dataset_structure = case.triplet_dataset.structure
    case.doublet_dataset = Dataset.objects.get(dataset__name="doublet")
    case.doublet_dataset_structure = case.doublet_dataset.structure
    case.singlet_dataset = Dataset.objects.get(
        dataset__name="singlet",
        dataset__dataset_file__endswith="singlet_cdt_large.csv")
    case.singlet_dataset_structure = case.singlet_dataset.structure
    case.singlet_3rows_dataset = Dataset.objects.get(
        dataset__name="singlet",
        dataset__dataset_file__endswith="step_0_singlet.csv")
    case.singlet_3rows_dataset_structure = case.singlet_3rows_dataset.structure
    case.raw_dataset = Dataset.objects.get(dataset__name="raw_DS")

    # MD5 calculated on doublet_remuxed_from_triplet.csv file.
    case.D1_in_dataset = Dataset.objects.get(
        MD5_checksum='542676b23e121d16db8d41ccdae65fd1')
    case.D1_in_dataset_structure = case.D1_in_dataset.structure

    case.C1_in_dataset = Dataset.objects.get(
        dataset__name="C1_in_triplet")
    case.C1_in_dataset_structure = case.C1_in_dataset.structure

    checksum = Dataset.objects.get(
        dataset__dataset_file__endswith="E11_32_output.csv").MD5_checksum
    case.C2_in_dataset = Dataset.objects.get(MD5_checksum=checksum,
                                                   dataset__isnull=True)
    case.C2_in_dataset_structure = case.C2_in_dataset.structure
    case.E11_32_output_dataset = Dataset.objects.get(
        dataset__name="E11_32 output doublet")
    case.E11_32_output_dataset_structure = case.E11_32_output_dataset.structure
    case.C1_out_dataset = Dataset.objects.get(dataset__name="raw")
    case.C1_out_dataset_structure = case.C1_out_dataset.structure
    case.C2_out_dataset = Dataset.objects.get(dataset__name="C2_out")
    case.C3_out_dataset = Dataset.objects.get(dataset__name="C3_out")

    case.triplet_3_rows_dataset = Dataset.objects.get(
        dataset__name="triplet",
        dataset__dataset_file__endswith="step_0_triplet_3_rows.csv")
    case.triplet_3_rows_dataset_structure = case.triplet_3_rows_dataset.structure
    case.E1_out_dataset = Dataset.objects.get(dataset__name="E1_out")
    case.E1_out_dataset_structure = case.E1_out_dataset.structure
    case.DNA_triplet_dataset = Dataset.objects.get(
        dataset__name="DNA_triplet")
    case.DNA_triplet_dataset_structure = case.DNA_triplet_dataset.structure
    case.E01_21_DNA_doublet_dataset = Dataset.objects.get(
        dataset__name="E01_21_DNA_doublet")
    case.E01_21_DNA_doublet_dataset_structure = case.E01_21_DNA_doublet_dataset.structure
    case.E21_41_DNA_doublet_dataset = Dataset.objects.get(
        dataset__name="E21_41_DNA_doublet")
    case.E21_41_DNA_doublet_dataset_structure = case.E21_41_DNA_doublet_dataset.structure


def create_librarian_test_environment(case):
    """
    Set up default state for Librarian unit testing.
    """
    create_eric_martin_test_environment(case)

    # Some ExecRecords, some failed, others not.
    i = 0
    for step in PipelineStep.objects.all():
        if step.is_subpipeline:
            continue
        run = step.pipeline.pipeline_instances.create(user=step.pipeline.user)
        run.save()
        runstep = RunStep(pipelinestep=step, run=run, reused=False)
        runstep.save()
        execlog = ExecLog.create(runstep, runstep)
        execlog.methodoutput.return_code = i % 2
        execlog.methodoutput.save()
        execrecord = ExecRecord(generator=execlog)
        execrecord.save()
        for step_input in step.transformation.inputs.all():
            dataset = Dataset.filter_by_user(step.pipeline.user).filter(
                structure__compounddatatype=step_input.compounddatatype).first()
            execrecord.execrecordins.create(dataset=dataset, generic_input=step_input)
        runstep.execrecord = execrecord
        runstep.save()
        i += 1


def load_librarian_test_environment(case):
    load_eric_martin_test_environment(case)


def create_removal_test_environment():
    # We need:
    # - a CodeResource with revisions
    # - a CodeResourceRevision with dependencies
    # - a Datatype
    # - a CDT using that Datatype
    # - a Dataset with that CDT
    # - a Method using that CDT
    # - a Pipeline containing that Method
    # - two Runs from that pipeline, the second reusing the first
    remover = User.objects.create_user("Rem Over", "rem@over.sucks", "baleeted")
    remover.save()
    remover.groups.add(everyone_group())
    remover.save()

    noop = make_first_revision(
        "Noop",
        "A noop script that simply writes its input to its output.",
        "noop.bash",
        """#!/bin/bash
cat "$1" > "$2"
""",
        remover,
        grant_everyone_access=False
    )

    pass_through = make_first_revision(
        "Pass Through", "A script that does nothing to its input and passes it through untouched.",
        "passthrough.bash",
        """#!/bin/bash
./noop.bash "$1" "$2"
""",
        remover,
        grant_everyone_access=False
    )
    # Use the defaults for path and filename.
    pass_through.dependencies.create(requirement=noop)

    # A toy Datatype.
    nucleotide_seq = new_datatype("Nucleotide sequence", "Sequences of A, C, G, and T",
                                  Datatype.objects.get(pk=datatypes.STR_PK),
                                  remover,
                                  grant_everyone_access=False)
    one_col_nuc_seq = CompoundDatatype(user=remover)
    one_col_nuc_seq.save()
    one_col_nuc_seq.members.create(datatype=nucleotide_seq, column_name="sequence", column_idx=1)

    seq_datafile = tempfile.NamedTemporaryFile(delete=False)
    seq_datafile.write("""sequence
ACGT
ATCG
GATTACA
TTCCTCTA
AAAAAAAG
GGGAGTTC
CCCTCCTC
""")
    seq_datafile.close()
    seq_dataset = Dataset.create_dataset(
        file_path=seq_datafile.name,
        user=remover,
        cdt=one_col_nuc_seq,
        keep_file=True,
        name="Removal test data",
        description="A dataset for use in the removal test case."
    )

    nuc_seq_noop = make_first_method(
        "Noop (nucleotide sequence)",
        "A noop on nucleotide sequences",
        noop,
        remover,
        grant_everyone_access=False
    )
    simple_method_io(nuc_seq_noop, one_col_nuc_seq, "nuc_seq_in", "nuc_seq_out")

    noop_pl = make_first_pipeline(
        "Nucleotide Sequence Noop",
        "A noop pipeline for nucleotide sequences.",
        remover,
        grant_everyone_access=False
        )
    create_linear_pipeline(noop_pl, [nuc_seq_noop], "noop_pipeline_in", "noop_pipeline_out")

    p_nested = make_first_pipeline("Nested pipeline",
                                   "Pipeline with one nested level",
                                   remover,
                                   grant_everyone_access=False)
    create_linear_pipeline(p_nested, [noop_pl, noop_pl], "nested_in", "nested_out")
    p_nested.create_outputs()
    p_nested.save()

    first_run_sdbx = sandbox.execute.Sandbox(remover, noop_pl, [seq_dataset], groups_allowed=[])
    first_run_sdbx.execute_pipeline()
    second_run_sdbx = sandbox.execute.Sandbox(remover, noop_pl, [seq_dataset], groups_allowed=[])
    second_run_sdbx.execute_pipeline()

    two_step_noop_pl = make_first_pipeline(
        "Nucleotide Sequence two-step Noop",
        "A two-step noop pipeline for nucleotide sequences.",
        remover,
        grant_everyone_access=False)
    create_linear_pipeline(two_step_noop_pl,
                           [nuc_seq_noop, nuc_seq_noop],
                           "noop_pipeline_in",
                           "noop_pipeline_out")

    two_step_seq_datafile = tempfile.NamedTemporaryFile(delete=False)
    two_step_seq_datafile.write("""sequence
AAAA
CCCCC
GGGGGG
TTTTTTC
""")
    two_step_seq_datafile.close()
    two_step_seq_dataset = Dataset.create_dataset(
        file_path=two_step_seq_datafile.name,
        user=remover,
        cdt=one_col_nuc_seq, keep_file=True,
        name="Removal test data for a two-step Pipeline",
        description="A dataset for use in the removal test case with the two-step Pipeline."
    )

    two_step_run_sdbx = sandbox.execute.Sandbox(remover, two_step_noop_pl, [two_step_seq_dataset], groups_allowed=[])
    two_step_run_sdbx.execute_pipeline()


def create_sandbox_testing_tools_environment(case):
    case.STR = Datatype.objects.get(pk=datatypes.STR_PK)

    # An ordinary user.
    case.user_bob = User.objects.create_user('bob', 'bob@talabs.com', 'verysecure')
    case.user_bob.save()
    case.user_bob.groups.add(everyone_group())
    case.user_bob.save()

    # Predefined datatypes.
    case.datatype_str = new_datatype("my_string", "sequences of ASCII characters", case.STR, case.user_bob)
    case.datatype_str.grant_everyone_access()

    # A CDT composed of only one column, strings.
    case.cdt_string = CompoundDatatype(user=case.user_bob)
    case.cdt_string.save()
    case.cdt_string.members.create(datatype=case.datatype_str, column_name="word", column_idx=1)
    case.cdt_string.grant_everyone_access()

    # A code resource which does nothing.
    case.coderev_noop = make_first_revision(
        "noop", "a script to do nothing", "noop.sh",
        '#!/bin/bash\n cat "$1" > "$2"',
        case.user_bob)
    case.coderev_noop.coderesource.grant_everyone_access()
    case.coderev_noop.grant_everyone_access()

    # A Method telling Shipyard how to use the noop code on string data.
    case.method_noop = make_first_method("string noop", "a method to do nothing to strings", case.coderev_noop,
                                         case.user_bob)
    case.method_noop.family.grant_everyone_access()
    case.method_noop.grant_everyone_access()
    simple_method_io(case.method_noop, case.cdt_string, "strings", "same_strings")

    # Another totally different Method that uses the same CodeRevision and yes it does the same thing.
    case.method_trivial = make_first_method(
        "string trivial",
        "a TOTALLY DIFFERENT method that TOTALLY does SOMETHING to strings by leaving them alone",
        case.coderev_noop,
        case.user_bob)
    case.method_trivial.family.grant_everyone_access()
    case.method_trivial.grant_everyone_access()
    simple_method_io(case.method_trivial, case.cdt_string, "strings", "untouched_strings")

    # A third one, only this one takes raw input.
    case.method_noop_raw = make_first_method("raw noop", "do nothing to raw data", case.coderev_noop,
                                             case.user_bob)
    case.method_noop_raw.family.grant_everyone_access()
    case.method_noop_raw.grant_everyone_access()
    simple_method_io(case.method_noop_raw, None, "raw", "same_raw")


def load_sandbox_testing_tools_environment(case):
    case.STR = Datatype.objects.get(pk=datatypes.STR_PK)
    case.user_bob = User.objects.get(username='bob')
    case.datatype_str = Datatype.objects.get(name="my_string")
    case.cdt_string = CompoundDatatype.objects.get(members__column_name="word")
    case.coderev_noop = CodeResourceRevision.objects.get(
        coderesource__name='noop')

    case.method_noop = Method.objects.get(family__name="string noop")
    case.method_trivial = Method.objects.get(family__name="string trivial")
    case.method_noop_raw = Method.objects.get(family__name="raw noop")


def destroy_sandbox_testing_tools_environment(case):
    """
    Clean up a TestCase where create_sandbox_testing_tools_environment has been called.
    # """
    clean_up_all_files()


def create_archive_test_environment(case):
    create_librarian_test_environment(case)
    create_sandbox_testing_tools_environment(case)


def load_archive_test_environment(case):
    load_librarian_test_environment(case)
    load_sandbox_testing_tools_environment(case)


def create_method_test_environment(case):
    """Set up default database state that includes some CRs, CRRs, Methods, etc."""
    # This sets up the DTs and CDTs used in our metadata tests.
    create_metadata_test_environment(case)

    fd_count("FD count on environment creation")

    # Define comp_cr
    case.comp_cr = CodeResource(
        name="complement",
        description="Complement DNA/RNA nucleotide sequences",
        filename="complement.py",
        user=case.myUser)
    case.comp_cr.save()
    case.comp_cr.grant_everyone_access()

    # Define compv1_crRev for comp_cr
    fn = "complement.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        case.compv1_crRev = CodeResourceRevision(
            coderesource=case.comp_cr,
            revision_name="v1",
            revision_desc="First version",
            content_file=File(f),
            user=case.myUser)
        # case.compv1_crRev.content_file.save(fn, File(f))
        case.compv1_crRev.full_clean()
        case.compv1_crRev.save()
    case.compv1_crRev.grant_everyone_access()

    # Define compv2_crRev for comp_cr
    fn = "complement_v2.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        case.compv2_crRev = CodeResourceRevision(
            coderesource=case.comp_cr,
            revision_name="v2",
            revision_desc="Second version: better docstring",
            revision_parent=case.compv1_crRev,
            content_file=File(f),
            user=case.myUser)
        # case.compv2_crRev.content_file.save(fn, File(f))
        case.compv2_crRev.full_clean()
        case.compv2_crRev.save()
    case.compv2_crRev.grant_everyone_access()

    # Define DNA reference to use as a dependency
    dna_resource = CodeResource(
        name="dna_ref",
        description="Reference DNA sequences",
        filename="good_dna.csv",
        user=case.myUser)
    dna_resource.save()
    dna_resource.grant_everyone_access()
    fn = "GoodDNANucSeq.csv"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        dna_resource_revision = CodeResourceRevision(
            coderesource=dna_resource,
            revision_name="Prototype",
            revision_desc="Reference DNA sequences",
            content_file=File(f),
            user=case.myUser)
        # case.compv2_crRev.content_file.save(fn, File(f))
        dna_resource_revision.full_clean()
        dna_resource_revision.save()
    dna_resource_revision.grant_everyone_access()

    case.compv2_crRev.dependencies.create(
        requirement=dna_resource_revision)

    # The following is for testing code resource dependencies.
    case.test_cr_1 = CodeResource(name="test_cr_1",
                                  filename="test_cr_1.py",
                                  description="CR1",
                                  user=case.myUser)
    case.test_cr_1.save()
    case.test_cr_1.grant_everyone_access()
    case.test_cr_1_rev1 = CodeResourceRevision(coderesource=case.test_cr_1,
                                               revision_name="v1",
                                               revision_desc="CR1-rev1",
                                               user=case.myUser)

    case.test_cr_2 = CodeResource(name="test_cr_2",
                                  filename="test_cr_2.py",
                                  description="CR2",
                                  user=case.myUser)
    case.test_cr_2.save()
    case.test_cr_2.grant_everyone_access()
    case.test_cr_2_rev1 = CodeResourceRevision(coderesource=case.test_cr_2,
                                               revision_name="v1",
                                               revision_desc="CR2-rev1",
                                               user=case.myUser)

    case.test_cr_3 = CodeResource(name="test_cr_3",
                                  filename="test_cr_3.py",
                                  description="CR3",
                                  user=case.myUser)
    case.test_cr_3.save()
    case.test_cr_3.grant_everyone_access()
    case.test_cr_3_rev1 = CodeResourceRevision(coderesource=case.test_cr_3,
                                               revision_name="v1",
                                               revision_desc="CR3-rev1",
                                               user=case.myUser)
    case.test_cr_3_rev1.save()

    case.test_cr_4 = CodeResource(name="test_cr_4",
                                  filename="test_cr_4.py",
                                  description="CR4",
                                  user=case.myUser)
    case.test_cr_4.save()
    case.test_cr_4.grant_everyone_access()
    case.test_cr_4_rev1 = CodeResourceRevision(coderesource=case.test_cr_4,
                                               revision_name="v1",
                                               revision_desc="CR4-rev1",
                                               user=case.myUser)
    case.test_cr_4_rev1.save()

    fn = "test_cr.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        md5gen = hashlib.md5()
        md5gen.update(f.read())
        f.seek(0)
        for crr in [case.test_cr_1_rev1, case.test_cr_2_rev1, case.test_cr_3_rev1, case.test_cr_4_rev1]:
            crr.MD5_checksum = md5gen.hexdigest()
            crr.content_file.save(fn, File(f))

    for crr in [case.test_cr_1_rev1, case.test_cr_2_rev1, case.test_cr_3_rev1, case.test_cr_4_rev1]:
        crr.save()
        crr.grant_everyone_access()

    # Define DNAcomp_mf
    case.DNAcomp_mf = MethodFamily(
        name="DNAcomplement",
        description="Complement DNA nucleotide sequences.",
        user=case.myUser)
    case.DNAcomp_mf.full_clean()
    case.DNAcomp_mf.save()
    case.DNAcomp_mf.grant_everyone_access()

    # Define DNAcompv1_m (method revision) for DNAcomp_mf with driver compv1_crRev
    case.DNAcompv1_m = case.DNAcomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=case.compv1_crRev,
        user=case.myUser)
    case.DNAcompv1_m.grant_everyone_access()

    # Add input DNAinput_cdt to DNAcompv1_m
    case.DNAinput_ti = case.DNAcompv1_m.create_input(
        compounddatatype=case.DNAinput_cdt,
        dataset_name="input",
        dataset_idx=1)
    case.DNAinput_ti.full_clean()
    case.DNAinput_ti.save()

    # Add output DNAoutput_cdt to DNAcompv1_m
    case.DNAoutput_to = case.DNAcompv1_m.create_output(
        compounddatatype=case.DNAoutput_cdt,
        dataset_name="output",
        dataset_idx=1)
    case.DNAoutput_to.full_clean()
    case.DNAoutput_to.save()

    # Define DNAcompv2_m for DNAcomp_mf with driver compv2_crRev
    # May 20, 2014: where previously the inputs/outputs would be
    # automatically copied over from the parent using save(), now
    # we explicitly call copy_io_from_parent.
    case.DNAcompv2_m = case.DNAcomp_mf.members.create(
        revision_name="v2",
        revision_desc="Second version",
        revision_parent=case.DNAcompv1_m,
        driver=case.compv2_crRev,
        user=case.myUser)
    case.DNAcompv2_m.full_clean()
    case.DNAcompv2_m.save()
    case.DNAcompv2_m.grant_everyone_access()
    case.DNAcompv2_m.copy_io_from_parent()

    # Define second family, RNAcomp_mf
    case.RNAcomp_mf = MethodFamily(
        name="RNAcomplement",
        description="Complement RNA nucleotide sequences.",
        user=case.myUser)
    case.RNAcomp_mf.full_clean()
    case.RNAcomp_mf.save()
    case.RNAcomp_mf.grant_everyone_access()

    # Define RNAcompv1_m for RNAcomp_mf with driver compv1_crRev
    case.RNAcompv1_m = case.RNAcomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=case.compv1_crRev,
        user=case.myUser)
    case.RNAcompv1_m.grant_everyone_access()

    # Add input RNAinput_cdt to RNAcompv1_m
    case.RNAinput_ti = case.RNAcompv1_m.create_input(
        compounddatatype=case.RNAinput_cdt,
        dataset_name="input",
        dataset_idx=1)
    case.RNAinput_ti.full_clean()
    case.RNAinput_ti.save()

    # Add output RNAoutput_cdt to RNAcompv1_m
    case.RNAoutput_to = case.RNAcompv1_m.create_output(
        compounddatatype=case.RNAoutput_cdt,
        dataset_name="output",
        dataset_idx=1)
    case.RNAoutput_to.full_clean()
    case.RNAoutput_to.save()

    # Define RNAcompv2_m for RNAcompv1_mf with driver compv2_crRev
    # May 20, 2014: again, we now explicitly copy over the inputs/outputs.
    case.RNAcompv2_m = case.RNAcomp_mf.members.create(
        revision_name="v2",
        revision_desc="Second version",
        revision_parent=case.RNAcompv1_m,
        driver=case.compv2_crRev,
        user=case.myUser)
    case.RNAcompv2_m.full_clean()
    case.RNAcompv2_m.save()
    case.RNAcompv2_m.copy_io_from_parent()
    case.RNAcompv2_m.grant_everyone_access()

    # Create method family for script_1_method / script_2_method / script_3_method
    case.test_mf = MethodFamily(name="Test method family",
                                description="Holds scripts 1/2/3",
                                user=case.myUser)
    case.test_mf.full_clean()
    case.test_mf.save()
    case.test_mf.grant_everyone_access()

    # script_1_sum_and_outputs.py
    # INPUT: 1 csv containing (x,y)
    # OUTPUT: 1 csv containing (x+y,xy)
    case.script_1_cr = CodeResource(name="Sum and product of x and y",
                                    filename="script_1_sum_and_products.py",
                                    description="Addition and multiplication",
                                    user=case.myUser)
    case.script_1_cr.save()
    case.script_1_cr.grant_everyone_access()

    # Add code resource revision for code resource (script_1_sum_and_products )
    fn = "script_1_sum_and_products.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        case.script_1_crRev = CodeResourceRevision(
            coderesource=case.script_1_cr,
            revision_name="v1",
            revision_desc="First version",
            user=case.myUser,
            content_file=File(f)
        )
        case.script_1_crRev.full_clean()
        case.script_1_crRev.save()
    case.script_1_crRev.grant_everyone_access()

    # Establish code resource revision as a method
    case.script_1_method = Method(
        revision_name="script1",
        revision_desc="script1",
        family=case.test_mf,
        driver=case.script_1_crRev,
        user=case.myUser)
    case.script_1_method.save()
    case.script_1_method.grant_everyone_access()

    # Assign tuple as both an input and an output to script_1_method
    case.script_1_method.create_input(compounddatatype=case.tuple_cdt,
                                      dataset_name="input_tuple",
                                      dataset_idx=1)
    case.script_1_method.create_output(compounddatatype=case.tuple_cdt,
                                       dataset_name="input_tuple",
                                       dataset_idx=1)
    case.script_1_method.full_clean()
    case.script_1_method.save()

    # script_2_square_and_means
    # INPUT: 1 csv containing (a,b,c)
    # OUTPUT-1: 1 csv containing triplet (a^2,b^2,c^2)
    # OUTPUT-2: 1 csv containing singlet mean(a,b,c)
    case.script_2_cr = CodeResource(name="Square and mean of (a,b,c)",
                                    filename="script_2_square_and_means.py",
                                    description="Square and mean - 2 CSVs",
                                    user=case.myUser)
    case.script_2_cr.save()
    case.script_2_cr.grant_everyone_access()

    # Add code resource revision for code resource (script_2_square_and_means)
    fn = "script_2_square_and_means.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        case.script_2_crRev = CodeResourceRevision(
            coderesource=case.script_2_cr,
            revision_name="v1",
            revision_desc="First version",
            user=case.myUser,
            content_file=File(f))
        case.script_2_crRev.full_clean()
        case.script_2_crRev.save()
    case.script_2_crRev.grant_everyone_access()

    # Establish code resource revision as a method
    case.script_2_method = Method(
        revision_name="script2",
        revision_desc="script2",
        family=case.test_mf,
        driver=case.script_2_crRev,
        user=case.myUser)
    case.script_2_method.save()
    case.script_2_method.grant_everyone_access()

    # Assign triplet as input and output,
    case.script_2_method.create_input(
        compounddatatype=case.triplet_cdt,
        dataset_name="a_b_c",
        dataset_idx=1)
    case.script_2_method.create_output(
        compounddatatype=case.triplet_cdt,
        dataset_name="a_b_c_squared",
        dataset_idx=1)
    case.script_2_method.create_output(
        compounddatatype=case.singlet_cdt,
        dataset_name="a_b_c_mean",
        dataset_idx=2)
    case.script_2_method.full_clean()
    case.script_2_method.save()

    # script_3_product
    # INPUT-1: Single column (k)
    # INPUT-2: Single-row, single column (r)
    # OUTPUT-1: Single column r*(k)
    case.script_3_cr = CodeResource(name="Scalar multiple of k",
                                    filename="script_3_product.py",
                                    description="Product of input",
                                    user=case.myUser)
    case.script_3_cr.save()
    case.script_3_cr.grant_everyone_access()

    # Add code resource revision for code resource (script_3_product)
    with open(os.path.join(samplecode_path, "script_3_product.py"), "rb") as f:
        case.script_3_crRev = CodeResourceRevision(
            coderesource=case.script_3_cr,
            revision_name="v1",
            revision_desc="First version",
            content_file=File(f),
            user=case.myUser)
        case.script_3_crRev.full_clean()
        case.script_3_crRev.save()
    case.script_3_crRev.grant_everyone_access()

    # Establish code resource revision as a method
    case.script_3_method = Method(
        revision_name="script3",
        revision_desc="script3",
        family=case.test_mf,
        driver=case.script_3_crRev,
        user=case.myUser)
    case.script_3_method.save()
    case.script_3_method.grant_everyone_access()

    # Assign singlet as input and output
    case.script_3_method.create_input(compounddatatype=case.singlet_cdt,
                                      dataset_name="k",
                                      dataset_idx=1)

    case.script_3_method.create_input(compounddatatype=case.singlet_cdt,
                                      dataset_name="r",
                                      dataset_idx=2,
                                      max_row=1,
                                      min_row=1)

    case.script_3_method.create_output(compounddatatype=case.singlet_cdt,
                                       dataset_name="kr",
                                       dataset_idx=1)
    case.script_3_method.full_clean()
    case.script_3_method.save()

    ####
    # This next bit was originally in pipeline.tests.

    # DNArecomp_mf is a MethodFamily called DNArecomplement
    case.DNArecomp_mf = MethodFamily(
        name="DNArecomplement",
        description="Re-complement DNA nucleotide sequences.",
        user=case.myUser)
    case.DNArecomp_mf.full_clean()
    case.DNArecomp_mf.save()
    case.DNArecomp_mf.grant_everyone_access()

    # Add to MethodFamily DNArecomp_mf a method revision DNArecomp_m
    case.DNArecomp_m = case.DNArecomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=case.compv2_crRev,
        user=case.myUser)
    case.DNArecomp_m.grant_everyone_access()

    # To this method revision, add inputs with CDT DNAoutput_cdt
    case.DNArecomp_m.create_input(
        compounddatatype=case.DNAoutput_cdt,
        dataset_name="complemented_seqs",
        dataset_idx=1)

    # To this method revision, add outputs with CDT DNAinput_cdt
    case.DNArecomp_m.create_output(
        compounddatatype=case.DNAinput_cdt,
        dataset_name="recomplemented_seqs",
        dataset_idx=1)

    # Setup used in the "2nd-wave" tests (this was originally in
    # Copperfish_Raw_Setup).

    # Define CR "script_4_raw_in_CSV_out.py"
    # input: raw [but contains (a,b,c) triplet]
    # output: CSV [3 CDT members of the form (a^2, b^2, c^2)]

    # Define CR in order to define CRR
    case.script_4_CR = CodeResource(
        name="Generate (a^2, b^2, c^2) using RAW input",
        filename="script_4_raw_in_CSV_out.py",
        description="Given (a,b,c), outputs (a^2,b^2,c^2)",
        user=case.myUser)
    case.script_4_CR.save()
    case.script_4_CR.grant_everyone_access()

    # Define CRR for this CR in order to define method
    with open(os.path.join(samplecode_path, "script_4_raw_in_CSV_out.py"), "rb") as f:
        case.script_4_1_CRR = CodeResourceRevision(
            coderesource=case.script_4_CR,
            revision_name="v1",
            revision_desc="v1",
            content_file=File(f),
            user=case.myUser)
        case.script_4_1_CRR.full_clean()
        case.script_4_1_CRR.save()
    case.script_4_1_CRR.grant_everyone_access()

    # Define MF in order to define method
    case.test_MF = MethodFamily(
        name="test method family",
        description="method family placeholder",
        user=case.myUser)
    case.test_MF.full_clean()
    case.test_MF.save()
    case.test_MF.grant_everyone_access()

    # Establish CRR as a method within a given method family
    case.script_4_1_M = Method(
        revision_name="s4",
        revision_desc="s4",
        family=case.test_MF,
        driver=case.script_4_1_CRR,
        user=case.myUser)
    case.script_4_1_M.save()
    case.script_4_1_M.grant_everyone_access()

    case.script_4_1_M.create_input(compounddatatype=case.triplet_cdt,
                                   dataset_name="s4_input",
                                   dataset_idx=1)
    case.script_4_1_M.full_clean()

    # A shorter alias
    case.testmethod = case.script_4_1_M

    # Some code for a no-op method.
    resource = CodeResource(name="noop", filename="noop.sh", user=case.myUser)
    resource.save()
    resource.grant_everyone_access()
    with tempfile.NamedTemporaryFile() as f:
        f.write("#!/bin/bash\ncat $1")
        case.noop_data_file = f.name
        revision = CodeResourceRevision(coderesource=resource,
                                        content_file=File(f),
                                        user=case.myUser)
        revision.clean()
        revision.save()
        revision.grant_everyone_access()

    # Retrieve the string type.
    string_dt = Datatype.objects.get(pk=datatypes.STR_PK)
    string_cdt = CompoundDatatype(user=case.myUser)
    string_cdt.save()
    string_cdt.members.create(datatype=string_dt, column_name="word", column_idx=1)
    string_cdt.grant_everyone_access()
    string_cdt.full_clean()

    mfamily = MethodFamily(name="noop", user=case.myUser)
    mfamily.save()
    mfamily.grant_everyone_access()
    case.noop_method = Method(family=mfamily,
                              driver=revision,
                              revision_name="1",
                              revision_desc="first version",
                              user=case.myUser)
    case.noop_method.save()
    case.noop_method.create_input(compounddatatype=string_cdt,
                                  dataset_name="noop_data",
                                  dataset_idx=1)
    case.noop_method.grant_everyone_access()
    case.noop_method.full_clean()

    # Some data.
    case.scratch_dir = tempfile.mkdtemp(
        dir=file_access_utils.sandbox_base_path()
    )
    file_access_utils.configure_sandbox_permissions(case.scratch_dir)
    try:
        fd, case.noop_infile = tempfile.mkstemp(dir=case.scratch_dir)
    finally:
        os.close(fd)
    try:
        fd, case.noop_outfile = tempfile.mkstemp(dir=case.scratch_dir)
    finally:
        os.close(fd)
    case.noop_indata = "word\nhello\nworld"

    with open(case.noop_infile, "w") as handle:
        handle.write(case.noop_indata)

    file_access_utils.configure_sandbox_permissions(case.noop_infile)
    file_access_utils.configure_sandbox_permissions(case.noop_outfile)


def destroy_method_test_environment(case):
    """
    Clean up a TestCase where create_method_test_environment has been called.
    """
    clean_up_all_files()
    shutil.rmtree(case.scratch_dir)
    CodeResource.objects.all().delete()


def create_pipeline_test_environment(case):
    """
    Sets up default database state for some Pipeline unit testing.

    This also sets up Methods, CR/CRR/CRDs, and DTs/CDTs as in the Metadata and Methods tests.
    """
    create_method_test_environment(case)
    case.workdir = tempfile.mkdtemp()

    case.user = User.objects.create_user('bob', 'bob@aol.com', '12345')
    case.user.save()

    # Define DNAcomp_pf
    case.DNAcomp_pf = PipelineFamily(name="DNAcomplement", description="DNA complement pipeline.",
                                     user=case.user)
    case.DNAcomp_pf.save()

    # Define DNAcompv1_p (pipeline revision)
    case.DNAcompv1_p = case.DNAcomp_pf.members.create(revision_name="v1", revision_desc="First version",
                                                      user=case.user)

    # Add Pipeline input CDT DNAinput_cdt to pipeline revision DNAcompv1_p
    case.DNAcompv1_p.create_input(
        compounddatatype=case.DNAinput_cdt,
        dataset_name="seqs_to_complement",
        dataset_idx=1)

    # Add a step to Pipeline revision DNAcompv1_p involving
    # a transformation DNAcompv2_m at step 1
    step1 = case.DNAcompv1_p.steps.create(
        transformation=case.DNAcompv2_m,
        step_num=1)

    # Add cabling (PipelineStepInputCable's) to (step1, DNAcompv1_p)
    # From step 0, output hole "seqs_to_complement" to
    # input hole "input" (of this step)
    step1.cables_in.create(dest=case.DNAcompv2_m.inputs.get(dataset_name="input"), source_step=0,
                           source=case.DNAcompv1_p.inputs.get(dataset_name="seqs_to_complement"))

    # Add output cabling (PipelineOutputCable) to DNAcompv1_p
    # From step 1, output hole "output", send output to
    # Pipeline output hole "complemented_seqs" at index 1
    case.DNAcompv1_p.create_outcable(source_step=1,
                                     source=step1.transformation.outputs.get(dataset_name="output"),
                                     output_name="complemented_seqs", output_idx=1)

    # Define PF in order to define pipeline
    case.test_PF = PipelineFamily(
        name="test pipeline family",
        description="pipeline family placeholder",
        user=case.user)
    case.test_PF.full_clean()
    case.test_PF.save()

    # Set up an empty Pipeline.
    family = PipelineFamily.filter_by_user(case.user).first()

    # Nothing defined.
    p = Pipeline(family=family, revision_name="foo", revision_desc="Foo version", user=case.user)
    p.save()


def destroy_pipeline_test_environment(case):
    """
    Clean up a TestCase where create_pipeline_test_environment has been called.
    """
    destroy_method_test_environment(case)
    Dataset.objects.all().delete()
    shutil.rmtree(case.workdir)


def create_sequence_manipulation_environment(case):
    create_sandbox_testing_tools_environment(case)

    # Alice is a Shipyard user.
    case.user_alice = User.objects.create_user('alice', 'alice@talabs.com', 'secure')
    case.user_alice.save()
    case.user_alice.groups.add(everyone_group())
    case.user_alice.save()

    # Alice's lab has two tasks - complement DNA, and reverse and complement DNA.
    # She wants to create a pipeline for each. In the background, this also creates
    # two new pipeline families.
    case.pipeline_complement = make_first_pipeline("DNA complement", "a pipeline to complement DNA", case.user_alice)
    case.pipeline_reverse = make_first_pipeline("DNA reverse", "a pipeline to reverse DNA", case.user_alice)
    case.pipeline_revcomp = make_first_pipeline("DNA revcomp", "a pipeline to reverse and complement DNA",
                                                case.user_alice)

    # Alice is only going to be manipulating DNA, so she creates a "DNA"
    # data type. A "string" datatype, which she will use for the headers,
    # has been predefined in Shipyard. She also creates a compound "record"
    # datatype for sequence + header.
    case.datatype_dna = new_datatype("DNA", "sequences of ATCG", case.STR, case.user_alice)
    case.cdt_record = CompoundDatatype(user=case.user_alice)
    case.cdt_record.save()
    case.cdt_record.members.create(datatype=case.datatype_str, column_name="header", column_idx=1)
    case.cdt_record.members.create(datatype=case.datatype_dna, column_name="sequence", column_idx=2)
    case.cdt_record.grant_everyone_access()

    # Alice uploads code to perform each of the tasks. In the background,
    # Shipyard creates new CodeResources for these scripts and sets her
    # uploaded files as the first CodeResourceRevisions.
    case.coderev_complement = make_first_revision(
        "DNA complement", "a script to complement DNA",
        "complement.sh",
        """#!/bin/bash
        cat "$1" | cut -d ',' -f 2 | tr 'ATCG' 'TAGC' | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
        """,
        case.user_alice)
    case.coderev_reverse = make_first_revision(
        "DNA reverse", "a script to reverse DNA", "reverse.sh",
        """#!/bin/bash
        cat "$1" | cut -d ',' -f 2 | rev | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
        """,
        case.user_alice)

    # To tell the system how to use her code, Alice creates two Methods,
    # one for each CodeResource. In the background, this creates two new
    # MethodFamilies with her Methods as the first member of each.
    case.method_complement = make_first_method("DNA complement", "a method to complement strings of DNA",
                                               case.coderev_complement,
                                               case.user_alice)
    simple_method_io(case.method_complement, case.cdt_record, "DNA_to_complement", "complemented_DNA")
    case.method_reverse = make_first_method("DNA reverse", "a method to reverse strings of DNA",
                                            case.coderev_complement,
                                            case.user_alice)
    simple_method_io(case.method_reverse, case.cdt_record, "DNA_to_reverse", "reversed_DNA")

    # Now Alice is ready to define her pipelines. She uses the GUI to drag
    # the "complement" method into the "complement" pipeline, creates
    # the pipeline's input and output, and connects them to the inputs and
    # output of the method.
    create_linear_pipeline(case.pipeline_complement, [case.method_complement], "lab_data",
                           "complemented_lab_data")
    case.pipeline_complement.create_outputs()
    create_linear_pipeline(case.pipeline_reverse, [case.method_reverse], "lab_data", "reversed_lab_data")
    case.pipeline_reverse.create_outputs()
    create_linear_pipeline(case.pipeline_revcomp, [case.method_reverse, case.method_complement], "lab_data",
                           "reverse_and_complemented_lab_data")
    case.pipeline_revcomp.create_outputs()

    # Here is some data which is sitting on Alice's hard drive.
    random.seed("Constant seed avoids intermittent failures.")
    case.labdata = "header,sequence\n"
    for i in range(10):
        seq = "".join([random.choice("ATCG") for _ in range(10)])
        case.labdata += "patient{},{}\n".format(i, seq)
    case.datafile = tempfile.NamedTemporaryFile(
        delete=False,
        dir=file_access_utils.sandbox_base_path()
    )
    case.datafile.write(case.labdata)
    case.datafile.close()
    file_access_utils.configure_sandbox_permissions(case.datafile.name)

    # Alice uploads the data to the system.
    case.dataset_labdata = Dataset.create_dataset(file_path=case.datafile.name, user=case.user_alice,
                                                cdt=case.cdt_record, keep_file=True, name="lab data",
                                                description="data from the lab")

    # Now Alice is ready to run her pipelines. The system creates a Sandbox
    # where she will run each of her pipelines.
    case.sandbox_complement = sandbox.execute.Sandbox(case.user_alice, case.pipeline_complement, [case.dataset_labdata])
    case.sandbox_revcomp = sandbox.execute.Sandbox(case.user_alice, case.pipeline_revcomp, [case.dataset_labdata])

    # A second version of the complement Pipeline which doesn't keep any output.
    case.pipeline_complement_v2 = Pipeline(family=case.pipeline_complement.family, revision_name="2",
                                           revision_desc="second version", user=case.user_alice)
    case.pipeline_complement_v2.save()
    create_linear_pipeline(case.pipeline_complement_v2,
                           [case.method_complement],
                           "lab_data",
                           "complemented_lab_data")
    case.pipeline_complement_v2.steps.last().add_deletion(case.method_complement.outputs.first())
    case.pipeline_complement_v2.outcables.first().delete()
    case.pipeline_complement_v2.create_outputs()

    # A second version of the reverse/complement Pipeline which doesn't keep
    # intermediate or final output.
    case.pipeline_revcomp_v2 = Pipeline(family=case.pipeline_revcomp.family, revision_name="2",
                                        revision_desc="second version", user=case.user_alice)
    case.pipeline_revcomp_v2.save()
    create_linear_pipeline(case.pipeline_revcomp_v2,
                           [case.method_reverse, case.method_complement],
                           "lab_data",
                           "revcomped_lab_data")
    case.pipeline_revcomp_v2.steps.get(step_num=1).add_deletion(case.method_reverse.outputs.first())
    case.pipeline_revcomp_v2.steps.get(step_num=2).add_deletion(case.method_complement.outputs.first())
    case.pipeline_revcomp_v2.outcables.first().delete()
    case.pipeline_revcomp_v2.create_outputs()

    # A third version of the reverse/complement Pipeline which keeps
    # final output, but not intermediate.
    case.pipeline_revcomp_v3 = Pipeline(family=case.pipeline_revcomp.family, revision_name="3",
                                        revision_desc="third version", user=case.user_alice)
    case.pipeline_revcomp_v3.save()
    create_linear_pipeline(case.pipeline_revcomp_v3, [case.method_reverse, case.method_complement],
                           "lab_data", "revcomped_lab_data")
    case.pipeline_revcomp_v3.steps.get(step_num=1).add_deletion(case.method_reverse.outputs.first())
    case.pipeline_revcomp_v3.create_outputs()

    # Another method which turns DNA into RNA.
    source = """\
#!/bin/bash
cat "$1" | cut -d ',' -f 2 | tr 'T' 'U' | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
"""
    case.coderev_DNA2RNA = make_first_revision("DNA to RNA",
                                               "a script to reverse DNA",
                                               "DNA2RNA.sh",
                                               source,
                                               case.user_alice)
    case.method_DNA2RNA = make_first_method("DNA to RNA", "a method to turn strings of DNA into RNA",
                                            case.coderev_DNA2RNA, case.user_alice)
    simple_method_io(case.method_DNA2RNA, case.cdt_record, "DNA_to_convert", "RNA")

    # A pipeline which reverses DNA, then turns it into RNA.
    case.pipeline_revRNA = make_first_pipeline(
        "DNA to reversed RNA",
        "a pipeline to reverse DNA and translate it to RNA",
        case.user_alice)
    create_linear_pipeline(case.pipeline_revRNA, [case.method_reverse, case.method_DNA2RNA], "lab_data",
                           "RNAd_lab_data")
    case.pipeline_revRNA.create_outputs()

    # Separator to print between Pipeline executions, to make viewing logs easier.
    case.sep = " "*80 + "\n" + "*"*80 + "\n" + " "*80 + "\n"

    # Figure out the MD5 of the output file created when the complement method
    # is run on Alice's data, so we can check it later.
    tmpdir = tempfile.mkdtemp(dir=file_access_utils.sandbox_base_path())
    file_access_utils.configure_sandbox_permissions(tmpdir)

    outfile = os.path.join(tmpdir, "output")
    complement_popen = case.method_complement.invoke_code(tmpdir, [case.datafile.name], [outfile])
    complement_popen.wait()
    case.labdata_compd_md5 = file_access_utils.compute_md5(open(outfile))
    shutil.rmtree(tmpdir)


def destroy_sequence_manipulation_environment(case):
    clean_up_all_files()
    if os.path.exists(case.datafile.name):
        os.remove(case.datafile.name)


def create_word_reversal_environment(case):
    """
    Create an environment with some word-reversal code and pipelines.
    """
    create_sandbox_testing_tools_environment(case)

    # A code resource which reverses a file.
    case.coderev_reverse = make_first_revision(
        "reverse",
        "a script to reverse lines of a file",
        "reverse.py",
        ("#!/usr/bin/env python\n"
         "import sys\n"
         "import csv\n"
         "with open(sys.argv[1]) as infile, open(sys.argv[2], 'w') as outfile:\n"
         "  reader = csv.reader(infile)\n"
         "  writer = csv.writer(outfile)\n"
         "  for row in reader:\n"
         "      writer.writerow([row[1][::-1], row[0][::-1]])\n"),
        case.user_bob)

    # A CDT with two columns, word and drow.
    case.cdt_wordbacks = CompoundDatatype(user=case.user_bob)
    case.cdt_wordbacks.save()
    case.cdt_wordbacks.members.create(datatype=case.datatype_str, column_name="word", column_idx=1)
    case.cdt_wordbacks.members.create(datatype=case.datatype_str, column_name="drow", column_idx=2)
    case.cdt_wordbacks.grant_everyone_access()

    # A second CDT, much like the first :]
    case.cdt_backwords = CompoundDatatype(user=case.user_bob)
    case.cdt_backwords.save()
    case.cdt_backwords.members.create(datatype=case.datatype_str, column_name="drow", column_idx=1)
    case.cdt_backwords.members.create(datatype=case.datatype_str, column_name="word", column_idx=2)
    case.cdt_backwords.grant_everyone_access()

    # Methods for the reverse CRR, and noop CRR.
    case.method_reverse = make_first_method("string reverse", "a method to reverse strings",
                                            case.coderev_reverse, case.user_bob)
    simple_method_io(case.method_reverse, case.cdt_wordbacks, "words_to_reverse", "reversed_words")
    case.method_re_reverse = make_first_method("string re-reverse", "a method to re-reverse strings",
                                               case.coderev_reverse, case.user_bob)
    simple_method_io(case.method_re_reverse, case.cdt_backwords, "words_to_rereverse", "rereversed_words")

    case.method_noop_wordbacks = make_first_method(
        "noop wordback",
        "a method to do nothing on two columns (word, drow)",
        case.coderev_noop,
        case.user_bob)
    simple_method_io(case.method_noop_wordbacks, case.cdt_wordbacks, "words", "more_words")
    case.method_noop_backwords = make_first_method(
        "noop backword",
        "a method to do nothing on two columns",
        case.coderev_noop,
        case.user_bob)
    simple_method_io(case.method_noop_backwords, case.cdt_backwords, "backwords", "more_backwords")

    # Some data of type (case.datatype_str: word).
    string_datafile = tempfile.NamedTemporaryFile(delete=False)
    string_datafile.write("word\n")
    string_datafile.close()
    os.system("head -1 /usr/share/dict/words >> {}".
              format(string_datafile.name))
    case.dataset_words = Dataset.create_dataset(
        file_path=string_datafile.name,
        user=case.user_bob,
        groups_allowed=[everyone_group()],
        cdt=case.cdt_string,
        keep_file=True,
        name="blahblah",
        description="blahblahblah"
    )

    os.remove(string_datafile.name)

    # Some data of type (case.datatype_str: word, case.datatype_str: drow).
    case.wordbacks_datafile = tempfile.NamedTemporaryFile(delete=False)
    writer = csv.writer(case.wordbacks_datafile)
    writer.writerow(["word", "drow"])
    random.seed("Constant seed avoids intermittent failures.")
    for _ in range(20):
        i = random.randint(1, 99171)
        sed = subprocess.Popen(["sed", "{}q;d".format(i), "/usr/share/dict/words"],
                               stdout=subprocess.PIPE)
        word, _ = sed.communicate()
        word = word.strip()
        writer.writerow([word, word[::-1]])
    case.wordbacks_datafile.close()

    case.backwords_datafile = tempfile.NamedTemporaryFile(delete=False)
    writer = csv.writer(case.backwords_datafile)
    writer.writerow(["drow", "word"])
    for _ in range(20):
        i = random.randint(1, 99171)
        sed = subprocess.Popen(["sed", "{}q;d".format(i), "/usr/share/dict/words"],
                               stdout=subprocess.PIPE)
        word, _ = sed.communicate()
        word = word.strip()
        writer.writerow([word[::-1], word])
    case.backwords_datafile.close()

    case.dataset_wordbacks = Dataset.create_dataset(
        file_path=case.wordbacks_datafile.name,
        user=case.user_bob,
        groups_allowed=[everyone_group()],
        cdt=case.cdt_wordbacks,
        keep_file=True,
        name="wordbacks",
        description="random reversed words"
    )

    case.dataset_backwords = Dataset.create_dataset(
        file_path=case.backwords_datafile.name,
        user=case.user_bob,
        groups_allowed=[everyone_group()],
        cdt=case.cdt_backwords,
        keep_file=True,
        name="backwords",
        description="random reversed words"
    )


def destroy_word_reversal_environment(case):
    clean_up_all_files()
    if hasattr(case, "words_datafile"):
        os.remove(case.words_datafile.name)


def make_crisscross_cable(cable):
    """
    Helper to take a cable whose source and destination CDTs both have two columns that can be
    reversed (e.g. string-string or int-int, etc.) and add "crisscross" wiring.
    """
    source_cdt = cable.source.structure.compounddatatype
    dest_cdt = cable.dest.structure.compounddatatype
    cable.custom_wires.create(source_pin=source_cdt.members.get(column_idx=1),
                              dest_pin=dest_cdt.members.get(column_idx=2))
    cable.custom_wires.create(source_pin=source_cdt.members.get(column_idx=2),
                              dest_pin=dest_cdt.members.get(column_idx=1))


def new_datatype(dtname, dtdesc, kivetype, user, grant_everyone_access=True):
    """
    Helper function to create a new datatype.
    """
    datatype = Datatype(name=dtname, description=dtdesc, user=user)
    datatype.save()
    datatype.restricts.add(Datatype.objects.get(pk=kivetype.pk))
    if grant_everyone_access:
        datatype.grant_everyone_access()
    # datatype.complete_clean()
    return datatype


def make_first_revision(resname, resdesc, resfn, contents, user, grant_everyone_access=True):
    """
    Helper function to make a CodeResource and the first version.
    """
    resource = CodeResource(name=resname, description=resdesc, filename=resfn, user=user)
    # resource.clean()
    resource.save()
    if grant_everyone_access:
        resource.grant_everyone_access()
    with tempfile.TemporaryFile() as f:
        f.write(contents)
        with transaction.atomic():
            revision = CodeResourceRevision(
                coderesource=resource,
                revision_name="1",
                revision_desc="first version",
                content_file=File(f),
                user=user)

            # We need to set the MD5.
            md5gen = hashlib.md5()
            md5gen.update(contents)
            revision.MD5_checksum = md5gen.hexdigest()

            revision.save()
            revision.clean()
    if grant_everyone_access:
        revision.grant_everyone_access()
    resource.clean()
    return revision


def make_first_method(famname, famdesc, driver, user, grant_everyone_access=True):
    """
    Helper function to make a new MethodFamily for a new Method.
    """
    family = MethodFamily(name=famname, description=famdesc, user=user)
    family.save()
    if grant_everyone_access:
        family.grant_everyone_access()
    with transaction.atomic():
        method = Method(
            revision_name="v1",
            revision_desc="first version",
            family=family,
            driver=driver,
            user=user)
        method.save()
        method.clean()
    if grant_everyone_access:
        method.grant_everyone_access()
    family.clean()
    return method


def simple_method_io(method, cdt, indataname, outdataname):
    """
    Helper function to create inputs and outputs for a simple
    Method with one input, one output, and the same CompoundDatatype
    for both incoming and outgoing data.
    """
    minput = method.create_input(compounddatatype=cdt,
                                 dataset_name=indataname,
                                 dataset_idx=1)
    minput.clean()
    moutput = method.create_output(compounddatatype=cdt,
                                   dataset_name=outdataname,
                                   dataset_idx=1)
    moutput.clean()
    method.clean()
    return minput, moutput


def make_first_pipeline(pname, pdesc, user, grant_everyone_access=True):
    """
    Helper function to make a new PipelineFamily and the first Pipeline
    member.
    """
    family = PipelineFamily(name=pname, description=pdesc, user=user)
    family.save()
    if grant_everyone_access:
        family.grant_everyone_access()
    pipeline = Pipeline(family=family, revision_name="v1", revision_desc="first version", user=user)
    pipeline.clean()
    pipeline.save()
    if grant_everyone_access:
        pipeline.grant_everyone_access()
    family.clean()
    return pipeline


def make_second_pipeline(pipeline, grant_everyone_access=True):
    """
    Create a second version of a Pipeline, in the same family as the first,
    without making any changes. Hook up the steps to each other, but don't
    create inputs and outputs for the new Pipeline.
    """
    new_pipeline = Pipeline(family=pipeline.family, revision_name="v2", revision_desc="second version",
                            user=pipeline.user)
    new_pipeline.save()
    if grant_everyone_access:
        new_pipeline.grant_everyone_access()

    for step in pipeline.steps.all():
        new_step = new_pipeline.steps.create(transformation=step.transformation, step_num=step.step_num)
        for cable in step.cables_in.all():
            if cable.source.transformation.__class__.__name__ == "PipelineStep":
                new_step.cables_in.create(source=cable.source, dest=cable.dest)
    return new_pipeline


def create_linear_pipeline(pipeline, methods, indata, outdata):
    """
    Helper function to create a "linear" pipeline, ie.

            ___       __
      in --|   |-...-|  |-- out
           |___|     |__|

    indata and outdata are the names of the input and output datasets.
    """
    # Create pipeline input.
    if methods[0].inputs.first().is_raw():
        cdt_in = None
    else:
        cdt_in = methods[0].inputs.first().structure.compounddatatype
    pipeline_in = pipeline.create_input(compounddatatype=cdt_in, dataset_name=indata, dataset_idx=1)

    # Create steps.
    steps = []
    for i, _method in enumerate(methods):
        step = pipeline.steps.create(transformation=methods[i], step_num=i+1)
        if i == 0:
            source = pipeline_in
        else:
            source = methods[i-1].outputs.first()
        step.cables_in.create(source_step=i, source=source, dest=methods[i].inputs.first())
        step.complete_clean()
        steps.append(step)

    # Create pipeline output.
    pipeline.create_outcable(output_name=outdata, output_idx=1, source_step=len(steps),
                             source=methods[-1].outputs.first())
    pipeline.create_outputs()
    pipeline.complete_clean()


# This is potentially slow so we don't just build it into the create_... function above.
# This is also kind of a hack -- depends on case.user_bob and case.cdt_string being present.
def make_words_dataset(case):
    """
    Set up a data file of words in the specified test case.

    PRE: the specified test case has a member CDT called cdt_string and user user_bob.
    """
    string_datafile = tempfile.NamedTemporaryFile(delete=False)
    string_datafile.write("word\n")
    string_datafile.close()
    os.system("head -1 /usr/share/dict/words >> {}".
              format(string_datafile.name))
    case.dataset_words = Dataset.create_dataset(
        file_path=string_datafile.name,
        user=case.user_bob,
        cdt=case.cdt_string,
        keep_file=True,
        name="blahblah",
        description="blahblahblah"
    )
    case.dataset_words.grant_everyone_access()
    case.dataset_words.save()

    os.remove(string_datafile.name)


# An environment resulting from a user that's messed things up.
def create_grandpa_sandbox_environment(case):
    create_sandbox_testing_tools_environment(case)

    # A guy who doesn't know what he is doing.
    # May 14, 2014: dag, yo -- RL
    # May 20, 2014: he's doing his best, man -- RL
    case.user_grandpa = User.objects.create_user('grandpa', 'gr@nd.pa', '123456')
    case.user_grandpa.save()
    case.user_grandpa.groups.add(everyone_group())
    case.user_grandpa.save()

    # A code resource, method, and pipeline which are empty.
    case.coderev_faulty = make_first_revision(
        "faulty",
        "a script...?",
        "faulty.sh", "",
        case.user_grandpa
    )
    case.method_faulty = make_first_method(
        "faulty",
        "a method to... uh...",
        case.coderev_faulty,
        case.user_grandpa
    )
    case.method_faulty.clean()
    simple_method_io(case.method_faulty, case.cdt_string, "strings", "i_dont_know")
    case.pipeline_faulty = make_first_pipeline("faulty pipeline", "a pipeline to do nothing", case.user_grandpa)
    create_linear_pipeline(case.pipeline_faulty, [case.method_faulty, case.method_noop], "data", "the_abyss")
    case.pipeline_faulty.create_outputs()

    # A code resource, method, and pipeline which fail.
    case.coderev_fubar = make_first_revision(
        "fubar", "a script which always fails",
        "fubar.sh", "#!/bin/bash\nexit 1",
        case.user_grandpa
    )
    case.method_fubar = make_first_method("fubar", "a method which always fails", case.coderev_fubar,
                                          case.user_grandpa)
    case.method_fubar.clean()
    simple_method_io(case.method_fubar, case.cdt_string, "strings", "broken_strings")
    case.pipeline_fubar = make_first_pipeline("fubar pipeline", "a pipeline which always fails", case.user_grandpa)
    create_linear_pipeline(case.pipeline_fubar,
                           [case.method_noop, case.method_fubar, case.method_noop], "indata", "outdata")
    case.pipeline_fubar.create_outputs()

    # Some data to run through the faulty pipelines.
    case.grandpa_datafile = tempfile.NamedTemporaryFile(delete=False)
    case.grandpa_datafile.write("word\n")
    random.seed("Constant seed avoids intermittent failures.")
    for _ in range(20):
        i = random.randint(1, 99171)
        case.grandpa_datafile.write("{}\n".format(i))
    case.grandpa_datafile.close()
    case.dataset_grandpa = Dataset.create_dataset(
        file_path=case.grandpa_datafile.name,
        user=case.user_grandpa,
        cdt=case.cdt_string,
        keep_file=True,
        name="numbers",
        description="numbers which are actually strings"
    )
    case.dataset_grandpa.clean()


def destroy_grandpa_sandbox_environment(case):
    clean_up_all_files()
    os.remove(case.grandpa_datafile.name)


def make_dataset(contents, CDT, keep_file, user, name, description, created_by, check):
    """
    Wrapper for create_dataset that creates a Dataset from a string.
    """
    with tempfile.TemporaryFile() as f:
        f.write(contents)
        test_dataset = Dataset.create_dataset(
            None,
            user,
            cdt=CDT,
            keep_file=keep_file,
            name=name,
            description=description,
            created_by=created_by,
            check=check,
            file_handle=f
        )

    return test_dataset
