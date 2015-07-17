import csv
import os
import random
import shutil
import subprocess
import tempfile

from django.contrib.auth.models import User
from django.core.files import File
from django.db import transaction

from constants import datatypes
import file_access_utils
from librarian.models import SymbolicDataset, ExecRecord
from metadata.models import CompoundDatatype, Datatype, everyone_group
from metadata.tests import clean_up_all_files, create_metadata_test_environment, samplecode_path
from method.models import CodeResource, CodeResourceRevision, Method, MethodFamily
from pipeline.models import Pipeline, PipelineFamily, PipelineStep
from archive.models import RunStep, ExecLog
import sandbox.execute


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
    # used in copperfish/tests.py.
    # CRs and CRRs:
    case.generic_cr = CodeResource(
        name="genericCR", description="Just a CR",
        filename="generic_script.py", user=case.myUser)
    case.generic_cr.save()
    case.generic_cr.grant_everyone_access()
    case.generic_crRev = CodeResourceRevision(
        coderesource=case.generic_cr, revision_name="v1", revision_desc="desc",
        user=case.myUser)
    with open(os.path.join(samplecode_path, "generic_script.py"), "rb") as f:
        case.generic_crRev.content_file.save("generic_script.py", File(f))
    case.generic_crRev.save()
    case.generic_crRev.grant_everyone_access()

    # Method family, methods, and their input/outputs
    case.mf = MethodFamily(name="method_family",description="Holds methods A/B/C", user=case.myUser)
    case.mf.save()
    case.mf.grant_everyone_access()
    case.mA = Method(revision_name="mA_name", revision_desc="A_desc", family=case.mf, driver=case.generic_crRev,
                     user=case.myUser)
    case.mA.save()
    case.mA.grant_everyone_access()
    case.A1_rawin = case.mA.create_input(dataset_name="A1_rawin", dataset_idx=1)
    case.A1_out = case.mA.create_output(compounddatatype=case.doublet_cdt,dataset_name="A1_out",dataset_idx=1)

    case.mB = Method(revision_name="mB_name", revision_desc="B_desc", family=case.mf, driver=case.generic_crRev,
                     user=case.myUser)
    case.mB.save()
    case.mB.grant_everyone_access()
    case.B1_in = case.mB.create_input(compounddatatype=case.doublet_cdt,dataset_name="B1_in",dataset_idx=1)
    case.B2_in = case.mB.create_input(compounddatatype=case.singlet_cdt,dataset_name="B2_in",dataset_idx=2)
    case.B1_out = case.mB.create_output(compounddatatype=case.triplet_cdt,dataset_name="B1_out",dataset_idx=1,max_row=5)

    case.mC = Method(revision_name="mC_name", revision_desc="C_desc", family=case.mf, driver=case.generic_crRev,
                     user=case.myUser)
    case.mC.save()
    case.mC.grant_everyone_access()
    case.C1_in = case.mC.create_input(compounddatatype=case.triplet_cdt,dataset_name="C1_in",dataset_idx=1)
    case.C2_in = case.mC.create_input(compounddatatype=case.doublet_cdt,dataset_name="C2_in",dataset_idx=2)
    case.C1_out = case.mC.create_output(compounddatatype=case.singlet_cdt,dataset_name="C1_out",dataset_idx=1)
    case.C2_rawout = case.mC.create_output(dataset_name="C2_rawout",dataset_idx=2)
    case.C3_rawout = case.mC.create_output(dataset_name="C3_rawout",dataset_idx=3)

    # Pipeline family, pipelines, and their input/outputs
    case.pf = PipelineFamily(name="Pipeline_family", description="PF desc", user=case.myUser); case.pf.save()
    case.pf.grant_everyone_access()
    case.pD = Pipeline(family=case.pf, revision_name="pD_name", revision_desc="D", user=case.myUser)
    case.pD.save()
    case.pD.grant_everyone_access()
    case.D1_in = case.pD.create_input(compounddatatype=case.doublet_cdt,dataset_name="D1_in",dataset_idx=1)
    case.D2_in = case.pD.create_input(compounddatatype=case.singlet_cdt,dataset_name="D2_in",dataset_idx=2)
    case.pE = Pipeline(family=case.pf, revision_name="pE_name", revision_desc="E", user=case.myUser)
    case.pE.save()
    case.pE.grant_everyone_access()
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
    case.pD_run = case.pD.pipeline_instances.create(user=case.myUser)
    case.pD_run.save()
    case.pD_run.grant_everyone_access()
    case.pE_run = case.pE.pipeline_instances.create(user=case.myUser,
                                                    name='pE_run')
    case.pE_run.save()
    case.pE_run.grant_everyone_access()

    # November 7, 2013: use a helper function (defined in
    # librarian.models) to define our SymDSs and DSs.

    # Define singlet, doublet, triplet, and raw uploaded datasets
    case.triplet_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "step_0_triplet.csv"),
                                                   case.myUser,
                                                   cdt=case.triplet_cdt, make_dataset=True,
                                                   name="triplet", description="lol",
                                                   groups_allowed=[everyone_group()])
    case.triplet_symDS_structure = case.triplet_symDS.structure
    case.triplet_DS = case.triplet_symDS.dataset

    case.doublet_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "doublet_cdt.csv"),
                                                   case.myUser,
                                                   cdt=case.doublet_cdt, name="doublet",
                                                   description="lol",
                                                   groups_allowed=[everyone_group()])
    case.doublet_symDS_structure = case.doublet_symDS.structure
    case.doublet_DS = case.doublet_symDS.dataset

    case.singlet_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "singlet_cdt_large.csv"),
                                                   case.myUser,
                                                   cdt=case.singlet_cdt, name="singlet",
                                                   description="lol",
                                                   groups_allowed=[everyone_group()])
    case.singlet_symDS_structure = case.singlet_symDS.structure
    case.singlet_DS = case.singlet_symDS.dataset

    # October 1, 2013: this is the same as the old singlet_symDS.
    case.singlet_3rows_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "step_0_singlet.csv"),
                                                         case.myUser,
                                                         cdt=case.singlet_cdt, name="singlet",
                                                         description="lol",
                                                         groups_allowed=[everyone_group()])
    case.singlet_3rows_symDS_structure = case.singlet_3rows_symDS.structure
    case.singlet_3rows_DS = case.singlet_3rows_symDS.dataset

    case.raw_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "step_0_raw.fasta"),
                                               user=case.myUser, cdt=None, name="raw_DS", description="lol",
                                               groups_allowed=[everyone_group()])
    case.raw_DS = case.raw_symDS.dataset

    # Added September 30, 2013: symbolic dataset that results from E01_21.
    # November 7, 2013: created a file that this SD actually represented,
    # even though it isn't in the database.
    case.D1_in_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "doublet_remuxed_from_triplet.csv"),
                                                 user=case.myUser,
                                                 cdt=case.doublet_cdt,
                                                 make_dataset=False,
                                                 groups_allowed=[everyone_group()])
    case.D1_in_symDS_structure = case.D1_in_symDS.structure

    case.C1_in_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "C1_in_triplet.csv"),
                                                 case.myUser,
                                                 cdt=case.triplet_cdt, name="C1_in_triplet",
                                                 description="triplet 3 rows",
                                                 groups_allowed=[everyone_group()])
    case.C1_in_symDS_structure = case.C1_in_symDS.structure
    case.C1_in_DS = case.C1_in_symDS.dataset

    # November 7, 2013: compute the MD5 checksum from the data file,
    # which is the same as below.
    case.C2_in_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "E11_32_output.csv"),
                                                 case.myUser,
                                                 cdt=case.doublet_cdt, make_dataset=False,
                                                 groups_allowed=[everyone_group()])
    case.C2_in_symDS_structure = case.C2_in_symDS.structure

    # October 16: an alternative to C2_in_symDS, which has existent data.
    case.E11_32_output_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "E11_32_output.csv"),
                                                         case.myUser,
                                                         cdt=case.doublet_cdt,
                                                         name="E11_32 output doublet",
                                                         description="result of E11_32 fed by doublet_cdt.csv",
                                                         groups_allowed=[everyone_group()])
    case.E11_32_output_symDS_structure = case.E11_32_output_symDS.structure
    case.E11_32_output_DS = case.E11_32_output_symDS.dataset

    case.C1_out_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "step_0_singlet.csv"),
                                                  case.myUser,
                                                  cdt=case.singlet_cdt, name="raw", description="lol",
                                                  groups_allowed=[everyone_group()])
    case.C1_out_symDS_structure = case.C1_out_symDS.structure
    case.C1_out_DS = case.C1_out_symDS.dataset

    case.C2_out_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "step_0_raw.fasta"),
                                                  case.myUser, cdt=None, name="C2_out", description="lol",
                                                  groups_allowed=[everyone_group()])
    case.C2_out_DS = case.C2_out_symDS.dataset

    case.C3_out_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "step_0_raw.fasta"),
                                                  case.myUser, cdt=None, name="C3_out", description="lol",
                                                  groups_allowed=[everyone_group()])
    case.C3_out_DS = case.C3_out_symDS.dataset

    case.triplet_3_rows_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), case.myUser, cdt=case.triplet_cdt,
        name="triplet", description="lol", groups_allowed=[everyone_group()])
    case.triplet_3_rows_symDS_structure = case.triplet_3_rows_symDS.structure
    case.triplet_3_rows_DS = case.triplet_3_rows_symDS.dataset

    # October 9, 2013: added as the result of cable E21_41.
    case.E1_out_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "doublet_remuxed_from_t3r.csv"),
                                                  case.myUser, cdt=case.doublet_cdt, name="E1_out",
                                                  description="doublet remuxed from triplet",
                                                  groups_allowed=[everyone_group()])
    case.E1_out_symDS_structure = case.E1_out_symDS.structure
    case.E1_out_DS = case.E1_out_symDS.dataset

    # October 15, 2013: SymbolicDatasets that go into and come out
    # of cable E01_21 and E21_41.
    case.DNA_triplet_symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path, "DNA_triplet.csv"),
                                                       case.myUser, cdt=case.DNA_triplet_cdt, name="DNA_triplet",
                                                       description="DNA triplet data",
                                                       groups_allowed=[everyone_group()])
    case.DNA_triplet_symDS_structure = case.DNA_triplet_symDS.structure
    case.DNA_triplet_DS = case.DNA_triplet_symDS.dataset

    case.E01_21_DNA_doublet_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "E01_21_DNA_doublet.csv"), case.myUser, cdt=case.DNA_doublet_cdt,
        name="E01_21_DNA_doublet",
        description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E01_21",
        groups_allowed=[everyone_group()])
    case.E01_21_DNA_doublet_symDS_structure = case.E01_21_DNA_doublet_symDS.structure
    case.E01_21_DNA_doublet_DS = case.E01_21_DNA_doublet_symDS.dataset

    case.E21_41_DNA_doublet_symDS = SymbolicDataset.create_SD(
        os.path.join(samplecode_path, "E21_41_DNA_doublet.csv"), case.myUser, cdt=case.DNA_doublet_cdt,
        name="E21_41_DNA_doublet",
        description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E21_41",
        groups_allowed=[everyone_group()])
    case.E21_41_DNA_doublet_symDS_structure = case.E21_41_DNA_doublet_symDS.structure
    case.E21_41_DNA_doublet_DS = case.E21_41_DNA_doublet_symDS.dataset


def create_librarian_test_environment(case):
    """
    Set up default state for Librarian unit testing.
    """
    create_eric_martin_test_environment(case)

    # Some ExecRecords, some failed, others not.
    i = 0
    for step in PipelineStep.objects.all():
        if step.is_subpipeline: continue
        run = step.pipeline.pipeline_instances.create(user=step.pipeline.user); run.save()
        runstep = RunStep(pipelinestep=step, run=run, reused=False); runstep.save()
        execlog = ExecLog.create(runstep, runstep)
        execlog.methodoutput.return_code = i%2; execlog.methodoutput.save()
        execrecord = ExecRecord(generator=execlog); execrecord.save()
        for step_input in step.transformation.inputs.all():
            sd = SymbolicDataset.filter_by_user(step.pipeline.user).filter(
                structure__compounddatatype=step_input.compounddatatype).first()
            execrecord.execrecordins.create(symbolicdataset=sd, generic_input=step_input)
        runstep.execrecord = execrecord; runstep.save()
        i += 1


def create_removal_test_environment():
    # We need:
    # - a CodeResource with revisions
    # - a CodeResourceRevision with dependencies
    # - a Datatype
    # - a CDT using that Datatype
    # - a SymbolicDataset with that CDT
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
                                        Datatype.objects.get(pk=datatypes.STR_PK), remover,
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
    seq_sd = SymbolicDataset.create_SD(seq_datafile.name,
        name="Removal test data", cdt=one_col_nuc_seq, user=remover,
        description="A dataset for use in the removal test case.", make_dataset=True)

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

    p_nested = make_first_pipeline("Nested pipeline", "Pipeline with one nested level", remover,
                                         grant_everyone_access=False)
    create_linear_pipeline(p_nested, [noop_pl, noop_pl], "nested_in", "nested_out")
    p_nested.create_outputs()
    p_nested.save()

    first_run_sdbx = sandbox.execute.Sandbox(remover, noop_pl, [seq_sd], groups_allowed=[])
    first_run_sdbx.execute_pipeline()
    second_run_sdbx = sandbox.execute.Sandbox(remover, noop_pl, [seq_sd], groups_allowed=[])
    second_run_sdbx.execute_pipeline()

    two_step_noop_pl = make_first_pipeline(
        "Nucleotide Sequence two-step Noop",
        "A two-step noop pipeline for nucleotide sequences.",
        remover,
        grant_everyone_access=False
        )
    create_linear_pipeline(two_step_noop_pl, [nuc_seq_noop, nuc_seq_noop],
                                 "noop_pipeline_in", "noop_pipeline_out")

    two_step_seq_datafile = tempfile.NamedTemporaryFile(delete=False)
    two_step_seq_datafile.write("""sequence
AAAA
CCCCC
GGGGGG
TTTTTTC
""")
    two_step_seq_datafile.close()
    two_step_seq_sd = SymbolicDataset.create_SD(two_step_seq_datafile.name,
        name="Removal test data for a two-step Pipeline", cdt=one_col_nuc_seq, user=remover,
        description="A dataset for use in the removal test case with the two-step Pipeline.", make_dataset=True)

    two_step_run_sdbx = sandbox.execute.Sandbox(remover, two_step_noop_pl, [two_step_seq_sd], groups_allowed=[])
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


def destroy_sandbox_testing_tools_environment(case):
    """
    Clean up a TestCase where create_sandbox_testing_tools_environment has been called.
    # """
    clean_up_all_files()


def create_archive_test_environment(case):
    create_librarian_test_environment(case)
    create_sandbox_testing_tools_environment(case)
    case.pE_run = case.pE.pipeline_instances.create(user=case.myUser)
    case.pE_run.grant_everyone_access()


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
    case.symds_labdata = SymbolicDataset.create_SD(case.datafile.name, user=case.user_alice,
                                                   name="lab data", cdt=case.cdt_record,
                                                   description="data from the lab",
                                                   make_dataset=True)

    # Now Alice is ready to run her pipelines. The system creates a Sandbox
    # where she will run each of her pipelines.
    case.sandbox_complement = sandbox.execute.Sandbox(case.user_alice, case.pipeline_complement, [case.symds_labdata])
    case.sandbox_revcomp = sandbox.execute.Sandbox(case.user_alice, case.pipeline_revcomp, [case.symds_labdata])

    # A second version of the complement Pipeline which doesn't keep any output.
    case.pipeline_complement_v2 = Pipeline(family=case.pipeline_complement.family, revision_name="2",
                                           revision_desc="second version", user=case.user_alice)
    case.pipeline_complement_v2.save()
    create_linear_pipeline(case.pipeline_complement_v2, [case.method_complement], "lab_data",
                                "complemented_lab_data")
    case.pipeline_complement_v2.steps.last().add_deletion(case.method_complement.outputs.first())
    case.pipeline_complement_v2.outcables.first().delete()
    case.pipeline_complement_v2.create_outputs()

    # A second version of the reverse/complement Pipeline which doesn't keep
    # intermediate or final output.
    case.pipeline_revcomp_v2 = Pipeline(family=case.pipeline_revcomp.family, revision_name="2",
                                        revision_desc="second version", user=case.user_alice)
    case.pipeline_revcomp_v2.save()
    create_linear_pipeline(case.pipeline_revcomp_v2, [case.method_reverse, case.method_complement],
                                 "lab_data", "revcomped_lab_data")
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
    case.coderev_DNA2RNA = make_first_revision("DNA to RNA", "a script to reverse DNA", "DNA2RNA.sh",
            """#!/bin/bash
            cat "$1" | cut -d ',' -f 2 | tr 'T' 'U' | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
            """,
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
    case.coderev_reverse = make_first_revision("reverse", "a script to reverse lines of a file", "reverse.py",
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
    case.symds_words = SymbolicDataset.create_SD(
        string_datafile.name,
        name="blahblah", cdt=case.cdt_string, user=case.user_bob,
        description="blahblahblah", make_dataset=True,
        groups_allowed=[everyone_group()])

    os.remove(string_datafile.name)

    # Some data of type (case.datatype_str: word, case.datatype_str: drow).
    case.wordbacks_datafile = tempfile.NamedTemporaryFile(delete=False)
    writer = csv.writer(case.wordbacks_datafile)
    writer.writerow(["word", "drow"])
    random.seed("Constant seed avoids intermittent failures.")
    for _ in range(20):
        i = random.randint(1,99171)
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
        i = random.randint(1,99171)
        sed = subprocess.Popen(["sed", "{}q;d".format(i), "/usr/share/dict/words"],
                               stdout=subprocess.PIPE)
        word, _ = sed.communicate()
        word = word.strip()
        writer.writerow([word[::-1], word])
    case.backwords_datafile.close()

    case.symds_wordbacks = SymbolicDataset.create_SD(
        case.wordbacks_datafile.name, user=case.user_bob,
        name="wordbacks", cdt=case.cdt_wordbacks,
        description="random reversed words", make_dataset=True,
        groups_allowed=[everyone_group()])

    case.symds_backwords = SymbolicDataset.create_SD(
        case.backwords_datafile.name, user=case.user_bob,
        name="backwords", cdt=case.cdt_backwords,
        description="random reversed words", make_dataset=True,
        groups_allowed=[everyone_group()])


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
        dataset_name = indataname,
        dataset_idx = 1)
    minput.clean()
    moutput = method.create_output(compounddatatype=cdt,
        dataset_name = outdataname,
        dataset_idx = 1)
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
                new_step.cables_in.create(source = cable.source, dest = cable.dest)
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
def make_words_symDS(case):
    """
    Set up a data file of words in the specified test case.

    PRE: the specified test case has a member CDT called cdt_string and user user_bob.
    """
    string_datafile = tempfile.NamedTemporaryFile(delete=False)
    string_datafile.write("word\n")
    string_datafile.close()
    os.system("head -1 /usr/share/dict/words >> {}".
              format(string_datafile.name))
    case.symds_words = SymbolicDataset.create_SD(string_datafile.name,
        name="blahblah", cdt=case.cdt_string, user=case.user_bob,
        description="blahblahblah", make_dataset=True)
    case.symds_words.grant_everyone_access()
    case.symds_words.save()

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
        i = random.randint(1,99171)
        case.grandpa_datafile.write("{}\n".format(i))
    case.grandpa_datafile.close()
    case.symds_grandpa = SymbolicDataset.create_SD(
        case.grandpa_datafile.name, user=case.user_grandpa,
        name="numbers", cdt=case.cdt_string,
        description="numbers which are actually strings", make_dataset=True)
    case.symds_grandpa.clean()


def destroy_grandpa_sandbox_environment(case):
    clean_up_all_files()
    os.remove(case.grandpa_datafile.name)


def make_SD(contents, CDT, make_dataset, user, name, description, created_by, check):
    """
    Wrapper for create_SD that creates a SymbolicDataset from a string.
    """
    with tempfile.TemporaryFile() as f:
        f.write(contents)
        test_SD = SymbolicDataset.create_SD(None, user, cdt=CDT, make_dataset=make_dataset,
                                            name=name, description=description, created_by=created_by,
                                            check=check, file_handle=f)

    return test_SD
