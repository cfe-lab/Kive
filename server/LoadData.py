#! /usr/bin/env python

# Script that loads test data into the database.  Must be run from the
# shell produced by manage.py.

# Basically, we take the setup of the test cases that we've run and
# load that stuff into the database so it can be played with in
# action.  If you do this, you should manually nuke CodeResources/ and
# VerificationScripts/ before running the tests again.

from django.utils import timezone;
from django.core.files import File;
import glob, os.path;

from copperfish.models import *;

samplecode_path = "../samplecode";

# Create Datatype "string" with validation code stringUT.py
with open(os.path.join(samplecode_path, "stringUT.py"), "rb") as f:
    string_dt = Datatype(name="string",
                         description="String (basically anything)",
                         verification_script=File(f),
                         Python_type="str");
    string_dt.save()
    string_dt = string_dt

# Create Datatype "DNANucSeq" with validation code DNANucSeqUT.py
with open(os.path.join(samplecode_path, "DNANucSeqUT.py"), "rb") as f:
    DNA_dt = Datatype(name="DNANucSeq",
                      description="String consisting of ACGTacgt",
                      verification_script=File(f),
                      Python_type="str");
    DNA_dt.save();

    # DNA_dt is a restricted type of string
    DNA_dt.restricts.add(string_dt);
    DNA_dt.save();

# Create Datatype "RNANucSeq" with validation code RNANucSeqUT.py, restricted by "string"
with open(os.path.join(samplecode_path, "RNANucSeqUT.py"), "rb") as f:
    RNA_dt = Datatype(name="RNANucSeq",
                      description="String consisting of ACGUacgu",
                      verification_script=File(f),
                      Python_type="str");
    RNA_dt.save();

    # RNA_dt is a restricted type of string
    RNA_dt.restricts.add(string_dt);
    RNA_dt.save();

# Make accessible outside of this function
string_dt = string_dt;
DNA_dt = DNA_dt;
RNA_dt = RNA_dt;

# Define test_cdt as containing 3 members:
# (label, PBMCseq, PLAseq) as (string,DNA,RNA)
test_cdt = CompoundDatatype();
test_cdt.save();

test_cdt.members.create(datatype=string_dt,
                            column_name="label",
                            column_idx=1);
test_cdt.members.create(datatype=DNA_dt,
                            column_name="PBMCseq",
                            column_idx=2);
test_cdt.members.create(datatype=RNA_dt,
                            column_name="PLAseq",
                            column_idx=3);
test_cdt.full_clean();
test_cdt.save();


# Define DNAinput_cdt (1 member)
DNAinput_cdt = CompoundDatatype();
DNAinput_cdt.save();
DNAinput_cdt.members.create(datatype=DNA_dt,
                                column_name="SeqToComplement",
                                column_idx=1);
DNAinput_cdt.full_clean();
DNAinput_cdt.save();

# Define DNAoutput_cdt (1 member)
DNAoutput_cdt = CompoundDatatype();
DNAoutput_cdt.save();
DNAoutput_cdt.members.create(datatype=DNA_dt,
                                 column_name="ComplementedSeq",
                                 column_idx=1);
DNAoutput_cdt.full_clean();
DNAoutput_cdt.save();

# Define RNAinput_cdt (1 column)
RNAinput_cdt = CompoundDatatype();
RNAinput_cdt.save();
RNAinput_cdt.members.create(datatype=RNA_dt,
                                 column_name="SeqToComplement",
                                 column_idx=1);
RNAinput_cdt.full_clean();
RNAinput_cdt.save();

# Define RNAoutput_cdt (1 column)
RNAoutput_cdt = CompoundDatatype();
RNAoutput_cdt.save();
RNAoutput_cdt.members.create(datatype=RNA_dt,
                                  column_name="ComplementedSeq",
                                  column_idx=1);
RNAoutput_cdt.full_clean();
RNAoutput_cdt.save();

# Define comp_cr
comp_cr = CodeResource(
        name="complement",
        description="Complement DNA/RNA nucleotide sequences",
        filename="complement.py");
comp_cr.save();

# Define compv1_crRev for comp_cr
with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
    compv1_crRev = CodeResourceRevision(
            coderesource=comp_cr,
            revision_name="v1",
            revision_desc="First version",
            content_file=File(f));
    compv1_crRev.full_clean();
    compv1_crRev.save();

# Define compv2_crRev for comp_cr
with open(os.path.join(samplecode_path, "complement_v2.py"), "rb") as f:
    compv2_crRev = CodeResourceRevision(
        coderesource=comp_cr,
        revision_name="v2",
        revision_desc="Second version: better docstring",
        revision_parent=compv1_crRev,
        content_file=File(f));
    compv2_crRev.full_clean();
    compv2_crRev.save();

# Define DNAcomp_mf
DNAcomp_mf = MethodFamily(
        name="DNAcomplement",
        description="Complement DNA nucleotide sequences.");
DNAcomp_mf.full_clean();
DNAcomp_mf.save();

# Define DNAcompv1_m (method revision) for DNAcomp_mf with driver compv1_crRev
DNAcompv1_m = DNAcomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=compv1_crRev);

# Add input DNAinput_cdt to DNAcompv1_m
DNAinput_ti = DNAcompv1_m.inputs.create(
        compounddatatype = DNAinput_cdt,
        dataset_name = "input",
        dataset_idx = 1);
DNAinput_ti.full_clean();
DNAinput_ti.save();

# Add output DNAoutput_cdt to DNAcompv1_m
DNAoutput_to = DNAcompv1_m.outputs.create(
        compounddatatype = DNAoutput_cdt,
        dataset_name = "output",
        dataset_idx = 1);
DNAoutput_to.full_clean();
DNAoutput_to.save();

# Define DNAcompv2_m for DNAcomp_mf with driver compv2_crRev
# Input/output should be copied from DNAcompv1_m
DNAcompv2_m = DNAcomp_mf.members.create(
        revision_name="v2",
        revision_desc="Second version",
        revision_parent=DNAcompv1_m,
        driver=compv2_crRev);
DNAcompv2_m.full_clean();
DNAcompv2_m.save();

# Define second family, RNAcomp_mf
RNAcomp_mf = MethodFamily(
        name="RNAcomplement",
        description="Complement RNA nucleotide sequences.");
RNAcomp_mf.full_clean();
RNAcomp_mf.save();

# Define RNAcompv1_m for RNAcomp_mf with driver compv1_crRev
RNAcompv1_m = RNAcomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=compv1_crRev);

# Add input RNAinput_cdt to RNAcompv1_m
RNAinput_ti = RNAcompv1_m.inputs.create(
        compounddatatype = RNAinput_cdt,
        dataset_name = "input",
        dataset_idx = 1);
RNAinput_ti.full_clean();
RNAinput_ti.save();

# Add output RNAoutput_cdt to RNAcompv1_m
RNAoutput_to = RNAcompv1_m.outputs.create(
        compounddatatype = RNAoutput_cdt,
        dataset_name = "output",
        dataset_idx = 1);
RNAoutput_to.full_clean();
RNAoutput_to.save();

# Define RNAcompv2_m for RNAcompv1_mf with driver compv2_crRev
# Input/outputs should be copied from RNAcompv1_m
RNAcompv2_m = RNAcomp_mf.members.create(
        revision_name="v2",
        revision_desc="Second version",
        revision_parent=RNAcompv1_m,
        driver=compv2_crRev);
RNAcompv2_m.full_clean();
RNAcompv2_m.save();

# Create method family for script_1_method / script_2_method / script_3_method
test_mf = MethodFamily(name="Test method family",
                            description="Holds scripts 1/2/3");
test_mf.full_clean();
test_mf.save();

# script_1_sum_and_outputs.py
# INPUT: 1 csv containing (x,y)
# OUTPUT: 1 csv containing (x+y,xy)
script_1_cr = CodeResource(name="Sum and product of x and y",
                                filename="script_1_sum_and_products.py",
                                description="Addition and multiplication")
script_1_cr.save()

# Add code resource revision for code resource (script_1_sum_and_products ) 
with open(os.path.join(samplecode_path, "script_1_sum_and_products.py"), "rb") as f:
    script_1_crRev = CodeResourceRevision(
        coderesource=script_1_cr,
        revision_name="v1",
        revision_desc="First version",
        content_file=File(f))
    script_1_crRev.save()

# Establish code resource revision as a method
script_1_method = Method(
    revision_name="script1",
    revision_desc="script1",
    family = test_mf,driver = script_1_crRev)
script_1_method.full_clean();
script_1_method.save()

# Define "tuple" CDT containing (x,y): members x and y exist at index 1 and 2
tuple_cdt = CompoundDatatype()
tuple_cdt.save()
tuple_cdt.members.create(datatype=string_dt,column_name="x",column_idx=1)
tuple_cdt.members.create(datatype=string_dt,column_name="y",column_idx=2)

# Assign tuple as both an input and an output to script_1_method
script_1_method.inputs.create(compounddatatype = tuple_cdt,
                                   dataset_name = "input_tuple",
                                   dataset_idx = 1)
script_1_method.outputs.create(compounddatatype = tuple_cdt,
                                   dataset_name = "input_tuple",
                                   dataset_idx = 1)
script_1_method.save()

# script_2_square_and_means
# INPUT: 1 csv containing (a,b,c)
# OUTPUT-1: 1 csv containing triplet (a^2,b^2,c^2)
# OUTPUT-2: 1 csv containing singlet mean(a,b,c)
script_2_cr = CodeResource(name="Square and mean of (a,b,c)",
                                filename="script_2_square_and_means.py",
                                description="Square and mean - 2 CSVs")
script_2_cr.save()

# Add code resource revision for code resource (script_2_square_and_means)
with open(os.path.join(samplecode_path, "script_2_square_and_means.py"), "rb") as f:
    script_2_crRev = CodeResourceRevision(
        coderesource=script_2_cr,
        revision_name="v1",
        revision_desc="First version",
        content_file=File(f))
    script_2_crRev.save()

# Establish code resource revision as a method
script_2_method = Method(
    revision_name="script2",
    revision_desc="script2",
    family = test_mf, driver = script_2_crRev)
script_2_method.full_clean();
script_2_method.save()

# Define "singlet" CDT containing CDT member (a) and "triplet" CDT with members (a,b,c)
singlet_cdt = CompoundDatatype()
singlet_cdt.save()
singlet_cdt.members.create(datatype=string_dt,column_name="a",column_idx=1)

triplet_cdt = CompoundDatatype()
triplet_cdt.save()
triplet_cdt.members.create(datatype=string_dt,column_name="a",column_idx=1)
triplet_cdt.members.create(datatype=string_dt,column_name="b",column_idx=2)
triplet_cdt.members.create(datatype=string_dt,column_name="c",column_idx=3)

# Assign triplet as input and output,
script_2_method.inputs.create(compounddatatype = triplet_cdt,
                                   dataset_name = "a_b_c",
                                   dataset_idx = 1)
script_2_method.outputs.create(compounddatatype = triplet_cdt,
                                   dataset_name = "a_b_c_squared",
                                   dataset_idx = 1)
script_2_method.outputs.create(compounddatatype = singlet_cdt,
                                   dataset_name = "a_b_c_mean",
                                   dataset_idx = 2)
script_2_method.save()


# script_3_product
# INPUT-1: Single column (k)
# INPUT-2: Single-row, single column (r)
# OUTPUT-1: Single column r*(k)
script_3_cr = CodeResource(name="Scalar multiple of k",
                                filename="script_3_product.py",
                                description="Product of input")
script_3_cr.save()

# Add code resource revision for code resource (script_3_product)
with open(os.path.join(samplecode_path, "script_3_product.py"), "rb") as f:
    script_3_crRev = CodeResourceRevision(
        coderesource=script_3_cr,
        revision_name="v1",
        revision_desc="First version",
        content_file=File(f))
    script_3_crRev.save()

# Establish code resource revision as a method
script_3_method = Method(
    revision_name="script3",
    revision_desc="script3",
    family = test_mf,
    driver = script_3_crRev)
script_3_method.full_clean();
script_3_method.save()

# Assign singlet as input and output
script_3_method.inputs.create(compounddatatype = singlet_cdt,
                                   dataset_name = "k",
                                   dataset_idx = 1)

script_3_method.inputs.create(compounddatatype = singlet_cdt,
                                   dataset_name = "r",
                                   dataset_idx = 2,
                                   max_row = 1,
                                   min_row = 1)

script_3_method.outputs.create(compounddatatype = singlet_cdt,
                                   dataset_name = "kr",
                                   dataset_idx = 1)
script_3_method.save()

#################### END OF METHOD DEFINITIONS #########################

# Define DNAcomp_pf
DNAcomp_pf = PipelineFamily(
        name="DNAcomplement",
        description="DNA complement pipeline.");
DNAcomp_pf.save();

# Define DNAcompv1_p (pipeline revision)
DNAcompv1_p = DNAcomp_pf.members.create(
        revision_name="v1",
        revision_desc="First version");

# Add Pipeline input CDT DNAinput_cdt to pipeline revision DNAcompv1_p
DNAcompv1_p.inputs.create(
        compounddatatype=DNAinput_cdt,
        dataset_name="seqs_to_complement",
        dataset_idx=1);

# Add a step to Pipeline revision DNAcompv1_p involving
# a transformation DNAcompv2_m at step 1
step1 = DNAcompv1_p.steps.create(
        transformation=DNAcompv2_m,
        step_num=1);

# Add cabling (PipelineStepInputCable's) to (step1, DNAcompv1_p)
# From step 0, output hole "seqs_to_comeplement" to
# input hole "input" (of this step)
step1.cables_in.create(transf_input=DNAcompv2_m.inputs.get(dataset_name="input"),
                      step_providing_input=0,
                      provider_output=DNAcompv1_p.inputs.get(
                          dataset_name="seqs_to_complement"));

# Add output cabling (PipelineOutputCable) to DNAcompv1_p
# From step 1, output hole "output", send output to
# Pipeline output hole "complemented_seqs" at index 1
outcabling = DNAcompv1_p.outcables.create(
        step_providing_output=1,
        provider_output=step1.transformation.outputs.get(dataset_name="output"),
        output_name="complemented_seqs",
        output_idx=1);

# DNArecomp_mf is a MethodFamily called DNArecomplement
DNArecomp_mf = MethodFamily(
        name="DNArecomplement",
        description="Re-complement DNA nucleotide sequences.");
DNArecomp_mf.full_clean();
DNArecomp_mf.save();

# Add to MethodFamily DNArecomp_mf a method revision DNArecomp_m
DNArecomp_m = DNArecomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=compv2_crRev);

# To this method revision, add inputs with CDT DNAoutput_cdt
DNArecomp_m.inputs.create(
        compounddatatype = DNAoutput_cdt,
        dataset_name = "complemented_seqs",
        dataset_idx = 1);

# To this method revision, add outputs with CDT DNAinput_cdt
DNArecomp_m.outputs.create(
        compounddatatype = DNAinput_cdt,
        dataset_name = "recomplemented_seqs",
        dataset_idx = 1);

f = open(os.path.join(samplecode_path, "stringUT.py"), "rb")
dt_1 = Datatype(name="dt_1",
                description="A string validated by stringUT.py",
                verification_script=File(f),
                Python_type="str");
dt_1.save()
dt_1 = dt_1

dt_2 = Datatype(name="dt_2",
                description="A string validated by stringUT.py",
                verification_script=File(f),
                Python_type="str");
dt_2.save()
dt_2 = dt_2

dt_3 = Datatype(name="dt_3",
                description="A string validated by stringUT.py",
                verification_script=File(f),
                Python_type="str");
dt_3.save()
dt_3 = dt_3

dt_4 = Datatype(name="dt_4",
                description="A string validated by stringUT.py",
                verification_script=File(f),
                Python_type="str");
dt_4.save()
dt_4 = dt_4

dt_5 = Datatype(name="dt_5",
                description="A string validated by stringUT.py",
                verification_script=File(f),
                Python_type="str");
dt_5.save()
dt_5 = dt_5

# The following is for testing code resource dependencies
test_cr_1 = CodeResource(name="test_cr_1",
                         filename="test_cr_1.py",
                         description="CR1")
test_cr_1.save()
test_cr_1_rev1 = CodeResourceRevision(coderesource=test_cr_1,
                                      revision_name="v1",
                                      revision_desc="CR1-rev1",
                                      content_file=File(f))
test_cr_1_rev1.save()
test_cr_1 = test_cr_1
test_cr_1_rev1 = test_cr_1_rev1

test_cr_2 = CodeResource(name="test_cr_2",
                         filename="test_cr_2.py",
                         description="CR2")
test_cr_2.save()
test_cr_2_rev1 = CodeResourceRevision(coderesource=test_cr_2,
                                      revision_name="v2",
                                      revision_desc="CR2-rev1",
                                      content_file=File(f))
test_cr_2_rev1.save()
test_cr_2 = test_cr_2
test_cr_2_rev1 = test_cr_2_rev1

test_cr_3 = CodeResource(name="test_cr_3",
                         filename="test_cr_3.py",
                         description="CR3")
test_cr_3.save()
test_cr_3_rev1 = CodeResourceRevision(coderesource=test_cr_3,
                                      revision_name="v3",
                                      revision_desc="CR3-rev1",
                                      content_file=File(f))
test_cr_3_rev1.save()
test_cr_3 = test_cr_3
test_cr_3_rev1 = test_cr_3_rev1

test_cr_4 = CodeResource(name="test_cr_4",
                         filename="test_cr_4.py",
                         description="CR4")
test_cr_4.save()
test_cr_4_rev1 = CodeResourceRevision(coderesource=test_cr_4,
                                      revision_name="v4",
                                      revision_desc="CR4-rev1",
                                      content_file=File(f))
test_cr_4_rev1.save()
test_cr_4 = test_cr_4
test_cr_4_rev1 = test_cr_4_rev1
