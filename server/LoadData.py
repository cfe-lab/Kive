#! /usr/bin/env python

# Script that loads test data into the database.  Must be run from the shell
# produced by manage.py.

from copperfish.models import Datatype, CompoundDatatype, CodeResourceRevision, CodeResource, Method, MethodFamily, TransformationInput, TransformationOutput;
from django.utils import timezone;
from django.core.files import File;
import glob, os.path;

samplecode_path = "../samplecode";

# Add in the string, DNA nucleotide sequence, and RNA nucleotide sequence
# Datatypes.
with open(os.path.join(samplecode_path, "stringUT.py"), "rb") as f:
    string_dt = Datatype(name="string", description="String (basically anything)",
                         #dateCreated=timezone.now(),
                         verification_script=File(f), Python_type="str");
    string_dt.full_clean();
    string_dt.save();

with open(os.path.join(samplecode_path, "DNANucSeqUT.py"), "rb") as f:
    DNA_dt = Datatype(name="DNA nucleotide sequence",
                      description="String consisting of ACGTacgt",
                      #dateCreated=timezone.now(),
                      verification_script=File(f), Python_type="str");
    DNA_dt.save();
    # This restricts String.  This step has to be after it's saved
    DNA_dt.restricts.add(string_dt);
    DNA_dt.full_clean();
    DNA_dt.save();

with open(os.path.join(samplecode_path, "RNANucSeqUT.py"), "rb") as f:
    RNA_dt = Datatype(name="RNA nucleotide sequence",
                      description="String consisting of ACGUacgu",
                      #dateCreated=timezone.now(),
                      verification_script=File(f), Python_type="str");

    RNA_dt.save();
    RNA_dt.restricts.add(string_dt);
    RNA_dt.full_clean();
    RNA_dt.save();


#####
# Define some compound datatypes
test_cd = CompoundDatatype();
test_cd.save();
test_cd.members.create(datatype=string_dt, column_name="label",
                       column_idx=1);
test_cd.members.create(datatype=DNA_dt, column_name="PBMCseq",
                       column_idx=2);
test_cd.members.create(datatype=RNA_dt, column_name="PLAseq",
                       column_idx=3);
test_cd.full_clean();
test_cd.save();

# Input compound datatype for complementation method
DNAinput_cd = CompoundDatatype();
DNAinput_cd.save();
DNAinput_cd.members.create(datatype=DNA_dt, column_name="SeqToComplement",
                        column_idx=1);
DNAinput_cd.full_clean();
DNAinput_cd.save();

# Output compound datatype for complementation method
DNAoutput_cd = CompoundDatatype();
DNAoutput_cd.save();
DNAoutput_cd.members.create(datatype=DNA_dt, column_name="ComplementedSeq",
                         column_idx=1);
DNAoutput_cd.full_clean();
DNAoutput_cd.save();


# Same but for RNA
RNAinput_cd = CompoundDatatype();
RNAinput_cd.save();
RNAinput_cd.members.create(datatype=RNA_dt, column_name="SeqToComplement",
                        column_idx=1);
RNAinput_cd.full_clean();
RNAinput_cd.save();

RNAoutput_cd = CompoundDatatype();
RNAoutput_cd.save();
RNAoutput_cd.members.create(datatype=RNA_dt, column_name="ComplementedSeq",
                         column_idx=1);
RNAoutput_cd.full_clean();
RNAoutput_cd.save();


####
# Code resources
comp_cr = CodeResource(
        name="complement.py",
        description="Script to complement DNA/RNA nucleotide sequences");
comp_cr.save();

# Add version 1 and version 2
with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
    compv1_rev = CodeResourceRevision(coderesource=comp_cr,
                                      revision_name="v1",
                                      revision_desc="First version",
                                      content_file=File(f));
    compv1_rev.full_clean();
    compv1_rev.save();
    comp_cr.revisions.add(compv1_rev);
    
with open(os.path.join(samplecode_path, "complement_v2.py"), "rb") as f:
    compv2_rev = CodeResourceRevision(
            coderesource=comp_cr,
            revision_name="v2",
            revision_desc="Second version: better docstring",
            revision_parent=compv1_rev,
            content_file=File(f));
    compv2_rev.full_clean();
    compv2_rev.save();
    comp_cr.revisions.add(compv2_rev);

comp_cr.save();

####
# Now add some methods
DNAcomp_mf = MethodFamily(name="DNAcomplement",
                          description="Complement DNA nucleotide sequences.");
DNAcomp_mf.full_clean();
DNAcomp_mf.save();

DNAcompv1_m = DNAcomp_mf.members.create(revision_name="v1",
                                        revision_desc="First version",
                                        driver=compv1_rev);

DNAinput_ti = DNAcompv1_m.inputs.create(compounddatatype = DNAinput_cd,
                                        dataset_name = "input",
                                        dataset_idx = 1);
DNAinput_ti.full_clean();
DNAinput_ti.save();
DNAoutput_to = DNAcompv1_m.outputs.create(compounddatatype = DNAoutput_cd,
                                          dataset_name = "output",
                                          dataset_idx = 1);
DNAoutput_to.full_clean();
DNAoutput_to.save();


DNAcompv2_m = DNAcomp_mf.members.create(revision_name="v2",
                                        revision_desc="Second version",
                                        revision_parent=DNAcompv1_m,
                                        driver=compv2_rev);
DNAcompv2_m.full_clean();
DNAcompv2_m.save();


# Same but for RNA
RNAcomp_mf = MethodFamily(name="RNAcomplement",
                          description="Complement RNA nucleotide sequences.");
RNAcomp_mf.full_clean();
RNAcomp_mf.save();

RNAcompv1_m = RNAcomp_mf.members.create(revision_name="v1",
                                        revision_desc="First version",
                                        driver=compv1_rev);

RNAinput_ti = RNAcompv1_m.inputs.create(compounddatatype = RNAinput_cd,
                                        dataset_name = "input",
                                        dataset_idx = 1);
RNAinput_ti.full_clean();
RNAinput_ti.save();
RNAoutput_to = RNAcompv1_m.outputs.create(compounddatatype = RNAoutput_cd,
                                          dataset_name = "output",
                                          dataset_idx = 1);
RNAoutput_to.full_clean();
RNAoutput_to.save();


RNAcompv2_m = RNAcomp_mf.members.create(revision_name="v2",
                                        revision_desc="Second version",
                                        revision_parent=RNAcompv1_m,
                                        driver=compv2_rev);
RNAcompv2_m.full_clean();
RNAcompv2_m.save();

# TO DO:
# program in a few pipelines
# write unit tests?
# check that Dataset works
