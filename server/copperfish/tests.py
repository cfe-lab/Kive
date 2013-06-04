"""
Unit tests for ShipYard (Copperfish)
"""

from django.test import TestCase;
from copperfish.models import *;
from django.core.files import File;
from django.core.exceptions import ValidationError;
import os;
import glob, os.path;
import hashlib;

samplecode_path = "../samplecode";

# All classes that inherit TestCase are evaluated by the TestUtility

class CopperfishMethodTests_setup(TestCase):
    """
    Set up a database state for unit testing

    3 data types (_dt), 5 compound data types (_cdt)
    1 code resource (_cr) with 2 revisions (_crRev)
    3 method families (_mf), 1 pipeline family (_pf)
    5 methods (_m), 1 pipeline (_p)

	Extends all other classes that require this state.
    """

    def setUp(self):
        """Setup default database state from which to perform unit testing."""

        # Create Datatype "string" with validation code stringUT.py
        with open(os.path.join(samplecode_path, "stringUT.py"), "rb") as f:
            string_dt = Datatype(name="string",
                                 description="String (basically anything)",
                                 verification_script=File(f),
                                 Python_type="str");
            string_dt.full_clean();
            string_dt.save();

        # Create Datatype "DNANucSeq" with validation code DNANucSeqUT.py
        with open(os.path.join(samplecode_path, "DNANucSeqUT.py"), "rb") as f:
            DNA_dt = Datatype(name="DNANucSeq",
                              description="String consisting of ACGTacgt",
                              verification_script=File(f),
                              Python_type="str");
            DNA_dt.save();

            # DNA_dt is a restricted type of string
            DNA_dt.restricts.add(string_dt);
            DNA_dt.full_clean();
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
            RNA_dt.full_clean();
            RNA_dt.save();

        # Make accessible outside of this function
        self.string_dt = string_dt;
        self.DNA_dt = DNA_dt;
        self.RNA_dt = RNA_dt;

        # Define test_cdt as containing 3 members:
        # (label, PBMCseq, PLAseq) as (string,DNA,RNA)
        self.test_cdt = CompoundDatatype();
        self.test_cdt.save();

        self.test_cdt.members.create(datatype=self.string_dt,
                                    column_name="label",
                                    column_idx=1);
        self.test_cdt.members.create(datatype=self.DNA_dt,
                                    column_name="PBMCseq",
                                    column_idx=2);
        self.test_cdt.members.create(datatype=self.RNA_dt,
                                    column_name="PLAseq",
                                    column_idx=3);
        self.test_cdt.full_clean();
        self.test_cdt.save();


        # Define DNAinput_cdt (1 member)
        self.DNAinput_cdt = CompoundDatatype();
        self.DNAinput_cdt.save();
        self.DNAinput_cdt.members.create(datatype=self.DNA_dt,
                                        column_name="SeqToComplement",
                                        column_idx=1);
        self.DNAinput_cdt.full_clean();
        self.DNAinput_cdt.save();

        # Define DNAoutput_cdt (1 member)
        self.DNAoutput_cdt = CompoundDatatype();
        self.DNAoutput_cdt.save();
        self.DNAoutput_cdt.members.create(datatype=self.DNA_dt,
                                         column_name="ComplementedSeq",
                                         column_idx=1);
        self.DNAoutput_cdt.full_clean();
        self.DNAoutput_cdt.save();

        # Define RNAinput_cdt (1 column)
        self.RNAinput_cdt = CompoundDatatype();
        self.RNAinput_cdt.save();
        self.RNAinput_cdt.members.create(datatype=self.RNA_dt,
                                         column_name="SeqToComplement",
                                         column_idx=1);
        self.RNAinput_cdt.full_clean();
        self.RNAinput_cdt.save();

        # Define RNAoutput_cdt (1 column)
        self.RNAoutput_cdt = CompoundDatatype();
        self.RNAoutput_cdt.save();
        self.RNAoutput_cdt.members.create(datatype=self.RNA_dt,
                                          column_name="ComplementedSeq",
                                          column_idx=1);
        self.RNAoutput_cdt.full_clean();
        self.RNAoutput_cdt.save();

        # Define comp_cr
        self.comp_cr = CodeResource(
                name="complement.py",
                description="Complement DNA/RNA nucleotide sequences");
        self.comp_cr.save();

        # Define compv1_crRev for comp_cr
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            self.compv1_crRev = CodeResourceRevision(
                    coderesource=self.comp_cr,
                    revision_name="v1",
                    revision_desc="First version",
                    content_file=File(f));
            self.compv1_crRev.full_clean();
            self.compv1_crRev.save();

        # Define compv2_crRev for comp_cr
        with open(os.path.join(samplecode_path, "complement_v2.py"), "rb") as f:
            self.compv2_crRev = CodeResourceRevision(
                coderesource=self.comp_cr,
                revision_name="v2",
                revision_desc="Second version: better docstring",
                revision_parent=self.compv1_crRev,
                content_file=File(f));
            self.compv2_crRev.full_clean();
            self.compv2_crRev.save();

        # Define DNAcomp_mf
        self.DNAcomp_mf = MethodFamily(
                name="DNAcomplement",
                description="Complement DNA nucleotide sequences.");
        self.DNAcomp_mf.full_clean();
        self.DNAcomp_mf.save();

        # Define DNAcompv1_m (method revision) for DNAcomp_mf with driver compv1_crRev
        self.DNAcompv1_m = self.DNAcomp_mf.members.create(
                revision_name="v1",
                revision_desc="First version",
                driver=self.compv1_crRev);

        # Add input DNAinput_cdt to DNAcompv1_m
        self.DNAinput_ti = self.DNAcompv1_m.inputs.create(
                compounddatatype = self.DNAinput_cdt,
                dataset_name = "input",
                dataset_idx = 1);
        self.DNAinput_ti.full_clean();
        self.DNAinput_ti.save();

        # Add output DNAoutput_cdt to DNAcompv1_m
        self.DNAoutput_to = self.DNAcompv1_m.outputs.create(
                compounddatatype = self.DNAoutput_cdt,
                dataset_name = "output",
                dataset_idx = 1);
        self.DNAoutput_to.full_clean();
        self.DNAoutput_to.save();

        # Define DNAcompv2_m for DNAcomp_mf with driver compv2_crRev
        # Input/output should be copied from DNAcompv1_m
        self.DNAcompv2_m = self.DNAcomp_mf.members.create(
                revision_name="v2",
                revision_desc="Second version",
                revision_parent=self.DNAcompv1_m,
                driver=self.compv2_crRev);
        self.DNAcompv2_m.full_clean();
        self.DNAcompv2_m.save();

        # Define second family, RNAcomp_mf
        self.RNAcomp_mf = MethodFamily(
                name="RNAcomplement",
                description="Complement RNA nucleotide sequences.");
        self.RNAcomp_mf.full_clean();
        self.RNAcomp_mf.save();

        # Define RNAcompv1_m for RNAcomp_mf with driver compv1_crRev
        self.RNAcompv1_m = self.RNAcomp_mf.members.create(
                revision_name="v1",
                revision_desc="First version",
                driver=self.compv1_crRev);
        
        # Add input RNAinput_cdt to RNAcompv1_m
        self.RNAinput_ti = self.RNAcompv1_m.inputs.create(
                compounddatatype = self.RNAinput_cdt,
                dataset_name = "input",
                dataset_idx = 1);
        self.RNAinput_ti.full_clean();
        self.RNAinput_ti.save();

        # Add output RNAoutput_cdt to RNAcompv1_m
        self.RNAoutput_to = self.RNAcompv1_m.outputs.create(
                compounddatatype = self.RNAoutput_cdt,
                dataset_name = "output",
                dataset_idx = 1);
        self.RNAoutput_to.full_clean();
        self.RNAoutput_to.save();

        # Define RNAcompv2_m for RNAcompv1_mf with driver compv2_crRev
        # Input/outputs should be copied from RNAcompv1_m
        self.RNAcompv2_m = self.RNAcomp_mf.members.create(
                revision_name="v2",
                revision_desc="Second version",
                revision_parent=self.RNAcompv1_m,
                driver=self.compv2_crRev);
        self.RNAcompv2_m.full_clean();
        self.RNAcompv2_m.save();

        #################### END OF METHOD DEFINITIONS #########################

        # Define DNAcomp_pf
        self.DNAcomp_pf = PipelineFamily(
                name="DNAcomplement",
                description="DNA complement pipeline.");
        self.DNAcomp_pf.save();

        # Define DNAcompv1_p (pipeline revision)
        self.DNAcompv1_p = self.DNAcomp_pf.members.create(
                revision_name="v1",
                revision_desc="First version");

        # Add Pipeline input CDT DNAinput_cdt to pipeline revision DNAcompv1_p
        self.DNAcompv1_p.inputs.create(
                compounddatatype=self.DNAinput_cdt,
                dataset_name="seqs_to_complement",
                dataset_idx=1);

        # Add a step to Pipeline revision DNAcompv1_p involving
        # a transformation DNAcompv2_m at step 1
        step1 = self.DNAcompv1_p.steps.create(
                transformation=self.DNAcompv2_m,
                step_num=1);

        # Add wiring (PipelineStepInputs) to (step1, DNAcompv1_p)
        # From step 0, output hole "seqs_to_comeplement" to
        # input hole "input" (of this step)
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="seqs_to_complement");

        # Add output wiring (PipelineOutputMapping) to DNAcompv1_p
        # From step 1, output hole "output", send output to
        # Pipeline output hole "complemented_seqs" at index 1
        mapping = self.DNAcompv1_p.outmap.create(
                step_providing_output=1,
                provider_output_name="output",
                output_name="complemented_seqs",
                output_idx=1);

        # Why do we resave DNAcompv1_p??
        self.DNAcompv1_p.save();

        # DNArecomp_mf is a MethodFamily called DNArecomplement
        self.DNArecomp_mf = MethodFamily(
                name="DNArecomplement",
                description="Re-complement DNA nucleotide sequences.");
        self.DNArecomp_mf.full_clean();
        self.DNArecomp_mf.save();

        # Add to MethodFamily DNArecomp_mf a method revision DNArecomp_m
        self.DNArecomp_m = self.DNArecomp_mf.members.create(
                revision_name="v1",
                revision_desc="First version",
                driver=self.compv2_crRev);

        # To this method revision, add inputs with CDT DNAoutput_cdt
        self.DNArecomp_m.inputs.create(
                compounddatatype = self.DNAoutput_cdt,
                dataset_name = "complemented_seqs",
                dataset_idx = 1);

        # To this method revision, add outputs with CDT DNAinput_cdt
        self.DNArecomp_m.outputs.create(
                compounddatatype = self.DNAinput_cdt,
                dataset_name = "recomplemented_seqs",
                dataset_idx = 1);

    def tearDown(self):
        filesToDelete = ["stringUT.py", "DNANucSeqUT.py", "RNANucSeqUT.py"];
        for f in filesToDelete:
            os.remove(os.path.join("VerificationScripts",f));

        filesToDelete = ["complement.py", "complement_v2.py"];

        for f in filesToDelete:
            os.remove(os.path.join("CodeResources",f));


class Datatype_tests(TestCase):
    
    def test_datatype_unicode(self):
        """
        Unicode representation must be the instance's name.

        """
        my_datatype = Datatype(name="fhqwhgads");
        self.assertEqual(unicode(my_datatype), "fhqwhgads");

    # UNIT TESTS TO CHECK CLEAN ("self" restriction)
        
    # Direct circular cases: start, middle, end
    # Start   dt1 restricts dt1, dt3, dt4
    # Middle  dt1 restricts dt3, dt1, dt4
    # End     dt1 restricts dt3, dt4, dt1
    # Good    dt1 restricts dt2, dt3, dt4

    # Recursive cases: start, middle, end
    # Start   dt1 restricts dt2, dt3, dt4 (dt2 restricts dt1)
    # Middle  dt1 restricts dt2, dt3, dt4 (dt3 restricts dt1)
    # End     dt1 restricts dt2, dt3, dt4 (dt4 restricts dt1)
    # Good-1  dt1 restricts dt2, dt3, dt4 (dt2 restricts dt5)
    # Good-2  dt1 restricts dt2, dt3, dt4 (dt3 restricts dt5)
    # Good-3  dt1 restricts dt2, dt3, dt4 (dt4 restricts dt5)
    # Good-4  dt1 restricts dt2, dt3, dt4 (dt2 restricts dt4)
    # Good-5  dt1 restricts dt2, dt3, dt4 (dt3 restricts dt4)
    # Good-6  dt1 restricts dt2, dt3, dt4 (dt4 restricts dt2)


    # UNIT TESTS TO VALIDATE is_restricted_by IN GENERAL
    
    # dt1 restricts dt2
    # dt1.is_restricted_by(dt2) - FALSE
    # dt2.is_restricted_by(dt1) - TRUE

    # dt1 exists, dt2 exists
    # dt1.is_restricted_by(dt2) - FALSE

    # dt1 restricts dt2, dt2 restricts dt3
    # dt1.is_restricted_by(dt3) - FALSE
    # dt3.is_restricted_by(dt1) - TRUE
    # dt1.is_restricted_by(dt2) - FALSE
    # dt2.is_restricted_by(dt1) - TRUE
    
    # dt1 restricts dt[2,3,4]
    # dt[2,3,4].is_restricted_by(dt1)

    # dt1 restricts dt[2,3,4], 1 of dt[2,3,4] restrict dt5 (3 cases)
    # dt1.is_restricted_by(dt[2,3,4]) - FALSE
    # dt1.is_restricted_by(dt5) - FALSE
    # Not necessary: dt[2,3,4].is_restricted_by(dt1) - TRUE
    # Not necessary: dt5.is_restricted_by(dt1) - TRUE

    # The following are not necessary:
    # dt1, dt2, dt3 restrict dt4
    # dt4.is_restricted_by(dt[1,2,3])
    # dt[1,2,3].is_restricted_by(dt4)




    def test_datatype_no_restriction_clean_good (self):
        """
        Datatype without any restrictions.

        """
        f = open(os.path.join(samplecode_path, "stringUT.py"), "rb")
        dt_1 = Datatype(name="dt_1",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_1.save();
        self.assertEqual(dt_1.clean(), None);

    def test_datatype_nested_valid_restrictions_clean_good(self):
        """
        Datatypes such that A restricts B, and B restricts C
        
        """

        f = open(os.path.join(samplecode_path, "stringUT.py"), "rb")
        dt_1 = Datatype(name="dt_1",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_1.save();

        dt_2 = Datatype(name="dt_2",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_2.save();

        dt_3 = Datatype(name="dt_2",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_3.save();
        
        dt_1.restricts.add(dt_2);
        dt_1.save();
        dt_2.restricts.add(dt_3);
        dt_2.save();

        self.assertEqual(dt_1.clean(), None);
        self.assertEqual(dt_2.clean(), None);
        self.assertEqual(dt_3.clean(), None);

    def test_datatype_nested_invalid_restrictions_scrambled_clean_bad(self):
        """
        Datatypes are restricted to constrain execution order such that:

        A restricts C
        A restricts B
        B restricts C
        C restricts A
        """

        f = open(os.path.join(samplecode_path, "stringUT.py"), "rb")
        dt_1 = Datatype(name="dt_1",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_1.save();

        dt_2 = Datatype(name="dt_2",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_2.save();

        dt_3 = Datatype(name="dt_2",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_3.save();
        
        dt_1.restricts.add(dt_3);
        dt_1.save();
        dt_1.restricts.add(dt_2);
        dt_1.save();
        dt_2.restricts.add(dt_3);
        dt_2.save();
        dt_3.restricts.add(dt_1);
        dt_3.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                dt_1.clean);


    def test_datatype_direct_circular_restriction_clean_bad(self):
        """
        Datatype directly restricts itself: A restricts A
        
        """

        # Define one datatype
        f = open(os.path.join(samplecode_path, "stringUT.py"), "rb")
        dt_1 = Datatype(name="dt_1",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_1.save();

        # And have it restrict itself
        dt_1.restricts.add(dt_1);
        dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                dt_1.clean);

    def test_datatype_circular_restriction_indirect_clean(self):
        """
        Datatype restricts itself through intermediary:

        A restricts B
        B restricts A
        
        """

        # Define two datatypes
        f = open(os.path.join(samplecode_path, "stringUT.py"), "rb")
        dt_1 = Datatype(name="dt_1",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_1.save();

        dt_2 = Datatype(name="dt_2",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_2.save();

        # And have them form a circular restriction path
        dt_1.restricts.add(dt_2);
        dt_1.save();
        dt_2.restricts.add(dt_1);
        dt_2.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                dt_1.clean);



class CompoundDatatypeMember_tests(CopperfishMethodTests_setup):
    def test_cdtMember_unicode(self):
        """
        Unicode of compoundDatatypeMember should return
        (column index, datatype name, column name)
        
        """
        self.assertEqual(unicode(self.test_cdt.members.all()[0]),
                         "1: <string> [label]");
        self.assertEqual(unicode(self.test_cdt.members.all()[1]),
                         "2: <DNANucSeq> [PBMCseq]");
        self.assertEqual(unicode(self.test_cdt.members.all()[2]),
                         "3: <RNANucSeq> [PLAseq]");

class CompoundDatatype_tests(CopperfishMethodTests_setup):

    def test_cdt_zero_member_unicode(self):
        """
        Unicode of empty CompoundDatatype should just be '()'.

        """
        empty_cdt = CompoundDatatype();
        empty_cdt.save();
        self.assertEqual(unicode(empty_cdt), "[empty CompoundDatatype]");

    def test_cdt_single_member_unicode(self):
        """
        Unicode of CompoundDatatype should return a list of members

        """
        self.assertEqual(unicode(self.DNAinput_cdt),
                         "(1: <DNANucSeq> [SeqToComplement])");

    def test_cdt_multiple_members_unicode(self):
        """
        Unicode of CompoundDatatype should return a list of members
        in the form of unicode(CompoundDatatypeMember)
        
        """
        self.assertEqual(unicode(self.test_cdt),
                         "(1: <string> [label], 2: <DNANucSeq> [PBMCseq], " +
                         "3: <RNANucSeq> [PLAseq])");

    def test_clean_single_index_good (self):
        """
        CompoundDatatype with single index equalling 1

        """
        sad_cdt = CompoundDatatype();
        sad_cdt.save();
        sad_cdt.members.create(	datatype=self.RNA_dt,
                                column_name="ColumnTwp",
                                column_idx=1);
        self.assertEqual(sadd_cdt.clean(), None);

    def test_clean_single_index_bad (self):
        """
        CompoundDatatype with single index not equalling 1.

        """
        sad_cdt = CompoundDatatype();
        sad_cdt.save();
        sad_cdt.members.create(	datatype=self.RNA_dt,
                                column_name="ColumnTwp",
                                column_idx=3);

        self.assertRaisesRegexp(
            ValidationError,
            "Column indices are not consecutive starting from 1",
            sad_cdt.clean);

    def test_clean_catches_consecutive_member_indices (self):
        """
        CompoundDatatype must have consecutive indices from 1 to n.
        
        Otherwise, throw a ValidationError.
        
        """
        # FIXME: make sure docstrings look like this.

        # test_cdt is valid
        self.assertEqual(self.test_cdt.clean(), None);

        # Define 2 member cdt with valid indexing
        good_cdt = CompoundDatatype();
        good_cdt.save();
        good_cdt.members.create(datatype=self.RNA_dt,
                               column_name="ColumnTwp",
                               column_idx=2);
        good_cdt.members.create(datatype=self.DNA_dt,
                               column_name="ColumnOne",
                               column_idx=1);
        self.assertEqual(good_cdt.clean(), None);

        # Define 2 member cdt with invalid indexing
        bad_cdt = CompoundDatatype();
        bad_cdt.save();
        bad_cdt.members.create(datatype=self.RNA_dt,
                              column_name="ColumnOne",
                              column_idx=3);
        
        bad_cdt.members.create(datatype=self.DNA_dt,
                              column_name="ColumnTwo",
                              column_idx=1);

        self.assertRaisesRegexp(
            ValidationError,
            "Column indices are not consecutive starting from 1",
            bad_cdt.clean);

class CodeResource_tests(CopperfishMethodTests_setup):
     
    def test_codeResource_unicode(self):
        """
        codeResource.unicode() should return it's name.
        """
        self.assertEquals(unicode(self.comp_cr), "complement.py");

    def test_codeResource_clean(self):
        """
        Check that CodeResource.name is valid for a filename.
        """
        pass

class CodeResourceRevision_tests(CopperfishMethodTests_setup):

    def test_codeResourceRevision_unicode(self):
        """
        CodeResourceRevision.unicode() should return it's code resource
        name and it's code resource revision name.

        Or, if no CodeResource has been linked, should display an error.
        """

        # Valid crRev should return it's cr.name and crRev.revision_name
        self.assertEquals(unicode(self.compv1_crRev), "complement.py v1");

        # Define a crRev without a linking cr, or a revision_name
        no_cr_set = CodeResourceRevision();
        self.assertEquals(unicode(no_cr_set), "[no code resource set] [no revision name]");

        # Define a crRev without a linking cr, with a revision_name of foo
        no_cr_set.revision_name = "foo";
        self.assertEquals(unicode(no_cr_set), "[no code resource set] foo");

    def test_clean_blank_MD5_on_codeResourceRevision_without_file(self):
        """
        If no file is specified, MD5 should be empty string.
        """
        # Create crRev with a codeResource but no file contents
        no_file_crRev = CodeResourceRevision(
                coderesource=self.comp_cr,
                revision_name="foo",
                revision_desc="foo");
  
        no_file_crRev.clean();

        # After clean(), MD5 checksum should be the empty string
        self.assertEquals(no_file_crRev.MD5_checksum, "");

    def test_clean_valid_MD5_on_codeResourceRevision_with_file(self):
        """
        If file contents are associated with a crRev, an MD5 should exist.
        """

        # Compute the reference MD5
        md5gen = hashlib.md5();
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            md5gen.update(f.read());

        # Revision should have the correct MD5 checksum
        self.assertEquals(
                md5gen.hexdigest(),
                self.comp_cr.revisions.get(revision_name="v1").MD5_checksum);

    def test_clean_for_colliding_dependency_paths(self):
        """
        CodeResourceDependencies cannot overwrite each other in the sandbox
        (Or, the original CodeResource that required it!!)
        """
        pass

class CodeResourceDependency_tests(CopperfishMethodTests_setup):

    def test_codeResourceDependency_unicode(self):
        """
        Unicode of CodeResourceDependency should return:
        <self.crRev> requires <referenced crRev> as <filePath>
        """

        # v1 is a revision of comp_cr such that revision_name = v1
        v1 = self.comp_cr.revisions.get(revision_name="v1");
        v2 = self.comp_cr.revisions.get(revision_name="v2");

        # Define a fake dependency where v1 requires v2 in subdir/foo.py
        test_crd = CodeResourceDependency(coderesourcerevision=v1,
                                          requirement=v2,
                                          where="subdir/foo.py");

        # Display unicode for this dependency under valid conditions
        self.assertEquals(
                unicode(test_crd),
                "complement.py v1 requires complement.py v2 as subdir/foo.py");

    def test_codeResourceDependency_bad_path_clean(self):
        """
        Check
        """
        pass

    def test_codeResourceDependency_good_path_with_filename_clean(self):
        """
        Check
        """
        pass

    def test_codeResourceDependency_good_path_but_missing_filename_clean(self):
        """
        Check
        """
        pass


class methodFamily_tests(CopperfishMethodTests_setup):

    def test_methodFamily_unicode(self):
        """
        unicode() for methodFamily should display it's name
        """
        
        self.assertEqual(unicode(self.DNAcomp_mf),
                         "DNAcomplement");

class method_tests(CopperfishMethodTests_setup):

    def test_method_with_family_unicode(self):
        """
        unicode() for method should return "Method revisionName and family name"
        """

        # DNAcompv1_m has method family DNAcomplement
        self.assertEqual(unicode(self.DNAcompv1_m),
                         "Method DNAcomplement v1");

    def test_method_without_family_unicode(self):
        """
        unicode() for Test unicode representation when family is unset.
        """
        nofamily = Method(revision_name="foo");

        self.assertEqual(unicode(nofamily),
                         "Method [family unset] foo");

    def test_method_single_valid_input_checkInputIndices_good(self):
        """
        check_input_indices() should return no exception if
        it's transformation only has valid input indices defined.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # Add one valid input cdt at index 1 named "oneinput" to transformation
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        # check_input_indices() should not raise a ValidationError
        self.assertEquals(foo.check_input_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_many_ordered_valid_inputs_checkInputIndices_good (self):
        """
        check_input_indices should return no exception if
        it's transformation only has valid input indices defined
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # Add several input cdts that together are valid
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=3);

        # No ValidationErrors should be raised
        self.assertEquals(foo.check_input_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_many_valid_inputs_scrambled_checkInputIndices_good (self):
        """
        check_input_indices should return no exception if
        it's transformation only has valid input indices defined
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # Add several input cdts that together are valid
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=3);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=1);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=2);

        # No ValidationErrors should be raised
        self.assertEquals(foo.check_input_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_one_invalid_input_checkInputIndices_bad(self):
        """
        Test input index check, one badly-indexed input case.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # Add one invalid input cdt at index 4 named "oneinput"
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=4);

        # check_input_indices() should raise a ValidationError
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.check_input_indices);

        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_method_many_nonconsective_inputs_scrambled_checkInputIndices_bad(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=6);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.check_input_indices);

        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_method_one_valid_output_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="oneoutput", dataset_idx=1);
        self.assertEquals(foo.check_output_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_many_valid_outputs_scrambled_checkOutputIndices_good (self):
        """Test output index check, well-indexed multi-output (scrambled order) case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="oneoutput", dataset_idx=3);
        foo.outputs.create(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="twooutput", dataset_idx=1);
        foo.outputs.create(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="threeoutput", dataset_idx=2);
        self.assertEquals(foo.check_output_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_one_invalid_output_checkOutputIndices_bad (self):
        """Test output index check, one badly-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="oneoutput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.check_output_indices);

        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_method_many_invalid_outputs_scrambled_checkOutputIndices_bad(self):
        """Test output index check, badly-indexed multi-output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        
        foo.outputs.create(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="oneoutput", dataset_idx=2);
        foo.outputs.create(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="twooutput", dataset_idx=6);
        foo.outputs.create(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="threeoutput", dataset_idx=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.check_output_indices);

        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);

      def test_method_no_copied_parent_parameters_save(self):
        """Test save when no method revision parent is specified."""

        # Define new Method with no parent
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # There should be no inputs
        self.assertEqual(foo.inputs.count(), 0);
        self.assertEqual(foo.outputs.count(), 0);

        # DNAcompv1_m also has no parents as it is the first revision
        self.DNAcompv1_m.save();

        # DNAcompv1_m was defined to have 1 input and 1 output
        self.assertEqual(self.DNAcompv1_m.inputs.count(), 1);
        self.assertEqual(self.DNAcompv1_m.inputs.all()[0],
                         self.DNAinput_ti);

        self.assertEqual(self.DNAcompv1_m.outputs.count(), 1);
        self.assertEqual(self.DNAcompv1_m.outputs.all()[0],
                         self.DNAoutput_to);

    def test_method_with_copied_parent_parameters_save(self):
        """Test save when revision parent is specified."""

        # DNAcompv2_m should have 1 input, copied from DNAcompv1
        self.assertEqual(self.DNAcompv2_m.inputs.count(), 1);
        curr_in = self.DNAcompv2_m.inputs.all()[0];
        self.assertEqual(curr_in.compounddatatype,
                         self.DNAinput_ti.compounddatatype);
        self.assertEqual(curr_in.dataset_name,
                         self.DNAinput_ti.dataset_name);
        self.assertEqual(curr_in.dataset_idx,
                         self.DNAinput_ti.dataset_idx);
         
        self.assertEqual(self.DNAcompv2_m.outputs.count(), 1);
        curr_out = self.DNAcompv2_m.outputs.all()[0];
        self.assertEqual(curr_out.compounddatatype,
                         self.DNAoutput_to.compounddatatype);
        self.assertEqual(curr_out.dataset_name,
                         self.DNAoutput_to.dataset_name);
        self.assertEqual(curr_out.dataset_idx,
                         self.DNAoutput_to.dataset_idx);
        
        # If there are already inputs and outputs specified, then
        # they should not be overwritten.

        old_cdt = self.DNAinput_ti.compounddatatype;
        old_name = self.DNAinput_ti.dataset_name;
        old_idx = self.DNAinput_ti.dataset_idx;
        
        self.DNAcompv1_m.revision_parent = self.RNAcompv2_m;
        self.DNAcompv1_m.save();
        self.assertEqual(self.DNAcompv1_m.inputs.count(), 1);
        curr_in = self.DNAcompv1_m.inputs.all()[0];
        self.assertEqual(curr_in.compounddatatype, old_cdt);
        self.assertEqual(curr_in.dataset_name, old_name);
        self.assertEqual(curr_in.dataset_idx, old_idx);
         
        old_cdt = self.DNAoutput_to.compounddatatype;
        old_name = self.DNAoutput_to.dataset_name;
        old_idx = self.DNAoutput_to.dataset_idx;
        
        self.assertEqual(self.DNAcompv2_m.outputs.count(), 1);
        curr_out = self.DNAcompv2_m.outputs.all()[0];
        self.assertEqual(curr_out.compounddatatype, old_cdt);
        self.assertEqual(curr_out.dataset_name, old_name);
        self.assertEqual(curr_out.dataset_idx, old_idx);
    
class pipeline_tests(CopperfishMethodTests_setup):
    
    def test_pipeline_one_valid_input_clean(self):
        """Test input index check, one well-indexed input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        self.assertEquals(foo.clean(), None);


    def test_pipeline_one_invalid_input_clean(self):
        """Test input index check, one badly-indexed input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_many_valid_inputs_clean(self):
        """Test input index check, well-indexed multi-input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=3);
        self.assertEquals(foo.clean(), None);

    def test_pipeline_many_valid_inputs_scrambled_clean(self):
        """Test input index check, well-indexed multi-input (scrambled order) case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=3);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=1);
        self.assertEquals(foo.clean(), None);


    def test_pipeline_many_invalid_inputs_clean(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=3);
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_one_valid_input_clean(self):
        """Test step index check, one well-indexed step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        
        self.assertEquals(foo.clean(), None);

    def test_pipeline_one_bad_input_clean(self):
        """Test step index check, one badly-indexed step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=10);
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_many_valid_steps_clean(self):
        """Test step index check, well-indexed multi-step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        
        self.assertEquals(foo.clean(), None);

    def test_pipeline_many_valid_steps_scrambled_clean(self):
        """Test step index check, well-indexed multi-step (scrambled order) case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        
        self.assertEquals(foo.clean(), None);

    def test_pipeline_many_invalid_steps_clean(self):
        """Test step index check, badly-indexed multi-step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=4);
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=5);
        
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_oneStep_valid_wiring_clean(self):
        """Test good step wiring, one-step pipeline."""

        # Define pipeline 'foo' in family 'DNAcomp_pf'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Add single, validly indexed pipeline input
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Add single validly indexed step, composed of the method DNAcompv2
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);

        # Add wiring from step 0 with input name "oneinput"
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertEquals(foo.clean(), None);
        self.assertEquals(step1.clean(), None);
        
    def test_pipeline_oneStep_invalid_step_numbers_clean(self):
        """Bad wiring: step not indexed 1."""

        # Define a pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        
        # Define a validly indexed pipeline input
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 2 without a step 1
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=2);

        # Give this step properly mapped wiring from the Pipeline input
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean);
        
    def test_pipeline_oneStep_invalid_wiring_nonexistent_referenced_output_clean (self):
        """Bad wiring: step looks for nonexistent input."""

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define pipeline input for 'foo'
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 1 of this pipeline by transformation DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Map a non-existent source input for the wiring to step 1
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="nonexistent");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Pipeline does not have input \"nonexistent\"",
                foo.clean);
        
    def test_pipeline_oneStep_invalid_wiring_incorrect_cdt_clean(self):
        """Bad wiring: input is of wrong CompoundDatatype."""

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define pipeline input "oneinput" for foo with CDT type test_cdt
        foo.inputs.create(compounddatatype=self.test_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 1 by transformation DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Reference correct name "oneinput" and wire to step "input"
        # of DNAcompv2_m - but of the wrong cdt
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 does not have the expected CompoundDatatype",
                foo.clean);
        
    def test_pipeline_oneStep_wiring_minrow_constraint_may_be_breached_clean (self):
        """Unverifiable wiring: step requests input with possibly too few rows"""

        # Define method 'curr_method' with driver compv2_crRev
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        curr_method.save();

        # Give curr_method properly indexed input with min_row = 10
        curr_method.inputs.create(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  min_row=10);

        # Give curr_method an output named 'output'
        curr_method.outputs.create(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define properly indexed pipeline input for 'foo'
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 1 of 'foo' to be curr_method
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1);

        # From row-unconstrained pipeline input, assign to curr_method
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # It's possible this step may have too few rows
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                foo.clean);
        
    def test_pipeline_oneStep_wiring_minrow_constraints_may_breach_each_other_clean (self):
        """Unverifiable wiring: step requests input with possibly too few rows"""
        
        # Define method curr_method
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        curr_method.save();

        # Give curr_method an input with min_row = 10
        curr_method.inputs.create(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1, min_row=10);

        # Give curr_method an unconstrained output
        curr_method.outputs.create(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define pipeline input of foo to have min_row of 5
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1,
                          min_row=5);

        # Assign curr_method to step1 of foo
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1);
        
        # Map min_row = 5 pipeline input to this step's input
        # which contains curr_method with min_row = 10
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                foo.clean);


    def test_pipeline_oneStep_wiring_maxRow_constraints_may_be_breached_clean(self):
        """Unverifiable wiring: step requests input with possibly too many rows"""

        # Define curr_method with input of max_row = 10
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        curr_method.save();
        curr_method.inputs.create(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1, max_row=10);
       
        curr_method.outputs.create(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline with unrestricted Pipeline input
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Assign curr_method to step 1 of foo, and wire the pipeline input to it
        step1 = foo.steps.create(transformation=curr_method, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # The pipeline input is unrestricted, but step 1 has max_row = 10
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                foo.clean);


    def test_pipeline_oneStep_wiring_maxRow_constraints_may_breach_each_other_clean (self):
        """Unverifiable wiring: step requests input with possibly too many rows (max_row set for pipeline input)."""
        
        # Define curr_method as having an input with max_row = 10
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        curr_method.save();
        curr_method.inputs.create(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  max_row=10);
        curr_method.outputs.create(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline foo with Pipeline input having max_row = 20
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1,
                          max_row=20);

        # Assign curr_method to foo step 1
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # The pipeline max_row is not good enough to guarentee correctness
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                foo.clean);
        
    def test_pipeline_oneStep_with_valid_outmap_clean(self):
        """Good output mapping, one-step pipeline."""

        # Define pipeline foo with unconstrained input
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Add DNAcompv2_m (Expecting 1 input) to step 1 of foo
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Map the pipeline input to step 1
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Connect the output of step 1 to the output of foo
        foo.outmap.create(output_name="oneoutput",
                          output_idx=1,
                          step_providing_output=1,
                          provider_output_name="output");
        self.assertEquals(foo.clean(), None);

    def test_pipeline_oneStep_outmap_references_nonexistent_step_clean(self):
        """Bad output mapping, one-step pipeline: request from nonexistent step"""

        # Define pipeline foo with validly indexed input and step 1 wiring
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Index a non-existent step to outmap
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=5,
                          provider_output_name="output");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                foo.clean);
        
    def test_pipeline_oneStep_outmap_references_nonexistent_outputName_clean (self):
        """Bad output mapping, one-step pipeline: request nonexistent step output"""

        # Define pipeline foo with validly indexed inputs, steps, and wiring
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Reference a correct step but non-existent output name
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="nonexistent");
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"nonexistent\"",
                foo.clean);
        
    def test_pipeline_oneStep_outmap_references_deleted_output_clean (self):
        """Bad output mapping, one-step pipeline: request deleted step output"""

        # Define pipeline foo with validly indexed inputs, steps, and wiring
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Mark DNAcompv2_m output as deletable
        step1.outputs_to_delete.create(dataset_to_delete="output");

        # Now try to map it to the pipeline output
        foo.outmap.create(output_name="oneoutput",
                          output_idx=1,
                          step_providing_output=1,
                          provider_output_name="output");
        self.assertRaisesRegexp(
                ValidationError,
                "Output \"output\" from step 1 is deleted prior to request",
                foo.clean);
        
    def test_pipeline_oneStep_bad_pipeline_output_indexing_clean(self):
        """Bad output mapping, one-step pipeline: output not indexed 1"""

        # Define pipeline with validly indexed inputs, steps, and wiring
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Outmap references a valid step and output, but is itself badly indexed
        foo.outmap.create(output_name="oneoutput",
                          output_idx=9,
                          step_providing_output=1,
                          provider_output_name="output");
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_manySteps_valid_internal_wiring_clean(self):
        """Test good step wiring, chained-step pipeline."""

        # Define pipeline 'foo' with validly indexed input and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Map pipeline input to step1
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Map step 1 to step 2
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");

        # Map step 2 to step 3
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertEquals(foo.clean(), None);
        
    def test_pipeline_manySteps_wiring_references_nonexistent_output_clean(self):
        """Bad wiring: later step requests nonexistent input from previous."""

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # step1 recieves input from Pipeline input
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # step2 recieves nonexistent output from step1
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="nonexistent");
        
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"nonexistent\"",
                foo.clean);
        
    def test_pipeline_manySteps_wiring_references_deleted_input_clean(self):
        """Bad wiring: later step requests input deleted by producing step."""

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Connect step 1 with pipeline input
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Connect step2 with output of step1
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");

        # Mark the output of step2 as deleted
        step2.outputs_to_delete.create(dataset_to_delete="recomplemented_seqs");

        # Connect step3 with the deleted output at step 2
        step3 = foo.steps.create(transformation=self.RNAcompv2_m,
                                 step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Input \"recomplemented_seqs\" from step 2 to step 3 is deleted prior to request",
                foo.clean);

    def test_pipeline_manySteps_wiring_references_incorrect_cdt_clean (self):
        """Bad wiring: later step requests input of wrong CompoundDatatype."""
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=self.RNAcompv2_m,
                                 step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 does not have the expected CompoundDatatype",
                foo.clean);

    def test_pipeline_manySteps_minRow_constraint_may_be_breached_clean (self):
        """Unverifiable wiring: later step requests input with possibly too few rows (min_row unset for providing step)."""

        # Define a method with validly indexed inputs and outputs
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step2method.save();
        step2method.inputs.create(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.outputs.create(compounddatatype=self.DNAinput_cdt,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1);

        # Define another method with validly indexed inputs and outputs
        # But with the inputs requiring min_row = 5
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step3method.save();

        step3method.inputs.create(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  min_row=5);
        step3method.outputs.create(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");

        # Step 3 requires min_row = 5 but step2 does not guarentee this
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                foo.clean);



    def test_pipeline_manySteps_minrow_constraints_may_breach_each_other_clean(self):
        """Bad wiring: later step requests input with possibly too few rows (providing step min_row is set)."""
        
        # Define method with outputs having a min row of 5
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step2method.save();
        step2method.inputs.create(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        
        step2method.outputs.create(compounddatatype=self.DNAinput_cdt,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1,
                                   min_row=5);

        # Define another method with input min row of 10
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step3method.save();
        step3method.inputs.create(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  min_row=10);
        step3method.outputs.create(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Recall the output of step2 has min_row = 5
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");

        # Recall the input of step3 has min_row = 10
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                foo.clean);


    def test_pipeline_manySteps_maxRow_constraint_may_be_breached_clean(self):
        """Bad wiring: later step requests input with possibly too many rows (max_row unset for providing step)."""

        # step2 has no constraints on it's output
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step2method.save();
        step2method.inputs.create(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.outputs.create(compounddatatype=self.DNAinput_cdt,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1);

        # step3 has an input with max_row = 100
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step3method.save();

        step3method.inputs.create(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  max_row=100);
        step3method.outputs.create(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too many rows",
                foo.clean);



    def test_pipeline_manySteps_wiring_maxRow_constraints_may_breach_each_other_clean (self):
        """Bad wiring: later step requests input with possibly too many rows (max_row for providing step is set)."""

        # step 2 has max_row = 100 on it's output
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step2method.save();
        step2method.inputs.create(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.outputs.create(compounddatatype=self.DNAinput_cdt,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1,
                                   max_row=100);

        # step3 has a max_row = 50 on it's input
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step3method.save();
        step3method.inputs.create(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  max_row=50);
        step3method.outputs.create(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too many rows",
                foo.clean);

        
    def test_pipeline_manySteps_valid_outmap_clean(self):
        """Good output mapping, chained-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");

        foo.outmap.create(output_name="outputone", output_idx=1,
                          step_providing_output=3,
                          provider_output_name="output");
        foo.outmap.create(output_name="outputtwo", output_idx=2,
                          step_providing_output=2,
                          provider_output_name="recomplemented_seqs");
        self.assertEquals(foo.clean(), None);


    def test_pipeline_manySteps_outmap_references_nonexistent_step_clean(self):
        """Bad output mapping, chained-step pipeline: request from nonexistent step"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");

        # step 5 doesn't exist
        foo.outmap.create(output_name="outputone", output_idx=1,
                          step_providing_output=5,
                          provider_output_name="nonexistent");
        foo.outmap.create(output_name="outputtwo", output_idx=2,
                          step_providing_output=2,
                          provider_output_name="recomplemented_seqs");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                foo.clean);

    def test_pipeline_manySteps_outmap_references_nonexistent_output_clean(self):
        """Bad output mapping, chained-step pipeline: request nonexistent step output"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");

        foo.outmap.create(output_name="outputone", output_idx=1,
                          step_providing_output=3,
                          provider_output_name="output");
        foo.outmap.create(output_name="outputtwo", output_idx=2,
                          step_providing_output=2,
                          provider_output_name="nonexistent");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 2 does not produce output \"nonexistent\"",
                foo.clean);

        
    def test_pipeline_manySteps_outmap_references_deleted_output_clean(self):
        """Bad output mapping, chained-step pipeline: request deleted step output"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        step3.outputs_to_delete.create(dataset_to_delete="output");

        foo.outmap.create(output_name="outputone", output_idx=1,
                          step_providing_output=3,
                          provider_output_name="output");
        foo.outmap.create(output_name="outputtwo", output_idx=2,
                          step_providing_output=2,
                          provider_output_name="recomplemented_seqs");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Output \"output\" from step 3 is deleted prior to request",
                foo.clean);

    def test_pipeline_manySteps_outmap_references_invalid_output_index_clean(self):
        """Bad output mapping, chain-step pipeline: outputs not consecutively numbered starting from 1"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");

        foo.outmap.create(output_name="outputone",
                          output_idx=5,
                          step_providing_output=3,
                          provider_output_name="output");
        foo.outmap.create(output_name="outputtwo",
                          output_idx=2,
                          step_providing_output=2,
                          provider_output_name="recomplemented_seqs");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);

class pipelineSteps_tests(CopperfishMethodTests_setup):

    def test_pipelineStep_without_pipeline_set_unicode(self):
        """Test unicode representation when no pipeline is set."""
        nopipeline = PipelineStep(step_num=2);
        self.assertEquals(unicode(nopipeline),
                          "[no pipeline assigned] step 2");

    def test_pipelineStep_with_pipeline_set_unicode(self):
        """Test unicode representation when pipeline is set."""
        pipelineset = self.DNAcompv1_p.steps.get(step_num=1);
        self.assertEquals(unicode(pipelineset),
                          "Pipeline DNAcomplement v1 step 1");

    def test_pipelineStep_invalid_request_for_future_step_data_clean(self):
        """Bad wiring: step requests data from after its execution step."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        # Step 1 invalidly requests data from step 2
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="oneinput");
     
        self.assertRaisesRegexp(
                ValidationError,
                "Input \"oneinput\" to step 1 does not come from a prior step",
                step1.clean);

    def test_pipelineStep_invalid_request_for_stepZero_input_clean(self):
        """Bad wiring: step feeds data to a nonexistent input."""

        # Define Pipeline
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define Pipeline input
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        # Create a step composed of method DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);

        # Reference an invalid input name from step 0
        step1.inputs.create(transf_input_name="thisisnonexistent",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 has no input named \"thisisnonexistent\"",
                step1.clean);

    def test_pipelineStep_oneStep_valid_wiring_with_valid_delete_clean(self):
        """Test good step wiring with deleted dataset, one-step pipeline."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define Pipeline input "oneinput"
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Add a step
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Map Pipeline input to step 1
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Mark step 1 "output" as deletable
        # step 1 "output" is defined by DNAcompv2_m
        step1.outputs_to_delete.create(dataset_to_delete="output");
        
        self.assertEquals(step1.clean(),
                          None);

    def test_pipelineStep_oneStep_valid_wiring_bad_delete_clean(self):
        """Bad wiring: deleting nonexistent dataset, one-step pipeline."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Add a valid pipeline input
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define valid pipeline step
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Create input wiring for this step
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Reference nonexistent dataset
        step1.outputs_to_delete.create(dataset_to_delete="nonexistent");
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 has no output named \"nonexistent\"",
                step1.clean);
         
    def test_pipelineStep_oneStep_wiring_directly_self_referential_transformation_clean(self):
        """Bad wiring: pipeline step contains the parent pipeline directly."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Give it a single validly indexed pipeline input
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Add a valid step 1, but reference itself as the transformation
        step1 = foo.steps.create(transformation=foo,
                                 step_num=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                step1.clean);
         
    def test_pipelineStep_oneStep_wiring_referenced_pipeline_references_parent_clean (self):
        """Bad wiring: pipeline step contains the parent pipeline in its lone recursive sub-step."""

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Give it a single validly indexed pipeline input
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 1 as executing DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Map the input at stpe 1 from Pipeline input "oneinput"
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");

        # Define pipeline output at index 1 from (step 1, output "output")
        foo.outmap.create(output_name="oneoutput",
                          output_idx=1,
                          step_providing_output=1,
                          provider_output_name="output");
        foo.save();

        # Define a second pipeline
        bar = Pipeline(family=self.DNAcomp_pf,
                       revision_name="bar",
                       revision_desc="Bar version");
        bar.save();

        # Give it a single validly indexed pipeline input
        bar.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="barinput",
                          dataset_idx=1);

        # At step 1, execute the transformation 'foo' defined above
        bstep1 = bar.steps.create(transformation=foo,
                                  step_num=1);

        # Map to foo.input "oneinput" from bar pipeline output "barinput"
        bstep1.inputs.create(transf_input_name="oneinput",
                             step_providing_input=0,
                             provider_output_name="barinput");

        # Map a single output, from step 1 foo.output = "oneoutput"
        bar.outmap.create(output_name="baroutput",
                          output_idx=1,
                          step_providing_output=1,
                          provider_output_name="oneoutput");
        bar.save();

        # Now refine foo's step 1 to point to bar
        step1.delete();
        foo.outputs.all().delete();

        # Have step 1 of foo point to bar (But bar points to foo!)
        badstep = foo.steps.create(transformation=bar,
                                   step_num=1);
        
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                badstep.clean);
         
    def test_pipelineStep_manySteps_wiring_referenced_pipeline_references_parent_clean(self):
        """Bad wiring: pipeline step contains the parent pipeline in some recursive sub-step."""

        # foo invokes DNAcompv2_m at step 1
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="output");
        foo.save();

        # bar invokes foo at step 1 and DNArecomp_m at step 2
        bar = Pipeline(family=self.DNAcomp_pf,
                       revision_name="bar",
                       revision_desc="Bar version");
        bar.save();
        bar.inputs.create(compounddatatype=self.DNAinput_cdt,
                          dataset_name="barinput",
                          dataset_idx=1);
        bstep1 = bar.steps.create(transformation=foo,
                                  step_num=1);
        
        bstep1.inputs.create(transf_input_name="oneinput",
                             step_providing_input=0,
                             provider_output_name="barinput");
        
        bstep2 = bar.steps.create(transformation=self.DNArecomp_m,
                                  step_num=2);
        bstep2.inputs.create(transf_input_name="complemented_seqs",
                             step_providing_input=1,
                             provider_output_name="oneoutput");
        bar.outmap.create(output_name="baroutputone",
                          output_idx=1,
                          step_providing_output=1,
                          provider_output_name="oneoutput");
        bar.outmap.create(output_name="baroutputtwo",
                          output_idx=2,
                          step_providing_output=2,
                          provider_output_name="recomplemented_seqs");
        bar.save();

        # foo is redefined to be circular
        step1.delete();
        foo.outputs.all().delete();
        badstep = foo.steps.create(transformation=bar,
                                   step_num=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                badstep.clean);
