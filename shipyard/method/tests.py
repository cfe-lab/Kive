"""
Unit tests for Shipyard method models.
"""

from django.test import TestCase
from django.core.exceptions import ValidationError
from django.core.files import File

import os.path
import logging
import shutil
import tempfile

from method.models import *
from metadata.models import *
import metadata.tests
from constants import error_messages

logging.getLogger().setLevel(10) # Debug messages
samplecode_path = "../samplecode"

class MethodTestSetup(metadata.tests.MetadataTestSetup):
    """
    Set up a database state for unit testing.
    
    This extends MetadataTestSetup, which set up some of the Datatypes
    and CDTs we use here.
    """
    def setUp(self):
        """Set up default database state for Method unit testing."""
        # This sets up the DTs and CDTs used in our metadata tests.
        super(MethodTestSetup, self).setUp()

        # Define comp_cr
        self.comp_cr = CodeResource(
                name="complement",
                description="Complement DNA/RNA nucleotide sequences",
                filename="complement.py")
        self.comp_cr.save()

        # Define compv1_crRev for comp_cr
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            self.compv1_crRev = CodeResourceRevision(
                    coderesource=self.comp_cr,
                    revision_name="v1",
                    revision_desc="First version",
                    content_file=File(f))
            self.compv1_crRev.full_clean()
            self.compv1_crRev.save()

        # Define compv2_crRev for comp_cr
        with open(os.path.join(samplecode_path, "complement_v2.py"), "rb") as f:
            self.compv2_crRev = CodeResourceRevision(
                coderesource=self.comp_cr,
                revision_name="v2",
                revision_desc="Second version: better docstring",
                revision_parent=self.compv1_crRev,
                content_file=File(f))
            self.compv2_crRev.full_clean()
            self.compv2_crRev.save()

        # The following is for testing code resource dependencies
        with open(os.path.join(samplecode_path, "test_cr.py"), "rb") as f:
            test_cr_1 = CodeResource(name="test_cr_1",
                                     filename="test_cr_1.py",
                                     description="CR1")
            test_cr_1.save()
            test_cr_1_rev1 = CodeResourceRevision(coderesource=test_cr_1,
                                                  revision_name="v1",
                                                  revision_desc="CR1-rev1",
                                                  content_file=File(f))
            test_cr_1_rev1.save()
            self.test_cr_1 = test_cr_1
            self.test_cr_1_rev1 = test_cr_1_rev1
            
            test_cr_2 = CodeResource(name="test_cr_2",
                                     filename="test_cr_2.py",
                                     description="CR2")
            test_cr_2.save()
            test_cr_2_rev1 = CodeResourceRevision(coderesource=test_cr_2,
                                                  revision_name="v2",
                                                  revision_desc="CR2-rev1",
                                                  content_file=File(f))
            test_cr_2_rev1.save()
            self.test_cr_2 = test_cr_2
            self.test_cr_2_rev1 = test_cr_2_rev1
    
            test_cr_3 = CodeResource(name="test_cr_3",
                                     filename="test_cr_3.py",
                                     description="CR3")
            test_cr_3.save()
            test_cr_3_rev1 = CodeResourceRevision(coderesource=test_cr_3,
                                                  revision_name="v3",
                                                  revision_desc="CR3-rev1",
                                                  content_file=File(f))
            test_cr_3_rev1.save()
            self.test_cr_3 = test_cr_3
            self.test_cr_3_rev1 = test_cr_3_rev1
    
            test_cr_4 = CodeResource(name="test_cr_4",
                                     filename="test_cr_4.py",
                                     description="CR4")
            test_cr_4.save()
            test_cr_4_rev1 = CodeResourceRevision(coderesource=test_cr_4,
                                                  revision_name="v4",
                                                  revision_desc="CR4-rev1",
                                                  content_file=File(f))
            test_cr_4_rev1.save()
            self.test_cr_4 = test_cr_4
            self.test_cr_4_rev1 = test_cr_4_rev1



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
        self.DNAinput_ti = self.DNAcompv1_m.create_input(
                compounddatatype = self.DNAinput_cdt,
                dataset_name = "input",
                dataset_idx = 1);
        self.DNAinput_ti.full_clean();
        self.DNAinput_ti.save();

        # Add output DNAoutput_cdt to DNAcompv1_m
        self.DNAoutput_to = self.DNAcompv1_m.create_output(
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
        self.RNAinput_ti = self.RNAcompv1_m.create_input(
                compounddatatype = self.RNAinput_cdt,
                dataset_name = "input",
                dataset_idx = 1);
        self.RNAinput_ti.full_clean();
        self.RNAinput_ti.save();

        # Add output RNAoutput_cdt to RNAcompv1_m
        self.RNAoutput_to = self.RNAcompv1_m.create_output(
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

        # Create method family for script_1_method / script_2_method / script_3_method
        self.test_mf = MethodFamily(name="Test method family",
                                    description="Holds scripts 1/2/3");
        self.test_mf.full_clean();
        self.test_mf.save();

        # script_1_sum_and_outputs.py
        # INPUT: 1 csv containing (x,y)
        # OUTPUT: 1 csv containing (x+y,xy)
        self.script_1_cr = CodeResource(name="Sum and product of x and y",
                                        filename="script_1_sum_and_products.py",
                                        description="Addition and multiplication")
        self.script_1_cr.save()

        # Add code resource revision for code resource (script_1_sum_and_products ) 
        with open(os.path.join(samplecode_path, "script_1_sum_and_products.py"), "rb") as f:
            self.script_1_crRev = CodeResourceRevision(
                coderesource=self.script_1_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f))
            self.script_1_crRev.save()

        # Establish code resource revision as a method
        self.script_1_method = Method(
            revision_name="script1",
            revision_desc="script1",
            family = self.test_mf,driver = self.script_1_crRev)
        self.script_1_method.save()

        # Assign tuple as both an input and an output to script_1_method
        self.script_1_method.create_input(compounddatatype = self.tuple_cdt,
                                           dataset_name = "input_tuple",
                                           dataset_idx = 1)
        self.script_1_method.create_output(compounddatatype = self.tuple_cdt,
                                           dataset_name = "input_tuple",
                                           dataset_idx = 1)
        self.script_1_method.full_clean()
        self.script_1_method.save()

        # script_2_square_and_means
        # INPUT: 1 csv containing (a,b,c)
        # OUTPUT-1: 1 csv containing triplet (a^2,b^2,c^2)
        # OUTPUT-2: 1 csv containing singlet mean(a,b,c)
        self.script_2_cr = CodeResource(name="Square and mean of (a,b,c)",
                                        filename="script_2_square_and_means.py",
                                        description="Square and mean - 2 CSVs")
        self.script_2_cr.save()

        # Add code resource revision for code resource (script_2_square_and_means)
        with open(os.path.join(samplecode_path, "script_2_square_and_means.py"), "rb") as f:
            self.script_2_crRev = CodeResourceRevision(
                coderesource=self.script_2_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f))
            self.script_2_crRev.save()

        # Establish code resource revision as a method
        self.script_2_method = Method(
            revision_name="script2",
            revision_desc="script2",
            family = self.test_mf, driver = self.script_2_crRev)
        self.script_2_method.save()

        # Assign triplet as input and output,
        self.script_2_method.create_input(
            compounddatatype = self.triplet_cdt,
            dataset_name = "a_b_c",
            dataset_idx = 1)
        self.script_2_method.create_output(
            compounddatatype = self.triplet_cdt,
            dataset_name = "a_b_c_squared",
            dataset_idx = 1)
        self.script_2_method.create_output(
            compounddatatype = self.singlet_cdt,
            dataset_name = "a_b_c_mean",
            dataset_idx = 2)
        self.script_2_method.full_clean()
        self.script_2_method.save()

        # script_3_product
        # INPUT-1: Single column (k)
        # INPUT-2: Single-row, single column (r)
        # OUTPUT-1: Single column r*(k)
        self.script_3_cr = CodeResource(name="Scalar multiple of k",
                                        filename="script_3_product.py",
                                        description="Product of input")
        self.script_3_cr.save()

        # Add code resource revision for code resource (script_3_product)
        with open(os.path.join(samplecode_path, "script_3_product.py"), "rb") as f:
            self.script_3_crRev = CodeResourceRevision(
                coderesource=self.script_3_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f))
            self.script_3_crRev.save()

        # Establish code resource revision as a method
        self.script_3_method = Method(
            revision_name="script3",
            revision_desc="script3",
            family = self.test_mf,
            driver = self.script_3_crRev)
        self.script_3_method.save()

        # Assign singlet as input and output
        self.script_3_method.create_input(compounddatatype = self.singlet_cdt,
                                           dataset_name = "k",
                                           dataset_idx = 1)

        self.script_3_method.create_input(compounddatatype = self.singlet_cdt,
                                           dataset_name = "r",
                                           dataset_idx = 2,
                                           max_row = 1,
                                           min_row = 1)

        self.script_3_method.create_output(compounddatatype = self.singlet_cdt,
                                           dataset_name = "kr",
                                           dataset_idx = 1)
        self.script_3_method.full_clean()
        self.script_3_method.save()

        ####
        # This next bit was originally in pipeline.tests.
        
        # DNArecomp_mf is a MethodFamily called DNArecomplement
        self.DNArecomp_mf = MethodFamily(
            name="DNArecomplement",
            description="Re-complement DNA nucleotide sequences.")
        self.DNArecomp_mf.full_clean()
        self.DNArecomp_mf.save()

        # Add to MethodFamily DNArecomp_mf a method revision DNArecomp_m
        self.DNArecomp_m = self.DNArecomp_mf.members.create(
            revision_name="v1",
            revision_desc="First version",
            driver=self.compv2_crRev)

        # To this method revision, add inputs with CDT DNAoutput_cdt
        self.DNArecomp_m.create_input(
            compounddatatype = self.DNAoutput_cdt,
            dataset_name = "complemented_seqs",
            dataset_idx = 1)

        # To this method revision, add outputs with CDT DNAinput_cdt
        self.DNArecomp_m.create_output(
            compounddatatype = self.DNAinput_cdt,
            dataset_name = "recomplemented_seqs",
            dataset_idx = 1)

        # Setup used in the "2nd-wave" tests (this was originally in
        # Copperfish_Raw_Setup).
        
        # Define CR "script_4_raw_in_CSV_out.py"
        # input: raw [but contains (a,b,c) triplet]
        # output: CSV [3 CDT members of the form (a^2, b^2, c^2)]

        # Define CR in order to define CRR
        self.script_4_CR = CodeResource(name="Generate (a^2, b^2, c^2) using RAW input",
            filename="script_4_raw_in_CSV_out.py",
            description="Given (a,b,c), outputs (a^2,b^2,c^2)")
        self.script_4_CR.save()

        # Define CRR for this CR in order to define method
        with open(os.path.join(samplecode_path, "script_4_raw_in_CSV_out.py"), "rb") as f:
            self.script_4_1_CRR = CodeResourceRevision(
                coderesource=self.script_4_CR,
                revision_name="v1",
                revision_desc="v1",
                content_file=File(f))
            self.script_4_1_CRR.save()

        # Define MF in order to define method
        self.test_MF = MethodFamily(
            name="test method family",
            description="method family placeholder");
        self.test_MF.full_clean()
        self.test_MF.save()

        # Establish CRR as a method within a given method family
        self.script_4_1_M = Method(
            revision_name="s4",
            revision_desc="s4",
            family = self.test_MF,
            driver = self.script_4_1_CRR)
        self.script_4_1_M.save()

        self.script_4_1_M.create_input(compounddatatype=self.triplet_cdt, 
            dataset_name="s4 input", dataset_idx = 1)
        self.script_4_1_M.full_clean()

        # A shorter alias
        self.testmethod = self.script_4_1_M

        # Some code for a no-op method.
        resource = CodeResource(name="noop", filename="noop.sh"); resource.save()
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write("#!/bin/bash\ncat $1")
            self.noop_data_file = f.name
            revision = CodeResourceRevision(coderesource = resource,
                content_file = File(f))
            revision.clean()
            revision.save()
        
        string_dt = Datatype(name="string", description="string", Python_type=Datatype.STR)
        string_dt.save()
        string_cdt = CompoundDatatype()
        string_cdt.save()
        string_cdt.members.create(datatype=string_dt, column_name="word", column_idx=1)
        string_cdt.full_clean()
        
        mfamily = MethodFamily(name="noop"); mfamily.save()
        self.noop_method = Method(family=mfamily, driver=revision,
            revision_name = "1", revision_desc = "first version")
        self.noop_method.save()
        self.noop_method.create_input(compounddatatype=string_cdt, dataset_name = "noop data", dataset_idx=1)
        self.noop_method.clean()
        self.noop_method.full_clean()

        # Some data.
        self.scratch_dir = tempfile.mkdtemp()
        fd, self.noop_infile = tempfile.mkstemp(dir=self.scratch_dir)
        self.noop_outfile = tempfile.mkstemp(dir=self.scratch_dir)[1]
        self.noop_indata = "word\nhello\nworld"

        handle = os.fdopen(fd, "w")
        handle.write(self.noop_indata)
        handle.close()

    def tearDown(self):
        shutil.rmtree(self.scratch_dir)


class CodeResourceTests(MethodTestSetup):
     
    def test_unicode(self):
        """
        unicode should return the codeResource name.
        """
        self.assertEquals(unicode(self.comp_cr), "complement");
  
    def test_valid_name_clean_good(self):
        """
        Clean passes when codeResource name is file-system valid
        """
        valid_cr = CodeResource(name="name",
                                filename="validName",
                                description="desc")
        valid_cr.save()
        self.assertEqual(valid_cr.clean(), None);

    def test_valid_name_with_special_symbols_clean_good(self):
        """
        Clean passes when codeResource name is file-system valid
        """
        valid_cr = CodeResource(name="anotherName",
                                filename="valid.Name with-spaces_and_underscores().py",
                                description="desc")
        valid_cr.save()
        self.assertEqual(valid_cr.clean(), None);

    def test_invalid_name_doubledot_clean_bad(self):
        """
        Clean fails when CodeResource name isn't file-system valid
        """

        invalid_cr = CodeResource(name="test",
                                  filename="../test.py",
                                  description="desc")
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError,"Invalid code resource filename",invalid_cr.clean)

    def test_invalid_name_starting_space_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="test",
                                  filename=" test.py",
                                  description="desc")
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError,"Invalid code resource filename",invalid_cr.clean)

    def test_invalid_name_invalid_symbol_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="name",
                                  filename="test$.py",
                                  description="desc")
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError,"Invalid code resource filename",invalid_cr.clean)

    def test_invalid_name_trailing_space_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="name",
                                  filename="test.py ",
                                  description="desc")
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError,"Invalid code resource filename",invalid_cr.clean)


class CodeResourceRevisionTests(MethodTestSetup):

    def test_unicode(self):
        """
        CodeResourceRevision.unicode() should return it's code resource
        name and it's code resource revision name.

        Or, if no CodeResource has been linked, should display an error.
        """

        # Valid crRev should return it's cr.name and crRev.revision_name
        self.assertEquals(unicode(self.compv1_crRev), "complement v1");

        # Define a crRev without a linking cr, or a revision_name
        no_cr_set = CodeResourceRevision();
        self.assertEquals(unicode(no_cr_set),
                          "[no code resource set] [no revision name]");

        # Define a crRev without a linking cr, with a revision_name of foo
        no_cr_set.revision_name = "foo";
        self.assertEquals(unicode(no_cr_set), "[no code resource set] foo");


    # Tests of has_circular_dependence and clean
    def test_has_circular_dependence_nodep(self):
        """A CRR with no dependencies should not have any circular dependence."""
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          False);
        self.assertEquals(self.test_cr_1_rev1.clean(), None);

    def test_has_circular_dependence_single_self_direct_dep(self):
        """A CRR has itself as its lone dependency."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_1_rev1,
                depPath=".",
                depFileName="foo");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True);
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean);

    def test_has_circular_dependence_single_other_direct_dep(self):
        """A CRR has a lone dependency (non-self)."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".",
                depFileName="foo");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          False);
        self.assertEquals(self.test_cr_1_rev1.clean(), None);

    def test_has_circular_dependence_several_direct_dep_noself(self):
        """A CRR with several direct dependencies (none are itself)."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".",
                depFileName="foo");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_3_rev1,
                depPath=".");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_4_rev1,
                depPath=".");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          False);
        self.assertEquals(self.test_cr_1_rev1.clean(), None);

    def test_has_circular_dependence_several_direct_dep_self_1(self):
        """A CRR with several dependencies has itself as the first dependency."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_1_rev1,
                depPath=".",
                depFileName="foo");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_3_rev1,
                depPath=".");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True);
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean);
        
    def test_has_circular_dependence_several_direct_dep_self_2(self):
        """A CRR with several dependencies has itself as the second dependency."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_1_rev1,
                depPath=".",
                depFileName="foo");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_3_rev1,
                depPath=".");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True);
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean);
        
    def test_has_circular_dependence_several_direct_dep_self_3(self):
        """A CRR with several dependencies has itself as the last dependency."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_3_rev1,
                depPath=".");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_1_rev1,
                depPath=".",
                depFileName="foo");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True);
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean);

    def test_has_circular_dependence_several_nested_dep_noself(self):
        """A CRR with several dependencies including a nested one."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_3_rev1,
                depPath=".");
        self.test_cr_3_rev1.dependencies.create(
                requirement=self.test_cr_4_rev1,
                depPath=".");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          False);
        self.assertEquals(self.test_cr_1_rev1.clean(), None);
        
    def test_has_circular_dependence_several_nested_dep_selfnested(self):
        """A CRR with several dependencies including itself as a nested one."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_3_rev1,
                depPath=".");
        self.test_cr_3_rev1.dependencies.create(
                requirement=self.test_cr_1_rev1,
                depPath=".");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True);
        self.assertEquals(self.test_cr_2_rev1.has_circular_dependence(),
                          False);
        # Note that test_cr_3_rev1 *is* circular, as it depends on 1 and
        # 1 has a circular dependence.
        self.assertEquals(self.test_cr_3_rev1.has_circular_dependence(),
                          True);
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean);
        
    def test_has_circular_dependence_nested_dep_has_circ(self):
        """A nested dependency is circular."""
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".");
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_3_rev1,
                depPath=".");
        self.test_cr_2_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath=".");
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True);
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean);
        self.assertEquals(self.test_cr_2_rev1.has_circular_dependence(),
                          True);
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_2_rev1.clean);
        
    def test_metapackage_cannot_have_file_bad_clean(self):
        """
        A CRR with a content file should have a filename associated with
        its parent CodeResource.
        """

        cr = CodeResource(
                name="complement",
                filename="",
                description="Complement DNA/RNA nucleotide sequences");
        cr.save();

        # So it's revision does not have a content_file
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v1 = CodeResourceRevision(
                    coderesource=cr,
                    revision_name="v1",
                    revision_desc="First version",
                    content_file=File(f));

        self.assertRaisesRegexp(
            ValidationError,
            "If content file exists, it must have a file name",
            cr_rev_v1.clean)

    def test_non_metapackage_must_have_file_bad_clean(self):
        """
        A CRR with no content file should not have a filename associated with
        its parent CodeResource.
        """

        cr = CodeResource(
                name="nonmetapackage",
                filename="foo",
                description="Associated CRRs should have a content file");
        cr.save();

        # Create a revision without a content_file.
        cr_rev_v1 = CodeResourceRevision(
                coderesource=cr,
                revision_name="v1",
                revision_desc="Has no content file!");

        self.assertRaisesRegexp(
            ValidationError,
            "Cannot have a filename specified in the absence of a content file",
            cr_rev_v1.clean)


    def test_clean_blank_MD5_on_codeResourceRevision_without_file(self):
        """
        If no file is specified, MD5 should be empty string.
        """
        cr = CodeResource(name="foo",
                          filename="",
                          description="Some metapackage");
        cr.save();
        
        # Create crRev with a codeResource but no file contents
        no_file_crRev = CodeResourceRevision(
                coderesource=cr,
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

    def test_dependency_depends_on_nothing_clean_good (self):
        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_current_folder_same_name_clean_bad(self):
        """
        A depends on B - current folder, same name
        """

        # test_cr_1_rev1 is needed by test_cr_2_rev1
        # It will have the same file name as test_cr_1
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.assertRaisesRegexp(ValidationError,
                                "Conflicting dependencies",
                                self.test_cr_1_rev1.clean)

    def test_dependency_current_folder_different_name_clean_good(self):
        """
        1 depends on 2 - current folder, different name
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_inner_folder_same_name_clean_good(self):
        """
        1 depends on 2 - different folder, same name
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="innerFolder/",
            depFileName=self.test_cr_1.filename)

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_inner_folder_different_name_clean_good(self):
        """
        1 depends on 2 - different folder, different name
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="innerFolder/",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_BC_same_folder_no_conflicts_clean_good(self):
        """
        A depends on B, A depends on C
        BC in same folder as A
        Nothing conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="name1.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="name2.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_BC_same_folder_B_conflicts_with_A_clean_bad(self):
        """
        A depends on B, A depends on C
        BC in same folder as A, B conflicts with A
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="name1.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_BC_same_folder_C_conflicts_with_A_clean_bad(self):
        """
        A depends on B, A depends on C
        BC in same folder as A, C conflicts with A
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="notConflicting.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_BC_same_folder_B_conflicts_with_C_clean_bad(self):
        """
        A depends on B, A depends on C
        BC in same folder as A, BC conflict
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="colliding_name.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="colliding_name.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_BC_B_in_same_folder_no_conflicts_clean_good(self):
        """
        BC in same folder as A, B conflicts with A
        B in same folder, C in different folder, nothing conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="no_collision.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="diffFolder",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_BC_B_in_same_folder_B_conflicts_A_clean_bad(self):
        """
        A depends on B, A depends on C
        B in same folder, C in different folder, B conflicts with A
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="diffFolder",
            depFileName="differentName.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_BC_C_in_same_folder_no_conflict_clean_good(self):
        """
        A depends on B, A depends on C
        B in different folder, C in same folder, nothing conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="diffFolder",
            depFileName=self.test_cr_1.filename)

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_BC_C_in_same_folder_C_conflicts_with_A_clean_bad(self):
        """
        A depends on B, A depends on C
        B in different folder, C in same folder, C conflicts with A
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="diffFolder",
            depFileName=self.test_cr_1.filename)

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B_B_depends_C_all_same_folder_no_conflict_clean_good(self):
        """
        A depends on B, B depends on C
        ABC in same folder - no conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="differentName.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="differetName2.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_B_B_depends_C_all_same_folder_A_conflicts_C_clean_bad(self):
        """
        A depends on B, B depends on C
        ABC in same folder - A conflicts with C
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="differentName.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B_B_depends_C_all_same_folder_B_conflicts_C_clean_bad(self):
        """
        A depends on B, B depends on C
        ABC in same folder - B conflicts with C
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="differentName.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B_B_depends_C_BC_is_nested_no_conflicts_clean_good(self):
        """
        A depends on B, B depends on C
        BC in nested folder - no conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nestedFolder",
            depFileName=self.test_cr_1.name)

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_B_B_depends_C_BC_is_nested_B_conflicts_C_clean_bad(self):
        """
        A depends on B, B depends on C
        BC in nested folder - B conflicts with C
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nestedFolder",
            depFileName="conflicting.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="conflicting.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B_B_depends_C_double_nested_clean_good(self):
        """
        A depends on B, B depends on C
        B in nested folder, C in double nested folder - no conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nestedFolder",
            depFileName="conflicting.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="nestedFolder",
            depFileName="conflicting.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_B1B2B3_B1_depends_C_all_same_folder_no_conflicts_clean_good(self):
        """
        A depends on B1/B2/B3, B1 depends on C
        A/B1B2B3/C in same folder - no conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="1.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="2.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="3.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="",
            depFileName="4.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_B1B2B3_B2_depends_C_B1B2B3C_in_nested_B3_conflicts_C_clean_bad(self):
        """
        A depends on B1/B2/B3, B2 depends on C
        B1B2B3C in nested folder - B3 conflicts with C
        """

        # A depends on B1
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nested",
            depFileName="1.py")

        # A depends on B2
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nested",
            depFileName="2.py")

        # A depends on B3***
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="nested",
            depFileName="conflict.py")

        # B2 depends on C
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="",
            depFileName="conflict.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B1B2B3_B3_depends_C_B2B3C_in_nested_B2_conflicts_B3_clean_bad(self):
        """
        A depends on B1/B2/B3, B3 depends on C
        B2B3 in nested folder - B2 conflicts with B3
        """

        # A depends on B1
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="1.py")

        # A depends on B2
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nested",
            depFileName="conflict.py")

        # A depends on B3
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="nested",
            depFileName="conflict.py")

        # B3 depends on C
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="",
            depFileName="4.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_list_all_filepaths_recursive_case_1 (self):
        """
        Ensure list_all_filepaths generates the correct list
        A depends on B1/B2, B1 depends on C
        B1 is nested, B2 is not nested, C is nested wrt B1
        """

        # A depends on B1 (Which is nested)
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="B1_nested",
            depFileName="B1.py")

        # A depends on B2 (Which is not nested)
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="B2.py")

        # B1 depends on C (Nested wrt B1)
        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="C_nested",
            depFileName="C.py")

        self.assertEqual(self.test_cr_1_rev1.list_all_filepaths(),
                         [u'test_cr_1.py',
                          u'B1_nested/B1.py',
                          u'B1_nested/C_nested/C.py',
                          u'B2.py']);

    def test_dependency_list_all_filepaths_recursive_case_2 (self):
        """
        Ensure list_all_filepaths generates the correct list
        A depends on B1/B2, B2 depends on C
        B1 is nested, B2 is not nested, C is nested wrt B2
        """
        # A depends on B1 (Which is nested)
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="B1_nested",
            depFileName="B1.py")

        # A depends on B2 (Which is not nested)
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="B2.py")

        # B2 depends on C (Nested wrt B2)
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="C_nested",
            depFileName="C.py")

        self.assertEqual(self.test_cr_1_rev1.list_all_filepaths(),
                         [u'test_cr_1.py',
                          u'B1_nested/B1.py',
                          u'B2.py',
                          u'C_nested/C.py']);

    def test_dependency_list_all_filepaths_with_metapackage(self):

        # Define a code with a blank filename (metapackage)
        # Give it dependencies
        # Give one more dependency a nested dependency

        # The following is for testing code resource dependencies
        test_cr_6 = CodeResource(name="test_cr_6",
                                 filename="",
                                 description="CR6")
        test_cr_6.save()

        # The revision has no content_file because it's a metapackage
        test_cr_6_rev1 = CodeResourceRevision(coderesource=test_cr_6,
                                              revision_name="v1_metapackage",
                                              revision_desc="CR6-rev1")
        test_cr_6_rev1.save()

        # Current-folder dependencies
        test_cr_6_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="B.py")

        # Sub-folder dependencies
        test_cr_6_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="nestedFolder",
            depFileName="C.py")

        # Nested dependencies
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="deeperNestedFolder",
            depFileName="D.py")

        self.assertEqual(test_cr_6_rev1.list_all_filepaths(),
                         [u'B.py',
                          u'nestedFolder/C.py',
                          u'nestedFolder/deeperNestedFolder/D.py']);

        # FIXME
        # test_cr_6_rev1.content_file.delete()
        # test_cr_6_rev1.delete()

    def test_dependency_list_all_filepaths_single_unnested_dep_blank_depFileName(self):
        """List all filepaths when dependency has no depFileName set and is not nested.
        """
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath="");
        self.assertEqual(self.test_cr_1_rev1.list_all_filepaths(),
                         [u'test_cr_1.py', u'test_cr_2.py']);

    def test_dependency_list_all_filepaths_single_nested_dep_blank_depFileName(self):
        """List all filepaths when dependency has no depFileName set and is nested.
        """
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath="nest_folder");
        self.assertEqual(self.test_cr_1_rev1.list_all_filepaths(),
                         [u'test_cr_1.py', u'nest_folder/test_cr_2.py']);

    # This seems like enough tests of the blank depFileName case, as we have
    # pretty thoroughly checked other paths with the above cases.


class CodeResourceDependencyTests(MethodTestSetup):

    def test_unicode(self):
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
                                          depPath="subdir",
                                          depFileName="foo.py");

        # Display unicode for this dependency under valid conditions
        self.assertEquals(
                unicode(test_crd),
                "complement v1 requires complement v2 as subdir/foo.py");

    def test_invalid_dotdot_path_clean(self):
        """
        Dependency tries to go into a path outside its sandbox.
        """
        v1 = self.comp_cr.revisions.get(revision_name="v1");
        v2 = self.comp_cr.revisions.get(revision_name="v2");

        bad_crd = CodeResourceDependency(coderesourcerevision=v1,
                                         requirement=v2,
                                         depPath="..",
                                         depFileName="foo.py");
        self.assertRaisesRegexp(
            ValidationError,
            "depPath cannot reference \.\./",
            bad_crd.clean)

        bad_crd_2 = CodeResourceDependency(coderesourcerevision=v1,
                                           requirement=v2,
                                           depPath="../test",
                                           depFileName="foo.py");
        self.assertRaisesRegexp(
            ValidationError,
            "depPath cannot reference \.\./",
            bad_crd_2.clean)
        
    def test_valid_path_with_dotdot_clean(self):
        """
        Dependency goes into a path with a directory containing ".." in the name.
        """
        v1 = self.comp_cr.revisions.get(revision_name="v1");
        v2 = self.comp_cr.revisions.get(revision_name="v2");

        good_crd = CodeResourceDependency(coderesourcerevision=v1,
                                          requirement=v2,
                                          depPath="..bar",
                                          depFileName="foo.py");
        self.assertEquals(good_crd.clean(), None);
        
        good_crd_2 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="bar..",
                                            depFileName="foo.py");
        self.assertEquals(good_crd_2.clean(), None);

        good_crd_3 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/bar..",
                                            depFileName="foo.py");
        self.assertEquals(good_crd_3.clean(), None);

        good_crd_4 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/..bar",
                                            depFileName="foo.py");
        self.assertEquals(good_crd_4.clean(), None);

        good_crd_5 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/..bar..",
                                            depFileName="foo.py");
        self.assertEquals(good_crd_5.clean(), None);

        good_crd_6 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="..baz/bar..",
                                            depFileName="foo.py");
        self.assertEquals(good_crd_6.clean(), None);

        # This case works because the ".." doesn't take us out of the sandbox
        good_crd_7 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/../bar",
                                            depFileName="foo.py");
        self.assertEquals(good_crd_7.clean(), None);

        good_crd_8 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/..bar../blah",
                                            depFileName="foo.py");
        self.assertEquals(good_crd_8.clean(), None);
        
    def test_cr_with_filename_dependency_with_good_path_and_filename_clean(self):
        """
        Check
        """
        # cr_no_filename has name="complement" and filename="complement.py"
        cr = CodeResource(
                name="complement",
                filename="complement.py",
                description="Complement DNA/RNA nucleotide sequences");
        cr.save();

        # Define cr_rev_v1 for cr
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v1 = CodeResourceRevision(
                    coderesource=cr,
                    revision_name="v1",
                    revision_desc="First version",
                    content_file=File(f));
            cr_rev_v1.full_clean();
            cr_rev_v1.save();

        # Define cr_rev_v2 for cr
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v2 = CodeResourceRevision(
                    coderesource=cr,
                    revision_name="v2",
                    revision_desc="Second version",
                    content_file=File(f));
            cr_rev_v2.full_clean();
            cr_rev_v2.save();

        # Define a code resource dependency for cr_rev_v1 with good paths and filenames
        good_crd = CodeResourceDependency(coderesourcerevision=cr_rev_v1,
                                          requirement=cr_rev_v2,
                                          depPath="testFolder/anotherFolder",
                                          depFileName="foo.py");

        self.assertEqual(good_crd.clean(), None)
        
    def test_metapackage_cannot_have_file_names_bad_clean(self):

        # Define a standard code resource
        cr = CodeResource(
                name="complement",
                filename="test.py",
                description="Complement DNA/RNA nucleotide sequences");
        cr.save();

        # Give it a file
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v1 = CodeResourceRevision(
                coderesource=cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f));
            cr_rev_v1.full_clean();
            cr_rev_v1.save();
        
        # Define a metapackage code resource (no file name)
        cr_meta = CodeResource(
                name="complement",
                filename="",
                description="Complement DNA/RNA nucleotide sequences");
        cr_meta.save();

        # Do not give it a file
        cr_meta_rev_v1 = CodeResourceRevision(
            coderesource=cr_meta,
            revision_name="v1",
            revision_desc="First version");
        cr_meta_rev_v1.full_clean();
        cr_meta_rev_v1.save();

        # Add metapackage as a dependency to cr_rev_v1, but invalidly give it a depFileName
        bad_crd = CodeResourceDependency(coderesourcerevision=cr_rev_v1,
                                         requirement=cr_meta_rev_v1,
                                         depPath="testFolder/anotherFolder",
                                         depFileName="foo.py");

        self.assertRaisesRegexp(
            ValidationError,
            "Metapackage dependencies cannot have a depFileName",
            bad_crd.clean)

    def test_metapackage_good_clean(self):

        # Define a standard code resource
        cr = CodeResource(
                name="complement",
                filename="test.py",
                description="Complement DNA/RNA nucleotide sequences");
        cr.save();

        # Give it a file
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v1 = CodeResourceRevision(
                coderesource=cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f));
            cr_rev_v1.full_clean();
            cr_rev_v1.save();
        
        # Define a metapackage code resource (no file name)
        cr_meta = CodeResource(
                name="complement",
                filename="",
                description="Complement DNA/RNA nucleotide sequences");
        cr_meta.save();

        # Do not give it a file
        cr_meta_rev_v1 = CodeResourceRevision(
            coderesource=cr_meta,
            revision_name="v1",
            revision_desc="First version");
        cr_meta_rev_v1.full_clean();
        cr_meta_rev_v1.save();

        # Add metapackage as a dependency to cr_rev_v1
        good_crd = CodeResourceDependency(coderesourcerevision=cr_rev_v1,
                                         requirement=cr_meta_rev_v1,
                                         depPath="testFolder/anotherFolder",
                                         depFileName="");

        self.assertEqual(good_crd.clean(), None)


class MethodTests(MethodTestSetup):

    def test_with_family_unicode(self):
        """
        unicode() for method should return "Method revisionName and family name"
        """

        # DNAcompv1_m has method family DNAcomplement
        self.assertEqual(unicode(self.DNAcompv1_m),
                         "Method DNAcomplement v1");

    def test_without_family_unicode(self):
        """
        unicode() for Test unicode representation when family is unset.
        """
        nofamily = Method(revision_name="foo");

        self.assertEqual(unicode(nofamily),
                         "Method [family unset] foo");

    def test_no_inputs_checkInputIndices_good(self):
        """
        Method with no inputs defined should have
        check_input_indices() return with no exception.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save()

        # check_input_indices() should not raise a ValidationError
        self.assertEquals(foo.check_input_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_single_valid_input_checkInputIndices_good(self):
        """
        Method with a single, 1-indexed input should have
        check_input_indices() return with no exception.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # Add one valid input cdt at index 1 named "oneinput" to transformation
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        # check_input_indices() should not raise a ValidationError
        self.assertEquals(foo.check_input_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_many_ordered_valid_inputs_checkInputIndices_good (self):
        """
        Test check_input_indices on a method with several inputs,
        correctly indexed and in order.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # Add several input cdts that together are valid
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=2);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=3);

        # No ValidationErrors should be raised
        self.assertEquals(foo.check_input_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_many_valid_inputs_scrambled_checkInputIndices_good (self):
        """
        Test check_input_indices on a method with several inputs,
        correctly indexed and in scrambled order.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # Add several input cdts that together are valid
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=3);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=1);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=2);

        # No ValidationErrors should be raised
        self.assertEquals(foo.check_input_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_one_invalid_input_checkInputIndices_bad(self):
        """
        Test input index check, one badly-indexed input case.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # Add one invalid input cdt at index 4 named "oneinput"
        foo.create_input(compounddatatype=self.DNAinput_cdt,
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

    def test_many_nonconsective_inputs_scrambled_checkInputIndices_bad(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=2);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=6);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.check_input_indices);

        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_no_outputs_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)

        self.assertEquals(foo.check_output_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_one_valid_output_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev)
        foo.save()
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="oneoutput", dataset_idx=1)
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)
        self.assertEquals(foo.check_output_indices(), None)
        self.assertEquals(foo.clean(), None)

    def test_many_valid_outputs_scrambled_checkOutputIndices_good (self):
        """Test output index check, well-indexed multi-output (scrambled order) case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="oneoutput", dataset_idx=3);
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="twooutput", dataset_idx=1);
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="threeoutput", dataset_idx=2);
        self.assertEquals(foo.check_output_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_one_invalid_output_checkOutputIndices_bad (self):
        """Test output index check, one badly-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="oneoutput", dataset_idx=4)
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.check_output_indices);

        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_many_invalid_outputs_scrambled_checkOutputIndices_bad(self):
        """Test output index check, badly-indexed multi-output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1);
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="oneoutput", dataset_idx=2);
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="twooutput", dataset_idx=6);
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="threeoutput", dataset_idx=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.check_output_indices);

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.clean);

    def test_no_copied_parent_parameters_save(self):
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

        # Test the multiple-input and multiple-output cases, using
        # script_2_method and script_3_method respectively.  Neither
        # of these have parents.
        self.script_2_method.save();
        # Script 2 has input:
        # compounddatatype = self.triplet_cdt
        # dataset_name = "a_b_c"
        # dataset_idx = 1
        curr_in = self.script_2_method.inputs.all()[0];
        self.assertEqual(curr_in.dataset_name, "a_b_c");
        self.assertEqual(curr_in.dataset_idx, 1);
        self.assertEqual(curr_in.get_cdt(), self.triplet_cdt);
        self.assertEqual(curr_in.get_min_row(), None);
        self.assertEqual(curr_in.get_max_row(), None);
        # Outputs:
        # self.triplet_cdt, "a_b_c_squared", 1
        # self.singlet_cdt, "a_b_c_mean", 2
        curr_out_1 = self.script_2_method.outputs.all()[0];
        curr_out_2 = self.script_2_method.outputs.all()[1];
        self.assertEqual(curr_out_1.dataset_name, "a_b_c_squared");
        self.assertEqual(curr_out_1.dataset_idx, 1);
        self.assertEqual(curr_out_1.get_cdt(), self.triplet_cdt);
        self.assertEqual(curr_out_1.get_min_row(), None);
        self.assertEqual(curr_out_1.get_max_row(), None);
        self.assertEqual(curr_out_2.dataset_name, "a_b_c_mean");
        self.assertEqual(curr_out_2.dataset_idx, 2);
        self.assertEqual(curr_out_2.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_out_2.get_min_row(), None);
        self.assertEqual(curr_out_2.get_max_row(), None);

        self.script_3_method.save();
        # Script 3 has inputs:
        # self.singlet_cdt, "k", 1
        # self.singlet_cdt, "r", 2, min_row = max_row = 1
        curr_in_1 = self.script_3_method.inputs.all()[0];
        curr_in_2 = self.script_3_method.inputs.all()[1];
        self.assertEqual(curr_in_1.dataset_name, "k");
        self.assertEqual(curr_in_1.dataset_idx, 1);
        self.assertEqual(curr_in_1.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_in_1.get_min_row(), None);
        self.assertEqual(curr_in_1.get_max_row(), None);
        self.assertEqual(curr_in_2.dataset_name, "r");
        self.assertEqual(curr_in_2.dataset_idx, 2);
        self.assertEqual(curr_in_2.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_in_2.get_min_row(), 1);
        self.assertEqual(curr_in_2.get_max_row(), 1);
        # Outputs:
        # self.singlet_cdt, "kr", 1
        curr_out = self.script_3_method.outputs.all()[0];
        self.assertEqual(curr_out.dataset_name, "kr");
        self.assertEqual(curr_out.dataset_idx, 1);
        self.assertEqual(curr_out.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_out.get_min_row(), None);
        self.assertEqual(curr_out.get_max_row(), None);
        

    def test_with_copied_parent_parameters_save(self):
        """Test save when revision parent is specified."""

        # DNAcompv2_m should have 1 input, copied from DNAcompv1
        self.assertEqual(self.DNAcompv2_m.inputs.count(), 1);
        curr_in = self.DNAcompv2_m.inputs.all()[0];
        self.assertEqual(curr_in.dataset_name,
                         self.DNAinput_ti.dataset_name);
        self.assertEqual(curr_in.dataset_idx,
                         self.DNAinput_ti.dataset_idx);
        self.assertEqual(curr_in.get_cdt(),
                         self.DNAinput_ti.get_cdt());
         
        self.assertEqual(self.DNAcompv2_m.outputs.count(), 1);
        curr_out = self.DNAcompv2_m.outputs.all()[0];
        self.assertEqual(curr_out.dataset_name,
                         self.DNAoutput_to.dataset_name);
        self.assertEqual(curr_out.dataset_idx,
                         self.DNAoutput_to.dataset_idx);
        self.assertEqual(curr_out.get_cdt(),
                         self.DNAoutput_to.get_cdt());

        # Multiple output case (using script_2_method).
        foo = Method(family=self.test_mf, driver=self.script_2_crRev,
                     revision_parent=self.script_2_method);
        foo.save();
        # Check that it has the same input as script_2_method:
        # self.triplet_cdt, "a_b_c", 1
        curr_in = foo.inputs.all()[0];
        self.assertEqual(curr_in.dataset_name, "a_b_c");
        self.assertEqual(curr_in.dataset_idx, 1);
        self.assertEqual(curr_in.get_cdt(), self.triplet_cdt);
        self.assertEqual(curr_in.get_min_row(), None);
        self.assertEqual(curr_in.get_max_row(), None);
        # Outputs:
        # self.triplet_cdt, "a_b_c_squared", 1
        # self.singlet_cdt, "a_b_c_mean", 2
        curr_out_1 = foo.outputs.all()[0];
        curr_out_2 = foo.outputs.all()[1];
        self.assertEqual(curr_out_1.get_cdt(), self.triplet_cdt);
        self.assertEqual(curr_out_1.dataset_name, "a_b_c_squared");
        self.assertEqual(curr_out_1.dataset_idx, 1);
        self.assertEqual(curr_out_1.get_min_row(), None);
        self.assertEqual(curr_out_1.get_max_row(), None);
        self.assertEqual(curr_out_2.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_out_2.dataset_name, "a_b_c_mean");
        self.assertEqual(curr_out_2.dataset_idx, 2);
        self.assertEqual(curr_out_2.get_min_row(), None);
        self.assertEqual(curr_out_2.get_max_row(), None);

        # Multiple input case (using script_3_method).
        bar = Method(family=self.test_mf, driver=self.script_3_crRev,
                     revision_parent=self.script_3_method);
        bar.save();
        # Check that the outputs match script_3_method:
        # self.singlet_cdt, "k", 1
        # self.singlet_cdt, "r", 2, min_row = max_row = 1
        curr_in_1 = bar.inputs.all()[0];
        curr_in_2 = bar.inputs.all()[1];
        self.assertEqual(curr_in_1.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_in_1.dataset_name, "k");
        self.assertEqual(curr_in_1.dataset_idx, 1);
        self.assertEqual(curr_in_1.get_min_row(), None);
        self.assertEqual(curr_in_1.get_max_row(), None);
        self.assertEqual(curr_in_2.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_in_2.dataset_name, "r");
        self.assertEqual(curr_in_2.dataset_idx, 2);
        self.assertEqual(curr_in_2.get_min_row(), 1);
        self.assertEqual(curr_in_2.get_max_row(), 1);
        # Outputs:
        # self.singlet_cdt, "kr", 1
        curr_out = bar.outputs.all()[0];
        self.assertEqual(curr_out.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_out.dataset_name, "kr");
        self.assertEqual(curr_out.dataset_idx, 1);
        self.assertEqual(curr_out.get_min_row(), None);
        self.assertEqual(curr_out.get_max_row(), None);
        
        
        # If there are already inputs and outputs specified, then
        # they should not be overwritten.

        old_cdt = self.DNAinput_ti.get_cdt();
        old_name = self.DNAinput_ti.dataset_name;
        old_idx = self.DNAinput_ti.dataset_idx;
        
        self.DNAcompv1_m.revision_parent = self.RNAcompv2_m;
        self.DNAcompv1_m.save();
        self.assertEqual(self.DNAcompv1_m.inputs.count(), 1);
        curr_in = self.DNAcompv1_m.inputs.all()[0];
        self.assertEqual(curr_in.get_cdt(), old_cdt);
        self.assertEqual(curr_in.dataset_name, old_name);
        self.assertEqual(curr_in.dataset_idx, old_idx);
         
        old_cdt = self.DNAoutput_to.get_cdt();
        old_name = self.DNAoutput_to.dataset_name;
        old_idx = self.DNAoutput_to.dataset_idx;
        
        self.assertEqual(self.DNAcompv2_m.outputs.count(), 1);
        curr_out = self.DNAcompv2_m.outputs.all()[0];
        self.assertEqual(curr_out.get_cdt(), old_cdt);
        self.assertEqual(curr_out.dataset_name, old_name);
        self.assertEqual(curr_out.dataset_idx, old_idx);

        # Only inputs specified.
        bar.outputs.all().delete();
        bar.save();
        self.assertEqual(bar.inputs.count(), 2);
        self.assertEqual(bar.outputs.count(), 0);
        curr_in_1 = bar.inputs.all()[0];
        curr_in_2 = bar.inputs.all()[1];
        self.assertEqual(curr_in_1.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_in_1.dataset_name, "k");
        self.assertEqual(curr_in_1.dataset_idx, 1);
        self.assertEqual(curr_in_1.get_min_row(), None);
        self.assertEqual(curr_in_1.get_max_row(), None);
        self.assertEqual(curr_in_2.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_in_2.dataset_name, "r");
        self.assertEqual(curr_in_2.dataset_idx, 2);
        self.assertEqual(curr_in_2.get_min_row(), 1);
        self.assertEqual(curr_in_2.get_max_row(), 1);
        
        # Only outputs specified.
        foo.inputs.all().delete();
        foo.save();
        self.assertEqual(foo.inputs.count(), 0);
        self.assertEqual(foo.outputs.count(), 2);
        curr_out_1 = foo.outputs.all()[0];
        curr_out_2 = foo.outputs.all()[1];
        self.assertEqual(curr_out_1.get_cdt(), self.triplet_cdt);
        self.assertEqual(curr_out_1.dataset_name, "a_b_c_squared");
        self.assertEqual(curr_out_1.dataset_idx, 1);
        self.assertEqual(curr_out_1.get_min_row(), None);
        self.assertEqual(curr_out_1.get_max_row(), None);
        self.assertEqual(curr_out_2.get_cdt(), self.singlet_cdt);
        self.assertEqual(curr_out_2.dataset_name, "a_b_c_mean");
        self.assertEqual(curr_out_2.dataset_idx, 2);
        self.assertEqual(curr_out_2.get_min_row(), None);
        self.assertEqual(curr_out_2.get_max_row(), None);

    def test_driver_is_metapackage(self):
        """
        A metapackage cannot be a driver for a Method.
        """
        # Create a CodeResourceRevision with no content file (ie. a Metapackage).
        res = CodeResource(); res.save()
        rev = CodeResourceRevision(coderesource=res, content_file=None); rev.clean(); rev.save()
        f = MethodFamily(); f.save()
        m = Method(family=f, driver=rev)
        m.save()
        m.create_input(compounddatatype = self.singlet_cdt,
            dataset_name = "input",
            dataset_idx = 1);
        self.assertRaisesRegexp(ValidationError,
            error_messages["driver_metapackage"].format(".*", ".*"),
            m.clean)

    def test_run_code_nooutput(self):
        """
        Run a no-output method (which just prints to stdout).
        """
        empty_dir = tempfile.mkdtemp()

        proc = self.noop_method.run_code(empty_dir, [self.noop_infile], [])
        proc_out, proc_err = proc.communicate()

        self.assertEqual(proc_out, self.noop_indata)

        shutil.rmtree(empty_dir)

    def test_run_code_dir_not_empty(self):
        """
        Trying to run code in a non-empty directory should fail.
        """
        self.assertRaisesRegexp(ValueError,
            "Directory .* nonempty; contains file .*",
            lambda : self.noop_method.run_code(self.scratch_dir, [self.noop_infile], []))

class MethodFamilyTests(MethodTestSetup):

    def test_unicode(self):
        """
        unicode() for MethodFamily should display it's name.
        """
        
        self.assertEqual(unicode(self.DNAcomp_mf), "DNAcomplement")
