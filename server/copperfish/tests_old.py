"""
Old unit tests for Shipyard (Copperfish).

These are ones that predate August 24, 2013 (the introduction of ExecRecord
and its relatives) and have been refactored.
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
            string_dt.save()
            self.string_dt = string_dt

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
                name="complement",
                description="Complement DNA/RNA nucleotide sequences",
                filename="complement.py");
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
        self.script_1_method.full_clean();
        self.script_1_method.save()

        # Define "tuple" CDT containing (x,y): members x and y exist at index 1 and 2
        self.tuple_cdt = CompoundDatatype()
        self.tuple_cdt.save()
        self.tuple_cdt.members.create(datatype=self.string_dt,column_name="x",column_idx=1)
        self.tuple_cdt.members.create(datatype=self.string_dt,column_name="y",column_idx=2)

        # Assign tuple as both an input and an output to script_1_method
        self.script_1_method.create_input(compounddatatype = self.tuple_cdt,
                                           dataset_name = "input_tuple",
                                           dataset_idx = 1)
        self.script_1_method.create_output(compounddatatype = self.tuple_cdt,
                                           dataset_name = "input_tuple",
                                           dataset_idx = 1)
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
        self.script_2_method.full_clean();
        self.script_2_method.save()

        # Define "singlet" CDT containing CDT member (a) and "triplet" CDT with members (a,b,c)
        self.singlet_cdt = CompoundDatatype()
        self.singlet_cdt.save()
        self.singlet_cdt.members.create(datatype=self.string_dt,column_name="a",column_idx=1)

        self.triplet_cdt = CompoundDatatype()
        self.triplet_cdt.save()
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="a",column_idx=1)
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="b",column_idx=2)
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="c",column_idx=3)

        # Assign triplet as input and output,
        self.script_2_method.create_input(compounddatatype = self.triplet_cdt,
                                           dataset_name = "a_b_c",
                                           dataset_idx = 1)
        self.script_2_method.create_output(compounddatatype = self.triplet_cdt,
                                           dataset_name = "a_b_c_squared",
                                           dataset_idx = 1)
        self.script_2_method.create_output(compounddatatype = self.singlet_cdt,
                                           dataset_name = "a_b_c_mean",
                                           dataset_idx = 2)
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
        self.script_3_method.full_clean();
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
        self.script_3_method.save()
        
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
        self.DNAcompv1_p.create_input(
                compounddatatype=self.DNAinput_cdt,
                dataset_name="seqs_to_complement",
                dataset_idx=1);

        # Add a step to Pipeline revision DNAcompv1_p involving
        # a transformation DNAcompv2_m at step 1
        step1 = self.DNAcompv1_p.steps.create(
                transformation=self.DNAcompv2_m,
                step_num=1);

        # Add cabling (PipelineStepInputCable's) to (step1, DNAcompv1_p)
        # From step 0, output hole "seqs_to_comeplement" to
        # input hole "input" (of this step)
        step1.cables_in.create(transf_input=self.DNAcompv2_m.inputs.get(dataset_name="input"),
                              step_providing_input=0,
                              provider_output=self.DNAcompv1_p.inputs.get(
                                  dataset_name="seqs_to_complement"));

        # Add output cabling (PipelineOutputCable) to DNAcompv1_p
        # From step 1, output hole "output", send output to
        # Pipeline output hole "complemented_seqs" at index 1
        outcabling = self.DNAcompv1_p.create_outcable(
                step_providing_output=1,
                provider_output=step1.transformation.outputs.get(dataset_name="output"),
                output_name="complemented_seqs",
                output_idx=1);

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
        self.DNArecomp_m.create_input(
                compounddatatype = self.DNAoutput_cdt,
                dataset_name = "complemented_seqs",
                dataset_idx = 1);

        # To this method revision, add outputs with CDT DNAinput_cdt
        self.DNArecomp_m.create_output(
                compounddatatype = self.DNAinput_cdt,
                dataset_name = "recomplemented_seqs",
                dataset_idx = 1);

        f = open(os.path.join(samplecode_path, "stringUT.py"), "rb")
        dt_1 = Datatype(name="dt_1",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_1.save()
        self.dt_1 = dt_1

        dt_2 = Datatype(name="dt_2",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_2.save()
        self.dt_2 = dt_2

        dt_3 = Datatype(name="dt_3",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_3.save()
        self.dt_3 = dt_3

        dt_4 = Datatype(name="dt_4",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_4.save()
        self.dt_4 = dt_4

        dt_5 = Datatype(name="dt_5",
                        description="A string validated by stringUT.py",
                        verification_script=File(f),
                        Python_type="str");
        dt_5.save()
        self.dt_5 = dt_5

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

    def tearDown(self):
        # Go through all CodeResourceRevisions and VerificationScripts
        # and delete them.
        # -- Changed August 24, 2013
        for crr in CodeResourceRevision.objects.all():
            # Remember that this can be empty.
            # if crr.content_file != None:
            #     crr.content_file.delete()
            # Weirdly, if crr.content_file == None,
            # it still entered the above.  This seems to be a bug
            # in Django!
            if crr.coderesource.filename != "":
                crr.content_file.delete()

        for ds in Datatype.objects.all():
            ds.verification_script.delete()
            
        for dataset in Dataset.objects.all():
            dataset.dataset_file.delete()


class Datatype_tests(CopperfishMethodTests_setup):
    
    def test_datatype_unicode(self):
        """
        Unicode representation must be the instance's name.

        """
        my_datatype = Datatype(name="fhqwhgads");
        self.assertEqual(unicode(my_datatype), "fhqwhgads");

    ### Unit tests for datatype.clean (Circular restrictions) ###
        
    # Direct circular cases: start, middle, end
    # Start   dt1 restricts dt1, dt3, dt4
    # Middle  dt1 restricts dt3, dt1, dt4
    # End     dt1 restricts dt3, dt4, dt1
    # Good    dt1 restricts dt2, dt3, dt4

    def test_datatype_circular_direct_start_clean_bad(self):
        """
        Circular, direct, start
        dt1 restricts dt1, dt3, dt4
        """
        self.dt_1.restricts.add(self.dt_1);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);

    def test_datatype_circular_direct_middle_clean_bad(self):
        """
        Circular, direct, middle
        dt1 restricts dt3, dt1, dt4
        """
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_1);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);

    def test_datatype_circular_direct_end_clean_bad(self):
        """
        Circular, direct, middle
        dt1 restricts dt3, dt4, dt1
        """
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.restricts.add(self.dt_1);
        self.dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);

    def test_datatype_circular_direct_clean_good(self):
        """
        dt1 restricts dt2, dt3, dt4
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_begin_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();

        self.dt_2.restricts.add(self.dt_1);
        self.dt_2.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);

    def test_datatype_circular_recursive_middle_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();

        self.dt_3.restricts.add(self.dt_1);
        self.dt_3.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);

    def test_datatype_circular_recursive_end_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();
        self.dt_4.restricts.add(self.dt_1);
        self.dt_4.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);


    def test_datatype_circular_recursive_clean_good1(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_5);
        self.dt_2.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good2(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();
        self.dt_3.restricts.add(self.dt_5);
        self.dt_3.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good3(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();
        self.dt_4.restricts.add(self.dt_5);
        self.dt_4.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good4(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt4
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_4);
        self.dt_2.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good5(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt4
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();
        self.dt_3.restricts.add(self.dt_4);
        self.dt_3.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good6(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt2
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);        
        self.dt_1.save();
        self.dt_4.restricts.add(self.dt_2);
        self.dt_4.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_direct_is_restricted_by_1(self):
        """
        dt1 restricts dt2
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - TRUE
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();

        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), True);

    def test_datatype_direct_is_restricted_by_2(self):
        """
        dt1 and dt2 exist but do not restrict each other
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - FALSE
        """
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), False);

    def test_datatype_recursive_is_restricted_by_1(self):
        """
        dt1 restricts dt2, dt2 restricts dt3

        dt1.is_restricted_by(dt3) - FALSE
        dt3.is_restricted_by(dt1) - TRUE
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - TRUE
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_3);
        self.dt_2.save();
        
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False);
        self.assertEqual(self.dt_3.is_restricted_by(self.dt_1), True);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), True);

    def test_datatype_recursive_is_restricted_by_2(self):
        """
        dt1 restricts dt[2,3,4]
        dt2 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_5);
        self.dt_2.save();
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False);
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True);

    def test_datatype_recursive_is_restricted_by_3(self):
        """
        dt1 restricts dt[2,3,4]
        dt3 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_3.restricts.add(self.dt_5);
        self.dt_3.save();
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False);
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True);

    def test_datatype_recursive_is_restricted_by_4(self):
        """
        dt1 restricts dt[2,3,4]
        dt4 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_4.restricts.add(self.dt_5);
        self.dt_4.save();
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False);
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True);

    def test_datatype_no_restriction_clean_good (self):
        """
        Datatype without any restrictions.
        """
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_nested_valid_restrictions_clean_good(self):
        """
        Datatypes such that A restricts B, and B restricts C
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_3);
        self.dt_2.save();
        self.assertEqual(self.dt_1.clean(), None);
        self.assertEqual(self.dt_2.clean(), None);
        self.assertEqual(self.dt_3.clean(), None);

    def test_datatype_nested_invalid_restrictions_scrambled_clean_bad(self):
        """
        Datatypes are restricted to constrain execution order such that:
        A restricts C
        A restricts B
        B restricts C
        C restricts A
        """
         
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.save();
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_3);
        self.dt_2.save();
        self.dt_3.restricts.add(self.dt_1);
        self.dt_3.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);


    def test_datatype_direct_circular_restriction_clean_bad(self):
        """
        Datatype directly restricts itself: A restricts A
        """

        self.dt_1.restricts.add(self.dt_1);
        self.dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);

    def test_datatype_circular_restriction_indirect_clean(self):
        """
        Datatype restricts itself through intermediary:
        A restricts B
        B restricts A
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_1);
        self.dt_2.save();

        self.assertRaisesRegexp(ValidationError,
                                "Circular Datatype restriction detected",
                                self.dt_1.clean);

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
        Unicode of empty CompoundDatatype should be empty.
        """
        empty_cdt = CompoundDatatype();
        empty_cdt.save();
        self.assertEqual(unicode(empty_cdt), "[empty CompoundDatatype]");

    def test_cdt_single_member_unicode(self):
        """
        Unicode on single-member cdt returns its member.
        """
        self.assertEqual(unicode(self.DNAinput_cdt),
                         "(1: <DNANucSeq> [SeqToComplement])");

    def test_cdt_multiple_members_unicode(self):
        """
        Unicode returns a list of it's Datatype members.

        Each member is in the form of unicode(CompoundDatatypeMember).
        """
        self.assertEqual(unicode(self.test_cdt),
                         "(1: <string> [label], 2: <DNANucSeq> [PBMCseq], " +
                         "3: <RNANucSeq> [PLAseq])");

    def test_clean_single_index_good (self):
        """
        CompoundDatatype with single index equalling 1.
        """
        sad_cdt = CompoundDatatype();
        sad_cdt.save();
        sad_cdt.members.create(	datatype=self.RNA_dt,
                                column_name="ColumnTwo",
                                column_idx=1);
        self.assertEqual(sad_cdt.clean(), None);

    def test_clean_single_index_bad (self):
        """
        CompoundDatatype with single index not equalling 1.
        """
        sad_cdt = CompoundDatatype();
        sad_cdt.save();
        sad_cdt.members.create(	datatype=self.RNA_dt,
                                column_name="ColumnTwo",
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
        self.assertEqual(self.test_cdt.clean(), None);

        good_cdt = CompoundDatatype();
        good_cdt.save();
        good_cdt.members.create(datatype=self.RNA_dt,
                               column_name="ColumnTwo",
                               column_idx=2);
        good_cdt.members.create(datatype=self.DNA_dt,
                               column_name="ColumnOne",
                               column_idx=1);
        self.assertEqual(good_cdt.clean(), None);

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
        unicode should return the codeResource name.
        """
        self.assertEquals(unicode(self.comp_cr), "complement");
  
    def test_codeResource_valid_name_clean_good(self):
        """
        Clean passes when codeResource name is file-system valid
        """
        valid_cr = CodeResource(name="name",
                                filename="validName",
                                description="desc")
        valid_cr.save()
        self.assertEqual(valid_cr.clean(), None);

    def test_codeResource_valid_name_with_special_symbols_clean_good(self):
        """
        Clean passes when codeResource name is file-system valid
        """
        valid_cr = CodeResource(name="anotherName",
                                filename="valid.Name with-spaces_and_underscores().py",
                                description="desc")
        valid_cr.save()
        self.assertEqual(valid_cr.clean(), None);

    def test_codeResource_invalid_name_doubledot_clean_bad(self):
        """
        Clean fails when CodeResource name isn't file-system valid
        """

        invalid_cr = CodeResource(name="test",
                                  filename="../test.py",
                                  description="desc")
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError,"Invalid code resource filename",invalid_cr.clean)

    def test_codeResource_invalid_name_starting_space_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="test",
                                  filename=" test.py",
                                  description="desc")
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError,"Invalid code resource filename",invalid_cr.clean)

    def test_codeResource_invalid_name_invalid_symbol_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="name",
                                  filename="test$.py",
                                  description="desc")
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError,"Invalid code resource filename",invalid_cr.clean)

    def test_codeResource_invalid_name_trailing_space_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="name",
                                  filename="test.py ",
                                  description="desc")
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError,"Invalid code resource filename",invalid_cr.clean)


class CodeResourceRevision_tests(CopperfishMethodTests_setup):

    def test_codeResourceRevision_unicode(self):
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
        
    def test_codeResourceRevision_metapackage_cannot_have_file_bad_clean(self):
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

    def test_codeResourceRevision_non_metapackage_must_have_file_bad_clean(self):
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
                                          depPath="subdir",
                                          depFileName="foo.py");

        # Display unicode for this dependency under valid conditions
        self.assertEquals(
                unicode(test_crd),
                "complement v1 requires complement v2 as subdir/foo.py");

    def test_codeResourceDependency_invalid_dotdot_path_clean(self):
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
        
    def test_codeResourceDependency_valid_path_with_dotdot_clean(self):
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
        
    def test_codeResourceDependency_cr_with_filename_dependency_with_good_path_and_filename_clean(self):
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
        
    def test_codeResourceDependency_metapackage_cannot_have_file_names_bad_clean(self):

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

    def test_codeResourceDependency_metapackage_good_clean(self):

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
        
class TransformationFamily_tests(CopperfishMethodTests_setup):

    def test_methodFamily_unicode(self):
        """
        unicode() for methodFamily should display it's name
        """
        
        self.assertEqual(unicode(self.DNAcomp_mf),
                         "DNAcomplement");

    def test_pipelineFamily_unicode(self):
        """
        unicode() for pipelineFamily should display it's name
        """
        
        self.assertEqual(unicode(self.DNAcomp_pf),
                         "DNAcomplement");


class Method_tests(CopperfishMethodTests_setup):

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

    def test_method_no_inputs_checkInputIndices_good(self):
        """
        Method with no inputs defined should have
        check_input_indices() return with no exception.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        # check_input_indices() should not raise a ValidationError
        self.assertEquals(foo.check_input_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_single_valid_input_checkInputIndices_good(self):
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

    def test_method_many_ordered_valid_inputs_checkInputIndices_good (self):
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

    def test_method_many_valid_inputs_scrambled_checkInputIndices_good (self):
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

    def test_method_one_invalid_input_checkInputIndices_bad(self):
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

    def test_method_many_nonconsective_inputs_scrambled_checkInputIndices_bad(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
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

    def test_method_no_outputs_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();

        self.assertEquals(foo.check_output_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_one_valid_output_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="oneoutput", dataset_idx=1);
        self.assertEquals(foo.check_output_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_many_valid_outputs_scrambled_checkOutputIndices_good (self):
        """Test output index check, well-indexed multi-output (scrambled order) case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="oneoutput", dataset_idx=3);
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="twooutput", dataset_idx=1);
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                           dataset_name="threeoutput", dataset_idx=2);
        self.assertEquals(foo.check_output_indices(), None);
        self.assertEquals(foo.clean(), None);

    def test_method_one_invalid_output_checkOutputIndices_bad (self):
        """Test output index check, one badly-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev);
        foo.save();
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
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
        

    def test_method_with_copied_parent_parameters_save(self):
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
    
class Pipeline_tests(CopperfishMethodTests_setup):
    
    def test_pipeline_one_valid_input_clean(self):
        """Test input index check, one well-indexed input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        self.assertEquals(foo.clean(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Pipeline foo has no steps",
            foo.complete_clean());


    def test_pipeline_one_invalid_input_clean(self):
        """Test input index check, one badly-indexed input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.complete_clean);


    def test_pipeline_many_valid_inputs_clean(self):
        """Test input index check, well-indexed multi-input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=2);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=3);
        self.assertEquals(foo.clean(), None);

    def test_pipeline_many_valid_inputs_scrambled_clean(self):
        """Test input index check, well-indexed multi-input (scrambled order) case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=2);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=3);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=1);
        self.assertEquals(foo.clean(), None);


    def test_pipeline_many_invalid_inputs_clean(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=2);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="twoinput", dataset_idx=3);
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="threeinput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_one_valid_step_clean(self):
        """Test step index check, one well-indexed step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);

        self.assertEquals(step1.clean(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"input\" to transformation at step 1 is not cabled",
            step1.complete_clean);
        self.assertEquals(foo.clean(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"input\" to transformation at step 1 is not cabled",
            foo.complete_clean);

    def test_pipeline_one_bad_step_clean(self):
        """Test step index check, one badly-indexed step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
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
        foo.create_input(compounddatatype=self.DNAinput_cdt,
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
        foo.create_input(compounddatatype=self.DNAinput_cdt,
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
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=4);
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=5);
        
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_oneStep_valid_cabling_clean(self):
        """Test good step cabling, one-step pipeline."""

        # Define pipeline 'foo' in family 'DNAcomp_pf'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Add single, validly indexed pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Add single validly indexed step, composed of the method DNAcompv2
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);

        # Add cabling from step 0 with input name "oneinput"
        cable = step1.cables_in.create(
            transf_input=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        self.assertEquals(cable.clean(), None);
        self.assertEquals(step1.clean(), None);
        self.assertEquals(step1.complete_clean(), None);
        self.assertEquals(foo.clean(), None);
        self.assertEquals(foo.complete_clean(), None);
        
    def test_pipeline_oneStep_invalid_step_numbers_clean(self):
        """Bad pipeline (step not indexed 1), step is complete and clean."""

        # Define a pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        
        # Define a validly indexed pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 2 without a step 1
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=2);

        # Give this step properly mapped cabling from the Pipeline input
        cable = step1.cables_in.create(
            transf_input=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        self.assertEquals(cable.clean(), None);
        self.assertEquals(step1.clean(), None);
        self.assertEquals(step1.complete_clean(), None);
        
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean);
        
    def test_pipeline_oneStep_invalid_cabling_invalid_pipeline_input_clean (self):
        """Bad cabling: step looks for input that does not belong to the pipeline."""

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define pipeline input for 'foo'
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 1 of this pipeline by transformation DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Cable a pipeline input that does not belong to the pipeline to step 1
        cable = step1.cables_in.create(
            transf_input=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=self.DNAcompv1_p.inputs.get(dataset_name="seqs_to_complement"));
        
        self.assertRaisesRegexp(
                ValidationError,
                "Pipeline does not have input \"\[Pipeline DNAcomplement v1\]:1 \(1: <DNANucSeq> \[SeqToComplement\]\) seqs_to_complement\"",
                cable.clean);
        # The following are just the same as the above, propagated upwards through clean()s.
        self.assertRaisesRegexp(
                ValidationError,
                "Pipeline does not have input \"\[Pipeline DNAcomplement v1\]:1 \(1: <DNANucSeq> \[SeqToComplement\]\) seqs_to_complement\"",
                step1.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Pipeline does not have input \"\[Pipeline DNAcomplement v1\]:1 \(1: <DNANucSeq> \[SeqToComplement\]\) seqs_to_complement\"",
                step1.complete_clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Pipeline does not have input \"\[Pipeline DNAcomplement v1\]:1 \(1: <DNANucSeq> \[SeqToComplement\]\) seqs_to_complement\"",
                foo.clean);
        
    def test_pipeline_oneStep_invalid_cabling_incorrect_cdt_clean(self):
        """Bad cabling: input is of wrong CompoundDatatype."""

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define pipeline input "oneinput" for foo with CDT type test_cdt
        foo.create_input(compounddatatype=self.test_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 1 by transformation DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Reference correct name "oneinput" and cable to step "input"
        # of DNAcompv2_m - but of the wrong cdt
        cable = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        self.assertEquals(cable.clean(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Custom wiring required for cable \"Pipeline DNAcomplement foo step 1:input\"",
            cable.clean_and_completely_wired);
        
    def test_pipeline_oneStep_cabling_minrow_constraint_may_be_breached_clean (self):
        """Unverifiable cabling: step requests input with possibly too
        few rows (input min_row unspecified)."""

        # Define method 'curr_method' with driver compv2_crRev
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        curr_method.save();

        # Give curr_method properly indexed input with min_row = 10
        curr_method.create_input(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  min_row=10);

        # Give curr_method an output named 'output'
        curr_method.create_output(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define properly indexed pipeline input for 'foo'
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 1 of 'foo' to be curr_method
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1);

        # From row-unconstrained pipeline input, assign to curr_method
        cable = step1.cables_in.create(
            transf_input=curr_method.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # It's possible this step may have too few rows
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                cable.clean);
        # This is just to check that the above propagated up.
        self.assertRaisesRegexp(
            ValidationError,
            "Data fed to input \"input\" of step 1 may have too few rows",
            foo.clean);
        
    def test_pipeline_oneStep_cabling_minrow_constraints_may_breach_each_other_clean (self):
        """Unverifiable cabling: step requests input with possibly too few rows
        (input min_row specified)."""
        
        # Define method curr_method
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        curr_method.save();

        # Give curr_method an input with min_row = 10
        curr_method.create_input(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1, min_row=10);

        # Give curr_method an unconstrained output
        curr_method.create_output(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define pipeline input of foo to have min_row of 5
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1,
                          min_row=5);

        # Assign curr_method to step1 of foo
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1);
        
        # Map min_row = 5 pipeline input to this step's input
        # which contains curr_method with min_row = 10
        cable = step1.cables_in.create(
            transf_input=curr_method.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                cable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                foo.clean);


    def test_pipeline_oneStep_cabling_maxRow_constraints_may_be_breached_clean(self):
        """Unverifiable cabling: step requests input with possibly too many rows
        (input max_row unspecified)"""

        # Define curr_method with input of max_row = 10
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        curr_method.save();
        curr_method.create_input(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1, max_row=10);
       
        curr_method.create_output(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline with unrestricted Pipeline input
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Assign curr_method to step 1 of foo, and cable the pipeline input to it
        step1 = foo.steps.create(transformation=curr_method, step_num=1);
        cable = step1.cables_in.create(
            transf_input=curr_method.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # The pipeline input is unrestricted, but step 1 has max_row = 10
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                cable.clean);
        # Check propagation of error.
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                foo.clean);


    def test_pipeline_oneStep_cabling_maxRow_constraints_may_breach_each_other_clean (self):
        """Unverifiable cabling: step requests input with possibly too
        many rows (max_row set for pipeline input)."""
        
        # Define curr_method as having an input with max_row = 10
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        curr_method.save();
        curr_method.create_input(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  max_row=10);
        curr_method.create_output(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline foo with Pipeline input having max_row = 20
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1,
                          max_row=20);

        # Assign curr_method to foo step 1
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1);
        cable = step1.cables_in.create(
            transf_input=curr_method.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # The pipeline max_row is not good enough to guarantee correctness
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                cable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                foo.clean);
        
    def test_pipeline_oneStep_with_valid_outcable_clean(self):
        """Good output cabling, one-step pipeline."""

        # Define pipeline foo with unconstrained input
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Add DNAcompv2_m (Expecting 1 input) to step 1 of foo
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Map the pipeline input to step 1
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Connect the output of step 1 to the output of foo
        outcable = foo.create_outcable(
            output_name="oneoutput",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        self.assertEquals(outcable.clean(), None);
        self.assertEquals(foo.clean(), None);

    def test_pipeline_oneStep_outcable_references_nonexistent_step_clean(self):
        """Bad output cabling, one-step pipeline: request from nonexistent step"""

        # Define pipeline foo with validly indexed input and step 1 cabling
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.cables_in.create(transf_input=step1.transformation.inputs.get(dataset_name="input"),
                              step_providing_input=0,
                              provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Index a non-existent step to outcable
        outcable = foo.outcables.create(
            output_name="oneoutput", output_idx=1,
            step_providing_output=5,
            provider_output=step1.transformation.outputs.all()[0],
            output_cdt=step1.transformation.outputs.all()[0].get_cdt());
        
        self.assertRaisesRegexp(
            ValidationError,
            "Output requested from a non-existent step",
            outcable.clean);
        # Check propagation of error.
        self.assertRaisesRegexp(
            ValidationError,
            "Output requested from a non-existent step",
            foo.clean);
        
    def test_pipeline_oneStep_outcable_references_invalid_output_clean (self):
        """Bad output cabling, one-step pipeline: request output not belonging to requested step"""

        # Define pipeline foo with validly indexed inputs, steps, and cabling
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(transf_input=self.DNAcompv2_m.inputs.get(dataset_name="input"),
                              step_providing_input=0,
                              provider_output=foo.inputs.get(dataset_name="oneinput"));
 
        # Reference a correct step but TransformationOutput from another Transformation.
        outcable = foo.outcables.create(
            output_name="oneoutput", output_idx=1,
            step_providing_output=1,
            provider_output=self.RNAoutput_to,
            output_cdt=self.RNAoutput_to.get_cdt());
        
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"\[Method RNAcomplement v1\]:1 \(1: <RNANucSeq> \[ComplementedSeq\]\) output\"",
                outcable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"\[Method RNAcomplement v1\]:1 \(1: <RNANucSeq> \[ComplementedSeq\]\) output\"",
                foo.clean);
        
    def test_pipeline_oneStep_outcable_references_deleted_output_clean (self):
        """Output cabling, one-step pipeline: request deleted step output (OK)"""

        # Define pipeline foo with validly indexed inputs, steps, and cabling
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(transf_input=self.DNAcompv2_m.inputs.get(dataset_name="input"),
                              step_providing_input=0,
                              provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Mark DNAcompv2_m output as deletable.
        # August 24, 2013: this is now OK.
        step1.add_deletion(
            dataset_to_delete=self.DNAcompv2_m.outputs.get(dataset_name="output"));

        # Now try to map it to the pipeline output.
        outcable = foo.create_outcable(
            output_name="oneoutput",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));

        self.assertEquals(outcable.clean(), None)
        self.assertEquals(foo.clean(), None)
        
    def test_pipeline_oneStep_bad_pipeline_output_indexing_clean(self):
        """Bad output cabling, one-step pipeline: output not indexed 1"""

        # Define pipeline with validly indexed inputs, steps, and cabling
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(transf_input=step1.transformation.inputs.get(dataset_name="input"),
                              step_providing_input=0,
                              provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Outcable references a valid step and output, but is itself badly indexed
        outcable = foo.create_outcable(
            output_name="oneoutput",
            output_idx=9,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        
        self.assertEquals(outcable.clean(), None);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_manySteps_valid_internal_cabling_clean(self):
        """Test good step cabling, chained-step pipeline."""

        # Define pipeline 'foo' with validly indexed input and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Map pipeline input to step1
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        cable1 = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Map step 1 to step 2
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        cable2 = step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));

        # Map step 2 to step 3
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3);
        cable3 = step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        self.assertEquals(cable1.clean(), None);
        self.assertEquals(cable2.clean(), None);
        self.assertEquals(cable3.clean(), None);
        self.assertEquals(step1.clean(), None);
        self.assertEquals(step1.complete_clean(), None);
        self.assertEquals(step2.clean(), None);
        self.assertEquals(step2.complete_clean(), None);
        self.assertEquals(step3.clean(), None);
        self.assertEquals(step3.complete_clean(), None);
        self.assertEquals(foo.clean(), None);
        
    def test_pipeline_manySteps_cabling_references_invalid_output_clean(self):
        """Bad cabling: later step requests invalid input from previous."""

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # step1 receives input from Pipeline input
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(transf_input=step1.transformation.inputs.get(dataset_name="input"),
                              step_providing_input=0,
                              provider_output=foo.inputs.get(dataset_name="oneinput"));

        # step2 receives output not coming from from step1's transformation
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        cable2 = step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=self.RNAoutput_to);
        
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3);
        step3.cables_in.create(transf_input=step3.transformation.inputs.get(dataset_name="input"),
                              step_providing_input=2,
                              provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"\[Method RNAcomplement v1\]:1 \(1: <RNANucSeq> \[ComplementedSeq\]\) output\"",
                cable2.clean);

        # Check propagation of error.
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"\[Method RNAcomplement v1\]:1 \(1: <RNANucSeq> \[ComplementedSeq\]\) output\"",
                step2.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"\[Method RNAcomplement v1\]:1 \(1: <RNANucSeq> \[ComplementedSeq\]\) output\"",
                foo.clean);
        
    def test_pipeline_manySteps_cabling_references_deleted_input_clean(self):
        """Cabling: later step requests input deleted by producing step (OK)."""

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Connect step 1 with pipeline input
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(transf_input=step1.transformation.inputs.get(dataset_name="input"),
                              step_providing_input=0,
                              provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Connect step2 with output of step1
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));

        # Mark the output of step2 as deleted
        step2.add_deletion(
            dataset_to_delete=step2.transformation.outputs.get(
                dataset_name="recomplemented_seqs"));

        self.assertEquals(foo.clean(), None);

        # Connect step3 with the deleted output at step 2
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3);
        cable3 = step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        # August 24, 2013: cabling from deleted outputs is now OK.
        self.assertEquals(cable3.clean(), None)
        self.assertEquals(step3.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_manySteps_cabling_references_incorrect_cdt_clean (self):
        """Bad cabling: later step requests input of wrong CompoundDatatype."""
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(transf_input=step1.transformation.inputs.get(dataset_name="input"),
                              step_providing_input=0,
                              provider_output=foo.inputs.get(dataset_name="oneinput"));
        
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        step2.cables_in.create(transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
                              step_providing_input=1,
                              provider_output=step1.transformation.outputs.get(dataset_name="output"));
        
        step3 = foo.steps.create(transformation=self.RNAcompv2_m,
                                 step_num=3);
        cable = step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        self.assertEquals(cable.clean(), None);
        self.assertRaisesRegexp(
                ValidationError,
                "Custom wiring required for cable \"Pipeline DNAcomplement foo step 3:input\"",
                cable.clean_and_completely_wired);
        self.assertRaisesRegexp(
                ValidationError,
                "Custom wiring required for cable \"Pipeline DNAcomplement foo step 3:input\"",
                step3.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Custom wiring required for cable \"Pipeline DNAcomplement foo step 3:input\"",
                foo.clean);

    def test_pipeline_manySteps_minRow_constraint_may_be_breached_clean (self):
        """Unverifiable cabling: later step requests input with possibly too few rows (min_row unset for providing step)."""

        # Define a method with validly indexed inputs and outputs
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step2method.save();
        step2method.create_input(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.create_output(compounddatatype=self.DNAinput_cdt,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1);

        # Define another method with validly indexed inputs and outputs
        # But with the inputs requiring min_row = 5
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step3method.save();

        step3method.create_input(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  min_row=5);
        step3method.create_output(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        
        step2.cables_in.create(
            transf_input=step2method.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));

        # Step 3 requires min_row = 5 but step2 does not guarentee this
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        
        cable = step3.cables_in.create(
            transf_input=step3method.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2method.outputs.get(dataset_name="recomplemented_seqs"));
        
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                cable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                foo.clean);


    def test_pipeline_manySteps_minrow_constraints_may_breach_each_other_clean(self):
        """Bad cabling: later step requests input with possibly too few rows (providing step min_row is set)."""
        
        # Define method with outputs having a min row of 5
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step2method.save();
        step2method.create_input(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        
        step2method.create_output(compounddatatype=self.DNAinput_cdt,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1,
                                   min_row=5);

        # Define another method with input min row of 10
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step3method.save();
        step3method.create_input(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  min_row=10);
        step3method.create_output(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Recall the output of step2 has min_row = 5
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.cables_in.create(
            transf_input=step2method.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));

        # Recall the input of step3 has min_row = 10
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        
        cable = step3.cables_in.create(
            transf_input=step3method.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2method.outputs.get(dataset_name="recomplemented_seqs"));
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                cable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                foo.clean);


    def test_pipeline_manySteps_maxRow_constraint_may_be_breached_clean(self):
        """Bad cabling: later step requests input with possibly too many rows (max_row unset for providing step)."""

        # step2 has no constraints on it's output
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step2method.save();
        step2method.create_input(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.create_output(compounddatatype=self.DNAinput_cdt,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1);

        # step3 has an input with max_row = 100
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step3method.save();

        step3method.create_input(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  max_row=100);
        step3method.create_output(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.cables_in.create(
            transf_input=step2method.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        cable = step3.cables_in.create(
            transf_input=step3method.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2method.outputs.get(dataset_name="recomplemented_seqs"));
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too many rows",
                cable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too many rows",
                foo.clean);



    def test_pipeline_manySteps_cabling_maxRow_constraints_may_breach_each_other_clean (self):
        """Bad cabling: later step requests input with possibly too many rows (max_row for providing step is set)."""

        # step 2 has max_row = 100 on it's output
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step2method.save();
        step2method.create_input(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.create_output(compounddatatype=self.DNAinput_cdt,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1,
                                   max_row=100);

        # step3 has a max_row = 50 on it's input
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev);
        step3method.save();
        step3method.create_input(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  max_row=50);
        step3method.create_output(compounddatatype=self.DNAoutput_cdt,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.cables_in.create(
            transf_input=step2method.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        cable = step3.cables_in.create(
            transf_input=step3method.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2method.outputs.get(dataset_name="recomplemented_seqs"));
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too many rows",
                cable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too many rows",
                foo.clean);

        
    def test_pipeline_manySteps_valid_outcable_clean(self):
        """Good output cabling, chained-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        outcable1 = foo.create_outcable(
            output_name="outputone", output_idx=1,
            step_providing_output=3,
            provider_output=step3.transformation.outputs.get(dataset_name="output"));
        outcable2 = foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            step_providing_output=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
        self.assertEquals(outcable1.clean(), None);
        self.assertEquals(outcable2.clean(), None);
        self.assertEquals(foo.clean(), None);


    def test_pipeline_manySteps_outcable_references_nonexistent_step_clean(self):
        """Bad output cabling, chained-step pipeline: request from nonexistent step"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        # step 5 doesn't exist
        outcable1 = foo.outcables.create(
            output_name="outputone", output_idx=1,
            step_providing_output=5,
            provider_output=step3.transformation.outputs.get(dataset_name="output"),
            output_cdt=step3.transformation.outputs.get(dataset_name="output").get_cdt());
        outcable2 = foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            step_providing_output=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
        
        self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                outcable1.clean);
        self.assertEquals(outcable2.clean(), None);
        self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                foo.clean);

    def test_pipeline_manySteps_outcable_references_invalid_output_clean(self):
        """Bad output cabling, chained-step pipeline: request output not belonging to requested step"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        outcable1 = foo.create_outcable(
            output_name="outputone", output_idx=1,
            step_providing_output=3,
            provider_output=step3.transformation.outputs.get(dataset_name="output"));
        outcable2 = foo.outcables.create(
            output_name="outputtwo", output_idx=2,
            step_providing_output=2,
            provider_output=step1.transformation.outputs.get(dataset_name="output"),
            output_cdt=step1.transformation.outputs.get(dataset_name="output").get_cdt());

        self.assertEquals(outcable1.clean(), None);
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 2 does not produce output \"\[Method DNAcomplement v2\]:1 \(1: <DNANucSeq> \[ComplementedSeq\]\) output\"",
                outcable2.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 2 does not produce output \"\[Method DNAcomplement v2\]:1 \(1: <DNANucSeq> \[ComplementedSeq\]\) output\"",
                foo.clean);

        
    def test_pipeline_manySteps_outcable_references_deleted_output_clean(self):
        """Output cabling, chained-step pipeline: request deleted step output (OK)"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
        step3.add_deletion(
            dataset_to_delete=step3.transformation.outputs.get(dataset_name="output"));

        outcable1 = foo.create_outcable(
            output_name="outputone", output_idx=1,
            step_providing_output=3,
            provider_output=step3.transformation.outputs.get(dataset_name="output"));
        outcable2 = foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            step_providing_output=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        # August 24, 2013: outcabling from deleted stuff is now OK.
        self.assertEquals(outcable1.clean(), None);
        self.assertEquals(outcable2.clean(), None);
        self.assertEquals(foo.clean(), None);

    def test_pipeline_manySteps_outcable_references_invalid_output_index_clean(self):
        """Bad output cabling, chain-step pipeline: outputs not consecutively numbered starting from 1"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        outcable1 = foo.create_outcable(
            output_name="outputone",
            output_idx=5,
            step_providing_output=3,
            provider_output=step3.transformation.outputs.get(dataset_name="output"));
        outcable2 = foo.create_outcable(
            output_name="outputtwo",
            output_idx=2,
            step_providing_output=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        self.assertEquals(outcable1.clean(), None);
        self.assertEquals(outcable2.clean(), None);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_with_1_step_and_2_inputs_both_cabled_good(self):
        """
        Pipeline with 1 step (script_3_product) with 2 inputs / 1 output
        Both inputs are cabled (good)

        Reminder on script_3_product
        Reminder: k is cdt singlet, r is cdt single-row singlet
        """
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # Pipeline inputs must be singlet_cdt to work with script_3_product
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_1_k",
                          dataset_idx=1)

        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_2_r",
                          dataset_idx=2,
                          max_row=1,
                          min_row=1)

        # Add script_3 as step 1 method
        step1 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        cable1 = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="k"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_k"));

        cable2 = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="r"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_2_r"));

        self.assertEquals(cable1.clean(), None)
        self.assertEquals(cable2.clean(), None)
        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_inputs_cabled_more_than_once_bad(self):
        """
        Pipeline with 1 step (script_3_product) with 2 inputs / 1 output
        r is cabled more than once (bad)

        Reminder on script_3_product
        Reminder: k is cdt singlet, r is cdt single-row singlet
        """
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # Pipeline inputs must be singlet_cdt to work with script_3_product
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_1_k",
                          dataset_idx=1)

        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_2_r",
                          dataset_idx=2,
                          max_row=1,
                          min_row=1)

        # Add script_3 as step 1 method
        step1 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        cable1 = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="k"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_k"));

        cable2 = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="r"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_2_r"));        

        # Send a cable to r more than once!
        cable3 = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="r"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_2_r"));

        self.assertEquals(cable1.clean(), None);
        self.assertEquals(cable2.clean(), None);
        self.assertEquals(cable3.clean(), None);
        
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"r\" to transformation at step 1 is cabled more than once",
            step1.clean);
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"r\" to transformation at step 1 is cabled more than once",
            step1.complete_clean);
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"r\" to transformation at step 1 is cabled more than once",
            foo.clean);

    def test_pipeline_with_1_step_and_2_inputs_cabled_more_than_once_different_cables_bad(self):
        """
        Pipeline with 1 step (script_3_product) with 2 inputs / 1 output
        r is cabled more than once (bad)

        Reminder on script_3_product
        Reminder: k is cdt singlet, r is cdt single-row singlet
        """
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # Pipeline inputs must be singlet_cdt to work with script_3_product
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_1_k",
                          dataset_idx=1)

        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_2_r",
                          dataset_idx=2,
                          max_row=1,
                          min_row=1)

        # Add script_3 as step 1 method
        step1 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="k"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_k"));

        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="r"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_2_r"));        

        # Send a cable to k from r.
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="k"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_2_r"));

        # We don't bother checking cables or propagation.
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"k\" to transformation at step 1 is cabled more than once",
            step1.clean);

    def test_pipeline_with_1_step_and_2_inputs_but_only_first_input_is_cabled_in_step_1_bad(self):
        """
        Pipeline with 1 step with 2 inputs / 1 output
        Only the first input is cabled (bad)
        """

        # Define pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc")
        foo.save()

        # foo has two inputs that match inputs for script_3_product
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_1_k",
                          dataset_idx=1)
        
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_2_r",
                          dataset_idx=2,
                          max_row=1,
                          min_row=1)

        # Add script_3 as step 1 method
        step1 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="k"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_k"))

        # Step is clean (cables are OK) but not complete (inputs not quenched).
        self.assertEquals(step1.clean(), None)
        self.assertRaisesRegexp(
                ValidationError,
                "Input \"r\" to transformation at step 1 is not cabled",
                step1.complete_clean);

    def test_pipeline_with_1_step_and_2_inputs_but_only_second_input_is_cabled_in_step_1_bad(self):
        """
        Pipeline with 1 step with 2 inputs / 1 output
        Only the second input is cabled (bad)
        """

        # Define pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_3_product
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_1_k",
                          dataset_idx=1)
        
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_input_2_r",
                          dataset_idx=2,
                          max_row=1,
                          min_row=1)

        # Add script_3 as step 1 method
        step1 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(transf_input=self.script_3_method.inputs.get(dataset_name="r"),
                              step_providing_input=0,
                              provider_output=foo.inputs.get(dataset_name="pipe_input_2_r"));

        # Step is clean (cables are OK) but not complete (inputs not quenched).
        self.assertEquals(step1.clean(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"k\" to transformation at step 1 is not cabled",
            step1.complete_clean);
        

    def test_pipeline_with_2_steps_and_2_inputs_one_cabled_from_step_0_other_from_undeleted_step_1_good(self):
        """
        Step 1 (script_2_square_and_means) with 1 input / 2 outputs
            Method has input "a_b_c" (cdt triplet),
            output "a_b_c_squared" (cdt triplet),
            and output "a_b_c_mean" (cdt singlet)

        Step 2 (script_3_product) with 2 inputs / 1 output
            Method has input "k" (cdt singlet),
            input "r" (single-row cdt singlet),
            output "kr" (cdt singlet)

        Pipeline has input triplet (pipe_a_b_c) for step 1 and single-row singlet (pipe_r) for step 2
        Step 2 depends on step 1 output singlet a_b_c_mean

        Step 1 a_b_c_mean not deleted (good)
        """

        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_a_b_c",
                          dataset_idx=1)
        
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_r",
                          dataset_idx=2,
                          max_row=1,
                          min_row=1)

        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        cable1 = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_a_b_c"));
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2);

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        cable21 = step2.cables_in.create(
            transf_input=self.script_3_method.inputs.get(dataset_name="r"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_r"));

        # singlet a_b_c_mean from step 1 feeds into singlet k at step 2
        cable22 = step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="k"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(
                dataset_name="a_b_c_mean"));

        self.assertEquals(cable1.clean(), None)
        self.assertEquals(cable21.clean(), None)
        self.assertEquals(cable22.clean(), None)
        self.assertEquals(step2.clean(), None)
        self.assertEquals(step2.complete_clean(), None)

    def test_pipeline_with_2_steps_and_2_inputs_one_cabled_from_step_0_other_from_step_1_with_irrelevent_deletion_good(self):
        """
        Step 1 (script_2_square_and_means) with 1 input / 2 outputs
            Method has input "a_b_c" (cdt triplet),
            output "a_b_c_squared" (cdt triplet),
            and output "a_b_c_mean" (cdt singlet)

        Step 2 (script_3_product) with 2 inputs / 1 output
            Method has input "k" (cdt singlet),
            input "r" (single-row cdt singlet),
            output "kr" (cdt singlet)

        Pipeline has input triplet (pipe_a_b_c) for step 1 and single-row singlet (pipe_r) for step 2
        Step 2 depends on step 1 output singlet a_b_c_mean

        Step 1 a_b_c_mean not deleted (good)
        """

        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_a_b_c",
                          dataset_idx=1)
        
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_r",
                          dataset_idx=2,
                          max_row=1,
                          min_row=1)

        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_a_b_c"));

        # Delete irrelevant output
        step1.add_deletion(
            dataset_to_delete = step1.transformation.outputs.get(dataset_name="a_b_c_squared"))
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2);

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="r"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_r"));

        # singlet a_b_c_mean from step 1 feeds into singlet k at step 2
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="k"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"));

        # Don't bother checking cables; their errors would propagate here anyway.
        self.assertEquals(step2.clean(), None)
        self.assertEquals(step2.complete_clean(), None)
        self.assertEquals(foo.clean(), None)


    def test_pipeline_with_2_steps_and_2_inputs_one_cabled_from_step_0_other_from_deleted_step_1_good(self):
        """
        Step 1 output a_b_c_mean is cabled into step 2, but is deleted.
        """
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_a_b_c",
                          dataset_idx=1)
        
        foo.create_input(compounddatatype=self.singlet_cdt,
                          dataset_name="pipe_r",
                          dataset_idx=2,
                          max_row=1,
                          min_row=1)

        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_a_b_c"));
        
        # This output required for subsequent steps
        # August 24, 2013: this is now allowed, so no error should be raised.
        step1.add_deletion(
            dataset_to_delete = step1.transformation.outputs.get(dataset_name="a_b_c_mean"))
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2);

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        cable1 = step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="r"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_r"));

        # singlet a_b_c_mean (deleted!) from step 1 feeds into singlet k at step 2
        cable2 = step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="k"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"));

        self.assertEquals(cable1.clean(), None)
        self.assertEquals(cable2.clean(), None)
        self.assertEquals(step2.clean(), None)
        self.assertEquals(foo.clean(), None)


    def test_pipeline_with_1_step_and_2_outputs_outcable_1st_output_that_is_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable 1st output, which is deleted (OK)
        """

        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();


        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 1st output (Which is deleted)
        # August 24, 2013: this is now OK
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is not deleted)
        outcable2 = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(outcable2.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_outputs_outcable_1st_output_with_second_output_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable 1st output, whhen the second output is deleted (good)
        """

        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        # Add outcable for 1st output (Which is not deleted)
        outcable = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(outcable.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_outputs_outcable_1st_output_with_nothing_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable 1st output, nothing is deleted (good)
        """

        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Add outcable for 1st output (Which is not deleted)
        outcable = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(outcable.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_outputs_outcable_2nd_output_that_is_deleted_OK(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable 2nd output, and 2nd is deleted (OK)
        """
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        # Add outcable for 2nd output (Which is deleted)
        # August 24, 2013: this is now OK.
        outcable = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(outcable.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_outputs_outcable_2nd_output_with_first_output_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable 2nd output, while first output is deleted (good)
        """
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));
        
        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is not deleted)
        outcable = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(outcable.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_outputs_outcable_2nd_output_with_nothing_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable 2nd output, nothing is deleted (good)
        """
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_a_b_c",
                          dataset_idx=1)

        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_a_b_c"))

        outcable = foo.create_outcable(
            output_name="aName",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(outcable.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_outputs_outcable_both_outputs_none_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable both outputs, neither deleted (good)
        """
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Add outcables for both outputs
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))
        outcable2 = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertRaisesRegexp(outcable1.clean(), None);
        self.assertRaisesRegexp(outcable2.clean(), None);
        self.assertRaisesRegexp(foo.clean(), None);

    def test_pipeline_with_1_step_and_2_outputs_outcable_both_outputs_1st_is_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable both outputs, and 1st is deleted (OK)
        """
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 1st output (Which is deleted)
        # August 24, 2013: this is now allowed, so no error should be raised later.
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is not deleted)
        outcable2 = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(outcable2.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_outputs_outcable_both_outputs_2nd_is_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable both outputs, and 2nd is deleted (which is fine)
        """
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        # Add outcable for 1st output (Which is not deleted)
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is deleted)
        # August 24, 2013: this is now allowed, so tests should be fine.
        outcable2 = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(foo.clean(), None)


        # Create a pipeline with outcables, save the outcables, CHANGE them, then see
        # all of the previous outcables were eliminated...
        #
        # Besides this, incorporate some create_output checks in the above pipeline cases


    def test_create_outputs(self):
        """
        Create outputs from output cablings; also change the output cablings
        and recreate the outputs to see if they're correct.
        """
        # This setup is copied from one of the above tests.
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc");
        foo.save();

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                          dataset_name="pipe_input_1_a_b_c",
                          dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1);

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Add outcable for 1st output (Which is not deleted)
        foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is deleted)
        foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(foo.clean(), None);

        foo.create_outputs();
        # The first output should be as follows:
        # compounddatatype: self.triplet_cdt
        # dataset_name: output_a_b_c_squared
        # dataset_idx: 1
        # min_row = None
        # max_row = None
        # The second:
        # self.singlet_cdt, output_a_b_c_mean, 2, None, None
        self.assertEquals(foo.outputs.count(), 2);
        curr_out_1 = foo.outputs.all()[0];
        self.assertEquals(curr_out_1.dataset_name, "output_a_b_c_squared");
        self.assertEquals(curr_out_1.dataset_idx, 1);
        self.assertEquals(curr_out_1.get_cdt(), self.triplet_cdt);
        self.assertEquals(curr_out_1.get_min_row(), None);
        self.assertEquals(curr_out_1.get_max_row(), None);
        curr_out_2 = foo.outputs.all()[1];
        self.assertEquals(curr_out_2.dataset_name, "output_a_b_c_mean");
        self.assertEquals(curr_out_2.dataset_idx, 2);
        self.assertEquals(curr_out_2.get_cdt(), self.singlet_cdt);
        self.assertEquals(curr_out_2.get_min_row(), None);
        self.assertEquals(curr_out_2.get_max_row(), None);

        # Now delete all the output cablings and make new ones; then check
        # and see if create_outputs worked.
        foo.outcables.all().delete();

        # Add outcable for 1st output (Which is not deleted)
        foo.create_outcable(
            output_name="foo",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        self.assertEquals(foo.clean(), None);

        foo.create_outputs();
        # Now there is one output:
        # self.triplet_cdt, "foo", 1, None, None
        self.assertEquals(foo.outputs.count(), 1);
        curr_out_new = foo.outputs.all()[0];
        self.assertEquals(curr_out_new.dataset_name, "foo");
        self.assertEquals(curr_out_new.dataset_idx, 1);
        self.assertEquals(curr_out_new.get_cdt(), self.triplet_cdt);
        self.assertEquals(curr_out_new.get_min_row(), None);
        self.assertEquals(curr_out_new.get_max_row(), None);


    def test_create_outputs_multi_step(self):
        """Testing create_outputs with a multi-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            transf_input=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            transf_input=step3.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        foo.create_outcable(
            output_name="outputone", output_idx=1,
            step_providing_output=3,
            provider_output=step3.transformation.outputs.get(dataset_name="output"));
        foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            step_providing_output=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
        self.assertEquals(foo.clean(), None);

        foo.create_outputs();
        # The outputs look like:
        # self.DNAoutput_cdt, "outputone", 1, None, None
        # self.DNAinput_cdt, "outputtwo", 2, None, None
        self.assertEquals(foo.outputs.count(), 2);
        curr_out_1 = foo.outputs.all()[0];
        self.assertEquals(curr_out_1.dataset_name, "outputone");
        self.assertEquals(curr_out_1.dataset_idx, 1);
        self.assertEquals(curr_out_1.get_cdt(), self.DNAoutput_cdt);
        self.assertEquals(curr_out_1.get_min_row(), None);
        self.assertEquals(curr_out_1.get_max_row(), None);
        curr_out_2 = foo.outputs.all()[1];
        self.assertEquals(curr_out_2.dataset_name, "outputtwo");
        self.assertEquals(curr_out_2.dataset_idx, 2);
        self.assertEquals(curr_out_2.get_cdt(), self.DNAinput_cdt);
        self.assertEquals(curr_out_2.get_min_row(), None);
        self.assertEquals(curr_out_2.get_max_row(), None);

        # Now recreate them and check it worked
        foo.outcables.all().delete();
        foo.create_outcable(
            output_name="foo", output_idx=1,
            step_providing_output=2,
            provider_output=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
        foo.create_outputs();
        # Now the only output is:
        # self.DNAinput_cdt, "foo", 2, None, None
        self.assertEquals(foo.outputs.count(), 1);
        curr_out_new = foo.outputs.all()[0];
        self.assertEquals(curr_out_new.dataset_name, "foo");
        self.assertEquals(curr_out_new.dataset_idx, 1);
        self.assertEquals(curr_out_new.get_cdt(), self.DNAinput_cdt);
        self.assertEquals(curr_out_new.get_min_row(), None);
        self.assertEquals(curr_out_new.get_max_row(), None);
 

class PipelineStep_tests(CopperfishMethodTests_setup):

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
        """Bad cabling: step requests data from after its execution step."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        # Step 1 invalidly requests data from step 2
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        cable = step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=2,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
     
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 requests input from a later step",
                cable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 requests input from a later step",
                step1.clean);

    def test_pipelineStep_oneStep_cable_to_invalid_step_input_clean(self):
        """Bad cabling: step cables to input not belonging to its transformation."""

        # Define Pipeline
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define Pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1);

        # Create a step composed of method DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);

        # Reference an invalid input name from step 0
        cable = step1.cables_in.create(
            transf_input=self.script_1_method.inputs.get(dataset_name="input_tuple"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not have input \"\[Method Test method family script1\]:1 \(1: <string> \[x\], 2: <string> \[y\]\) input_tuple\"",
                cable.clean);
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not have input \"\[Method Test method family script1\]:1 \(1: <string> \[x\], 2: <string> \[y\]\) input_tuple\"",
                step1.clean);

    def test_pipelineStep_oneStep_valid_cabling_with_valid_delete_clean(self):
        """Test good step cabling with deleted dataset, one-step pipeline."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Define Pipeline input "oneinput"
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Add a step
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Map Pipeline input to step 1
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Mark step 1 "output" as deletable
        # step 1 "output" is defined by DNAcompv2_m
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="output"));

        self.assertEquals(step1.clean(), None);

    def test_pipelineStep_oneStep_valid_cabling_bad_delete_clean(self):
        """Bad cabling: deleting dataset that doesn't belong to this step, one-step pipeline."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Add a valid pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define valid pipeline step
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Create input cabling for this step
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Reference TransformationOutput not belonging to this step's
        # transformation.
        step1.add_deletion(
            dataset_to_delete=self.script_2_method.outputs.all()[0]);
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not have output \"\[Method Test method family script2\]:1 \(1: <string> \[a\], 2: <string> \[b\], 3: <string> \[c\]\) a_b_c_squared\"",
                step1.clean);
         
    def test_pipelineStep_oneStep_cabling_directly_self_referential_transformation_clean(self):
        """Bad step: pipeline step contains the parent pipeline directly."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Give it a single validly indexed pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Add a valid step 1, but reference itself as the transformation
        step1 = foo.steps.create(transformation=foo,
                                 step_num=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                step1.clean);
         
    def test_pipelineStep_oneStep_cabling_referenced_pipeline_references_parent_clean (self):
        """Bad step: pipeline step contains the parent pipeline in its lone recursive sub-step."""
        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();

        # Give it a single validly indexed pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);

        # Define step 1 as executing DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);

        # Map the input at stpe 1 from Pipeline input "oneinput"
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));

        # Define pipeline output at index 1 from (step 1, output "output")
        foo.create_outcable(
            output_name="oneoutput",
            output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        foo.create_outputs();
        foo.save();

        # Define a second pipeline
        bar = Pipeline(family=self.DNAcomp_pf,
                       revision_name="bar",
                       revision_desc="Bar version");
        bar.save();

        # Give it a single validly indexed pipeline input
        bar.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="barinput",
                          dataset_idx=1);

        # At step 1, execute the transformation 'foo' defined above
        bstep1 = bar.steps.create(transformation=foo,
                                  step_num=1);

        # Map to foo.input "oneinput" from bar pipeline output "barinput"
        bstep1.cables_in.create(
            transf_input=foo.inputs.get(dataset_name="oneinput"),
            step_providing_input=0,
            provider_output=bar.inputs.get(dataset_name="barinput"));

        # Map a single output, from step 1 foo.output = "oneoutput"
        bar.create_outcable(
            output_name="baroutput",
            output_idx=1,
            step_providing_output=1,
            provider_output=bstep1.transformation.outputs.get(dataset_name="oneoutput"));
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
         
    def test_pipelineStep_manySteps_cabling_referenced_pipeline_references_parent_clean(self):
        """Bad step: pipeline step contains the parent pipeline in some recursive sub-step."""

        # foo invokes DNAcompv2_m at step 1
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1);
        step1.cables_in.create(
            transf_input=step1.transformation.inputs.get(dataset_name="input"),
            step_providing_input=0,
            provider_output=foo.inputs.get(dataset_name="oneinput"));
        foo.create_outcable(
            output_name="oneoutput", output_idx=1,
            step_providing_output=1,
            provider_output=step1.transformation.outputs.get(dataset_name="output"));
        foo.create_outputs();
        foo.save();

        # bar invokes foo at step 1 and DNArecomp_m at step 2
        bar = Pipeline(family=self.DNAcomp_pf,
                       revision_name="bar",
                       revision_desc="Bar version");
        bar.save();
        bar.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="barinput",
                          dataset_idx=1);
        bstep1 = bar.steps.create(transformation=foo,
                                  step_num=1);
        
        bstep1.cables_in.create(
            transf_input=bstep1.transformation.inputs.get(dataset_name="oneinput"),
            step_providing_input=0,
            provider_output=bar.inputs.get(dataset_name="barinput"));
        
        bstep2 = bar.steps.create(transformation=self.DNArecomp_m,
                                  step_num=2);
        bstep2.cables_in.create(
            transf_input=bstep2.transformation.inputs.get(dataset_name="complemented_seqs"),
            step_providing_input=1,
            provider_output=bstep1.transformation.outputs.get(dataset_name="oneoutput"));
        bar.create_outcable(
            output_name="baroutputone",
            output_idx=1,
            step_providing_output=1,
            provider_output=bstep1.transformation.outputs.get(dataset_name="oneoutput"));
        bar.create_outcable(
            output_name="baroutputtwo",
            output_idx=2,
            step_providing_output=2,
            provider_output=bstep2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
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


class Copperfish_Raw_Setup (TestCase):

    def setUp(self):

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
            self.script_4_1_CRR = CodeResourceRevision(coderesource=self.script_4_CR,revision_name="v1",revision_desc="v1",content_file=File(f))
            self.script_4_1_CRR.save()

        # Define MF in order to define method
        self.test_MF = MethodFamily(name="test method family",description="method family placeholder");
        self.test_MF.full_clean()
        self.test_MF.save()

        # Establish CRR as a method within a given method family
        self.script_4_1_M = Method(revision_name="s4",revision_desc="s4",family = self.test_MF,driver = self.script_4_1_CRR)
        self.script_4_1_M.full_clean()
        self.script_4_1_M.save()

        # A shorter alias
        self.testmethod = self.script_4_1_M;

        # Define DT "string" to define CDT with CDT members
        with open(os.path.join(samplecode_path, "stringUT.py"), "rb") as f:
            self.string_dt = Datatype(name="string", description="A String", verification_script=File(f), Python_type="str");
            self.string_dt.save()

        # Create Datatype "DNANucSeq" with validation code DNANucSeqUT.py
        # and make it a restriction of string_dt
        with open(os.path.join(samplecode_path, "DNANucSeqUT.py"), "rb") as f:
            self.DNA_dt = Datatype(name="DNANucSeq",
                                   description="String consisting of ACGTacgt",
                                   verification_script=File(f),
                                   Python_type="str");
            self.DNA_dt.save();

            # DNA_dt is a restricted type of string
            self.DNA_dt.restricts.add(self.string_dt);
            self.DNA_dt.save();

        # Define CDT "triplet_cdt" with 3 members for use as an input/output
        self.triplet_cdt = CompoundDatatype()
        self.triplet_cdt.save()
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="a^2",column_idx=1)
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="b^2",column_idx=2)
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="c^2",column_idx=3)

        # A CDT with mixed Datatypes
        self.mix_triplet_cdt = CompoundDatatype()
        self.mix_triplet_cdt.save()
        self.mix_triplet_cdt.members.create(datatype=self.string_dt,column_name="StrCol1",column_idx=1)
        self.mix_triplet_cdt.members.create(datatype=self.DNA_dt,column_name="DNACol2",column_idx=2)
        self.mix_triplet_cdt.members.create(datatype=self.string_dt,column_name="StrCol3",column_idx=3)

        # Define CDT "doublet_cdt" with 2 members for use as an input/output
        self.doublet_cdt = CompoundDatatype()
        self.doublet_cdt.save();
        self.doublet_cdt.members.create(datatype=self.string_dt,column_name="StrCol1",column_idx=1)
        self.doublet_cdt.members.create(datatype=self.string_dt,column_name="StrCol2",column_idx=2)
        
        # Define CDT "DNAdoublet_cdt" with 2 members for use as an input/output
        self.DNAdoublet_cdt = CompoundDatatype()
        self.DNAdoublet_cdt.save();
        self.DNAdoublet_cdt.members.create(datatype=self.DNA_dt,column_name="DNACol1",column_idx=1)
        self.DNAdoublet_cdt.members.create(datatype=self.DNA_dt,column_name="DNACol2",column_idx=2)

        # Define PF in order to define pipeline
        self.test_PF = PipelineFamily(name="test pipeline family",description="pipeline family placeholder");
        self.test_PF.full_clean()
        self.test_PF.save()

        # Needed for Datasets to be created
        self.myUser = User.objects.create_user('john', 'lennon@thebeatles.com', 'johnpassword')
        self.myUser.last_name = 'Lennon'
        self.myUser.save()

    def tearDown(self):
        # Go through all CodeResourceRevisions and VerificationScripts
        # and delete them.
        for crr in CodeResourceRevision.objects.all():
            crr.content_file.delete()

        for ds in Datatype.objects.all():
            ds.verification_script.delete()
            
        for dataset in Dataset.objects.all():
            dataset.dataset_file.delete()

class PipelineStepRawDelete_tests(Copperfish_Raw_Setup):

    def test_PipelineStep_clean_raw_output_to_be_deleted_good(self):
        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(
            compounddatatype=self.triplet_cdt,
            dataset_name="a_b_c_squared",
            dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(
            dataset_name="a_b_c_squared_raw", dataset_idx=2)
        self.script_4_1_M.clean()

        # Define 1-step pipeline with a single raw pipeline input
        pipeline_1 = self.test_PF.members.create(
            revision_name="foo",revision_desc="Foo version");
        pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        step1.add_deletion(dataset_to_delete=raw_output)

        self.assertEquals(step1.clean(), None)
        self.assertEquals(pipeline_1.clean(), None)

    def test_PipelineStep_clean_delete_single_existent_raw_to_good(self):
        # Define a single raw output for self.script_4_1_M
        raw_output = self.script_4_1_M.create_output(
            dataset_name="a_b_c_squared_raw", dataset_idx=1)

        # Define 1-step pipeline
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        step1.add_deletion(dataset_to_delete=raw_output)

        self.assertEquals(step1.clean(), None)

    def test_PipelineStep_clean_delete_non_existent_tro_bad(self):
        # Define a 1-step pipeline containing self.script_4_1_M which has a raw_output
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=1)
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a 1-step pipeline containing self.script_4_2_M which has a raw_output
        self.script_4_2_M = Method(revision_name="s42",revision_desc="s42",family = self.test_MF,driver = self.script_4_1_CRR)
        self.script_4_2_M.save()
        raw_output_unrelated = self.script_4_2_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=1)
        pipeline_unrelated = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        step1_unrelated = pipeline_unrelated.steps.create(transformation=self.script_4_2_M,step_num=1)

        # For pipeline 1, mark a raw output to be deleted in an unrelated method
        step1.add_deletion(dataset_to_delete=raw_output_unrelated)

        errorMessage = "Transformation at step 1 does not have output \"\[Method test method family s42\]:raw1 a_b_c_squared_raw\""

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.clean)

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            pipeline_1.clean)
        
    def test_PipelineStep_clean_raw_output_to_be_deleted_in_different_pipeline_bad(self):
        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        self.script_4_2_M = Method(revision_name="s42",revision_desc="s42",family = self.test_MF,driver = self.script_4_1_CRR)
        self.script_4_2_M.save()
        unrelated_raw_output = self.script_4_2_M.create_output(dataset_name="unrelated_raw_output",dataset_idx=1)

        # Define 1-step pipeline with a single raw pipeline input
        pipeline_1 = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define second 1-step pipeline with a single raw pipeline input
        pipeline_2 = self.test_PF.members.create(revision_name="bar",revision_desc="Bar version");
        pipeline_2.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1_unrelated = pipeline_2.steps.create(transformation=self.script_4_2_M,step_num=1)

        # For pipeline 1, mark a raw output to be deleted in a different pipeline (pipeline_2)
        step1.add_deletion(dataset_to_delete=unrelated_raw_output)

        error_msg = "Transformation at step 1 does not have output \"\[Method test method family s42\]:raw1 unrelated_raw_output\""

        self.assertRaisesRegexp(ValidationError, error_msg, step1.clean)

        self.assertRaisesRegexp(ValidationError, error_msg, pipeline_1.clean)


class RawOutputCable_tests(Copperfish_Raw_Setup):

    def test_PipelineOutputCable_raw_outcable_references_valid_step_good(self):

        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1);

        # Outmap a raw cable from a valid step
        outcable1 = self.pipeline_1.create_raw_outcable(raw_output_name="validName",
            raw_output_idx=1,
            step_providing_raw_output=1,
            provider_raw_output=raw_output)

        # Note: pipeline + pipeline step 1 complete_clean would fail (not all inputs are quenched)
        self.pipeline_1.create_outputs()
        self.assertEquals(step1.clean(), None)
        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(self.pipeline_1.clean(), None)
        
    def test_PipelineOutputCable_raw_outcable_references_deleted_output_good(self):

        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        # Define 2-step pipeline with a single raw pipeline input
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)
        step2 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=2)

        # Outmap a raw cable from a valid step + valid output
        outcable1 = pipeline_1.create_raw_outcable(raw_output_name="validName",
                                                    raw_output_idx=1,
                                                    step_providing_raw_output=1,
                                                    provider_raw_output=raw_output)

        # It's not actually deleted yet - so no error
        self.assertEquals(outcable1.clean(), None)

        # Mark raw output of step1 as deleted
        step1.add_deletion(dataset_to_delete=raw_output)

        # Now it's deleted.
        # NOTE August 23, 2013: this doesn't break anymore.
        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(pipeline_1.clean(), None)
        self.assertEquals(step1.clean(), None)

    def test_PipelineOutputCable_raw_outcable_references_valid_step_but_invalid_raw_TO_bad(self):
        
        # Define 1 raw input, and 1 raw + 1 CSV (self.triplet_cdt) output for method self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        # Define an unrelated method and give it a raw output
        unrelated_method = Method(revision_name="s4 - unrelated",revision_desc="s4 - unrelated",family = self.test_MF,driver = self.script_4_1_CRR)
        unrelated_method.save()
        unrelated_method.clean()
        unrelated_raw_output = unrelated_method.create_output(dataset_name="unrelated raw output",dataset_idx=1)

        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1);

        # Outmap a raw cable to a valid step but a TransformationRawOutput that does not exist at the specified PS
        outcable1 = self.pipeline_1.outcables.create(
            output_name="validName",
            output_idx=1,
            step_providing_output=1,
            provider_output=unrelated_raw_output)

        self.assertRaisesRegexp(
            ValidationError,
            "Transformation at step 1 does not produce output \"\[Method test method family s4 - unrelated\]:raw1 unrelated raw output\"",
            outcable1.clean)

    def test_PipelineOutputCable_raw_outcable_references_invalid_step_bad(self):
        
        # Define 1 raw input, and 1 raw + 1 CSV (self.triplet_cdt) output for method self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1);

        # Outmap a raw cable to an invalid step
        outcable1 = self.pipeline_1.outcables.create(
            output_name="validName",
            output_idx=1,
            step_providing_output=2,
            provider_output=raw_output)

        error_msg = "Output requested from a non-existent step"

        self.assertRaisesRegexp(ValidationError, error_msg, outcable1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg, self.pipeline_1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg,
                                self.pipeline_1.complete_clean)

class RawInputCable_tests(Copperfish_Raw_Setup):
    def test_PSIC_raw_cable_comes_from_pipeline_input_good(self):
        """
        Cable is fed from a pipeline input.
        """

        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)

        # Define 2 identical steps within the pipeline
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1);
        step2 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=2);

        # Cable the pipeline input into step1's transformation's only raw input hole
        rawcable1 = step1.create_raw_cable(
            transf_raw_input=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            pipeline_raw_input=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        rawcable2 = step2.create_raw_cable(
            transf_raw_input=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            pipeline_raw_input=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        # These raw cables were both cabled from the pipeline input and are valid
        self.assertEquals(rawcable1.clean(), None)
        self.assertEquals(rawcable2.clean(), None)
        
    def test_PSIC_raw_cable_comes_from_pipeline_input_step_num_not_zero_bad(self):
        """
        Cable is fed from a pipeline input but the step number is not 0.
        """

        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)

        # Define 2 identical steps within the pipeline
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1);
        step2 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=2);

        # Cable the pipeline input into step1's transformation's only raw input hole,
        # but provide the wrong step number.
        rawcable1 = step1.cables_in.create(
            transf_input=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            step_providing_input=2,
            provider_output=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        rawcable2 = step2.cables_in.create(
            transf_input=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            step_providing_input=1,
            provider_output=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        self.assertRaisesRegexp(
            ValidationError,
            "Cable \"Pipeline test pipeline family v1 step 1:a_b_c\(raw\)\" must have step 0 for a source",
            rawcable1.clean)
        self.assertRaisesRegexp(
            ValidationError,
            "Cable \"Pipeline test pipeline family v1 step 2:a_b_c\(raw\)\" must have step 0 for a source",
            rawcable2.clean)

    def test_PSIC_raw_cable_comes_from_internal_step_bad(self):
        """
        pipeline_raw_input comes from within the pipeline, not from a pipeline RawInput
        """
        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)

        # Define 2 identical steps within the pipeline
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1);
        step2 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=2);

        # Cable the pipeline input into step1's transformation's only raw input hole
        rawcable1 = step1.create_raw_cable(
            transf_raw_input=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            pipeline_raw_input=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        # Invalidly raw cable from step 1 to step 2
        rawcable2 = step2.cables_in.create(
            transf_input=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=self.script_4_1_M.inputs.get(dataset_name="a_b_c"));

        # Second method is invalidly raw cabled from the input of the first method
        self.assertEquals(rawcable1.clean(), None)

        error_msg = "Step 2 requests raw input not coming from parent pipeline"
        self.assertRaisesRegexp(ValidationError, error_msg, rawcable2.clean) 
        self.assertRaisesRegexp(ValidationError, error_msg, step2.clean)
        self.assertRaisesRegexp(ValidationError, error_msg, step2.complete_clean) 
        self.assertRaisesRegexp(ValidationError, error_msg, self.pipeline_1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg, self.pipeline_1.complete_clean)

    def test_PSIC_raw_cable_leads_to_foreign_pipeline_bad(self):
        """
        transf_raw_input (the destination) must belong to a PS Transformation in THIS pipeline
        """
        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define two different 1-step pipelines with 1 raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version")
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1_pipeline_1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        self.pipeline_2 = self.test_PF.members.create(revision_name="v2",revision_desc="Second version")
        self.pipeline_2.save()
        self.pipeline_2.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1_pipeline_2 = self.pipeline_2.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a raw cable into Pipeline2step1 from Pipeline1's raw inputs (Cross-pipeline contamination!)
        rawcable1 = step1_pipeline_2.cables_in.create(
            transf_input=step1_pipeline_2.transformation.inputs.get(dataset_name="a_b_c"),
            step_providing_input=0,
            provider_output=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        self.assertRaisesRegexp(ValidationError,"Step 1",rawcable1.clean) 
        self.assertRaisesRegexp(ValidationError,"Step 1",step1_pipeline_2.clean)
        self.assertRaisesRegexp(ValidationError,"Step 1",step1_pipeline_2.complete_clean) 
        self.assertRaisesRegexp(ValidationError,"Step 1",self.pipeline_2.clean)

    def test_PSIC_raw_cable_does_not_map_to_raw_input_of_this_step_bad(self):
        """
        transf_raw_input does not specify a TransformationRawInput of THIS pipeline step
        """
        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c_method",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        # Define second unrelated method not part of any pipeline but containing a raw input with the same name (a_b_c)
        self.script_4_2_M = Method(revision_name="s4",revision_desc="s4",family = self.test_MF,driver = self.script_4_1_CRR)
        self.script_4_2_M.save()
        self.script_4_2_M.create_input(dataset_name="a_b_c_method",dataset_idx=1)

        # Define pipeline with a single raw pipeline input and a single step
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1);

        # Cable the pipeline input into a raw input hole but from an irrelevent method
        rawcable1 = step1.cables_in.create(
            transf_input=self.script_4_2_M.inputs.get(dataset_name="a_b_c_method"),
            step_providing_input=0,
            provider_output=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        error_msg = "Transformation at step 1 does not have raw input \"\[Method test method family s4\]:raw1 a_b_c_method\"";
        self.assertRaisesRegexp(ValidationError,error_msg,rawcable1.clean)
        self.assertRaisesRegexp(ValidationError,error_msg,step1.clean)
        self.assertRaisesRegexp(ValidationError,error_msg,step1.complete_clean)
        self.assertRaisesRegexp(ValidationError,error_msg,self.pipeline_1.clean)
        self.assertRaisesRegexp(ValidationError,error_msg,self.pipeline_1.complete_clean)

        
    def test_PSIC_raw_cable_has_custom_wiring_defined(self):
        """
        Raw PSIC has custom wiring defined.
        """

        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)

        # Define 2 identical steps within the pipeline
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1);
        step2 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=2);

        # Cable the pipeline input into step1's transformation's only raw input hole
        rawcable1 = step1.create_raw_cable(
            transf_raw_input=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            pipeline_raw_input=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        rawcable2 = step2.create_raw_cable(
            transf_raw_input=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            pipeline_raw_input=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        # Define custom wiring (totally nonsensical) on rawcable1.
        rawcable1.custom_wires.create(
            source_pin=self.doublet_cdt.members.all()[0],
            dest_pin=self.doublet_cdt.members.all()[0])
        
        self.assertRaisesRegexp(
            ValidationError,
            "Cable \"Pipeline test pipeline family v1 step 1:a_b_c\(raw\)\" is raw and should not have custom wiring defined",
            rawcable1.clean)

class RawSave_tests(Copperfish_Raw_Setup):
    def test_method_with_raw_input_defined_do_not_copy_raw_xputs_to_new_revision(self):
        # Give script_4_1_M a raw input
        self.script_4_1_M.create_input(dataset_name="a_b_c", dataset_idx=1)

        # Make a method without a parent
        self.script_4_2_M = Method(revision_name="s4",revision_desc="s4",family = self.test_MF, driver = self.script_4_1_CRR)
        self.script_4_2_M.save()

        # There should be no raw inputs/outputs
        self.assertEqual(self.script_4_2_M.inputs.count(), 0)
        self.assertEqual(self.script_4_2_M.outputs.count(), 0)
        
    def test_method_with_raw_output_defined_do_not_copy_raw_xputs_to_new_revision(self):
        # Give script_4_1_M a raw output
        self.script_4_1_M.create_output(dataset_name="a_b_c", dataset_idx=1)

        # Make a method without a parent
        self.script_4_2_M = Method(revision_name="s4",revision_desc="s4",family = self.test_MF, driver = self.script_4_1_CRR)
        self.script_4_2_M.save()

        # There should be no raw inputs/outputs
        self.assertEqual(self.script_4_2_M.inputs.count(), 0)
        self.assertEqual(self.script_4_2_M.outputs.count(), 0)

    def test_method_with_no_xputs_defined_copy_raw_xputs_to_new_revision(self):

        # Give script_4_1_M a raw input
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        # Make a method with a parent, and do not specify inputs/outputs
        self.script_4_2_M = Method(revision_parent=self.script_4_1_M, revision_name="s4",revision_desc="s4",family = self.test_MF, driver = self.script_4_1_CRR)
        self.script_4_2_M.save()

        # The input should have been copied over (SUBOPTIMAL TEST)
        self.assertEqual(self.script_4_1_M.inputs.all()[0].dataset_name,
                         self.script_4_2_M.inputs.all()[0].dataset_name);
        self.assertEqual(self.script_4_1_M.inputs.all()[0].dataset_idx,
                         self.script_4_2_M.inputs.all()[0].dataset_idx);


# August 23, 2013: these are kind of redundant now but what the hey.
class SingleRawInput_tests(Copperfish_Raw_Setup):
    def test_transformation_rawinput_coexists_with_nonraw_inputs_clean_good(self):

        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.create_input(dataset_name = "a_b_c", dataset_idx = 1)

        # Define input "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_input(
            compounddatatype = self.triplet_cdt,
            dataset_name = "a_b_c_squared",
            dataset_idx = 2)
        self.script_4_1_M.save()

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None);
        self.assertEquals(self.script_4_1_M.check_output_indices(), None);
        self.assertEquals(self.script_4_1_M.clean(), None);

    def test_transformation_rawinput_coexists_with_nonraw_inputs_but_not_consecutive_indexed_bad(self):

        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        # Define input name "a_b_c_squared" of type "triplet_cdt" at nonconsecutive index = 3
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 3)
        self.script_4_1_M.save()

        # The indices are not consecutive
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_input_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean)
        
    def test_PipelineStep_completeClean_check_quenching_of_raw_inputs_good(self):
        # Wire 1 raw input to a pipeline step that expects only 1 input
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        raw_input_cable_1 = step1.create_raw_cable(transf_raw_input = method_raw_in,
                                                       pipeline_raw_input = pipeline_input)

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)

    def test_PipelineStep_completeClean_check_overquenching_doubled_source_of_raw_inputs_bad(self):

        # Wire 1 raw input to a pipeline step that expects only 1 input
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        raw_input_cable_1 = step1.create_raw_cable(transf_raw_input = method_raw_in,
                                                       pipeline_raw_input = pipeline_input)

        raw_input_cable_2 = step1.create_raw_cable(transf_raw_input = method_raw_in,
                                                       pipeline_raw_input = pipeline_input)

        errorMessage = "Input \"a_b_c\" to transformation at step 1 is cabled more than once"
        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.clean)

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.complete_clean)

    def test_PipelineStep_completeClean_check_overquenching_different_sources_of_raw_inputs_bad(self):

        # Wire 1 raw input to a pipeline step that expects only 1 input
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        pipeline_input_2 = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline_2",dataset_idx=2)

        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        raw_input_cable_1 = step1.create_raw_cable(transf_raw_input = method_raw_in,
                                                       pipeline_raw_input = pipeline_input)

        raw_input_cable_2 = step1.create_raw_cable(transf_raw_input = method_raw_in,
                                                       pipeline_raw_input = pipeline_input_2)

        errorMessage = "Input \"a_b_c\" to transformation at step 1 is cabled more than once"
        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.clean)

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.complete_clean)

        
    def test_PipelineStep_completeClean_check_underquenching_of_raw_inputs_bad(self):

        # Wire 1 raw input to a pipeline step that expects only 1 input
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        errorMessage = "Input \"a_b_c\" to transformation at step 1 is not cabled'"

        self.assertEquals(step1.clean(), None)

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.complete_clean)


class SeveralRawInputs_tests(Copperfish_Raw_Setup):
    def test_transformation_several_rawinputs_coexists_with_several_nonraw_inputs_clean_good(self):
        # Note that this method wouldn't actually run -- inputs don't match.

        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define raw input "RawIn3" at index = 3
        self.script_4_1_M.create_input(dataset_name = "RawIn3",dataset_idx = 3)

        # Define input "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 2)

        # Define input "Input4" of type "doublet_cdt" at index = 4
        self.script_4_1_M.create_input(compounddatatype = self.doublet_cdt,dataset_name = "Input4",dataset_idx = 4)

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None);
        self.assertEquals(self.script_4_1_M.check_output_indices(), None);
        self.assertEquals(self.script_4_1_M.clean(), None);

    def test_transformation_several_rawinputs_several_nonraw_inputs_not1based_bad(self):
        # Note that this method wouldn't actually run -- inputs don't match.

        # Define raw input "a_b_c" at index = 2
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 2)
        
        # Define raw input "RawIn3" at index = 3
        self.script_4_1_M.create_input(dataset_name = "RawIn3",dataset_idx = 3)

        # Define input "a_b_c_squared" of type "triplet_cdt" at index = 4
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 4)

        # Define input "Input4" of type "doublet_cdt" at index = 5
        self.script_4_1_M.create_input(compounddatatype = self.doublet_cdt,dataset_name = "Input4",dataset_idx = 5)

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_input_indices);
        self.assertEquals(self.script_4_1_M.check_output_indices(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean);

    def test_transformation_several_rawinputs_several_nonraw_inputs_nonconsecutive_bad(self):
        # Note that this method wouldn't actually run -- inputs don't match.

        # Define raw input "a_b_c" at index = 2
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 2)
        
        # Define raw input "RawIn3" at index = 3
        self.script_4_1_M.create_input(dataset_name = "RawIn3",dataset_idx = 3)

        # Define input "a_b_c_squared" of type "triplet_cdt" at index = 5
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 5)

        # Define input "Input4" of type "doublet_cdt" at index = 6
        self.script_4_1_M.create_input(compounddatatype = self.doublet_cdt,dataset_name = "Input6",dataset_idx = 6)

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_input_indices);
        self.assertEquals(self.script_4_1_M.check_output_indices(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean);

    def test_pipeline_several_rawinputs_coexists_with_several_nonraw_inputs_clean_good(self):

        # Define 1-step pipeline with conflicting inputs
        pipeline_1 = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_1.create_input(dataset_name="input_1_raw",dataset_idx=1)
        pipeline_1.create_input(compounddatatype=self.triplet_cdt,dataset_name="input_2",dataset_idx=2)
        pipeline_1.create_input(dataset_name="input_3_raw",dataset_idx=3)
        pipeline_1.create_input(compounddatatype=self.triplet_cdt,dataset_name="input_4",dataset_idx=4)

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(pipeline_1.check_input_indices(), None)
        self.assertEquals(pipeline_1.clean(), None)

    # We consider this enough for the multiple input case, as the
    # single case was thoroughly checked.

    def test_PipelineStep_completeClean_check_overquenching_different_sources_of_raw_inputs_bad(self):

        # Define 2 inputs for the method
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "method_in_1",dataset_idx = 1)
        method_raw_in_2 = self.script_4_1_M.create_input(dataset_name = "method_in_2",dataset_idx = 2)
        
        # Define 1-step pipeline with 2 raw pipeline inputs
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        pipeline_input_2 = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline_2",dataset_idx=2)

        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        raw_input_cable_1 = step1.create_raw_cable(transf_raw_input = method_raw_in,
                                                       pipeline_raw_input = pipeline_input)

        raw_input_cable_2 = step1.create_raw_cable(transf_raw_input = method_raw_in_2,
                                                       pipeline_raw_input = pipeline_input_2)

        raw_input_cable_over = step1.create_raw_cable(transf_raw_input = method_raw_in,
                                                          pipeline_raw_input = pipeline_input_2)

        errorMessage = "Input \"method_in_1\" to transformation at step 1 is cabled more than once"
        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.clean)

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.complete_clean)


# August 23, 2013: these also seem pretty redundant, but let's just leave 'em.
class SingleRawOutput_tests(Copperfish_Raw_Setup):
    def test_transformation_rawoutput_coexists_with_nonraw_outputs_clean_good(self):

        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)

        # Define output name "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 2)
        self.script_4_1_M.save()

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None);
        self.assertEquals(self.script_4_1_M.check_output_indices(), None);
        self.assertEquals(self.script_4_1_M.clean(), None);

    def test_transformation_rawoutput_coexists_with_nonraw_outputs_but_not_consecutive_indexed_bad(self):
        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)

        # Define output name "a_b_c" of type "triplet_cdt" at invalid index = 3
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 3)
        self.script_4_1_M.save()

        # The indices are invalid
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_output_indices) 
        
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean)



class SeveralRawOutputs_tests(Copperfish_Raw_Setup):

    def test_transformation_several_rawoutputs_coexists_with_several_nonraw_outputs_clean_good(self):
        # Note: the method we define here doesn't correspond to reality; the
        # script doesn't have all of these outputs.

        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define raw output "RawOutput4" at index = 4
        self.script_4_1_M.create_output(dataset_name = "RawOutput4",dataset_idx = 4)

        # Define output name "foo" of type "doublet_cdt" at index = 3
        self.script_4_1_M.create_output(compounddatatype = self.doublet_cdt,dataset_name = "Output3",dataset_idx = 3)
            
        # Define output name "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 2)

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None);
        self.assertEquals(self.script_4_1_M.check_output_indices(), None);
        self.assertEquals(self.script_4_1_M.clean(), None);
        
    def test_transformation_several_rawoutputs_with_several_nonraw_outputs_clean_indices_nonconsecutive_bad(self):
        # Note: the method we define here doesn't correspond to reality; the
        # script doesn't have all of these outputs.

        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define raw output "RawOutput4" at index = 2
        self.script_4_1_M.create_output(dataset_name = "RawOutput2",dataset_idx = 2)

        # Define output name "foo" of type "doublet_cdt" at index = 5
        self.script_4_1_M.create_output(compounddatatype = self.doublet_cdt,dataset_name = "Output5",dataset_idx = 5)
            
        # Define output name "a_b_c_squared" of type "triplet_cdt" at index = 10
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 10)

        # Neither the names nor the indices conflict, but numbering is bad.
        self.assertEquals(self.script_4_1_M.check_input_indices(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_output_indices);
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean);


class CustomWiring_tests(Copperfish_Raw_Setup):

    def test_CustomCableWire_wires_from_pipeline_input_identical_dt_good(self):
        """Custom wiring that connects identical datatypes together, on a cable leading from pipeline input (not PS output)."""

        # Define a pipeline with single pipeline input of type triplet_cdt
        my_pipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_in = my_pipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipe_in_1",dataset_idx=1)

        # Define method to have an input with the same CDT, add it as a step, cable it
        method_in = self.testmethod.create_input(dataset_name="TestIn", dataset_idx=1,compounddatatype=self.triplet_cdt)
        my_step1 = my_pipeline.steps.create(transformation=self.testmethod, step_num=1)
        my_cable1 = my_step1.cables_in.create(transf_input=method_in, step_providing_input=0, provider_output=pipeline_in)

        # Both CDTs exactly match
        self.assertEquals(my_cable1.clean(), None)
        self.assertEquals(my_cable1.clean_and_completely_wired(), None)

        # But we can add custom wires anyways
        wire1 = my_cable1.custom_wires.create(
            source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
            dest_pin=method_in.get_cdt().members.get(column_idx=1))
        
        # This wire is clean, and the cable is also clean - but not completely wired
        self.assertEquals(wire1.clean(), None)
        self.assertEquals(my_cable1.clean(), None)

        self.assertRaisesRegexp(
            ValidationError,
            "Destination member \"2: <string> \[b\^2\]\" has no wires leading to it",
            my_cable1.clean_and_completely_wired)

        # Here, we wire the remaining 2 CDT members
        for i in range(2,4):
            my_cable1.custom_wires.create(
                source_pin=pipeline_in.get_cdt().members.get(column_idx=i),
                dest_pin=method_in.get_cdt().members.get(column_idx=i))

        # All the wires are clean - and now the cable is completely wired
        for wire in my_cable1.custom_wires.all():
            self.assertEquals(wire.clean(), None)

        self.assertEquals(my_cable1.clean(), None);
        self.assertEquals(my_cable1.clean_and_completely_wired(), None);



    def test_CustomCableWire_clean_for_datatype_compatibility(self):
        # Wiring test 1 - Datatypes are identical (x -> x)
        # Wiring test 2 - Datatypes are not identical, but compaitible (y restricts x, y -> x)
        # Wiring test 3 - Datatypes are not compaitible (z does not restrict x, z -> x) 

        # Define 2 CDTs3 datatypes - one identical, one compaitible, and one incompaitible + make a new CDT composed of them
        # Regarding datatypes, recall [self.DNA_dt] restricts [self.string_dt]

        # Define a datatype that has nothing to do with anything
        with open(os.path.join(samplecode_path, "incompaitible_DT.py"), "rb") as f:
            self.incompaitible_dt = Datatype(
                name="Not compaitible",
                description="A datatype not having anything to do with anything",
                verification_script=File(f),
                Python_type="str")
            
            self.incompaitible_dt.save()

        # Define 2 CDTs that are unequal: (DNA, string, string), and (string, DNA, incompaitible)
        cdt_1 = CompoundDatatype()
        cdt_1.save()
        cdt_1.members.create(datatype=self.DNA_dt,column_name="col_1",column_idx=1)
        cdt_1.members.create(datatype=self.string_dt,column_name="col_2",column_idx=2)
        cdt_1.members.create(datatype=self.string_dt,column_name="col_3",column_idx=3)

        cdt_2 = CompoundDatatype()
        cdt_2.save()
        cdt_2.members.create(datatype=self.string_dt,column_name="col_1",column_idx=1)
        cdt_2.members.create(datatype=self.DNA_dt,column_name="col_2",column_idx=2)
        cdt_2.members.create(datatype=self.incompaitible_dt,column_name="col_3",column_idx=3)

        # Define a pipeline with single pipeline input of type cdt_1
        my_pipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_in = my_pipeline.create_input(compounddatatype=cdt_1,dataset_name="pipe_in_1",dataset_idx=1)

        # Define method to have an input with cdt_2, add it as a step, cable it
        method_in = self.testmethod.create_input(dataset_name="TestIn", dataset_idx=1,compounddatatype=cdt_2)
        my_step1 = my_pipeline.steps.create(transformation=self.testmethod, step_num=1)
        my_cable1 = my_step1.cables_in.create(transf_input=method_in, step_providing_input=0, provider_output=pipeline_in)

        # CDTs are not equal, so this cable requires custom wiring
        self.assertRaisesRegexp(
            ValidationError,
            "Custom wiring required for cable \"Pipeline test pipeline family foo step 1:TestIn\"",
            my_step1.clean);

        # Wiring case 1: Datatypes are identical (DNA -> DNA)
        wire1 = my_cable1.custom_wires.create(
            source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
            dest_pin=method_in.get_cdt().members.get(column_idx=2))

        # Wiring case 2: Datatypes are compaitible (DNA -> string)
        wire2 = my_cable1.custom_wires.create(
            source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
            dest_pin=method_in.get_cdt().members.get(column_idx=1))
        
        # Wiring case 3: Datatypes are compaitible (DNA -> incompaitible CDT)
        wire3_bad = my_cable1.custom_wires.create(
            source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
            dest_pin=method_in.get_cdt().members.get(column_idx=3))

        self.assertEquals(wire1.clean(), None)
        self.assertEquals(wire2.clean(), None)

        errorMessage = "The datatype of the source pin \"1: <DNANucSeq> \[col_1\]\" is incompatible with the datatype of the destination pin \"3: <Not compaitible> \[col_3\]\"'\]"
        
        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            wire3_bad.clean)

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            my_cable1.clean)

    def test_CustomCableWire_clean_source_and_dest_pin_do_not_come_from_cdt_bad(self):
        # For source_pin and dest_pin, give a CDTM from an unrelated CDT

        # Define a datatype that has nothing to do with anything
        with open(os.path.join(samplecode_path, "incompaitible_DT.py"), "rb") as f:
            self.incompatible_dt = Datatype(name="poop",description="poop!!",verification_script=File(f),Python_type="str")
            self.incompatible_dt.save()

        # Define 2 different CDTs: (DNA, string, string), and (string, DNA, incompaitible)
        cdt_1 = CompoundDatatype()
        cdt_1.save()
        cdt_1.members.create(datatype=self.DNA_dt,column_name="col_1",column_idx=1)
        cdt_1.members.create(datatype=self.string_dt,column_name="col_2",column_idx=2)
        cdt_1.members.create(datatype=self.string_dt,column_name="col_3",column_idx=3)

        cdt_2 = CompoundDatatype()
        cdt_2.save()
        cdt_2.members.create(datatype=self.string_dt,column_name="col_1",column_idx=1)
        cdt_2.members.create(datatype=self.DNA_dt,column_name="col_2",column_idx=2)
        cdt_2.members.create(datatype=self.incompatible_dt,column_name="col_3",column_idx=3)

        # Define 2 methods with different inputs
        method_1 = Method(revision_name="s4",revision_desc="s4",family = self.test_MF,driver = self.script_4_1_CRR)
        method_1.save()
        method_1_in = method_1.create_input(
            dataset_name="TestIn", dataset_idx=1, compounddatatype=cdt_1)
        
        method_2 = Method(revision_name="s4",revision_desc="s4",family = self.test_MF,driver = self.script_4_1_CRR)
        method_2.save()
        method_2_in = method_2.create_input(
            dataset_name="TestIn", dataset_idx=1, compounddatatype=cdt_2)

        # Define 2 pipelines
        pipeline_1 = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_1_in = pipeline_1.create_input(compounddatatype=cdt_1,dataset_name="pipe_in_1",dataset_idx=1)
        pipeline_1_step = pipeline_1.steps.create(transformation=method_1, step_num=1)
        pipeline_1_cable = pipeline_1_step.cables_in.create(transf_input=method_1_in, step_providing_input=0, provider_output=pipeline_1_in)

        pipeline_2 = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_2_in = pipeline_2.create_input(compounddatatype=cdt_2,dataset_name="pipe_in_1",dataset_idx=1)
        pipeline_2_step = pipeline_2.steps.create(transformation=method_2, step_num=1)
        pipeline_2_cable = pipeline_2_step.cables_in.create(transf_input=method_2_in, step_providing_input=0, provider_output=pipeline_2_in)

        # Within pipeline_1_cable, wire into method 1 idx 1 (Expects DNA) a dest_pin from pipeline 2 idx 3
        # (incompatible dt, cdtm from unrelated cdt)
        wire1 = pipeline_1_cable.custom_wires.create(
            source_pin=pipeline_2_in.get_cdt().members.get(column_idx=3),
            dest_pin=method_1_in.get_cdt().members.get(column_idx=1))

        errorMessage = "Source pin .* does not come from compounddatatype .*"
        
        self.assertRaisesRegexp(ValidationError,errorMessage,wire1.clean)

        wire1.delete()

        # Within pipeline_1_cable, wire into method 1 idx 1 (Expects DNA) a dest_pin from pipeline 2 idx 1
        # (same dt, cdtm from unrelated cdt)
        wire1_alt = pipeline_1_cable.custom_wires.create(
            source_pin=pipeline_2_in.get_cdt().members.get(column_idx=3),
            dest_pin=method_1_in.get_cdt().members.get(column_idx=1))

        self.assertRaisesRegexp(ValidationError,errorMessage,wire1_alt.clean)

        # Try to wire something into cable 2 with a source_pin from cable 1
        wire2 = pipeline_2_cable.custom_wires.create(
            source_pin=pipeline_1_in.get_cdt().members.get(column_idx=3),
            dest_pin=method_2_in.get_cdt().members.get(column_idx=1))
            
        self.assertRaisesRegexp(ValidationError,errorMessage,wire2.clean)


# August 23, 2013: This is pretty redundant now.
class PipelineOutputCable_raw_tests(Copperfish_Raw_Setup):
    
    def test_pipeline_check_for_colliding_outputs_clean_good(self):

        # Define 1-step pipeline with 2 raw pipeline inputs
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version")
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        script_4_1_M = self.script_4_1_M

        output_1 = script_4_1_M.create_output(
            compounddatatype=self.mix_triplet_cdt,
            dataset_name="scriptOutput1",
            dataset_idx=1)

        output_3 = script_4_1_M.create_output(
            compounddatatype=self.mix_triplet_cdt,
            dataset_name="scriptOutput3",
            dataset_idx=3)

        raw_output_2 = script_4_1_M.create_output(
            dataset_name="scriptOutput2",
            dataset_idx=2)

        raw_output_4 = script_4_1_M.create_output(
            dataset_name="scriptOutput4",
            dataset_idx=4)

        self.pipeline_1.create_raw_outcable(
            raw_output_name="pipeline_output_1",
            raw_output_idx=1,
            step_providing_raw_output=1,
            provider_raw_output=raw_output_2)

        self.pipeline_1.create_raw_outcable(
            raw_output_name="pipeline_output_3",
            raw_output_idx=3,
            step_providing_raw_output=1,
            provider_raw_output=raw_output_4)

        self.pipeline_1.create_outcable(
            output_name="pipeline_output_2",
            output_idx=2,
            step_providing_output=1,
            provider_output=output_3)

        self.assertEquals(self.pipeline_1.clean(), None)

class CustomRawOutputCabling_tests(Copperfish_Raw_Setup):

    def test_Pipeline_create_multiple_raw_outputs_with_raw_outmap(self):
        self.my_pipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");

        pipeline_in = self.my_pipeline.create_input(
            compounddatatype=self.triplet_cdt,
            dataset_name="pipeline_in_1",
            dataset_idx=1)

        # Give the method self.triplet_cdt output
        method_raw_out = self.testmethod.create_output(
            dataset_name="RawTestOut",
            dataset_idx=1)

        # Add a step
        my_step1 = self.my_pipeline.steps.create(
            transformation=self.testmethod,
            step_num=1)

        # Add raw outmap
        outmap = self.my_pipeline.create_raw_outcable(
            raw_output_name="raw_out",
            raw_output_idx=1,
            step_providing_raw_output=1,
            provider_raw_output=method_raw_out)

        self.assertEquals(self.my_pipeline.outputs.all().count(), 0)     
        self.my_pipeline.create_outputs()
        self.assertEquals(self.my_pipeline.outputs.all().count(), 1)

        raw_output = self.my_pipeline.outputs.all()[0]

        self.assertEquals(raw_output.dataset_name, "raw_out")
        self.assertEquals(raw_output.dataset_idx, 1)

        # Add another raw outmap
        outmap2 = self.my_pipeline.create_raw_outcable(
            raw_output_name="raw_out_2",
            raw_output_idx=2,
            step_providing_raw_output=1,
            provider_raw_output=method_raw_out)

        self.my_pipeline.create_outputs()
        self.assertEquals(self.my_pipeline.outputs.all().count(), 2)

        raw_output_2 = self.my_pipeline.outputs.all()[1]

        self.assertEquals(raw_output_2.dataset_name, "raw_out_2")
        self.assertEquals(raw_output_2.dataset_idx, 2)

        
class PipelineStepInputCable_tests(Copperfish_Raw_Setup):

    def test_PSIC_clean_and_completely_wired_CDT_equal_no_wiring_good(self):
        # Define pipeline with mix_triplet_cdt (string, DNA, string) pipeline input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        myPipeline_input = myPipeline.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="pipe_in",dataset_idx=1)

        # Define method with doublet_cdt input (string, string), add it to the pipeline, and cable it
        method_input = self.testmethod.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="method_in",dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        pipeline_cable = pipelineStep.cables_in.create(transf_input=method_input, step_providing_input=0, provider_output=myPipeline_input)

        self.assertEquals(pipeline_cable.clean(), None)
        self.assertEquals(pipeline_cable.clean_and_completely_wired(), None)
        self.assertEquals(pipelineStep.clean(), None)
        self.assertEquals(pipelineStep.complete_clean(), None)


    def test_PSIC_clean_and_completely_wired_CDT_not_equal_wires_exist_shuffled_wiring_good(self):
        # Wire from a triplet into a double:
        # A -> z
        # B -> NULL (Not necessary)
        # C -> x

        # Define pipeline with mix_triplet_cdt (string, DNA, string) pipeline input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        myPipeline_input = myPipeline.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="pipe_in",dataset_idx=1)

        # Define method with doublet_cdt input (string, string), add it to the pipeline, and cable it
        method_input = self.testmethod.create_input(compounddatatype=self.doublet_cdt,dataset_name="method_in",dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        pipeline_cable = pipelineStep.cables_in.create(transf_input=method_input, step_providing_input=0, provider_output=myPipeline_input)

            # wire1 = string->string
        wire1 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=3),
            dest_pin=method_input.get_cdt().members.get(column_idx=2))

        # The cable is clean but not complete
        errorMessage = "Destination member .* has no wires leading to it"
        self.assertEquals(pipeline_cable.clean(), None)
        self.assertRaisesRegexp(ValidationError,errorMessage,pipeline_cable.clean_and_completely_wired)

        # wire2 = DNA->string
        wire2 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=2),
            dest_pin=method_input.get_cdt().members.get(column_idx=1))

        self.assertEquals(wire1.clean(), None)
        self.assertEquals(wire2.clean(), None)
        self.assertEquals(pipeline_cable.clean(), None)
        self.assertEquals(pipeline_cable.clean_and_completely_wired(), None)
        self.assertEquals(pipelineStep.clean(), None)
        self.assertEquals(pipelineStep.complete_clean(), None)


    def test_PSIC_clean_and_completely_wired_CDT_not_equal_wires_exist_compaitible_wiring_good(self):
        # A -> x
        # A -> y

        # Define pipeline with mix_triplet_cdt (string, DNA, string) pipeline input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        myPipeline_input = myPipeline.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="pipe_in",dataset_idx=1)

        # Define method with doublet_cdt input (string, string), add it to the pipeline, and cable it
        method_input = self.testmethod.create_input(compounddatatype=self.doublet_cdt,dataset_name="method_in",dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        pipeline_cable = pipelineStep.cables_in.create(transf_input=method_input, step_providing_input=0, provider_output=myPipeline_input)

        # wire1 = string->string
        wire1 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=2),
            dest_pin=method_input.get_cdt().members.get(column_idx=2))

        # wire2 = DNA->string
        wire2 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=2),
            dest_pin=method_input.get_cdt().members.get(column_idx=1))

        self.assertEquals(wire1.clean(), None)
        self.assertEquals(wire2.clean(), None)
        self.assertEquals(pipeline_cable.clean(), None)
        self.assertEquals(pipelineStep.clean(), None)
        self.assertEquals(pipelineStep.complete_clean(), None)

    def test_PSIC_clean_and_completely_wired_not_quenched(self):
        # x -> x
        # NULL -> y
        # z -> z

        # Define pipeline with mix_triplet_cdt (string, DNA, string) pipeline input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        myPipeline_input = myPipeline.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="pipe_in",dataset_idx=1)

        # Define method with triplet_cdt input (string, string, string), add it to the pipeline, and cable it
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        pipeline_cable = pipelineStep.cables_in.create(transf_input=method_input, step_providing_input=0, provider_output=myPipeline_input)
        
        # wire1 = string->string
        wire1 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=1),
            dest_pin=method_input.get_cdt().members.get(column_idx=1))

        wire3 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=3),
            dest_pin=method_input.get_cdt().members.get(column_idx=3))

        self.assertEquals(wire1.clean(), None)
        self.assertEquals(wire3.clean(), None)
        self.assertEquals(pipeline_cable.clean(), None)

        # FIXME: Should pipelineStep.clean invoke pipeline_cable.clean_and_completely_quenched() ?
        errorMessage = "Destination member \"2.*\" has no wires leading to it"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipeline_cable.clean_and_completely_wired)
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelineStep.clean)
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelineStep.complete_clean)
        self.assertRaisesRegexp(ValidationError,errorMessage,myPipeline.complete_clean)

class Dataset_new_tests(Copperfish_Raw_Setup):

    def test_dataset_clean_incorrect_number_of_CSV_header_fields_bad(self):

        uploaded_sd = SymbolicDataset()
        uploaded_sd.save()
        uploaded_dataset = None
        with open(os.path.join(samplecode_path, "script_2_output_2.csv"), "rb") as f:
            uploaded_dataset = Dataset(
                user=self.myUser,name="uploaded_dataset",
                description="hehe",dataset_file=File(f),
                symbolicdataset=uploaded_sd)
            uploaded_dataset.save()
        new_structure = DatasetStructure(dataset=uploaded_dataset,
                                         compounddatatype=self.triplet_cdt)
        new_structure.save()

        errorMessage = "Dataset .* does not have the same number of columns as its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

    def test_dataset_clean_correct_number_of_CSV_header_fields_but_incorrect_contents_bad(self):

        uploaded_sd = SymbolicDataset()
        uploaded_sd.save()
        uploaded_dataset = None
        with open(os.path.join(samplecode_path, "three_random_columns.csv"), "rb") as f:
            uploaded_dataset = Dataset(
                user=self.myUser,name="uploaded_raw_dataset",
                description="hehe",dataset_file=File(f),
                symbolicdataset=uploaded_sd)
            uploaded_dataset.save()
        new_structure = DatasetStructure(dataset=uploaded_dataset,
                                         compounddatatype=self.triplet_cdt)
        new_structure.save()

        errorMessage = "Column .* of Dataset .* is named .*, not .* as specified by its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)



# August 29, 2013: reworked to handle new design for outcables.
class CustomOutputWiring_tests(Copperfish_Raw_Setup):

    def test_CustomOutputCableWire_clean_references_invalid_CDTM(self):

        self.my_pipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");

        pipeline_in = self.my_pipeline.create_input(
            compounddatatype=self.triplet_cdt,
            dataset_name="pipeline_in_1",
            dataset_idx=1)

        # Give the method self.triplet_cdt output
        method_out = self.testmethod.create_output(
            dataset_name="TestOut",
            dataset_idx=1,
            compounddatatype=self.triplet_cdt);

        # Add a step
        my_step1 = self.my_pipeline.steps.create(transformation=self.testmethod, step_num=1);

        # Add an output cable
        outcable1 = self.my_pipeline.create_outcable(
            output_name="blah",
            output_idx=1,
            step_providing_output=1,
            provider_output=method_out)

        # Add custom wiring from an irrelevent CDTM
        badwire = outcable1.custom_outwires.create(
            source_pin=self.doublet_cdt.members.all()[0],
            dest_pin=self.triplet_cdt.members.all()[0])

        errorMessage = "Source pin \"1: <string> \[StrCol1\]\" does not come from compounddatatype \"\(1: <string> \[a\^2\], 2: <string> \[b\^2\], 3: <string> \[c\^2\]\)\""

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            badwire.clean)

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            outcable1.clean)

        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            self.my_pipeline.clean)

        

    def test_Pipeline_create_outputs_for_creation_of_output_CDT(self):
        self.my_pipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");

        pipeline_in = self.my_pipeline.create_input(
            compounddatatype=self.triplet_cdt,
            dataset_name="pipeline_in_1",
            dataset_idx=1)

        # Give the method self.triplet_cdt output
        method_out = self.testmethod.create_output(
            dataset_name="TestOut",
            dataset_idx=1,
            compounddatatype=self.mix_triplet_cdt);

        # Add a step
        my_step1 = self.my_pipeline.steps.create(
            transformation=self.testmethod, step_num=1);

        # Add an output cable with the following output CDT:
        # column 1: "col1_str", type string_dt (from 1st col of triplet)
        # column 2: "col2_DNA", type DNA_dt (from 2nd col of triplet)
        # column 3: "col3_str", type string_dt (from 1st col of triplet)
        # column 4: "col4_str", type string_dt (from 3rd col of triplet)
        new_cdt = CompoundDatatype()
        new_cdt.save()
        pin1 = new_cdt.members.create(column_name="col1_str", column_idx=1,
                                      datatype=self.string_dt)
        pin2 = new_cdt.members.create(column_name="col2_DNA", column_idx=2,
                                      datatype=self.DNA_dt)
        pin3 = new_cdt.members.create(column_name="col3_str", column_idx=3,
                                      datatype=self.string_dt)
        pin4 = new_cdt.members.create(column_name="col4_str", column_idx=4,
                                      datatype=self.string_dt)
        
        outcable1 = self.my_pipeline.outcables.create(
            output_name="blah",
            output_idx=1,
            step_providing_output=1,
            provider_output=method_out,
            output_cdt=new_cdt)
        
        # Add wiring
        wire1 = outcable1.custom_outwires.create(
            source_pin=method_out.get_cdt().members.all()[0],
            dest_pin=pin1)

        wire2 = outcable1.custom_outwires.create(
            source_pin=method_out.get_cdt().members.all()[1],
            dest_pin=pin2)

        wire3 = outcable1.custom_outwires.create(
            source_pin=method_out.get_cdt().members.all()[0],
            dest_pin=pin3)

        wire4 = outcable1.custom_outwires.create(
            source_pin=method_out.get_cdt().members.all()[2],
            dest_pin=pin4)

        self.assertEquals(self.my_pipeline.outputs.all().count(), 0)
        self.my_pipeline.create_outputs()
        self.assertEquals(self.my_pipeline.outputs.all().count(), 1)
        
        pipeline_out_members = self.my_pipeline.outputs.all()[0].get_cdt().members.all()
        
        self.assertEquals(pipeline_out_members.count(),4)

        member = pipeline_out_members.get(column_idx=1)
        self.assertEquals(member.column_name, "col{}_str".format(1))
        self.assertEquals(member.datatype, self.string_dt)

        member = pipeline_out_members.get(column_idx=2)
        self.assertEquals(member.column_name, "col{}_DNA".format(2))
        self.assertEquals(member.datatype, self.DNA_dt)

        member = pipeline_out_members.get(column_idx=3)
        self.assertEquals(member.column_name, "col{}_str".format(3))
        self.assertEquals(member.datatype, self.string_dt)

        member = pipeline_out_members.get(column_idx=4)
        self.assertEquals(member.column_name, "col{}_str".format(4))
        self.assertEquals(member.datatype, self.string_dt)
