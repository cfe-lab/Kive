"""
Unit tests for the Copperfish.
"""

from django.test import TestCase;
from copperfish.models import *;
from django.core.files import File;
from django.core.exceptions import ValidationError;

import glob, os.path;
samplecode_path = "../samplecode";
import hashlib;


class DatatypeMethodTests(TestCase):
    def test_unicode(self):
        """Test that the unicode representation is the instance's name."""
        my_datatype = Datatype(name="fhqwhgads");
        self.assertEqual(unicode(my_datatype), "fhqwhgads");

class CopperfishMethodTests(TestCase):
    def setUp(self):
        """Define some variables to use in testing Copperfish."""
        ####
        # First, some Datatype and CompoundDatatype stuff
        with open(os.path.join(samplecode_path, "stringUT.py"), "rb") as f:
            string_dt = Datatype(name="string",
                                 description="String (basically anything)",
                                 verification_script=File(f), Python_type="str");
            string_dt.full_clean();
            string_dt.save();

        with open(os.path.join(samplecode_path, "DNANucSeqUT.py"), "rb") as f:
            DNA_dt = Datatype(name="DNANucSeq",
                              description="String consisting of ACGTacgt",
                              verification_script=File(f), Python_type="str");
            DNA_dt.save();
            # This restricts String.  This step has to be after it's saved
            DNA_dt.restricts.add(string_dt);
            DNA_dt.full_clean();
            DNA_dt.save();

        with open(os.path.join(samplecode_path, "RNANucSeqUT.py"), "rb") as f:
            RNA_dt = Datatype(name="RNANucSeq",
                              description="String consisting of ACGUacgu",
                              verification_script=File(f), Python_type="str");

            RNA_dt.save();
            RNA_dt.restricts.add(string_dt);
            RNA_dt.full_clean();
            RNA_dt.save();

        self.string_dt = string_dt;
        self.DNA_dt = DNA_dt;
        self.RNA_dt = RNA_dt;
        
        self.test_cd = CompoundDatatype();
        self.test_cd.save();
        self.test_cd.members.create(datatype=self.string_dt,
                                    column_name="label",
                                    column_idx=1);
        self.test_cd.members.create(datatype=self.DNA_dt,
                                    column_name="PBMCseq",
                                    column_idx=2);
        self.test_cd.members.create(datatype=self.RNA_dt,
                                    column_name="PLAseq",
                                    column_idx=3);
        self.test_cd.full_clean();
        self.test_cd.save();


        # Input compound datatype for complementation method
        self.DNAinput_cd = CompoundDatatype();
        self.DNAinput_cd.save();
        self.DNAinput_cd.members.create(datatype=self.DNA_dt,
                                        column_name="SeqToComplement",
                                        column_idx=1);
        self.DNAinput_cd.full_clean();
        self.DNAinput_cd.save();

        # Output compound datatype for complementation method
        self.DNAoutput_cd = CompoundDatatype();
        self.DNAoutput_cd.save();
        self.DNAoutput_cd.members.create(datatype=self.DNA_dt,
                                         column_name="ComplementedSeq",
                                         column_idx=1);
        self.DNAoutput_cd.full_clean();
        self.DNAoutput_cd.save();


        # Same but for RNA
        self.RNAinput_cd = CompoundDatatype();
        self.RNAinput_cd.save();
        self.RNAinput_cd.members.create(datatype=self.RNA_dt,
                                        column_name="SeqToComplement",
                                        column_idx=1);
        self.RNAinput_cd.full_clean();
        self.RNAinput_cd.save();

        self.RNAoutput_cd = CompoundDatatype();
        self.RNAoutput_cd.save();
        self.RNAoutput_cd.members.create(datatype=self.RNA_dt,
                                         column_name="ComplementedSeq",
                                         column_idx=1);
        self.RNAoutput_cd.full_clean();
        self.RNAoutput_cd.save();

        ####
        # CodeResource testing stuff
        self.comp_cr = CodeResource(
                name="complement.py",
                description="Script to complement DNA/RNA nucleotide sequences");
        self.comp_cr.save();

        # Add version 1 and version 2
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            self.compv1_rev = CodeResourceRevision(
                    coderesource=self.comp_cr,
                    revision_name="v1",
                    revision_desc="First version",
                    content_file=File(f));
            self.compv1_rev.full_clean();
            self.compv1_rev.save();
            #self.comp_cr.revisions.add(self.compv1_rev);

        with open(os.path.join(samplecode_path, "complement_v2.py"), "rb") as f:
            self.compv2_rev = CodeResourceRevision(
                coderesource=self.comp_cr,
                revision_name="v2",
                revision_desc="Second version: better docstring",
                revision_parent=self.compv1_rev,
                content_file=File(f));
            self.compv2_rev.full_clean();
            self.compv2_rev.save();
            #self.comp_cr.revisions.add(self.compv2_rev);


        ####
        # Now add some methods
        self.DNAcomp_mf = MethodFamily(
                name="DNAcomplement",
                description="Complement DNA nucleotide sequences.");
        self.DNAcomp_mf.full_clean();
        self.DNAcomp_mf.save();

        self.DNAcompv1_m = self.DNAcomp_mf.members.create(
                revision_name="v1",
                revision_desc="First version",
                driver=self.compv1_rev);

        self.DNAinput_ti = self.DNAcompv1_m.inputs.create(
                compounddatatype = self.DNAinput_cd,
                dataset_name = "input",
                dataset_idx = 1);
        self.DNAinput_ti.full_clean();
        self.DNAinput_ti.save();
        self.DNAoutput_to = self.DNAcompv1_m.outputs.create(
                compounddatatype = self.DNAoutput_cd,
                dataset_name = "output",
                dataset_idx = 1);
        self.DNAoutput_to.full_clean();
        self.DNAoutput_to.save();

        self.DNAcompv2_m = self.DNAcomp_mf.members.create(
                revision_name="v2",
                revision_desc="Second version",
                revision_parent=self.DNAcompv1_m,
                driver=self.compv2_rev);
        self.DNAcompv2_m.full_clean();
        self.DNAcompv2_m.save();


        # Same but for RNA
        self.RNAcomp_mf = MethodFamily(
                name="RNAcomplement",
                description="Complement RNA nucleotide sequences.");
        self.RNAcomp_mf.full_clean();
        self.RNAcomp_mf.save();

        self.RNAcompv1_m = self.RNAcomp_mf.members.create(
                revision_name="v1",
                revision_desc="First version",
                driver=self.compv1_rev);
        
        self.RNAinput_ti = self.RNAcompv1_m.inputs.create(
                compounddatatype = self.RNAinput_cd,
                dataset_name = "input",
                dataset_idx = 1);
        self.RNAinput_ti.full_clean();
        self.RNAinput_ti.save();
        self.RNAoutput_to = self.RNAcompv1_m.outputs.create(
                compounddatatype = self.RNAoutput_cd,
                dataset_name = "output",
                dataset_idx = 1);
        self.RNAoutput_to.full_clean();
        self.RNAoutput_to.save();

        self.RNAcompv2_m = self.RNAcomp_mf.members.create(
                revision_name="v2",
                revision_desc="Second version",
                revision_parent=self.RNAcompv1_m,
                driver=self.compv2_rev);
        self.RNAcompv2_m.full_clean();
        self.RNAcompv2_m.save();


        ####
        # Pipeline stuff
        self.DNAcomp_pf = PipelineFamily(
                name="DNAcomplement",
                description="DNA complement pipeline.");
        self.DNAcomp_pf.save();

        self.DNAcompv1_p = self.DNAcomp_pf.members.create(
                revision_name="v1", revision_desc="First version");
        # This is already saved; create an input.
        self.DNAcompv1_p.inputs.create(
                compounddatatype=self.DNAinput_cd,
                dataset_name="seqs_to_complement",
                dataset_idx=1);

        # Create a step
        step1 = self.DNAcompv1_p.steps.create(
                transformation=self.DNAcompv2_m,
                step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="seqs_to_complement");

        # Create an output mapping
        mapping = self.DNAcompv1_p.outmap.create(
                output_name="complemented_seqs",
                output_idx=1,
                step_providing_output=1,
                provider_output_name="output");

        self.DNAcompv1_p.save();


        # Method that can be used to chain with DNAcomplement
        self.DNArecomp_mf = MethodFamily(
                name="DNArecomplement",
                description="Re-complement DNA nucleotide sequences.");
        self.DNArecomp_mf.full_clean();
        self.DNArecomp_mf.save();

        self.DNArecomp_m = self.DNArecomp_mf.members.create(
                revision_name="v1",
                revision_desc="First version",
                driver=self.compv2_rev);

        self.DNArecomp_m.inputs.create(
                compounddatatype = self.DNAoutput_cd,
                dataset_name = "complemented_seqs",
                dataset_idx = 1);
        
        self.DNArecomp_m.outputs.create(
                compounddatatype = self.DNAinput_cd,
                dataset_name = "recomplemented_seqs",
                dataset_idx = 1);
        

    ####
    # CompoundDatatype tests       
    def test_compounddatatype_member_unicode(self):
        """Test unicode method of CompoundDatatypeMember."""
        self.assertEqual(unicode(self.test_cd.members.all()[0]),
                         "1: <string> [label]");
        self.assertEqual(unicode(self.test_cd.members.all()[1]),
                         "2: <DNANucSeq> [PBMCseq]");
        self.assertEqual(unicode(self.test_cd.members.all()[2]),
                         "3: <RNANucSeq> [PLAseq]");
        
        self.assertEqual(unicode(self.DNAinput_cd.members.all()[0]),
                         "1: <DNANucSeq> [SeqToComplement]");
        self.assertEqual(unicode(self.DNAoutput_cd.members.all()[0]),
                         "1: <DNANucSeq> [ComplementedSeq]");
        
        self.assertEqual(unicode(self.RNAinput_cd.members.all()[0]),
                         "1: <RNANucSeq> [SeqToComplement]");
        self.assertEqual(unicode(self.RNAoutput_cd.members.all()[0]),
                         "1: <RNANucSeq> [ComplementedSeq]");

    def test_compounddatatype_unicode_single_member(self):
        """Test unicode representation, single-member case."""
        self.assertEqual(unicode(self.DNAinput_cd),
                         "(1: <DNANucSeq> [SeqToComplement])");
        self.assertEqual(unicode(self.DNAoutput_cd),
                         "(1: <DNANucSeq> [ComplementedSeq])");
        
        self.assertEqual(unicode(self.RNAinput_cd),
                         "(1: <RNANucSeq> [SeqToComplement])");
        self.assertEqual(unicode(self.RNAoutput_cd),
                         "(1: <RNANucSeq> [ComplementedSeq])");
    
    def test_compounddatatype_unicode_multi_member(self):
        """Test unicode representation is correct, multiple-member case."""
        self.assertEqual(unicode(self.test_cd),
                         "(1: <string> [label], 2: <DNANucSeq> [PBMCseq], " +
                         "3: <RNANucSeq> [PLAseq])");

    def test_compounddatatype_clean_single_member(self):
        """Test that column numbering check works, single-member case."""
        self.assertEqual(self.DNAinput_cd.clean(), None);
        self.assertEqual(self.DNAoutput_cd.clean(), None);
        self.assertEqual(self.RNAinput_cd.clean(), None);
        self.assertEqual(self.RNAoutput_cd.clean(), None);

        bad_cd = CompoundDatatype();
        bad_cd.save();
        bad_cd.members.create(datatype=self.RNA_dt,
                              column_name="ColumnOne",
                              column_idx=3);
        with self.assertRaisesRegexp(
                ValidationError, "Column indices are not consecutive starting from 1"):
            bad_cd.clean();

    def test_compounddatatype_clean_multi_member(self):
        """Test that column numbering check works, multiple-member case."""
        self.assertEqual(self.test_cd.clean(), None);
        
        good_cd = CompoundDatatype();
        good_cd.save();
        good_cd.members.create(datatype=self.RNA_dt,
                               column_name="ColumnTwp",
                               column_idx=2);
        good_cd.members.create(datatype=self.DNA_dt,
                               column_name="ColumnOne",
                               column_idx=1);
        self.assertEqual(good_cd.clean(), None);

        bad_cd = CompoundDatatype();
        bad_cd.save();
        bad_cd.members.create(datatype=self.RNA_dt,
                              column_name="ColumnOne",
                              column_idx=3);
        bad_cd.members.create(datatype=self.DNA_dt,
                              column_name="ColumnTwo",
                              column_idx=1);

        with self.assertRaisesRegexp(
                ValidationError, "Column indices are not consecutive starting from 1"):
            bad_cd.clean();
            

    # CodeResource testing
    def test_cr_unicode(self):
        """Test unicode representation."""
        self.assertEquals(unicode(self.comp_cr), "complement.py");

    def test_crr_unicode(self):
        """Test unicode representation of CodeResourceRevision."""
        self.assertEquals(unicode(self.compv1_rev), "complement.py v1");
        self.assertEquals(unicode(self.compv2_rev), "complement.py v2");

        no_cr_set = CodeResourceRevision();
        self.assertEquals(unicode(no_cr_set), "[no code resource set] ");
        no_cr_set.revision_name = "foo";
        self.assertEquals(unicode(no_cr_set), "[no code resource set] foo");

    def test_crr_clean_nofile(self):
        """No file specified; MD5 should be empty string."""
        no_file_crr = CodeResourceRevision(
                coderesource=self.comp_cr,
                revision_name="foo",
                revision_desc="foo");
        no_file_crr.clean();
        self.assertEquals(no_file_crr.MD5_checksum, "");

    def test_crr_clean_withfile(self):
        """File specified; check MD5 is as it should be."""
        md5gen = hashlib.md5();
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            md5gen.update(f.read());
        self.assertEquals(
                md5gen.hexdigest(),
                self.comp_cr.revisions.get(revision_name="v1").MD5_checksum);

        v2_md5 = hashlib.md5();
        with open(os.path.join(samplecode_path, "complement_v2.py"), "rb") as f:
            v2_md5.update(f.read());
        self.assertEquals(
                v2_md5.hexdigest(),
                self.comp_cr.revisions.get(revision_name="v2").MD5_checksum);

    def test_crd_unicode(self):
        """Test Unicode representation of CodeResourceDependency."""
        v1 = self.comp_cr.revisions.get(revision_name="v1");
        v2 = self.comp_cr.revisions.get(revision_name="v2");
        test_crd = CodeResourceDependency(coderesourcerevision=v1,
                                          requirement=v2,
                                          where="subdir/foo.py");
        self.assertEquals(
                unicode(test_crd),
                "complement.py v1 requires complement.py v2 as subdir/foo.py");


    ####
    # Method stuff
    def test_methodfamily_unicode(self):
        """Test unicode representation of MethodFamily."""
        self.assertEqual(unicode(self.DNAcomp_mf),
                         "DNAcomplement");
        self.assertEqual(unicode(self.RNAcomp_mf),
                         "RNAcomplement");

    def test_method_unicode_nofamily(self):
        """Test unicode representation when family is unset."""
        nofamily = Method(revision_name="foo");

        self.assertEqual(unicode(nofamily),
                         "Method [family unset] foo");


    def test_method_unicode_withfamily(self):
        """Test unicode representation when family is set."""
        self.assertEqual(unicode(self.DNAcompv1_m),
                         "Method DNAcomplement v1");
        self.assertEqual(unicode(self.DNAcompv2_m),
                         "Method DNAcomplement v2");
        self.assertEqual(unicode(self.RNAcompv1_m),
                         "Method RNAcomplement v1");
        self.assertEqual(unicode(self.RNAcompv2_m),
                         "Method RNAcomplement v2");

    def test_method_check_input_indices_oneinput_good(self):
        """Test input index check, one well-indexed input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        self.assertEquals(foo.check_input_indices(), None);

    def test_method_check_input_indices_oneinput_bad(self):
        """Test input index check, one badly-indexed input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.check_input_indices);

    def test_method_check_input_indices_multiinput_good(self):
        """Test input index check, well-indexed multi-input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=3);
        self.assertEquals(foo.check_input_indices(), None);

    def test_method_check_input_indices_multiinput_good_scramble(self):
        """Test input index check, well-indexed multi-input (scrambled order) case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=3);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=1);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=2);
        self.assertEquals(foo.check_input_indices(), None);

    def test_method_check_input_indices_multiinput_bad(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=6);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.check_input_indices);

    def test_method_check_output_indices_oneoutput_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=1);
        self.assertEquals(foo.check_output_indices(), None);

    def test_method_check_output_indices_oneoutput_bad(self):
        """Test output index check, one badly-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.check_output_indices);

    def test_method_check_output_indices_multioutput_good(self):
        """Test output index check, well-indexed multi-output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=1);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="twooutput", dataset_idx=2);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="threeoutput", dataset_idx=3);
        self.assertEquals(foo.check_output_indices(), None);

    def test_method_check_output_indices_multioutput_good_scramble(self):
        """Test output index check, well-indexed multi-output (scrambled order) case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=3);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="twooutput", dataset_idx=1);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="threeoutput", dataset_idx=2);
        self.assertEquals(foo.check_output_indices(), None);

    def test_method_check_output_indices_multioutput_bad(self):
        """Test output index check, badly-indexed multi-output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=2);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="twooutput", dataset_idx=6);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="threeoutput", dataset_idx=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.check_output_indices);

    def test_method_clean_oneinput_good(self):
        """Test input index check, one well-indexed input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        self.assertEquals(foo.clean(), None);

    def test_method_clean_oneinput_bad(self):
        """Test input index check, one badly-indexed input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_method_clean_multiinput_good(self):
        """Test input index check, well-indexed multi-input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=3);
        self.assertEquals(foo.clean(), None);

    def test_method_clean_multiinput_good_scramble(self):
        """Test input index check, well-indexed multi-input (scrambled order) case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=3);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=1);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=2);
        self.assertEquals(foo.clean(), None);

    def test_method_clean_multiinput_bad(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=6);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_method_clean_oneoutput_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=1);
        self.assertEquals(foo.clean(), None);

    def test_method_clean_oneoutput_bad(self):
        """Test output index check, one badly-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_method_clean_multioutput_good(self):
        """Test output index check, well-indexed multi-output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=1);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="twooutput", dataset_idx=2);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="threeoutput", dataset_idx=3);
        self.assertEquals(foo.clean(), None);

    def test_method_clean_multioutput_good_scramble(self):
        """Test output index check, well-indexed multi-output (scrambled order) case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=3);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="twooutput", dataset_idx=1);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="threeoutput", dataset_idx=2);
        self.assertEquals(foo.clean(), None);

    def test_method_clean_multioutput_bad(self):
        """Test output index check, badly-indexed multi-output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="oneoutput", dataset_idx=2);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="twooutput", dataset_idx=6);
        foo.outputs.create(compounddatatype=self.DNAoutput_cd,
                           dataset_name="threeoutput", dataset_idx=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);
        
    def test_method_save_noparent(self):
        """Test save when no revision parent is specified."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_rev);
        foo.save();
        self.assertEqual(foo.inputs.count(), 0);
        self.assertEqual(foo.outputs.count(), 0);

        # If there are inputs and outputs defined, they should still
        # be there.
        self.DNAcompv1_m.save();
        self.assertEqual(self.DNAcompv1_m.inputs.count(), 1);
        self.assertEqual(self.DNAcompv1_m.inputs.all()[0],
                         self.DNAinput_ti);
        self.assertEqual(self.DNAcompv1_m.outputs.count(), 1);
        self.assertEqual(self.DNAcompv1_m.outputs.all()[0],
                         self.DNAoutput_to);

        self.RNAcompv1_m.save();
        self.assertEqual(self.RNAcompv1_m.inputs.count(), 1);
        self.assertEqual(self.RNAcompv1_m.inputs.all()[0],
                         self.RNAinput_ti);
        self.assertEqual(self.RNAcompv1_m.outputs.count(), 1);
        self.assertEqual(self.RNAcompv1_m.outputs.all()[0],
                         self.RNAoutput_to);

        
    def test_method_save_withparent(self):
        """Test save when revision parent is specified."""
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

        old_cd = self.DNAinput_ti.compounddatatype;
        old_name = self.DNAinput_ti.dataset_name;
        old_idx = self.DNAinput_ti.dataset_idx;
        
        self.DNAcompv1_m.revision_parent = self.RNAcompv2_m;
        self.DNAcompv1_m.save();
        self.assertEqual(self.DNAcompv1_m.inputs.count(), 1);
        curr_in = self.DNAcompv1_m.inputs.all()[0];
        self.assertEqual(curr_in.compounddatatype, old_cd);
        self.assertEqual(curr_in.dataset_name, old_name);
        self.assertEqual(curr_in.dataset_idx, old_idx);
         
        old_cd = self.DNAoutput_to.compounddatatype;
        old_name = self.DNAoutput_to.dataset_name;
        old_idx = self.DNAoutput_to.dataset_idx;
        
        self.assertEqual(self.DNAcompv2_m.outputs.count(), 1);
        curr_out = self.DNAcompv2_m.outputs.all()[0];
        self.assertEqual(curr_out.compounddatatype, old_cd);
        self.assertEqual(curr_out.dataset_name, old_name);
        self.assertEqual(curr_out.dataset_idx, old_idx);
        
    ####
    # Pipeline tests
    def test_pipeline_step_unicode_nopipeline(self):
        """Test unicode representation when no pipeline is set."""
        nopipeline = PipelineStep(step_num=2);
        self.assertEquals(unicode(nopipeline),
                          "[no pipeline assigned] step 2");

    def test_pipeline_step_unicode_withpipeline(self):
        """Test unicode representation when pipeline is set."""
        pipelineset = self.DNAcompv1_p.steps.get(step_num=1);
        self.assertEquals(unicode(pipelineset),
                          "Pipeline DNAcomplement v1 step 1");

    def test_pipeline_clean_oneinput_good(self):
        """Test input index check, one well-indexed input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        self.assertEquals(foo.clean(), None);


    def test_pipeline_clean_oneinput_bad(self):
        """Test input index check, one badly-indexed input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_clean_multiinput_good(self):
        """Test input index check, well-indexed multi-input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=3);
        self.assertEquals(foo.clean(), None);

    def test_pipeline_clean_multiinput_good_scramble(self):
        """Test input index check, well-indexed multi-input (scrambled order) case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=3);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=1);
        self.assertEquals(foo.clean(), None);


    def test_pipeline_clean_multiinput_bad(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=2);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="twoinput", dataset_idx=3);
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="threeinput", dataset_idx=4);
        self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_clean_onestep_good(self):
        """Test step index check, one well-indexed step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        
        self.assertEquals(foo.clean(), None);

    def test_pipeline_clean_onestep_bad(self):
        """Test step index check, one badly-indexed step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=10);
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean);

    def test_pipeline_clean_multistep_good(self):
        """Test step index check, well-indexed multi-step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        
        self.assertEquals(foo.clean(), None);

    def test_pipeline_clean_multistep_good_scramble(self):
        """Test step index check, well-indexed multi-step (scrambled order) case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        
        self.assertEquals(foo.clean(), None);

    def test_pipeline_clean_multistep_bad(self):
        """Test step index check, badly-indexed multi-step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=4);
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=5);
        
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean);


    ####
    # Wiring stuff, one-step pipeline
 
    def test_pipelinestep_clean_onestep_wiring_good(self):
        """Test good step wiring, one-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertEquals(step1.clean(), None);
        
    def test_pipelinestep_clean_onestep_request_later_data(self):
        """Bad wiring: step requests data after its step number."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="oneinput");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Input \"oneinput\" to step 1 does not come from a prior step",
                step1.clean);
        
    def test_pipelinestep_clean_onestep_feed_nonexistent_input(self):
        """Bad wiring: step feeds data to a nonexistent input."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="thisisnonexistent",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 has no input named \"thisisnonexistent\"",
                step1.clean);

    def test_pipelinestep_clean_onestep_wiring_good_with_delete(self):
        """Test good step wiring with deleted dataset, one-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step1.outputs_to_delete.create(dataset_to_delete="output");
        self.assertEquals(step1.clean(), None);

    def test_pipelinestep_clean_onestep_wiring_bad_delete(self):
        """Bad wiring: deleting nonexistent dataset, one-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step1.outputs_to_delete.create(dataset_to_delete="nonexistent");
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 has no output named \"nonexistent\"",
                step1.clean);
         
    def test_pipelinestep_clean_onestep_wiring_contains_parent_pipeline_direct(self):
        """Bad wiring: pipeline step contains the parent pipeline directly."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=foo, step_num=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                step1.clean);
         
    def test_pipelinestep_clean_onestep_wiring_contains_parent_pipeline_recursive_lone_step(self):
        """Bad wiring: pipeline step contains the parent pipeline in its lone recursive sub-step."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="output");
        foo.save();

        bar = Pipeline(family=self.DNAcomp_pf, revision_name="bar",
                       revision_desc="Bar version");
        bar.save();
        bar.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="barinput", dataset_idx=1);
        bstep1 = bar.steps.create(transformation=foo, step_num=1);
        bstep1.inputs.create(transf_input_name="oneinput",
                             step_providing_input=0,
                             provider_output_name="barinput");
        bar.outmap.create(output_name="baroutput", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="oneoutput");
        bar.save();

        # Now feed bar back into foo as a step.
        step1.delete();
        foo.outputs.all().delete();
        badstep = foo.steps.create(transformation=bar, step_num=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                badstep.clean);
         
    def test_pipelinestep_clean_onestep_wiring_contains_parent_pipeline_recursive_severalstep(self):
        """Bad wiring: pipeline step contains the parent pipeline in some recursive sub-step."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="output");
        foo.save();

        bar = Pipeline(family=self.DNAcomp_pf, revision_name="bar",
                       revision_desc="Bar version");
        bar.save();
        bar.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="barinput", dataset_idx=1);
        bstep1 = bar.steps.create(transformation=foo, step_num=1);
        bstep1.inputs.create(transf_input_name="oneinput",
                             step_providing_input=0,
                             provider_output_name="barinput");
        bstep2 = bar.steps.create(transformation=self.DNArecomp_m, step_num=2);
        bstep2.inputs.create(transf_input_name="complemented_seqs",
                             step_providing_input=1,
                             provider_output_name="oneoutput");
        bar.outmap.create(output_name="baroutputone", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="oneoutput");
        bar.outmap.create(output_name="baroutputtwo", output_idx=2,
                          step_providing_output=2,
                          provider_output_name="recomplemented_seqs");
        bar.save();

        # Now feed bar back into foo as a step.
        step1.delete();
        foo.outputs.all().delete();
        badstep = foo.steps.create(transformation=bar, step_num=1);
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                badstep.clean);

    ####
    # Now test Pipeline itself
        
    def test_pipeline_clean_onestep_wiring_good(self):
        """Test good step wiring, one-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertEquals(foo.clean(), None);
        
    def test_pipeline_clean_onestep_wiring_bad_idx(self):
        """Bad wiring: step not indexed 1."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=2);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean);
        
    def test_pipeline_clean_onestep_wiring_bad_pipeline_input(self):
        """Bad wiring: step looks for nonexistent input."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="nonexistent");
        self.assertRaisesRegexp(
                ValidationError,
                "Pipeline does not have input \"nonexistent\"",
                foo.clean);
        
    def test_pipeline_clean_onestep_wiring_bad_input_cd(self):
        """Bad wiring: input is of wrong CompoundDatatype."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.test_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 does not have the expected CompoundDatatype",
                foo.clean);
        
    def test_pipeline_clean_onestep_wiring_minrow_nomatch_pipeline_input_unset(self):
        """Bad wiring: step requests input with possibly too few rows (min_row unset for pipeline input)."""
        # Make a method with a restricted min_row.
        curr_method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        curr_method.save();
        curr_method.inputs.create(compounddatatype=self.DNAinput_cd,
                                  dataset_name="input",
                                  dataset_idx=1, min_row=10);
        curr_method.outputs.create(compounddatatype=self.DNAoutput_cd,
                                   dataset_name="output",
                                   dataset_idx=1);

        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=curr_method, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                foo.clean);
        
    def test_pipeline_clean_onestep_wiring_minrow_nomatch_pipeline_input_set(self):
        """Bad wiring: step requests input with possibly too few rows (min_row set for pipeline input)."""
        # Make a method with a restricted min_row.
        curr_method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        curr_method.save();
        curr_method.inputs.create(compounddatatype=self.DNAinput_cd,
                                  dataset_name="input",
                                  dataset_idx=1, min_row=10);
        curr_method.outputs.create(compounddatatype=self.DNAoutput_cd,
                                   dataset_name="output",
                                   dataset_idx=1);

        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1,
                          min_row=5);
        step1 = foo.steps.create(transformation=curr_method, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                foo.clean);


    def test_pipeline_clean_onestep_wiring_maxrow_nomatch_pipeline_input_unset(self):
        """Bad wiring: step requests input with possibly too many rows (max_row unset for pipeline input)."""
        # Make a method with a restricted min_row.
        curr_method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        curr_method.save();
        curr_method.inputs.create(compounddatatype=self.DNAinput_cd,
                                  dataset_name="input",
                                  dataset_idx=1, max_row=10);
        curr_method.outputs.create(compounddatatype=self.DNAoutput_cd,
                                   dataset_name="output",
                                   dataset_idx=1);

        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=curr_method, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                foo.clean);


    def test_pipeline_clean_onestep_wiring_maxrow_nomatch_pipeline_input_set(self):
        """Bad wiring: step requests input with possibly too many rows (max_row set for pipeline input)."""
        # Make a method with a restricted min_row.
        curr_method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        curr_method.save();
        curr_method.inputs.create(compounddatatype=self.DNAinput_cd,
                                  dataset_name="input",
                                  dataset_idx=1, max_row=10);
        curr_method.outputs.create(compounddatatype=self.DNAoutput_cd,
                                   dataset_name="output",
                                   dataset_idx=1);

        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1,
                          max_row=20);
        step1 = foo.steps.create(transformation=curr_method, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                foo.clean);


        
    def test_pipeline_clean_onestep_outmap_good(self):
        """Good output mapping, one-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="output");
        self.assertEquals(foo.clean(), None);

    def test_pipeline_clean_onestep_outmap_nonexistent_step(self):
        """Bad output mapping, one-step pipeline: request from nonexistent step"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=5,
                          provider_output_name="output");
        self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                foo.clean);
        
    def test_pipeline_clean_onestep_outmap_nonexistent_output(self):
        """Bad output mapping, one-step pipeline: request nonexistent step output"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="nonexistent");
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"nonexistent\"",
                foo.clean);
        
    def test_pipeline_clean_onestep_outmap_deleted_output(self):
        """Bad output mapping, one-step pipeline: request deleted step output"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step1.outputs_to_delete.create(dataset_to_delete="output");
        foo.outmap.create(output_name="oneoutput", output_idx=1,
                          step_providing_output=1,
                          provider_output_name="output");
        self.assertRaisesRegexp(
                ValidationError,
                "Output \"output\" from step 1 is deleted prior to request",
                foo.clean);
        
    def test_pipeline_clean_onestep_outmap_bad_indexing(self):
        """Bad output mapping, one-step pipeline: output not indexed 1"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        foo.outmap.create(output_name="oneoutput", output_idx=9,
                          step_providing_output=1,
                          provider_output_name="output");
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);


    ####
    # Chained, sequential steps
        
    def test_pipeline_clean_chainstep_wiring_good(self):
        """Test good step wiring, chained-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
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
        self.assertEquals(foo.clean(), None);
        
    def test_pipeline_clean_chainstep_wiring_later_step_nonexistent_input(self):
        """Bad wiring: later step requests nonexistent input from previous."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="nonexistent");
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Transformation at step 1 does not produce output \"nonexistent\"",
                foo.clean);
        
    def test_pipeline_clean_chainstep_wiring_later_step_deleted_input(self):
        """Bad wiring: later step requests input deleted by producing step."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step2.outputs_to_delete.create(dataset_to_delete="recomplemented_seqs");
        step3 = foo.steps.create(transformation=self.RNAcompv2_m, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Input \"recomplemented_seqs\" from step 2 to step 3 is deleted prior to request",
                foo.clean);

        
    def test_pipeline_clean_chainstep_wiring_later_step_bad_input_cd(self):
        """Bad wiring: later step requests input of wrong CompoundDatatype."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=self.RNAcompv2_m, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 does not have the expected CompoundDatatype",
                foo.clean);

    def test_pipeline_clean_chainstep_wiring_later_step_minrow_nomatch_providing_step_unset(self):
        """Bad wiring: later step requests input with possibly too few rows (min_row unset for providing step)."""
        # Make a method with a restricted min_row.
        step2method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        step2method.save();
        step2method.inputs.create(compounddatatype=self.DNAoutput_cd,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.outputs.create(compounddatatype=self.DNAinput_cd,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1);

        step3method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        step3method.save();
        step3method.inputs.create(compounddatatype=self.DNAinput_cd,
                                  dataset_name="input",
                                  dataset_idx=1, min_row=5);
        step3method.outputs.create(compounddatatype=self.DNAoutput_cd,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=step2method, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=step3method, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                foo.clean);



    def test_pipeline_clean_chainstep_wiring_later_step_minrow_nomatch_providing_step_set(self):
        """Bad wiring: later step requests input with possibly too few rows (providing step min_row is set)."""
        # Make a method with a restricted min_row.
        step2method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        step2method.save();
        step2method.inputs.create(compounddatatype=self.DNAoutput_cd,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.outputs.create(compounddatatype=self.DNAinput_cd,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1,
                                   min_row=5);

        step3method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        step3method.save();
        step3method.inputs.create(compounddatatype=self.DNAinput_cd,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  min_row=10);
        step3method.outputs.create(compounddatatype=self.DNAoutput_cd,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=step2method, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=step3method, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                foo.clean);


    def test_pipeline_clean_chainstep_wiring_later_step_maxrow_nomatch_providing_step_unset(self):
        """Bad wiring: later step requests input with possibly too many rows (max_row unset for providing step)."""
        # Make a method with a restricted min_row.
        step2method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        step2method.save();
        step2method.inputs.create(compounddatatype=self.DNAoutput_cd,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.outputs.create(compounddatatype=self.DNAinput_cd,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1);

        step3method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        step3method.save();
        step3method.inputs.create(compounddatatype=self.DNAinput_cd,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  max_row=100);
        step3method.outputs.create(compounddatatype=self.DNAoutput_cd,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=step2method, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=step3method, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too many rows",
                foo.clean);



    def test_pipeline_clean_chainstep_wiring_later_step_maxrow_nomatch_providing_step_set(self):
        """Bad wiring: later step requests input with possibly too many rows (max_row for providing step is set)."""
        # Make a method with a restricted min_row.
        step2method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        step2method.save();
        step2method.inputs.create(compounddatatype=self.DNAoutput_cd,
                                  dataset_name="complemented_seqs",
                                  dataset_idx=1);
        step2method.outputs.create(compounddatatype=self.DNAinput_cd,
                                   dataset_name="recomplemented_seqs",
                                   dataset_idx=1,
                                   max_row=100);

        step3method = Method(family=self.DNAcomp_mf, revision_name="foo",
                             revision_desc="foo", driver=self.compv2_rev);
        step3method.save();
        step3method.inputs.create(compounddatatype=self.DNAinput_cd,
                                  dataset_name="input",
                                  dataset_idx=1,
                                  max_row=50);
        step3method.outputs.create(compounddatatype=self.DNAoutput_cd,
                                   dataset_name="output",
                                   dataset_idx=1);
        
        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
                          dataset_name="oneinput", dataset_idx=1);
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1);
        step1.inputs.create(transf_input_name="input",
                            step_providing_input=0,
                            provider_output_name="oneinput");
        step2 = foo.steps.create(transformation=step2method, step_num=2);
        step2.inputs.create(transf_input_name="complemented_seqs",
                            step_providing_input=1,
                            provider_output_name="output");
        step3 = foo.steps.create(transformation=step3method, step_num=3);
        step3.inputs.create(transf_input_name="input",
                            step_providing_input=2,
                            provider_output_name="recomplemented_seqs");
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too many rows",
                foo.clean);

        
    def test_pipeline_clean_chainstep_outmap_good(self):
        """Good output mapping, chained-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
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


    def test_pipeline_clean_chainstep_outmap_nonexistent_step(self):
        """Bad output mapping, chained-step pipeline: request from nonexistent step"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
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
                          step_providing_output=5,
                          provider_output_name="nonexistent");
        foo.outmap.create(output_name="outputtwo", output_idx=2,
                          step_providing_output=2,
                          provider_output_name="recomplemented_seqs");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                foo.clean);

    def test_pipeline_clean_chainstep_outmap_nonexistent_output(self):
        """Bad output mapping, chained-step pipeline: request nonexistent step output"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
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

        
    def test_pipeline_clean_chainstep_outmap_deleted_output(self):
        """Bad output mapping, chained-step pipeline: request deleted step output"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
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

    def test_pipeline_clean_chainstep_outmap_bad_indexing(self):
        """Bad output mapping, chain-step pipeline: outputs not consecutively numbered starting from 1"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
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

        foo.outmap.create(output_name="outputone", output_idx=5,
                          step_providing_output=3,
                          provider_output_name="output");
        foo.outmap.create(output_name="outputtwo", output_idx=2,
                          step_providing_output=2,
                          provider_output_name="recomplemented_seqs");
        
        self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                foo.clean);


    def test_pipeline_clean_chainstep_wiring_good(self):
        """Test good step wiring, chained-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version");
        foo.save();
        foo.inputs.create(compounddatatype=self.DNAinput_cd,
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
        self.assertEquals(foo.clean(), None);
