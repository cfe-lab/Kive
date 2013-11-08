"""
Shipyard unit tests pertaining to Pipeline and its relatives.
"""

from django.test import TestCase;
from django.core.files import File;
from django.core.exceptions import ValidationError;

import os.path;

from metadata.models import *
from method.models import *
from pipeline.models import *
import method.tests

samplecode_path = "../samplecode"

# All classes that inherit TestCase are evaluated by the TestUtility

class PipelineTestSetup(method.tests.MethodTestSetup):
    """
    Set up a database state for unit testing Pipeline.

    This extends MethodTestSetup, which itself extended
    MetadataTestSetup.
    """
    def setUp(self):
        """Set up default database state for Pipeline unit testing."""
        # Methods, CR/CRR/CRDs, and DTs/CDTs are set up by calling this.
        super(PipelineTestSetup, self).setUp()
        
        # Define DNAcomp_pf
        self.DNAcomp_pf = PipelineFamily(
            name="DNAcomplement",
            description="DNA complement pipeline.")
        self.DNAcomp_pf.save()

        # Define DNAcompv1_p (pipeline revision)
        self.DNAcompv1_p = self.DNAcomp_pf.members.create(
            revision_name="v1",
            revision_desc="First version")

        # Add Pipeline input CDT DNAinput_cdt to pipeline revision DNAcompv1_p
        self.DNAcompv1_p.create_input(
            compounddatatype=self.DNAinput_cdt,
            dataset_name="seqs_to_complement",
            dataset_idx=1)

        # Add a step to Pipeline revision DNAcompv1_p involving
        # a transformation DNAcompv2_m at step 1
        step1 = self.DNAcompv1_p.steps.create(
            transformation=self.DNAcompv2_m,
            step_num=1)

        # Add cabling (PipelineStepInputCable's) to (step1, DNAcompv1_p)
        # From step 0, output hole "seqs_to_complement" to
        # input hole "input" (of this step)
        step1.cables_in.create(
            dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            source_step=0,
            source=self.DNAcompv1_p.inputs.get(
                dataset_name="seqs_to_complement"))

        # Add output cabling (PipelineOutputCable) to DNAcompv1_p
        # From step 1, output hole "output", send output to
        # Pipeline output hole "complemented_seqs" at index 1
        outcabling = self.DNAcompv1_p.create_outcable(
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"),
            output_name="complemented_seqs",
            output_idx=1)

        #############################
        
        # Setup used in the "2nd-wave" tests (this was originally in
        # Copperfish_Raw_Setup).

        # Define PF in order to define pipeline
        self.test_PF = PipelineFamily(
            name="test pipeline family",
            description="pipeline family placeholder");
        self.test_PF.full_clean()
        self.test_PF.save()

class PipelineFamilyTests(PipelineTestSetup):

    def test_unicode(self):
        """
        unicode() for PipelineFamily should display it's name
        """
        self.assertEqual(unicode(self.DNAcomp_pf),
                         "DNAcomplement");
    
class PipelineTests(PipelineTestSetup):
    
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
            dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
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
            dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

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
            dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            source_step=0,
            source=self.DNAcompv1_p.inputs.get(dataset_name="seqs_to_complement"));
        
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

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
            dest=curr_method.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

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
        """Unverifiable cabling: step requests input with possibly too few rows (input min_row specified)."""
        
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
            dest=curr_method.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        
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
            dest=curr_method.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

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
            dest=curr_method.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

        # Connect the output of step 1 to the output of foo
        outcable = foo.create_outcable(
            output_name="oneoutput",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
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
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                              source_step=0,
                              source=foo.inputs.get(dataset_name="oneinput"));

        # Index a non-existent step to outcable
        outcable = foo.outcables.create(
            output_name="oneoutput", output_idx=1,
            source_step=5,
            source=step1.transformation.outputs.all()[0],
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
        step1.cables_in.create(dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
                              source_step=0,
                              source=foo.inputs.get(dataset_name="oneinput"));
 
        # Reference a correct step but TransformationOutput from another Transformation.
        outcable = foo.outcables.create(
            output_name="oneoutput", output_idx=1,
            source_step=1,
            source=self.RNAoutput_to,
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
        step1.cables_in.create(dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
                              source_step=0,
                              source=foo.inputs.get(dataset_name="oneinput"));

        # Mark DNAcompv2_m output as deletable.
        # August 24, 2013: this is now OK.
        step1.add_deletion(
            dataset_to_delete=self.DNAcompv2_m.outputs.get(dataset_name="output"));

        # Now try to map it to the pipeline output.
        outcable = foo.create_outcable(
            output_name="oneoutput",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));

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
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                              source_step=0,
                              source=foo.inputs.get(dataset_name="oneinput"));

        # Outcable references a valid step and output, but is itself badly indexed
        outcable = foo.create_outcable(
            output_name="oneoutput",
            output_idx=9,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

        # Map step 1 to step 2
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        cable2 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));

        # Map step 2 to step 3
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3);
        cable3 = step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

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
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                              source_step=0,
                              source=foo.inputs.get(dataset_name="oneinput"));

        # step2 receives output not coming from from step1's transformation
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        cable2 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=self.RNAoutput_to);
        
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3);
        step3.cables_in.create(dest=step3.transformation.inputs.get(dataset_name="input"),
                              source_step=2,
                              source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
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
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                              source_step=0,
                              source=foo.inputs.get(dataset_name="oneinput"));

        # Connect step2 with output of step1
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));

        # Mark the output of step2 as deleted
        step2.add_deletion(
            dataset_to_delete=step2.transformation.outputs.get(
                dataset_name="recomplemented_seqs"));

        self.assertEquals(foo.clean(), None);

        # Connect step3 with the deleted output at step 2
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3);
        cable3 = step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

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
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                              source_step=0,
                              source=foo.inputs.get(dataset_name="oneinput"));
        
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2);
        step2.cables_in.create(dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
                              source_step=1,
                              source=step1.transformation.outputs.get(dataset_name="output"));
        
        step3 = foo.steps.create(transformation=self.RNAcompv2_m,
                                 step_num=3);
        cable = step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        
        step2.cables_in.create(
            dest=step2method.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));

        # Step 3 requires min_row = 5 but step2 does not guarentee this
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        
        cable = step3.cables_in.create(
            dest=step3method.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2method.outputs.get(dataset_name="recomplemented_seqs"));
        
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

        # Recall the output of step2 has min_row = 5
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.cables_in.create(
            dest=step2method.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));

        # Recall the input of step3 has min_row = 10
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        
        cable = step3.cables_in.create(
            dest=step3method.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2method.outputs.get(dataset_name="recomplemented_seqs"));
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.cables_in.create(
            dest=step2method.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        cable = step3.cables_in.create(
            dest=step3method.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2method.outputs.get(dataset_name="recomplemented_seqs"));
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2);
        step2.cables_in.create(
            dest=step2method.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3);
        cable = step3.cables_in.create(
            dest=step3method.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2method.outputs.get(dataset_name="recomplemented_seqs"));
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        outcable1 = foo.create_outcable(
            output_name="outputone", output_idx=1,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"));
        outcable2 = foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        # step 5 doesn't exist
        outcable1 = foo.outcables.create(
            output_name="outputone", output_idx=1,
            source_step=5,
            source=step3.transformation.outputs.get(dataset_name="output"),
            output_cdt=step3.transformation.outputs.get(dataset_name="output").get_cdt());
        outcable2 = foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
        
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        outcable1 = foo.create_outcable(
            output_name="outputone", output_idx=1,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"));
        outcable2 = foo.outcables.create(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step1.transformation.outputs.get(dataset_name="output"),
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
        step3.add_deletion(
            dataset_to_delete=step3.transformation.outputs.get(dataset_name="output"));

        outcable1 = foo.create_outcable(
            output_name="outputone", output_idx=1,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"));
        outcable2 = foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        outcable1 = foo.create_outcable(
            output_name="outputone",
            output_idx=5,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"));
        outcable2 = foo.create_outcable(
            output_name="outputtwo",
            output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

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
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_k"));

        cable2 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"));

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
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_k"));

        cable2 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"));        

        # Send a cable to r more than once!
        cable3 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"));

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
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_k"));

        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"));        

        # Send a cable to k from r.
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"));

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
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_k"))

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
        step1.cables_in.create(dest=self.script_3_method.inputs.get(dataset_name="r"),
                              source_step=0,
                              source=foo.inputs.get(dataset_name="pipe_input_2_r"));

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_a_b_c"));
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2);

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        cable21 = step2.cables_in.create(
            dest=self.script_3_method.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_r"));

        # singlet a_b_c_mean from step 1 feeds into singlet k at step 2
        cable22 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="k"),
            source_step=1,
            source=step1.transformation.outputs.get(
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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_a_b_c"));

        # Delete irrelevant output
        step1.add_deletion(
            dataset_to_delete = step1.transformation.outputs.get(dataset_name="a_b_c_squared"))
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2);

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_r"));

        # singlet a_b_c_mean from step 1 feeds into singlet k at step 2
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="k"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"));

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_a_b_c"));
        
        # This output required for subsequent steps
        # August 24, 2013: this is now allowed, so no error should be raised.
        step1.add_deletion(
            dataset_to_delete = step1.transformation.outputs.get(dataset_name="a_b_c_mean"))
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2);

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        cable1 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_r"));

        # singlet a_b_c_mean (deleted!) from step 1 feeds into singlet k at step 2
        cable2 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="k"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"));

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 1st output (Which is deleted)
        # August 24, 2013: this is now OK
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is not deleted)
        outcable2 = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        # Add outcable for 1st output (Which is not deleted)
        outcable = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Add outcable for 1st output (Which is not deleted)
        outcable = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        # Add outcable for 2nd output (Which is deleted)
        # August 24, 2013: this is now OK.
        outcable = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));
        
        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is not deleted)
        outcable = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_a_b_c"))

        outcable = foo.create_outcable(
            output_name="aName",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Add outcables for both outputs
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))
        outcable2 = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 1st output (Which is deleted)
        # August 24, 2013: this is now allowed, so no error should be raised later.
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is not deleted)
        outcable2 = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Delete data in step 1
        step1.add_deletion(
            dataset_to_delete=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        # Add outcable for 1st output (Which is not deleted)
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is deleted)
        # August 24, 2013: this is now allowed, so tests should be fine.
        outcable2 = foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
        foo.create_input(
            compounddatatype=self.triplet_cdt,
dataset_name="pipe_input_1_a_b_c",
            dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"));

        # Add outcable for 1st output (Which is not deleted)
        foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is deleted)
        foo.create_outcable(
            output_name="output_a_b_c_mean",
            output_idx=2,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2);
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3);
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));

        foo.create_outcable(
            output_name="outputone", output_idx=1,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"));
        foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
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
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
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
 

class PipelineStepTests(PipelineTestSetup):

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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=foo.inputs.get(dataset_name="oneinput"));
     
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
            dest=self.script_1_method.inputs.get(dataset_name="input_tuple"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));

        # Define pipeline output at index 1 from (step 1, output "output")
        foo.create_outcable(
            output_name="oneoutput",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
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
            dest=foo.inputs.get(dataset_name="oneinput"),
            source_step=0,
            source=bar.inputs.get(dataset_name="barinput"));

        # Map a single output, from step 1 foo.output = "oneoutput"
        bar.create_outcable(
            output_name="baroutput",
            output_idx=1,
            source_step=1,
            source=bstep1.transformation.outputs.get(dataset_name="oneoutput"));
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
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"));
        foo.create_outcable(
            output_name="oneoutput", output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"));
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
            dest=bstep1.transformation.inputs.get(dataset_name="oneinput"),
            source_step=0,
            source=bar.inputs.get(dataset_name="barinput"));
        
        bstep2 = bar.steps.create(transformation=self.DNArecomp_m,
                                  step_num=2);
        bstep2.cables_in.create(
            dest=bstep2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=bstep1.transformation.outputs.get(dataset_name="oneoutput"));
        bar.create_outcable(
            output_name="baroutputone",
            output_idx=1,
            source_step=1,
            source=bstep1.transformation.outputs.get(dataset_name="oneoutput"));
        bar.create_outcable(
            output_name="baroutputtwo",
            output_idx=2,
            source_step=2,
            source=bstep2.transformation.outputs.get(dataset_name="recomplemented_seqs"));
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

class PipelineStepRawDeleteTests(PipelineTestSetup):

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


class RawOutputCableTests(PipelineTestSetup):

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
            source_step=1,
            source=raw_output)

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
                                                    source_step=1,
                                                    source=raw_output)

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
            source_step=1,
            source=unrelated_raw_output)

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
            source_step=2,
            source=raw_output)

        error_msg = "Output requested from a non-existent step"

        self.assertRaisesRegexp(ValidationError, error_msg, outcable1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg, self.pipeline_1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg,
                                self.pipeline_1.complete_clean)

class RawInputCableTests(PipelineTestSetup):
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
            dest=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        rawcable2 = step2.create_raw_cable(
            dest=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        # These raw cables were both cabled from the pipeline input and are valid
        self.assertEquals(rawcable1.clean(), None)
        self.assertEquals(rawcable2.clean(), None)

    def test_PSIC_raw_cable_leads_to_foreign_pipeline_bad(self):
        """
        Destination must belong to a PS Transformation in THIS pipeline.
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

        # Define a raw cable into Pipeline2step1 from Pipeline1's raw
        # inputs (Cross-pipeline contamination!)
        rawcable1 = step1_pipeline_2.cables_in.create(
            dest=step1_pipeline_2.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        error_msg = "Pipeline does not have input .*"
        self.assertRaisesRegexp(ValidationError, error_msg, rawcable1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg,
                                step1_pipeline_2.clean)
        self.assertRaisesRegexp(ValidationError, error_msg,
                                step1_pipeline_2.complete_clean)
        self.assertRaisesRegexp(ValidationError, error_msg,
                                self.pipeline_2.clean)

    def test_PSIC_raw_cable_does_not_map_to_raw_input_of_this_step_bad(self):
        """
        dest does not specify a TransformationRawInput of THIS pipeline step
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
            dest=self.script_4_2_M.inputs.get(dataset_name="a_b_c_method"),
            source_step=0,
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        error_msg = "Transformation at step 1 does not have input \"\[Method test method family s4\]:raw1 a_b_c_method\"";
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
            dest=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        rawcable2 = step2.create_raw_cable(
            dest=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"));

        # Define custom wiring (totally nonsensical) on rawcable1.
        rawcable1.custom_wires.create(
            source_pin=self.doublet_cdt.members.all()[0],
            dest_pin=self.doublet_cdt.members.all()[0])
        
        self.assertRaisesRegexp(
            ValidationError,
            "Cable \"Pipeline test pipeline family v1 step 1:a_b_c\(raw\)\" is raw and should not have custom wiring defined",
            rawcable1.clean)

class RawSaveTests(PipelineTestSetup):
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
class SingleRawInputTests(PipelineTestSetup):
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

        raw_input_cable_1 = step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)

    def test_PipelineStep_completeClean_check_overquenching_doubled_source_of_raw_inputs_bad(self):

        # Wire 1 raw input to a pipeline step that expects only 1 input
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        raw_input_cable_1 = step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

        raw_input_cable_2 = step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

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

        raw_input_cable_1 = step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

        raw_input_cable_2 = step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input_2)

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


class SeveralRawInputsTests(PipelineTestSetup):
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

        raw_input_cable_1 = step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

        raw_input_cable_2 = step1.create_raw_cable(
            dest = method_raw_in_2,
            source = pipeline_input_2)

        raw_input_cable_over = step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input_2)

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
class SingleRawOutputTests(PipelineTestSetup):
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



class SeveralRawOutputsTests(PipelineTestSetup):

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


class CustomWiringTests(PipelineTestSetup):

    def test_CustomCableWire_wires_from_pipeline_input_identical_dt_good(self):
        """Custom wiring that connects identical datatypes together, on a cable leading from pipeline input (not PS output)."""
        # Define a pipeline with single pipeline input of type triplet_cdt
        my_pipeline = self.test_PF.members.create(
            revision_name="foo", revision_desc="Foo version");
        pipeline_in = my_pipeline.create_input(
            compounddatatype=self.triplet_cdt,
            dataset_name="pipe_in_1",
            dataset_idx=1)

        # Define method to have an input with the same CDT, add it as a step, cable it
        method_in = self.testmethod.create_input(
            dataset_name="TestIn",
            dataset_idx=1,
            compounddatatype=self.triplet_cdt)
        my_step1 = my_pipeline.steps.create(
            transformation=self.testmethod, step_num=1)
        my_cable1 = my_step1.cables_in.create(
            dest=method_in, source_step=0, source=pipeline_in)

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
            "Destination member \"2: <string> \[b\]\" has no wires leading to it",
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
        # Wiring test 2 - Datatypes are not identical, but compatible (y restricts x, y -> x)
        # Wiring test 3 - Datatypes are not compatible (z does not restrict x, z -> x) 

        # Define 2 CDTs3 datatypes - one identical, one compatible, and one incompatible + make a new CDT composed of them
        # Regarding datatypes, recall [self.DNA_dt] restricts [self.string_dt]

        # Define a datatype that has nothing to do with anything.
        self.incompatible_dt = Datatype(
            name="Not compatible",
            description="A datatype not having anything to do with anything",
            Python_type=Datatype.STR)
        self.incompatible_dt.save()

        # Define 2 CDTs that are unequal: (DNA, string, string), and (string, DNA, incompatible)
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

        # Define a pipeline with single pipeline input of type cdt_1
        my_pipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_in = my_pipeline.create_input(compounddatatype=cdt_1,dataset_name="pipe_in_1",dataset_idx=1)

        # Define method to have an input with cdt_2, add it as a step, cable it
        method_in = self.testmethod.create_input(dataset_name="TestIn", dataset_idx=1,compounddatatype=cdt_2)
        my_step1 = my_pipeline.steps.create(transformation=self.testmethod, step_num=1)
        my_cable1 = my_step1.cables_in.create(dest=method_in, source_step=0, source=pipeline_in)

        # CDTs are not equal, so this cable requires custom wiring
        self.assertRaisesRegexp(
            ValidationError,
            "Custom wiring required for cable \"Pipeline test pipeline family foo step 1:TestIn\"",
            my_step1.clean);

        # Wiring case 1: Datatypes are identical (DNA -> DNA)
        wire1 = my_cable1.custom_wires.create(
            source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
            dest_pin=method_in.get_cdt().members.get(column_idx=2))

        # Wiring case 2: Datatypes are compatible (DNA -> string)
        wire2 = my_cable1.custom_wires.create(
            source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
            dest_pin=method_in.get_cdt().members.get(column_idx=1))
        
        # Wiring case 3: Datatypes are compatible (DNA -> incompatible CDT)
        wire3_bad = my_cable1.custom_wires.create(
            source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
            dest_pin=method_in.get_cdt().members.get(column_idx=3))

        self.assertEquals(wire1.clean(), None)
        self.assertEquals(wire2.clean(), None)

        errorMessage = "The datatype of the source pin \"1: <DNANucSeq> \[col_1\]\" is incompatible with the datatype of the destination pin \"3: <Not compatible> \[col_3\]\"'\]"
        
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

        # Define a datatype that has nothing to do with anything.
        self.incompatible_dt = Datatype(
            name="poop",
            description="poop!!",
            Python_type="str")
        self.incompatible_dt.save()

        # Define 2 different CDTs: (DNA, string, string), and (string, DNA, incompatible)
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
        pipeline_1_cable = pipeline_1_step.cables_in.create(dest=method_1_in, source_step=0, source=pipeline_1_in)

        pipeline_2 = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_2_in = pipeline_2.create_input(compounddatatype=cdt_2,dataset_name="pipe_in_1",dataset_idx=1)
        pipeline_2_step = pipeline_2.steps.create(transformation=method_2, step_num=1)
        pipeline_2_cable = pipeline_2_step.cables_in.create(dest=method_2_in, source_step=0, source=pipeline_2_in)

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
class PipelineOutputCableRawTests(PipelineTestSetup):
    
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
            source_step=1,
            source=raw_output_2)

        self.pipeline_1.create_raw_outcable(
            raw_output_name="pipeline_output_3",
            raw_output_idx=3,
            source_step=1,
            source=raw_output_4)

        self.pipeline_1.create_outcable(
            output_name="pipeline_output_2",
            output_idx=2,
            source_step=1,
            source=output_3)

        self.assertEquals(self.pipeline_1.clean(), None)

class CustomRawOutputCablingTests(PipelineTestSetup):

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
            source_step=1,
            source=method_raw_out)

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
            source_step=1,
            source=method_raw_out)

        self.my_pipeline.create_outputs()
        self.assertEquals(self.my_pipeline.outputs.all().count(), 2)

        raw_output_2 = self.my_pipeline.outputs.all()[1]

        self.assertEquals(raw_output_2.dataset_name, "raw_out_2")
        self.assertEquals(raw_output_2.dataset_idx, 2)

        
class PipelineStepInputCable_tests(PipelineTestSetup):

    def test_PSIC_clean_and_completely_wired_CDT_equal_no_wiring_good(self):
        # Define pipeline with mix_triplet_cdt (string, DNA, string) pipeline input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        myPipeline_input = myPipeline.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="pipe_in",dataset_idx=1)

        # Define method with doublet_cdt input (string, string), add it to the pipeline, and cable it
        method_input = self.testmethod.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="method_in",dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        pipeline_cable = pipelineStep.cables_in.create(dest=method_input, source_step=0, source=myPipeline_input)

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
        pipeline_cable = pipelineStep.cables_in.create(dest=method_input, source_step=0, source=myPipeline_input)

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


    def test_PSIC_clean_and_completely_wired_CDT_not_equal_wires_exist_compatible_wiring_good(self):
        # A -> x
        # A -> y

        # Define pipeline with mix_triplet_cdt (string, DNA, string) pipeline input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        myPipeline_input = myPipeline.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="pipe_in",dataset_idx=1)

        # Define method with doublet_cdt input (string, string), add it to the pipeline, and cable it
        method_input = self.testmethod.create_input(compounddatatype=self.doublet_cdt,dataset_name="method_in",dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        pipeline_cable = pipelineStep.cables_in.create(dest=method_input, source_step=0, source=myPipeline_input)

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
        pipeline_cable = pipelineStep.cables_in.create(dest=method_input, source_step=0, source=myPipeline_input)
        
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


# August 29, 2013: reworked to handle new design for outcables.
class CustomOutputWiringTests(PipelineTestSetup):

    def test_CustomOutputCableWire_clean_references_invalid_CDTM(self):

        self.my_pipeline = self.test_PF.members.create(
            revision_name="foo", revision_desc="Foo version");

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
        my_step1 = self.my_pipeline.steps.create(
            transformation=self.testmethod, step_num=1);

        # Add an output cable
        outcable1 = self.my_pipeline.create_outcable(
            output_name="blah",
            output_idx=1,
            source_step=1,
            source=method_out)

        # Add custom wiring from an irrelevent CDTM
        badwire = outcable1.custom_outwires.create(
            source_pin=self.doublet_cdt.members.all()[0],
            dest_pin=self.triplet_cdt.members.all()[0])

        errorMessage = "Source pin \"1: <string> \[x\]\" does not come from compounddatatype \"\(1: <string> \[a\], 2: <string> \[b\], 3: <string> \[c\]\)\""

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
            source_step=1,
            source=method_out,
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
