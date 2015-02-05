"""
Shipyard unit tests pertaining to Pipeline and its relatives.
"""

from django.core.exceptions import ValidationError
from django.db.models import Count
from django.contrib.auth.models import Group
from django.test import TestCase
from django.utils import timezone

from constants import datatypes
import json
import os.path
import re
import shutil
import tempfile

from metadata.models import CompoundDatatype, Datatype
from method.models import Method
from pipeline.models import Pipeline, PipelineFamily, \
    PipelineSerializationException, PipelineStep, PipelineStepInputCable, \
    PipelineOutputCable
from librarian.models import SymbolicDataset
from archive.models import Dataset, ExecLog, User
import method.tests
import sandbox.testing_utils as tools

from django.core import serializers

from constants import datatypes, groups

samplecode_path = "../samplecode"


def create_pipeline_test_environment(case):
    """
    Sets up default database state for some Pipeline unit testing.

    This also sets up Methods, CR/CRR/CRDs, and DTs/CDTs as in the Metadata and Methods tests.
    """
    method.tests.create_method_test_environment(case)
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

    temporary_file, safe_fn = tempfile.mkstemp(dir=case.workdir)
    os.close(temporary_file)
    case.datafile = open(safe_fn, "w")
    case.datafile.write(",".join([m.column_name for m in case.DNAinput_cdt.members.all()]))
    case.datafile.write("\n")
    case.datafile.write("ATCG\n")
    case.datafile.close()
    case.DNAinput_symDS = SymbolicDataset.create_SD(safe_fn, user=case.user, cdt=case.DNAinput_cdt,
                                                    name="DNA input", description="input for DNAcomp pipeline")

    # Define PF in order to define pipeline
    case.test_PF = PipelineFamily(
        name="test pipeline family",
        description="pipeline family placeholder",
        user=case.user)
    case.test_PF.full_clean()
    case.test_PF.save()

    # Set up an empty Pipeline.
    family = PipelineFamily.objects.first()
    CompoundDatatype.objects.first()

    # Nothing defined.
    p = Pipeline(family=family, revision_name="foo", revision_desc="Foo version", user=case.user)
    p.save()


def destroy_pipeline_test_environment(case):
    """
    Clean up a TestCase where create_pipeline_test_environment has been called.
    """
    method.tests.destroy_method_test_environment(case)
    Dataset.objects.all().delete()
    shutil.rmtree(case.workdir)


class PipelineTestCase(TestCase):
    """
    Set up a database state for unit testing Pipeline.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        """Set up default database state for Pipeline unit testing."""
        create_pipeline_test_environment(self)

    def tearDown(self):
        destroy_pipeline_test_environment(self)


class PipelineFamilyTests(PipelineTestCase):

    def test_unicode(self):
        """
        unicode() for PipelineFamily should display it's name
        """
        self.assertEqual(unicode(self.DNAcomp_pf), "DNAcomplement")

    def test_delete_pipeline_family(self):
        """Can I delete a PipelineFamily?"""
        self.assertIsNone(PipelineFamily.objects.first().delete())


class PipelineTests(PipelineTestCase):
    """Tests for basic Pipeline functionality."""
    
    def test_pipeline_one_valid_input_no_steps(self):
        """A Pipeline with one valid input, but no steps, is clean but not complete."""
        p = Pipeline.objects.filter(steps__isnull=True, inputs__isnull=True).first()
        p.create_input(compounddatatype=self.DNAinput_cdt, dataset_name="oneinput", dataset_idx=1)
        self.assertIsNone(p.clean())
        self.assertRaisesRegexp(ValidationError, "Pipeline {} has no steps".format(p), p.complete_clean)

    def test_pipeline_one_invalid_input_clean(self):
        """A Pipeline with one input not numbered "1" is not clean."""
        p = Pipeline.objects.filter(inputs__isnull=True).first()
        cdt = CompoundDatatype.objects.first()
        p.create_input(compounddatatype=cdt, dataset_name="oneinput", dataset_idx=4)
        error = "Inputs are not consecutively numbered starting from 1"
        self.assertRaisesRegexp(ValidationError, error, p.clean)
        self.assertRaisesRegexp(ValidationError, error, p.complete_clean)

    def test_pipeline_many_valid_inputs_clean(self):
        """A Pipeline with multiple, properly indexed inputs is clean."""
        p = Pipeline.objects.filter(inputs__isnull=True).first()
        cdt = CompoundDatatype.objects.first()
        p.create_input(compounddatatype=cdt, dataset_name="oneinput", dataset_idx=1)
        p.create_input(compounddatatype=cdt, dataset_name="twoinput", dataset_idx=2)
        p.create_input(compounddatatype=cdt, dataset_name="threeinput", dataset_idx=3)
        self.assertIsNone(p.clean())

    def test_pipeline_many_valid_inputs_scrambled_clean(self):
        """A Pipeline with multiple, properly indexed inputs, in any order, is clean."""
        p = Pipeline.objects.filter(inputs__isnull=True).first()
        cdt = CompoundDatatype.objects.first()
        p.create_input(compounddatatype=cdt, dataset_name="oneinput", dataset_idx=2)
        p.create_input(compounddatatype=cdt, dataset_name="twoinput", dataset_idx=3)
        p.create_input(compounddatatype=cdt, dataset_name="threeinput", dataset_idx=1)
        self.assertIsNone(p.clean())

    def test_pipeline_many_invalid_inputs_clean(self):
        """A Pipeline with multiple, badly indexed inputs is not clean."""
        p = Pipeline.objects.filter(inputs__isnull=True).first()
        p.create_input(compounddatatype=self.DNAinput_cdt, dataset_name="oneinput", dataset_idx=2)
        p.create_input(compounddatatype=self.DNAinput_cdt, dataset_name="twoinput", dataset_idx=3)
        p.create_input(compounddatatype=self.DNAinput_cdt, dataset_name="threeinput", dataset_idx=4)
        self.assertRaisesRegexp(ValidationError, "Inputs are not consecutively numbered starting from 1", p.clean)

    def test_pipeline_one_valid_step_clean(self):
        """A Pipeline with one validly indexed step and input is clean.
        
        The PipelineStep and Pipeline are not complete unless there is a
        cable in place.
        """
        p = Pipeline.objects.filter(steps__isnull=True, inputs__isnull=True).first()
        m = Method.objects.annotate(Count("inputs")).filter(inputs__count=1, inputs__structure__isnull=False).first()
        cdt = m.inputs.first().structure.compounddatatype
        p.create_input(compounddatatype=cdt, dataset_name="oneinput", dataset_idx=1)
        step1 = p.steps.create(transformation=m, step_num=1)

        error = 'Input "input" to transformation at step 1 is not cabled'
        self.assertIsNone(step1.clean())
        self.assertRaisesRegexp(ValidationError, error, step1.complete_clean)
        self.assertIsNone(p.clean())
        self.assertRaisesRegexp(ValidationError, error, p.complete_clean)

    def test_pipeline_one_bad_step_clean(self):
        """Test step index check, one badly-indexed step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)
        foo.steps.create(transformation=self.DNAcompv2_m, step_num=10)
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean)

    def test_pipeline_many_valid_steps_clean(self):
        """Test step index check, well-indexed multi-step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)

        foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        foo.steps.create(transformation=self.DNAcompv2_m, step_num=3)
        
        self.assertEquals(foo.clean(), None)

    def test_pipeline_many_valid_steps_scrambled_clean(self):
        """Test step index check, well-indexed multi-step (scrambled order) case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)

        foo.steps.create(transformation=self.DNAcompv2_m, step_num=3)
        foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        
        self.assertEquals(foo.clean(), None)

    def test_pipeline_many_invalid_steps_clean(self):
        """Test step index check, badly-indexed multi-step case."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)

        foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        foo.steps.create(transformation=self.DNArecomp_m, step_num=4)
        foo.steps.create(transformation=self.DNAcompv2_m, step_num=5)
        
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean)

    def test_pipeline_oneStep_valid_cabling_clean(self):
        """Test good step cabling, one-step pipeline."""

        # Define pipeline 'foo' in family 'DNAcomp_pf'
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Add single, validly indexed pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1)

        # Add single validly indexed step, composed of the method DNAcompv2
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)

        # Add cabling from step 0 with input name "oneinput"
        cable = step1.cables_in.create(dest=self.DNAcompv2_m.inputs.get(dataset_name="input"), source_step=0,
                                       source=foo.inputs.get(dataset_name="oneinput"))
        self.assertEquals(cable.clean(), None)
        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(foo.clean(), None)
        self.assertEquals(foo.complete_clean(), None)
        
    def test_pipeline_oneStep_invalid_step_numbers_clean(self):
        """Bad pipeline (step not indexed 1), step is complete and clean."""

        # Define a pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        
        # Define a validly indexed pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)

        # Define step 2 without a step 1
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=2)

        # Give this step properly mapped cabling from the Pipeline input
        cable = step1.cables_in.create(
            dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        self.assertEquals(cable.clean(), None)
        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        
        self.assertRaisesRegexp(
                ValidationError,
                "Steps are not consecutively numbered starting from 1",
                foo.clean)
        
    def test_pipeline_oneStep_invalid_cabling_invalid_pipeline_input_clean (self):
        """Bad cabling: step looks for input that does not belong to the pipeline."""

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Define pipeline input for 'foo'
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1)

        # Define step 1 of this pipeline by transformation DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)

        # Cable a pipeline input that does not belong to the pipeline to step 1
        cable = step1.cables_in.create(
            dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
            source_step=0,
            source=self.DNAcompv1_p.inputs.get(dataset_name="seqs_to_complement"))
        
        self.assertRaisesRegexp(ValidationError,
                'Pipeline does not have input ".*"',
                cable.clean)
        # The following are just the same as the above, propagated upwards through clean()s.
        self.assertRaisesRegexp(ValidationError,
                'Pipeline does not have input ".*"',
                step1.clean)
        self.assertRaisesRegexp(ValidationError,
                'Pipeline does not have input ".*"',
                step1.complete_clean)
        self.assertRaisesRegexp(ValidationError,
                'Pipeline does not have input ".*"',
                foo.clean)
        
    def test_pipeline_oneStep_invalid_cabling_incorrect_cdt_clean(self):
        """Bad cabling: input is of wrong CompoundDatatype."""

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Define pipeline input "oneinput" for foo with CDT type test_cdt
        foo.create_input(compounddatatype=self.test_cdt, dataset_name="oneinput", dataset_idx=1)

        # Define step 1 by transformation DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)

        # Reference correct name "oneinput" and cable to step "input"
        # of DNAcompv2_m - but of the wrong cdt
        cable = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        self.assertIsNone(cable.clean())
        self.assertRaisesRegexp(ValidationError,
            'Custom wiring required for cable "{}"'.format(cable),
            cable.clean_and_completely_wired)
        
    def test_pipeline_oneStep_cabling_minrow_constraint_may_be_breached_clean (self):
        """Unverifiable cabling: step requests input with possibly too
        few rows (input min_row unspecified)."""

        # Define method 'curr_method' with driver compv2_crRev
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        curr_method.save()

        # Give curr_method properly indexed input with min_row = 10
        curr_method.create_input(compounddatatype=self.DNAinput_cdt,
                                 dataset_name="input",
                                 dataset_idx=1,
                                 min_row=10)

        # Give curr_method an output named 'output'
        curr_method.create_output(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="output",
                                  dataset_idx=1)

        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Define properly indexed pipeline input for 'foo'
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)

        # Define step 1 of 'foo' to be curr_method
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1)

        # From row-unconstrained pipeline input, assign to curr_method
        cable = step1.cables_in.create(
            dest=curr_method.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # It's possible this step may have too few rows
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                cable.clean)
        # This is just to check that the above propagated up.
        self.assertRaisesRegexp(
            ValidationError,
            "Data fed to input \"input\" of step 1 may have too few rows",
            foo.clean)
        
    def test_pipeline_oneStep_cabling_minrow_constraints_may_breach_each_other_clean (self):
        """Unverifiable cabling: step requests input with possibly too few rows (input min_row specified)."""
        
        # Define method curr_method
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        curr_method.save()

        # Give curr_method an input with min_row = 10
        curr_method.create_input(compounddatatype=self.DNAinput_cdt,
                                 dataset_name="input",
                                 dataset_idx=1, min_row=10)

        # Give curr_method an unconstrained output
        curr_method.create_output(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="output",
                                  dataset_idx=1)

        # Define pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Define pipeline input of foo to have min_row of 5
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1,
                         min_row=5)

        # Assign curr_method to step1 of foo
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1)
        
        # Map min_row = 5 pipeline input to this step's input
        # which contains curr_method with min_row = 10
        cable = step1.cables_in.create(
            dest=curr_method.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                cable.clean)
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                foo.clean)

    def test_pipeline_oneStep_cabling_maxRow_constraints_may_be_breached_clean(self):
        """Unverifiable cabling: step requests input with possibly too many rows
        (input max_row unspecified)"""

        # Define curr_method with input of max_row = 10
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        curr_method.save()
        curr_method.create_input(compounddatatype=self.DNAinput_cdt,
                                 dataset_name="input",
                                 dataset_idx=1, max_row=10)
       
        curr_method.create_output(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="output",
                                  dataset_idx=1)

        # Define pipeline with unrestricted Pipeline input
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)

        # Assign curr_method to step 1 of foo, and cable the pipeline input to it
        step1 = foo.steps.create(transformation=curr_method, step_num=1)
        cable = step1.cables_in.create(
            dest=curr_method.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # The pipeline input is unrestricted, but step 1 has max_row = 10
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                cable.clean)
        # Check propagation of error.
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                foo.clean)

    def test_pipeline_oneStep_cabling_maxRow_constraints_may_breach_each_other_clean (self):
        """Unverifiable cabling: step requests input with possibly too
        many rows (max_row set for pipeline input)."""
        
        # Define curr_method as having an input with max_row = 10
        curr_method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        curr_method.save()
        curr_method.create_input(compounddatatype=self.DNAinput_cdt,
                                 dataset_name="input",
                                 dataset_idx=1,
                                 max_row=10)
        curr_method.create_output(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="output",
                                  dataset_idx=1)

        # Define pipeline foo with Pipeline input having max_row = 20
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1,
                         max_row=20)

        # Assign curr_method to foo step 1
        step1 = foo.steps.create(transformation=curr_method,
                                 step_num=1)
        cable = step1.cables_in.create(
            dest=curr_method.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # The pipeline max_row is not good enough to guarantee correctness
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                cable.clean)
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too many rows",
                foo.clean)
        
    def test_pipeline_oneStep_with_valid_outcable_clean(self):
        """Good output cabling, one-step pipeline."""

        # Define pipeline foo with unconstrained input
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1)

        # Add DNAcompv2_m (Expecting 1 input) to step 1 of foo
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)

        # Map the pipeline input to step 1
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # Connect the output of step 1 to the output of foo
        outcable = foo.create_outcable(
            output_name="oneoutput",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        self.assertEquals(outcable.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_oneStep_outcable_references_nonexistent_step_clean(self):
        """Bad output cabling, one-step pipeline: request from nonexistent step"""

        # Define pipeline foo with validly indexed input and step 1 cabling
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)

        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                               source_step=0,
                               source=foo.inputs.get(dataset_name="oneinput"))

        # Index a non-existent step to outcable
        outcable = foo.outcables.create(
            output_name="oneoutput", output_idx=1,
            source_step=5,
            source=step1.transformation.outputs.all()[0],
            output_cdt=step1.transformation.outputs.all()[0].get_cdt())
        
        self.assertRaisesRegexp(
            ValidationError,
            "Output requested from a non-existent step",
            outcable.clean)
        # Check propagation of error.
        self.assertRaisesRegexp(
            ValidationError,
            "Output requested from a non-existent step",
            foo.clean)
        
    def test_pipeline_oneStep_outcable_references_invalid_output_clean (self):
        """Bad output cabling, one-step pipeline: request output not belonging to requested step"""

        # Define pipeline foo with validly indexed inputs, steps, and cabling
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        step1.cables_in.create(dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
                               source_step=0,
                               source=foo.inputs.get(dataset_name="oneinput"))
 
        # Reference a correct step but TransformationOutput from another Transformation.
        outcable = foo.outcables.create(output_name="oneoutput", output_idx=1,
            source_step=1,
            source=self.RNAoutput_to,
            output_cdt=self.RNAoutput_to.get_cdt())
        
        self.assertRaisesRegexp(ValidationError,
            'Transformation at step 1 does not produce output ".*"',
            outcable.clean)
        self.assertRaisesRegexp(ValidationError,
            'Transformation at step 1 does not produce output ".*"',
            foo.clean)
        
    def test_pipeline_oneStep_outcable_references_deleted_output_clean (self):
        """Output cabling, one-step pipeline: request deleted step output (OK)"""

        # Define pipeline foo with validly indexed inputs, steps, and cabling
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(dest=self.DNAcompv2_m.inputs.get(dataset_name="input"),
                               source_step=0,
                               source=foo.inputs.get(dataset_name="oneinput"))

        # Mark DNAcompv2_m output as deletable.
        # August 24, 2013: this is now OK.
        step1.add_deletion(
            self.DNAcompv2_m.outputs.get(dataset_name="output"))

        # Now try to map it to the pipeline output.
        outcable = foo.create_outcable(output_name="oneoutput", output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))

        self.assertEquals(outcable.clean(), None)
        self.assertEquals(foo.clean(), None)
        
    def test_pipeline_oneStep_bad_pipeline_output_indexing_clean(self):
        """Bad output cabling, one-step pipeline: output not indexed 1"""

        # Define pipeline with validly indexed inputs, steps, and cabling
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                               source_step=0,
                               source=foo.inputs.get(dataset_name="oneinput"))

        # Outcable references a valid step and output, but is itself badly indexed
        outcable = foo.create_outcable(
            output_name="oneoutput",
            output_idx=9,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        
        self.assertEquals(outcable.clean(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_pipeline_manySteps_valid_internal_cabling_clean(self):
        """Test good step cabling, chained-step pipeline."""

        # Define pipeline 'foo' with validly indexed input and steps
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)

        # Map pipeline input to step1
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        cable1 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # Map step 1 to step 2
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2)
        cable2 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))

        # Map step 2 to step 3
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3)
        cable3 = step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        self.assertEquals(cable1.clean(), None)
        self.assertEquals(cable2.clean(), None)
        self.assertEquals(cable3.clean(), None)
        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)
        self.assertEquals(step2.clean(), None)
        self.assertEquals(step2.complete_clean(), None)
        self.assertEquals(step3.clean(), None)
        self.assertEquals(step3.complete_clean(), None)
        self.assertEquals(foo.clean(), None)
        
    def test_pipeline_manySteps_cabling_references_invalid_output_clean(self):
        """Bad cabling: later step requests invalid input from previous."""

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)

        # step1 receives input from Pipeline input
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                               source_step=0,
                               source=foo.inputs.get(dataset_name="oneinput"))

        # step2 receives output not coming from from step1's transformation
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2)
        cable2 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=self.RNAoutput_to)
        
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3)
        step3.cables_in.create(dest=step3.transformation.inputs.get(dataset_name="input"),
                               source_step=2,
                               source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))
        self.assertRaisesRegexp(ValidationError,
            'Transformation at step 1 does not produce output ".*"',
            cable2.clean)

        # Check propagation of error.
        self.assertRaisesRegexp(ValidationError,
            'Transformation at step 1 does not produce output ".*"',
            step2.clean)
        self.assertRaisesRegexp(ValidationError,
            'Transformation at step 1 does not produce output ".*"',
            foo.clean)
        
    def test_pipeline_manySteps_cabling_references_deleted_input_clean(self):
        """Cabling: later step requests input deleted by producing step (OK)."""

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)

        # Connect step 1 with pipeline input
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                               source_step=0,
                               source=foo.inputs.get(dataset_name="oneinput"))

        # Connect step2 with output of step1
        step2 = foo.steps.create(transformation=self.DNArecomp_m,
                                 step_num=2)
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))

        # Mark the output of step2 as deleted
        step2.add_deletion(
            step2.transformation.outputs.get(
                dataset_name="recomplemented_seqs"))

        self.assertEquals(foo.clean(), None)

        # Connect step3 with the deleted output at step 2
        step3 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=3)
        cable3 = step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        # August 24, 2013: cabling from deleted outputs is now OK.
        self.assertEquals(cable3.clean(), None)
        self.assertEquals(step3.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_manySteps_cabling_references_incorrect_cdt_clean (self):
        """Bad cabling: later step requests input of wrong CompoundDatatype."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt, dataset_name="oneinput", dataset_idx=1)
        
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(dest=step1.transformation.inputs.get(dataset_name="input"),
                               source_step=0,
                               source=foo.inputs.get(dataset_name="oneinput"))
        
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        step2.cables_in.create(dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
                               source_step=1,
                               source=step1.transformation.outputs.get(dataset_name="output"))
        
        step3 = foo.steps.create(transformation=self.RNAcompv2_m, step_num=3)
        cable = step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        self.assertEquals(cable.clean(), None)
        error_msg = 'Custom wiring required for cable "{}"'.format(str(cable))
        for fun in [cable.clean_and_completely_wired, step3.clean, foo.clean]:
            self.assertRaisesRegexp(ValidationError, error_msg, fun)

    def test_pipeline_manySteps_minRow_constraint_may_be_breached_clean (self):
        """Unverifiable cabling: later step requests input with possibly too few rows (min_row unset for providing step)."""

        # Define a method with validly indexed inputs and outputs
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        step2method.save()
        step2method.create_input(compounddatatype=self.DNAoutput_cdt,
                                 dataset_name="complemented_seqs",
                                 dataset_idx=1)
        step2method.create_output(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="recomplemented_seqs",
                                  dataset_idx=1)

        # Define another method with validly indexed inputs and outputs
        # But with the inputs requiring min_row = 5
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        step3method.save()

        step3method.create_input(compounddatatype=self.DNAinput_cdt,
                                 dataset_name="input",
                                 dataset_idx=1,
                                 min_row=5)
        step3method.create_output(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="output",
                                  dataset_idx=1)
        
        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2)
        
        step2.cables_in.create(
            dest=step2method.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))

        # Step 3 requires min_row = 5 but step2 does not guarentee this
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3)
        
        cable = step3.cables_in.create(
            dest=step3method.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2method.outputs.get(dataset_name="recomplemented_seqs"))
        
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                cable.clean)
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                foo.clean)

    def test_pipeline_manySteps_minrow_constraints_may_breach_each_other_clean(self):
        """Bad cabling: later step requests input with possibly too few rows (providing step min_row is set)."""
        
        # Define method with outputs having a min row of 5
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        step2method.save()
        step2method.create_input(compounddatatype=self.DNAoutput_cdt,
                                 dataset_name="complemented_seqs",
                                 dataset_idx=1)
        
        step2method.create_output(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="recomplemented_seqs",
                                  dataset_idx=1,
                                  min_row=5)

        # Define another method with input min row of 10
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        step3method.save()
        step3method.create_input(compounddatatype=self.DNAinput_cdt,
                                 dataset_name="input",
                                 dataset_idx=1,
                                 min_row=10)
        step3method.create_output(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="output",
                                  dataset_idx=1)

        # Define pipeline foo with validly indexed inputs and steps
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # Recall the output of step2 has min_row = 5
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2)
        step2.cables_in.create(
            dest=step2method.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))

        # Recall the input of step3 has min_row = 10
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3)
        
        cable = step3.cables_in.create(
            dest=step3method.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2method.outputs.get(dataset_name="recomplemented_seqs"))
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                cable.clean)
        self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 3 may have too few rows",
                foo.clean)

    def test_pipeline_manySteps_maxRow_constraint_may_be_breached_clean(self):
        """Bad cabling: later step requests input with possibly too many rows (max_row unset for providing step)."""

        # step2 has no constraints on it's output
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        step2method.save()
        step2method.create_input(compounddatatype=self.DNAoutput_cdt,
                                 dataset_name="complemented_seqs",
                                 dataset_idx=1)
        step2method.create_output(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="recomplemented_seqs",
                                  dataset_idx=1)

        # step3 has an input with max_row = 100
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        step3method.save()

        step3method.create_input(compounddatatype=self.DNAinput_cdt,
                                 dataset_name="input",
                                 dataset_idx=1,
                                 max_row=100)
        step3method.create_output(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="output",
                                  dataset_idx=1)
        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2)
        step2.cables_in.create(
            dest=step2method.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3)
        cable = step3.cables_in.create(
            dest=step3method.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2method.outputs.get(dataset_name="recomplemented_seqs"))
        self.assertRaisesRegexp(
            ValidationError,
            "Data fed to input \"input\" of step 3 may have too many rows",
            cable.clean)
        self.assertRaisesRegexp(
            ValidationError,
            "Data fed to input \"input\" of step 3 may have too many rows",
            foo.clean)

    def test_pipeline_manySteps_cabling_maxRow_constraints_may_breach_each_other_clean (self):
        """Bad cabling: later step requests input with possibly too many rows (max_row for providing step is set)."""

        # step 2 has max_row = 100 on it's output
        step2method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        step2method.save()
        step2method.create_input(compounddatatype=self.DNAoutput_cdt,
                                 dataset_name="complemented_seqs",
                                 dataset_idx=1)
        step2method.create_output(compounddatatype=self.DNAinput_cdt,
                                  dataset_name="recomplemented_seqs",
                                  dataset_idx=1,
                                  max_row=100)

        # step3 has a max_row = 50 on it's input
        step3method = Method(family=self.DNAcomp_mf,
                             revision_name="foo",
                             revision_desc="foo",
                             driver=self.compv2_crRev,
                             user=self.user)
        step3method.save()
        step3method.create_input(compounddatatype=self.DNAinput_cdt,
                                 dataset_name="input",
                                 dataset_idx=1,
                                 max_row=50)
        step3method.create_output(compounddatatype=self.DNAoutput_cdt,
                                  dataset_name="output",
                                  dataset_idx=1)
        
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=step2method,
                                 step_num=2)
        step2.cables_in.create(
            dest=step2method.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        step3 = foo.steps.create(transformation=step3method,
                                 step_num=3)
        cable = step3.cables_in.create(
            dest=step3method.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2method.outputs.get(dataset_name="recomplemented_seqs"))
        self.assertRaisesRegexp(
            ValidationError,
            "Data fed to input \"input\" of step 3 may have too many rows",
            cable.clean)
        self.assertRaisesRegexp(
            ValidationError,
            "Data fed to input \"input\" of step 3 may have too many rows",
            foo.clean)

    def test_pipeline_manySteps_valid_outcable_clean(self):
        """Good output cabling, chained-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3)
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        outcable1 = foo.create_outcable(
            output_name="outputone", output_idx=1,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"))
        outcable2 = foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))
        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(outcable2.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_manySteps_outcable_references_nonexistent_step_clean(self):
        """Bad output cabling, chained-step pipeline: request from nonexistent step"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3)
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        # step 5 doesn't exist
        outcable1 = foo.outcables.create(
            output_name="outputone", output_idx=1,
            source_step=5,
            source=step3.transformation.outputs.get(dataset_name="output"),
            output_cdt=step3.transformation.outputs.get(dataset_name="output").get_cdt())
        outcable2 = foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))
        
        self.assertRaisesRegexp(
            ValidationError,
            "Output requested from a non-existent step",
            outcable1.clean)
        self.assertEquals(outcable2.clean(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "Output requested from a non-existent step",
            foo.clean)

    def test_pipeline_manySteps_outcable_references_invalid_output_clean(self):
        """Bad output cabling, chained-step pipeline: request output not belonging to requested step"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3)
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        outcable1 = foo.create_outcable(
            output_name="outputone", output_idx=1,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"))
        outcable2 = foo.outcables.create(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step1.transformation.outputs.get(dataset_name="output"),
            output_cdt=step1.transformation.outputs.get(dataset_name="output").get_cdt())

        self.assertEquals(outcable1.clean(), None)
        self.assertRaisesRegexp(ValidationError,
            'Transformation at step 2 does not produce output ".*"',
            outcable2.clean)
        self.assertRaisesRegexp(ValidationError,
            'Transformation at step 2 does not produce output ".*"',
            foo.clean)
        
    def test_pipeline_manySteps_outcable_references_deleted_output_clean(self):
        """Output cabling, chained-step pipeline: request deleted step output (OK)"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3)
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))
        step3.add_deletion(
            step3.transformation.outputs.get(dataset_name="output"))

        outcable1 = foo.create_outcable(output_name="outputone", output_idx=1,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"))
        outcable2 = foo.create_outcable(output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        # August 24, 2013: outcabling from deleted stuff is now OK.
        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(outcable2.clean(), None)
        self.assertEquals(foo.clean(), None)

    def test_pipeline_manySteps_outcable_references_invalid_output_index_clean(self):
        """Bad output cabling, chain-step pipeline: outputs not consecutively numbered starting from 1"""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3)
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        outcable1 = foo.create_outcable(
            output_name="outputone",
            output_idx=5,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"))
        outcable2 = foo.create_outcable(
            output_name="outputtwo",
            output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(outcable2.clean(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_pipeline_with_1_step_and_2_inputs_both_cabled_good(self):
        """
        Pipeline with 1 step (script_3_product) with 2 inputs / 1 output
        Both inputs are cabled (good)

        Reminder on script_3_product
        Reminder: k is cdt singlet, r is cdt single-row singlet
        """
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

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
                                 step_num=1)

        # Add cabling to step 1 from step 0
        cable1 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_k"))

        cable2 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

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
                                 step_num=1)

        # Add cabling to step 1 from step 0
        cable1 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_k"))

        cable2 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"));        

        # Send a cable to r more than once!
        cable3 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"))

        self.assertEquals(cable1.clean(), None)
        self.assertEquals(cable2.clean(), None)
        self.assertEquals(cable3.clean(), None)
        
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"r\" to transformation at step 1 is cabled more than once",
            step1.clean)
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"r\" to transformation at step 1 is cabled more than once",
            step1.complete_clean)
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"r\" to transformation at step 1 is cabled more than once",
            foo.clean)

    def test_pipeline_with_1_step_and_2_inputs_cabled_more_than_once_different_cables_bad(self):
        """
        Pipeline with 1 step (script_3_product) with 2 inputs / 1 output
        r is cabled more than once (bad)

        Reminder on script_3_product
        Reminder: k is cdt singlet, r is cdt single-row singlet
        """
        
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

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
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_k"))

        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"));        

        # Send a cable to k from r.
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="k"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_2_r"))

        # We don't bother checking cables or propagation.
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"k\" to transformation at step 1 is cabled more than once",
            step1.clean)

    def test_pipeline_with_1_step_and_2_inputs_but_only_first_input_is_cabled_in_step_1_bad(self):
        """
        Pipeline with 1 step with 2 inputs / 1 output
        Only the first input is cabled (bad)
        """

        # Define pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc",
                       user=self.user)
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
            step1.complete_clean)

    def test_pipeline_with_1_step_and_2_inputs_but_only_second_input_is_cabled_in_step_1_bad(self):
        """
        Pipeline with 1 step with 2 inputs / 1 output
        Only the second input is cabled (bad)
        """

        # Define pipeline foo
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

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
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(dest=self.script_3_method.inputs.get(dataset_name="r"),
                               source_step=0,
                               source=foo.inputs.get(dataset_name="pipe_input_2_r"))

        # Step is clean (cables are OK) but not complete (inputs not quenched).
        self.assertEquals(step1.clean(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "Input \"k\" to transformation at step 1 is not cabled",
            step1.complete_clean)

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_a_b_c",
                         dataset_idx=1)
        
        foo.create_input(compounddatatype=self.singlet_cdt,
                         dataset_name="pipe_r",
                         dataset_idx=2,
                         max_row=1,
                         min_row=1)

        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        cable1 = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_a_b_c"))
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2)

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        cable21 = step2.cables_in.create(
            dest=self.script_3_method.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_r"))

        # singlet a_b_c_mean from step 1 feeds into singlet k at step 2
        cable22 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="k"),
            source_step=1,
            source=step1.transformation.outputs.get(
                dataset_name="a_b_c_mean"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_a_b_c",
                         dataset_idx=1)
        
        foo.create_input(compounddatatype=self.singlet_cdt,
                         dataset_name="pipe_r",
                         dataset_idx=2,
                         max_row=1,
                         min_row=1)

        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_a_b_c"))

        # Delete irrelevant output
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="a_b_c_squared"))
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2)

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_r"))

        # singlet a_b_c_mean from step 1 feeds into singlet k at step 2
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="k"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_a_b_c",
                         dataset_idx=1)
        
        foo.create_input(compounddatatype=self.singlet_cdt,
                         dataset_name="pipe_r",
                         dataset_idx=2,
                         max_row=1,
                         min_row=1)

        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_a_b_c"))
        
        # This output required for subsequent steps
        # August 24, 2013: this is now allowed, so no error should be raised.
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="a_b_c_mean"))
        
        step2 = foo.steps.create(transformation=self.script_3_method,
                                 step_num=2)

        # single-row singlet pipe_r from step 0 feeds into r at step 2 
        cable1 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="r"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_r"))

        # singlet a_b_c_mean (deleted!) from step 1 feeds into singlet k at step 2
        cable2 = step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="k"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()


        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_input_1_a_b_c",
                         dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))

        # Delete data in step 1
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_input_1_a_b_c",
                         dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))

        # Delete data in step 1
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_input_1_a_b_c",
                         dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_input_1_a_b_c",
                         dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))

        # Delete data in step 1
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_input_1_a_b_c",
                         dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))
        
        # Delete data in step 1
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_a_b_c",
                         dataset_idx=1)

        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_input_1_a_b_c",
                         dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))

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
        self.assertRaisesRegexp(outcable1.clean(), None)
        self.assertRaisesRegexp(outcable2.clean(), None)
        self.assertRaisesRegexp(foo.clean(), None)

    def test_pipeline_with_1_step_and_2_outputs_outcable_both_outputs_1st_is_deleted_good(self):
        """
        Pipeline 1 output, with an internal step with 1 input and 2 outputs
        Outcable both outputs, and 1st is deleted (OK)
        """
        foo = Pipeline(family=self.DNAcomp_pf,
                       revision_name="transformation.revision_name",
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_input_1_a_b_c",
                         dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))

        # Delete data in step 1
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt,
                         dataset_name="pipe_input_1_a_b_c",
                         dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))

        # Delete data in step 1
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="a_b_c_mean"))

        # Add outcable for 1st output (Which is not deleted)
        outcable1 = foo.create_outcable(
            output_name="output_a_b_c_squared",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        # Add outcable for 2nd output (Which is deleted)
        # August 24, 2013: this is now allowed, so tests should be fine.
        foo.create_outcable(
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
                       revision_desc="transformation.revision_desc",
                       user=self.user)
        foo.save()

        # foo has two inputs which must match inputs for script_2
        foo.create_input(compounddatatype=self.triplet_cdt, dataset_name="pipe_input_1_a_b_c", dataset_idx=1)
        
        # Add script_2 as step 1 method (Has outputs a_b_c_squared and a_b_c_mean)
        step1 = foo.steps.create(transformation=self.script_2_method,
                                 step_num=1)

        # Add cabling to step 1 from step 0
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="a_b_c"),
            source_step=0,
            source=foo.inputs.get(dataset_name="pipe_input_1_a_b_c"))

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
        self.assertEquals(foo.clean(), None)

        foo.create_outputs()
        # The first output should be as follows:
        # compounddatatype: self.triplet_cdt
        # dataset_name: output_a_b_c_squared
        # dataset_idx: 1
        # min_row = None
        # max_row = None
        # The second:
        # self.singlet_cdt, output_a_b_c_mean, 2, None, None
        self.assertEquals(foo.outputs.count(), 2)
        curr_out_1 = foo.outputs.get(dataset_idx=1)
        self.assertEquals(curr_out_1.dataset_name, "output_a_b_c_squared")
        self.assertEquals(curr_out_1.dataset_idx, 1)
        self.assertEquals(curr_out_1.get_cdt(), self.triplet_cdt)
        self.assertEquals(curr_out_1.get_min_row(), None)
        self.assertEquals(curr_out_1.get_max_row(), None)
        curr_out_2 = foo.outputs.get(dataset_idx=2)
        self.assertEquals(curr_out_2.dataset_name, "output_a_b_c_mean")
        self.assertEquals(curr_out_2.dataset_idx, 2)
        self.assertEquals(curr_out_2.get_cdt(), self.singlet_cdt)
        self.assertEquals(curr_out_2.get_min_row(), None)
        self.assertEquals(curr_out_2.get_max_row(), None)

        # Now delete all the output cablings and make new ones; then check
        # and see if create_outputs worked.
        foo.outcables.all().delete()

        # Add outcable for 1st output (Which is not deleted)
        foo.create_outcable(
            output_name="foo",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="a_b_c_squared"))

        self.assertEquals(foo.clean(), None)

        foo.create_outputs()
        # Now there is one output:
        # self.triplet_cdt, "foo", 1, None, None
        self.assertEquals(foo.outputs.count(), 1)
        curr_out_new = foo.outputs.all()[0]
        self.assertEquals(curr_out_new.dataset_name, "foo")
        self.assertEquals(curr_out_new.dataset_idx, 1)
        self.assertEquals(curr_out_new.get_cdt(), self.triplet_cdt)
        self.assertEquals(curr_out_new.get_min_row(), None)
        self.assertEquals(curr_out_new.get_max_row(), None)

    def test_create_outputs_multi_step(self):
        """Testing create_outputs with a multi-step pipeline."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo",
                       revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        step2 = foo.steps.create(transformation=self.DNArecomp_m, step_num=2)
        step2.cables_in.create(
            dest=step2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        step3 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=3)
        step3.cables_in.create(
            dest=step3.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))

        foo.create_outcable(
            output_name="outputone", output_idx=1,
            source_step=3,
            source=step3.transformation.outputs.get(dataset_name="output"))
        foo.create_outcable(
            output_name="outputtwo", output_idx=2,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))
        self.assertEquals(foo.clean(), None)

        foo.create_outputs()
        # The outputs look like:
        # self.DNAoutput_cdt, "outputone", 1, None, None
        # self.DNAinput_cdt, "outputtwo", 2, None, None
        self.assertEquals(foo.outputs.count(), 2)
        curr_out_1 = foo.outputs.get(dataset_idx=1)
        self.assertEquals(curr_out_1.dataset_name, "outputone")
        self.assertEquals(curr_out_1.dataset_idx, 1)
        self.assertEquals(curr_out_1.get_cdt(), self.DNAoutput_cdt)
        self.assertEquals(curr_out_1.get_min_row(), None)
        self.assertEquals(curr_out_1.get_max_row(), None)
        curr_out_2 = foo.outputs.get(dataset_idx=2)
        self.assertEquals(curr_out_2.dataset_name, "outputtwo")
        self.assertEquals(curr_out_2.dataset_idx, 2)
        self.assertEquals(curr_out_2.get_cdt(), self.DNAinput_cdt)
        self.assertEquals(curr_out_2.get_min_row(), None)
        self.assertEquals(curr_out_2.get_max_row(), None)

        # Now recreate them and check it worked
        foo.outcables.all().delete()
        foo.create_outcable(
            output_name="foo", output_idx=1,
            source_step=2,
            source=step2.transformation.outputs.get(dataset_name="recomplemented_seqs"))
        foo.create_outputs()
        # Now the only output is:
        # self.DNAinput_cdt, "foo", 2, None, None
        self.assertEquals(foo.outputs.count(), 1)
        curr_out_new = foo.outputs.all()[0]
        self.assertEquals(curr_out_new.dataset_name, "foo")
        self.assertEquals(curr_out_new.dataset_idx, 1)
        self.assertEquals(curr_out_new.get_cdt(), self.DNAinput_cdt)
        self.assertEquals(curr_out_new.get_min_row(), None)
        self.assertEquals(curr_out_new.get_max_row(), None)

    def test_delete_pipeline(self):
        """Deleting a Pipeline is possible."""
        family = PipelineFamily(user=self.user); family.save()
        pipeline = Pipeline(family=family, user=self.user); pipeline.save()
        self.assertIsNone(pipeline.delete())


class PipelineStepTests(PipelineTestCase):

    def test_pipelineStep_without_pipeline_set_unicode(self):
        """Test unicode representation when no pipeline is set."""
        nopipeline = PipelineStep(step_num=2)
        self.assertEquals(unicode(nopipeline), "2: ")

    def test_pipelineStep_with_pipeline_set_unicode(self):
        """Test unicode representation when pipeline is set."""
        pipelineset = self.DNAcompv1_p.steps.get(step_num=1)
        self.assertEquals(unicode(pipelineset), "1: ")

    def test_pipelineStep_invalid_request_for_future_step_data_clean(self):
        """Bad cabling: step requests data from after its execution step."""
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)

        # Step 1 invalidly requests data from step 2
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)
        cable = step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=2,
            source=foo.inputs.get(dataset_name="oneinput"))
     
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 requests input from a later step",
                cable.clean)
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 requests input from a later step",
                step1.clean)

    def test_pipelineStep_oneStep_cable_to_invalid_step_input_clean(self):
        """Bad cabling: step cables to input not belonging to its transformation."""

        # Define Pipeline
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Define Pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput", dataset_idx=1)

        # Create a step composed of method DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)

        # Reference an invalid input name from step 0
        cable = step1.cables_in.create(
            dest=self.script_1_method.inputs.get(dataset_name="input_tuple"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        
        self.assertRaisesRegexp(ValidationError,
                'Transformation at step 1 does not have input ".*"',
                cable.clean)
        self.assertRaisesRegexp(ValidationError,
                'Transformation at step 1 does not have input ".*"',
                step1.clean)

    def test_pipelineStep_oneStep_valid_cabling_with_valid_delete_clean(self):
        """Test good step cabling with deleted dataset, one-step pipeline."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Define Pipeline input "oneinput"
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1)

        # Add a step
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)

        # Map Pipeline input to step 1
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # Mark step 1 "output" as deletable
        # step 1 "output" is defined by DNAcompv2_m
        step1.add_deletion(
            step1.transformation.outputs.get(dataset_name="output"))

        self.assertEquals(step1.clean(), None)

    def test_pipelineStep_oneStep_valid_cabling_bad_delete_clean(self):
        """Bad cabling: deleting dataset that doesn't belong to this step, one-step pipeline."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Add a valid pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt, dataset_name="oneinput", dataset_idx=1)

        # Define valid pipeline step
        step1 = foo.steps.create(transformation=self.DNAcompv2_m, step_num=1)

        # Create input cabling for this step
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # Reference TransformationOutput not belonging to this step's
        # transformation.
        step1.add_deletion(self.script_2_method.outputs.all()[0])
        self.assertRaisesRegexp(ValidationError,
                'Transformation at step 1 does not have output ".*"',
                step1.clean)
         
    def test_pipelineStep_oneStep_cabling_directly_self_referential_transformation_clean(self):
        """Bad step: pipeline step contains the parent pipeline directly."""

        # Define pipeline
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Give it a single validly indexed pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1)

        # Add a valid step 1, but reference itself as the transformation
        step1 = foo.steps.create(transformation=foo, step_num=1)
        self.assertRaisesRegexp(ValidationError,
                "Step 1 contains the parent pipeline",
                step1.clean)
         
    def test_pipelineStep_oneStep_cabling_referenced_pipeline_references_parent_clean (self):
        """Bad step: pipeline step contains the parent pipeline in its lone recursive sub-step."""
        # Define pipeline 'foo'
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()

        # Give it a single validly indexed pipeline input
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="oneinput",
                          dataset_idx=1)

        # Define step 1 as executing DNAcompv2_m
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)

        # Map the input at stpe 1 from Pipeline input "oneinput"
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))

        # Define pipeline output at index 1 from (step 1, output "output")
        foo.create_outcable(
            output_name="oneoutput",
            output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        foo.create_outputs()
        foo.save()

        # Define a second pipeline
        bar = Pipeline(family=self.DNAcomp_pf, revision_name="bar", revision_desc="Bar version", user=self.user)
        bar.save()

        # Give it a single validly indexed pipeline input
        bar.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="barinput",
                          dataset_idx=1)

        # At step 1, execute the transformation 'foo' defined above
        bstep1 = bar.steps.create(transformation=foo,
                                  step_num=1)

        # Map to foo.input "oneinput" from bar pipeline output "barinput"
        bstep1.cables_in.create(
            dest=foo.inputs.get(dataset_name="oneinput"),
            source_step=0,
            source=bar.inputs.get(dataset_name="barinput"))

        # Map a single output, from step 1 foo.output = "oneoutput"
        bar.create_outcable(
            output_name="baroutput",
            output_idx=1,
            source_step=1,
            source=bstep1.transformation.outputs.get(dataset_name="oneoutput"))
        bar.save()

        # Now refine foo's step 1 to point to bar
        step1.delete()
        foo.outputs.all().delete()

        # Have step 1 of foo point to bar (But bar points to foo!)
        badstep = foo.steps.create(transformation=bar,
                                   step_num=1)
        
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                badstep.clean)
         
    def test_pipelineStep_manySteps_cabling_referenced_pipeline_references_parent_clean(self):
        """Bad step: pipeline step contains the parent pipeline in some recursive sub-step."""

        # foo invokes DNAcompv2_m at step 1
        foo = Pipeline(family=self.DNAcomp_pf, revision_name="foo", revision_desc="Foo version", user=self.user)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput",
                         dataset_idx=1)
        step1 = foo.steps.create(transformation=self.DNAcompv2_m,
                                 step_num=1)
        step1.cables_in.create(
            dest=step1.transformation.inputs.get(dataset_name="input"),
            source_step=0,
            source=foo.inputs.get(dataset_name="oneinput"))
        foo.create_outcable(
            output_name="oneoutput", output_idx=1,
            source_step=1,
            source=step1.transformation.outputs.get(dataset_name="output"))
        foo.create_outputs()
        foo.save()

        # bar invokes foo at step 1 and DNArecomp_m at step 2
        bar = Pipeline(family=self.DNAcomp_pf, revision_name="bar", revision_desc="Bar version", user=self.user)
        bar.save()
        bar.create_input(compounddatatype=self.DNAinput_cdt,
                          dataset_name="barinput",
                          dataset_idx=1)
        bstep1 = bar.steps.create(transformation=foo,
                                  step_num=1)
        
        bstep1.cables_in.create(
            dest=bstep1.transformation.inputs.get(dataset_name="oneinput"),
            source_step=0,
            source=bar.inputs.get(dataset_name="barinput"))
        
        bstep2 = bar.steps.create(transformation=self.DNArecomp_m,
                                  step_num=2)
        bstep2.cables_in.create(
            dest=bstep2.transformation.inputs.get(dataset_name="complemented_seqs"),
            source_step=1,
            source=bstep1.transformation.outputs.get(dataset_name="oneoutput"))
        bar.create_outcable(
            output_name="baroutputone",
            output_idx=1,
            source_step=1,
            source=bstep1.transformation.outputs.get(dataset_name="oneoutput"))
        bar.create_outcable(
            output_name="baroutputtwo",
            output_idx=2,
            source_step=2,
            source=bstep2.transformation.outputs.get(dataset_name="recomplemented_seqs"))
        bar.save()

        # foo is redefined to be circular
        step1.delete()
        foo.outputs.all().delete()
        badstep = foo.steps.create(transformation=bar,
                                   step_num=1)
        self.assertRaisesRegexp(
                ValidationError,
                "Step 1 contains the parent pipeline",
                badstep.clean)

    def test_pipelinestep_outputs_to_delete(self):
        """
        Make sure marking an output for deletion actually does so.
        """
        step = self.DNAcompv1_p.steps.first()
        output = step.transformation.outputs.first()
        step.add_deletion(output)
        self.assertEqual(len(step.outputs_to_retain()), 0)
        self.assertEqual(step.outputs_to_delete.count(), 1)
        step.outputs_to_delete.remove(output)
        self.assertEqual(len(step.outputs_to_retain()), 1)
        self.assertEqual(step.outputs_to_delete.count(), 0)

    def test_delete_pipeline_step(self):
        """Deleting a PipelineStep is possible."""
        PipelineStep.objects.first().delete()


class PipelineStepInputCableTests(PipelineTestCase):
    def test_delete_pipeline_step_input_cable(self):
        """Deleting a PipelineStepInputCable is possible."""
        self.assertIsNone(PipelineStepInputCable.objects.first().delete())


class PipelineOutputCableTests(PipelineTestCase):
    def test_delete_pipeline_output_cable(self):
        """Deleting a PipelineOutputCable is possible."""
        self.assertIsNone(PipelineOutputCable.objects.first().delete())


class PipelineStepRawDeleteTests(PipelineTestCase):

    def test_PipelineStep_clean_raw_output_to_be_deleted_good(self):
        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(
            compounddatatype=self.triplet_cdt,
            dataset_name="a_b_c_squared",
            dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw", dataset_idx=2)
        self.script_4_1_M.clean()

        # Define 1-step pipeline with a single raw pipeline input
        pipeline_1 = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        step1.add_deletion(raw_output)

        self.assertEquals(step1.clean(), None)
        self.assertEquals(pipeline_1.clean(), None)

    def test_PipelineStep_clean_delete_single_existent_raw_to_good(self):
        # Define a single raw output for self.script_4_1_M
        raw_output = self.script_4_1_M.create_output(
            dataset_name="a_b_c_squared_raw", dataset_idx=1)

        # Define 1-step pipeline
        pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version", user=self.user)
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        step1.add_deletion(raw_output)

        self.assertEquals(step1.clean(), None)

    def test_PipelineStep_clean_delete_non_existent_tro_bad(self):
        # Define a 1-step pipeline containing self.script_4_1_M which has a raw_output
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=1)
        pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version", user=self.user)
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a 1-step pipeline containing self.script_4_2_M which has a raw_output
        self.script_4_2_M = Method(revision_name="s42", revision_desc="s42",
                                   family = self.test_MF, driver = self.script_4_1_CRR,
                                   user=self.user)
        self.script_4_2_M.save()
        raw_output_unrelated = self.script_4_2_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=1)
        pipeline_unrelated = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version",
                                                         user=self.user)
        pipeline_unrelated.steps.create(transformation=self.script_4_2_M, step_num=1)

        # For pipeline 1, mark a raw output to be deleted in an unrelated method
        step1.add_deletion(raw_output_unrelated)

        errorMessage = 'Transformation at step 1 does not have output "1: a_b_c_squared_raw"'
        self.assertRaisesRegexp(ValidationError, errorMessage, step1.clean)
        self.assertRaisesRegexp(ValidationError, errorMessage, pipeline_1.clean)
        
    def test_PipelineStep_clean_raw_output_to_be_deleted_in_different_pipeline_bad(self):
        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        self.script_4_2_M = Method(revision_name="s42", revision_desc="s42",
                                   family = self.test_MF, driver = self.script_4_1_CRR,
                                   user=self.user)
        self.script_4_2_M.save()
        unrelated_raw_output = self.script_4_2_M.create_output(dataset_name="unrelated_raw_output",dataset_idx=1)

        # Define 1-step pipeline with a single raw pipeline input
        pipeline_1 = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define second 1-step pipeline with a single raw pipeline input
        pipeline_2 = self.test_PF.members.create(revision_name="bar",revision_desc="Bar version", user=self.user)
                                                 
        pipeline_2.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        pipeline_2.steps.create(transformation=self.script_4_2_M,step_num=1)

        # For pipeline 1, mark a raw output to be deleted in a different pipeline (pipeline_2)
        step1.add_deletion(unrelated_raw_output)

        error_msg = 'Transformation at step 1 does not have output "1: unrelated_raw_output"'
        self.assertRaisesRegexp(ValidationError, error_msg, step1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg, pipeline_1.clean)


class RawOutputCableTests(PipelineTestCase):

    def test_PipelineOutputCable_raw_outcable_references_valid_step_good(self):

        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.outputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

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
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.outputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        # Define 2-step pipeline with a single raw pipeline input
        pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)
        pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=2)

        # Outmap a raw cable from a valid step + valid output
        outcable1 = pipeline_1.create_raw_outcable(raw_output_name="validName",
                                                    raw_output_idx=1,
                                                    source_step=1,
                                                    source=raw_output)

        # It's not actually deleted yet - so no error
        self.assertEquals(outcable1.clean(), None)

        # Mark raw output of step1 as deleted
        step1.add_deletion(raw_output)

        # Now it's deleted.
        # NOTE August 23, 2013: this doesn't break anymore.
        self.assertEquals(outcable1.clean(), None)
        self.assertEquals(pipeline_1.clean(), None)
        self.assertEquals(step1.clean(), None)

    def test_PipelineOutputCable_raw_outcable_references_valid_step_but_invalid_raw_TO_bad(self):
        
        # Define 1 raw input, and 1 raw + 1 CSV (self.triplet_cdt) output for method self.script_4_1_M
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.outputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        # Define an unrelated method and give it a raw output
        unrelated_method = Method(
            revision_name="s4 - unrelated", revision_desc="s4 - unrelated",
            family = self.test_MF, driver = self.script_4_1_CRR, user=self.user
        )
        unrelated_method.save()
        unrelated_method.clean()
        unrelated_raw_output = unrelated_method.create_output(dataset_name="unrelated raw output",dataset_idx=1)

        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Outmap a raw cable to a valid step but a TransformationRawOutput that does not exist at the specified PS
        outcable1 = self.pipeline_1.outcables.create(
            output_name="validName",
            output_idx=1,
            source_step=1,
            source=unrelated_raw_output)

        self.assertRaisesRegexp(
            ValidationError,
            'Transformation at step 1 does not produce output "{}"'.format(unrelated_raw_output),
            outcable1.clean)

    def test_PipelineOutputCable_raw_outcable_references_invalid_step_bad(self):
        
        # Define 1 raw input, and 1 raw + 1 CSV (self.triplet_cdt) output for method self.script_4_1_M
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.outputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        raw_output = self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Outmap a raw cable to an invalid step
        outcable1 = self.pipeline_1.outcables.create(output_name="validName",
                output_idx=1, source_step=2, source=raw_output)

        error_msg = "Output requested from a non-existent step"

        self.assertRaisesRegexp(ValidationError, error_msg, outcable1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg, self.pipeline_1.clean)
        self.assertRaisesRegexp(ValidationError, error_msg,
                                self.pipeline_1.complete_clean)


class RawInputCableTests(PipelineTestCase):
    def test_PSIC_raw_cable_comes_from_pipeline_input_good(self):
        """
        Cable is fed from a pipeline input.
        """

        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.outputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)

        # Define 2 identical steps within the pipeline
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)
        step2 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=2)

        # Cable the pipeline input into step1's transformation's only raw input hole
        rawcable1 = step1.create_raw_cable(
            dest=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        rawcable2 = step2.create_raw_cable(
            dest=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        # These raw cables were both cabled from the pipeline input and are valid
        self.assertEquals(rawcable1.clean(), None)
        self.assertEquals(rawcable2.clean(), None)

    def test_PSIC_raw_cable_leads_to_foreign_pipeline_bad(self):
        """
        Destination must belong to a PS Transformation in THIS pipeline.
        """
        # Define a single raw input, and a raw + CSV (self.triplet_cdt) output for self.script_4_1_M
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.outputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define two different 1-step pipelines with 1 raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        self.pipeline_2 = self.test_PF.members.create(revision_name="v2", revision_desc="Second version",
                                                      user=self.user)
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
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.outputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c_method",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)

        # Define second unrelated method not part of any pipeline but containing a raw input with the same name (a_b_c)
        self.script_4_2_M = Method(
            revision_name="s4", revision_desc="s4", 
            family = self.test_MF, driver = self.script_4_1_CRR,
            user=self.user
        )
        self.script_4_2_M.save()
        self.script_4_2_M.create_input(dataset_name="a_b_c_method",dataset_idx=1)

        # Define pipeline with a single raw pipeline input and a single step
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Cable the pipeline input into a raw input hole but from an irrelevent method
        rawcable1 = step1.cables_in.create(
            dest=self.script_4_2_M.inputs.get(dataset_name="a_b_c_method"),
            source_step=0,
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        error_msg = 'Transformation at step 1 does not have input "{}"'.format(rawcable1.dest)
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
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.outputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c",dataset_idx=1)
        self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt, dataset_name="a_b_c_squared",dataset_idx=1)
        self.script_4_1_M.create_output(dataset_name="a_b_c_squared_raw",dataset_idx=2)
        self.script_4_1_M.clean()

        # Define pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)

        # Define 2 identical steps within the pipeline
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)
        step2 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=2)

        # Cable the pipeline input into step1's transformation's only raw input hole
        rawcable1 = step1.create_raw_cable(
            dest=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        step2.create_raw_cable(
            dest=self.script_4_1_M.inputs.get(dataset_name="a_b_c"),
            source=self.pipeline_1.inputs.get(dataset_name="a_b_c_pipeline"))

        # Define custom wiring (totally nonsensical) on rawcable1.
        rawcable1.custom_wires.create(
            source_pin=self.doublet_cdt.members.all()[0],
            dest_pin=self.doublet_cdt.members.all()[0])
        
        self.assertRaisesRegexp(ValidationError,
            re.escape('Cable "{}" is raw and should not have custom wiring defined'.format(rawcable1)),
            rawcable1.clean)


class RawSaveTests(PipelineTestCase):
    def test_method_with_raw_input_defined_do_not_copy_raw_xputs_to_new_revision(self):
        # Give script_4_1_M a raw input
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.create_input(dataset_name="a_b_c", dataset_idx=1)

        # Make a method without a parent
        self.script_4_2_M = Method(
            revision_name="s4", revision_desc="s4", 
            family = self.test_MF, driver = self.script_4_1_CRR,
            user=self.user
        )
        self.script_4_2_M.save()

        # There should be no raw inputs/outputs
        self.assertEqual(self.script_4_2_M.inputs.count(), 0)
        self.assertEqual(self.script_4_2_M.outputs.count(), 0)
        
    def test_method_with_raw_output_defined_do_not_copy_raw_xputs_to_new_revision(self):
        # Give script_4_1_M a raw output
        self.script_4_1_M.create_output(dataset_name="a_b_c", dataset_idx=1)

        # Make a method without a parent
        self.script_4_2_M = Method(revision_name="s4", revision_desc="s4", 
            family = self.test_MF, driver = self.script_4_1_CRR,
            user=self.user
        )
        self.script_4_2_M.save()

        # There should be no raw inputs/outputs
        self.assertEqual(self.script_4_2_M.inputs.count(), 0)
        self.assertEqual(self.script_4_2_M.outputs.count(), 0)

    def test_method_with_no_xputs_defined_copy_raw_xputs_to_new_revision(self):

        # Give script_4_1_M a raw input
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        # Make a method with a parent, and do not specify inputs/outputs
        self.script_4_2_M = Method(
            revision_parent=self.script_4_1_M, revision_name="s4", revision_desc="s4",
            family = self.test_MF, driver = self.script_4_1_CRR,
            user=self.user)
        self.script_4_2_M.save()
        self.script_4_2_M.copy_io_from_parent()

        # The input should have been copied over (SUBOPTIMAL TEST)
        self.assertEqual(self.script_4_1_M.inputs.all()[0].dataset_name,
                         self.script_4_2_M.inputs.all()[0].dataset_name)
        self.assertEqual(self.script_4_1_M.inputs.all()[0].dataset_idx,
                         self.script_4_2_M.inputs.all()[0].dataset_idx)


# August 23, 2013: these are kind of redundant now but what the hey.
class SingleRawInputTests(PipelineTestCase):
    def test_transformation_rawinput_coexists_with_nonraw_inputs_clean_good(self):

        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.create_input(dataset_name = "a_b_c", dataset_idx = 1)

        # Define input "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_input(
            compounddatatype = self.triplet_cdt,
            dataset_name = "a_b_c_squared",
            dataset_idx = 2)
        self.script_4_1_M.save()

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None)
        self.assertEquals(self.script_4_1_M.check_output_indices(), None)
        self.assertEquals(self.script_4_1_M.clean(), None)

    def test_transformation_rawinput_coexists_with_nonraw_inputs_but_not_consecutive_indexed_bad(self):

        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        # Define input name "a_b_c_squared" of type "triplet_cdt" at nonconsecutive index = 3
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 3)
        self.script_4_1_M.save()

        # The indices are not consecutive
        self.assertRaisesRegexp(ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_input_indices)

        self.assertRaisesRegexp(ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean)
        
    def test_PipelineStep_completeClean_check_quenching_of_raw_inputs_good(self):
        # Wire 1 raw input to a pipeline step that expects only 1 input
        self.script_4_1_M.inputs.all().delete()
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

        self.assertEquals(step1.clean(), None)
        self.assertEquals(step1.complete_clean(), None)

    def test_PipelineStep_completeClean_check_overquenching_doubled_source_of_raw_inputs_bad(self):

        # Wire 1 raw input to a pipeline step that expects only 1 input
        self.script_4_1_M.inputs.all().delete()
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

        step1.create_raw_cable(
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
        self.script_4_1_M.inputs.all().delete()
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        pipeline_input_2 = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline_2",dataset_idx=2)

        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

        step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input_2)

        errorMessage = "Input \"a_b_c\" to transformation at step 1 is cabled more than once"
        self.assertRaisesRegexp(ValidationError, errorMessage, step1.clean)
        self.assertRaisesRegexp(ValidationError, errorMessage,
                step1.complete_clean)

    def test_PipelineStep_completeClean_check_underquenching_of_raw_inputs_bad(self):
        # Wire 1 raw input to a pipeline step that expects only 1 input
        self.script_4_1_M.inputs.all().delete()
        self.script_4_1_M.create_input(dataset_name = "a_b_c", dataset_idx = 1)
        
        # Define 1-step pipeline with a single raw pipeline input
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M, step_num=1)

        errorMessage = "Input \"a_b_c\" to transformation at step 1 is not cabled'"
        self.assertEquals(step1.clean(), None)
        self.assertRaisesRegexp(
            ValidationError,
            errorMessage,
            step1.complete_clean)


class SeveralRawInputsTests(PipelineTestCase):
    def test_transformation_several_rawinputs_coexists_with_several_nonraw_inputs_clean_good(self):
        # Note that this method wouldn't actually run -- inputs don't match.

        self.script_4_1_M.inputs.all().delete()
        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define raw input "RawIn3" at index = 3
        self.script_4_1_M.create_input(dataset_name = "RawIn3",dataset_idx = 3)

        # Define input "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 2)

        # Define input "Input4" of type "doublet_cdt" at index = 4
        self.script_4_1_M.create_input(compounddatatype = self.doublet_cdt,dataset_name = "Input4",dataset_idx = 4)

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None)
        self.assertEquals(self.script_4_1_M.check_output_indices(), None)
        self.assertEquals(self.script_4_1_M.clean(), None)

    def test_transformation_several_rawinputs_several_nonraw_inputs_not1based_bad(self):
        # Note that this method wouldn't actually run -- inputs don't match.
        self.script_4_1_M.inputs.all().delete()

        # Define raw input "a_b_c" at index = 2
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 2)
        
        # Define raw input "RawIn3" at index = 3
        self.script_4_1_M.create_input(dataset_name = "RawIn3",dataset_idx = 3)

        # Define input "a_b_c_squared" of type "triplet_cdt" at index = 4
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 4)

        # Define input "Input4" of type "doublet_cdt" at index = 5
        self.script_4_1_M.create_input(compounddatatype = self.doublet_cdt,dataset_name = "Input4",dataset_idx = 5)

        self.assertRaisesRegexp(ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_input_indices)
        self.assertEquals(self.script_4_1_M.check_output_indices(), None)
        self.assertRaisesRegexp(ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean)

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
            self.script_4_1_M.check_input_indices)
        self.assertEquals(self.script_4_1_M.check_output_indices(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean)

    def test_pipeline_several_rawinputs_coexists_with_several_nonraw_inputs_clean_good(self):
        # Define 1-step pipeline with conflicting inputs
        pipeline_1 = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version",
                                                 user=self.user)
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
        self.script_4_1_M.inputs.all().delete()
        method_raw_in = self.script_4_1_M.create_input(dataset_name = "method_in_1",dataset_idx = 1)
        method_raw_in_2 = self.script_4_1_M.create_input(dataset_name = "method_in_2",dataset_idx = 2)
        
        # Define 1-step pipeline with 2 raw pipeline inputs
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        pipeline_input = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline",dataset_idx=1)
        pipeline_input_2 = self.pipeline_1.create_input(dataset_name="a_b_c_pipeline_2",dataset_idx=2)

        step1 = self.pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        step1.create_raw_cable(
            dest = method_raw_in,
            source = pipeline_input)

        step1.create_raw_cable(
            dest = method_raw_in_2,
            source = pipeline_input_2)

        step1.create_raw_cable(
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
class SingleRawOutputTests(PipelineTestCase):
    def test_transformation_rawoutput_coexists_with_nonraw_outputs_clean_good(self):

        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)

        # Define output name "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 2)
        self.script_4_1_M.save()

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None)
        self.assertEquals(self.script_4_1_M.check_output_indices(), None)
        self.assertEquals(self.script_4_1_M.clean(), None)

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


class SeveralRawOutputsTests(PipelineTestCase):

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
        self.assertEquals(self.script_4_1_M.check_input_indices(), None)
        self.assertEquals(self.script_4_1_M.check_output_indices(), None)
        self.assertEquals(self.script_4_1_M.clean(), None)
        
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
        self.assertEquals(self.script_4_1_M.check_input_indices(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_output_indices)
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean)


class CustomWiringTests(PipelineTestCase):

    def test_CustomCableWire_wires_from_pipeline_input_identical_dt_good(self):
        """Custom wiring that connects identical datatypes together, on a cable leading from pipeline input (not PS output)."""
        # Define a pipeline with single pipeline input of type triplet_cdt
        my_pipeline = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        my_pipeline.inputs.all().delete()
        pipeline_in = my_pipeline.create_input(
            compounddatatype=self.triplet_cdt,
            dataset_name="pipe_in_1",
            dataset_idx=1)

        # Define method to have an input with the same CDT, add it as a step, cable it
        self.testmethod.inputs.all().delete()
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

        self.assertRaisesRegexp(ValidationError,
            'Destination member "string: (b|c)" has no wires leading to it',
            my_cable1.clean_and_completely_wired)

        # Here, we wire the remaining 2 CDT members
        for i in range(2,4):
            my_cable1.custom_wires.create(
                source_pin=pipeline_in.get_cdt().members.get(column_idx=i),
                dest_pin=method_in.get_cdt().members.get(column_idx=i))

        # All the wires are clean - and now the cable is completely wired
        for wire in my_cable1.custom_wires.all():
            self.assertEquals(wire.clean(), None)

        self.assertEquals(my_cable1.clean(), None)
        self.assertEquals(my_cable1.clean_and_completely_wired(), None)

    def test_CustomCableWire_clean_for_datatype_compatibility(self):
        # Wiring test 1 - Datatypes are identical (x -> x)
        # Wiring test 2 - Datatypes are not identical, but compatible (y restricts x, y -> x)
        # Wiring test 3 - Datatypes are not compatible (z does not restrict x, z -> x) 

        # Define 2 CDTs3 datatypes - one identical, one compatible, and
        # one incompatible + make a new CDT composed of them 
        # Regarding datatypes, recall [self.DNA_dt] restricts [self.string_dt]


        # Define a datatype that has nothing to do with anything and have it restrict
        # the builtin Shipyard string Datatype.
        self.incompatible_dt = Datatype(name="Not compatible",
                                        description="A datatype not having anything to do with anything")
        self.incompatible_dt.save()
        self.incompatible_dt.restricts.add(Datatype.objects.get(pk=datatypes.STR_PK))

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
        my_pipeline = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        pipeline_in = my_pipeline.create_input(compounddatatype=cdt_1,dataset_name="pipe_in_1",dataset_idx=1)

        # Define method to have an input with cdt_2, add it as a step, cable it
        self.testmethod.inputs.all().delete()
        method_in = self.testmethod.create_input(dataset_name="TestIn", dataset_idx=1,compounddatatype=cdt_2)
        my_step1 = my_pipeline.steps.create(transformation=self.testmethod, step_num=1)
        my_cable1 = my_step1.cables_in.create(dest=method_in, source_step=0, source=pipeline_in)

        # CDTs are not equal, so this cable requires custom wiring
        self.assertRaisesRegexp(ValidationError,
            'Custom wiring required for cable "{}"'.format(my_cable1),
            my_step1.clean)

        # Wiring case 1: Datatypes are identical (DNA -> DNA)
        wire1 = my_cable1.custom_wires.create(source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
                                              dest_pin=method_in.get_cdt().members.get(column_idx=2))

        # Wiring case 2: Datatypes are compatible (DNA -> string)
        wire2 = my_cable1.custom_wires.create(source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
                                              dest_pin=method_in.get_cdt().members.get(column_idx=1))
        
        # Wiring case 3: Datatypes are compatible (DNA -> incompatible CDT)
        wire3_bad = my_cable1.custom_wires.create(source_pin=pipeline_in.get_cdt().members.get(column_idx=1),
                                                  dest_pin=method_in.get_cdt().members.get(column_idx=3))

        self.assertIsNone(wire1.clean())
        self.assertIsNone(wire2.clean())

        errorMessage = ('The datatype of the source pin "DNANucSeq: col_1" is incompatible with the datatype of the '
                        'destination pin "Not compatible: col_3"')
        
        self.assertRaisesRegexp(ValidationError, errorMessage, wire3_bad.clean)
        self.assertRaisesRegexp(ValidationError, errorMessage, my_cable1.clean)

    def test_CustomCableWire_clean_source_and_dest_pin_do_not_come_from_cdt_bad(self):
        # For source_pin and dest_pin, give a CDTM from an unrelated CDT

        # Define a datatype that has nothing to do with anything.
        self.incompatible_dt = Datatype(name="poop", description="poop!!")
        self.incompatible_dt.save()
        self.incompatible_dt.restricts.add(Datatype.objects.get(pk=datatypes.STR_PK))


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
        method_1 = Method(revision_name="s4", revision_desc="s4", family = self.test_MF, driver = self.script_4_1_CRR,
                          user=self.user)
        method_1.save()
        method_1_in = method_1.create_input(dataset_name="TestIn", dataset_idx=1, compounddatatype=cdt_1)
        
        method_2 = Method(revision_name="s5", revision_desc="s5", family = self.test_MF, driver = self.script_4_1_CRR,
                          user=self.user)
        method_2.save()
        method_2_in = method_2.create_input(dataset_name="TestIn", dataset_idx=1, compounddatatype=cdt_2)

        # Define 2 pipelines
        pipeline_1 = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        pipeline_1_in = pipeline_1.create_input(compounddatatype=cdt_1, dataset_name="pipe_in_1", dataset_idx=1)
        pipeline_1_step = pipeline_1.steps.create(transformation=method_1, step_num=1)
        pipeline_1_cable = pipeline_1_step.cables_in.create(dest=method_1_in, source_step=0, source=pipeline_1_in)

        pipeline_2 = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        pipeline_2_in = pipeline_2.create_input(compounddatatype=cdt_2, dataset_name="pipe_in_1", dataset_idx=1)
        pipeline_2_step = pipeline_2.steps.create(transformation=method_2, step_num=1)
        pipeline_2_cable = pipeline_2_step.cables_in.create(dest=method_2_in, source_step=0, source=pipeline_2_in)

        # Within pipeline_1_cable, wire into method 1 idx 1 (Expects DNA) a dest_pin from pipeline 2 idx 3
        # (incompatible dt, cdtm from unrelated cdt)
        wire1 = pipeline_1_cable.custom_wires.create(source_pin=pipeline_2_in.get_cdt().members.get(column_idx=3),
                                                     dest_pin=method_1_in.get_cdt().members.get(column_idx=1))

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Source pin "{}" does not come from compounddatatype "{}"'
                                          .format(wire1.source_pin, cdt_1)),
                                wire1.clean)
        wire1.delete()

        # Within pipeline_1_cable, wire into method 1 idx 1 (Expects DNA) a dest_pin from pipeline 2 idx 1
        # (same dt, cdtm from unrelated cdt)
        wire1_alt = pipeline_1_cable.custom_wires.create(source_pin=pipeline_2_in.get_cdt().members.get(column_idx=3),
                                                         dest_pin=method_1_in.get_cdt().members.get(column_idx=1))

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Source pin "{}" does not come from compounddatatype "{}"'
                                          .format(wire1_alt.source_pin, cdt_1)),
                                wire1_alt.clean)

        # Try to wire something into cable 2 with a source_pin from cable 1
        wire2 = pipeline_2_cable.custom_wires.create(source_pin=pipeline_1_in.get_cdt().members.get(column_idx=3),
                                                     dest_pin=method_2_in.get_cdt().members.get(column_idx=1))
            
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Source pin "{}" does not come from compounddatatype "{}"'
                                          .format(wire2.source_pin, cdt_2)),
                                wire2.clean)


# August 23, 2013: This is pretty redundant now.
class PipelineOutputCableRawTests(PipelineTestCase):
    
    def test_pipeline_check_for_colliding_outputs_clean_good(self):

        # Define 1-step pipeline with 2 raw pipeline inputs
        self.pipeline_1 = self.test_PF.members.create(revision_name="v1", revision_desc="First version",
                                                      user=self.user)
        self.pipeline_1.create_input(dataset_name="a_b_c_pipeline", dataset_idx=1)
        self.pipeline_1.steps.create(transformation=self.script_4_1_M, step_num=1)

        script_4_1_M = self.script_4_1_M

        script_4_1_M.create_output(
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


class CustomRawOutputCablingTests(PipelineTestCase):

    def test_Pipeline_create_multiple_raw_outputs_with_raw_outmap(self):
        self.my_pipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version",
                                                       user=self.user)

        self.my_pipeline.create_input(
            compounddatatype=self.triplet_cdt,
            dataset_name="pipeline_in_1",
            dataset_idx=1)

        # Give the method self.triplet_cdt output
        method_raw_out = self.testmethod.create_output(
            dataset_name="RawTestOut",
            dataset_idx=1)

        # Add a step
        self.my_pipeline.steps.create(
            transformation=self.testmethod,
            step_num=1)

        # Add raw outmap
        self.my_pipeline.create_raw_outcable(
            raw_output_name="raw_out",
            raw_output_idx=1,
            source_step=1,
            source=method_raw_out)

        self.assertEquals(self.my_pipeline.outputs.all().count(), 0)     
        self.my_pipeline.create_outputs()
        self.assertEquals(self.my_pipeline.outputs.all().count(), 1)

        raw_output = self.my_pipeline.outputs.get(dataset_idx=1)

        self.assertEquals(raw_output.dataset_name, "raw_out")

        # Add another raw outmap
        self.my_pipeline.create_raw_outcable(
            raw_output_name="raw_out_2",
            raw_output_idx=2,
            source_step=1,
            source=method_raw_out)

        self.my_pipeline.create_outputs()
        self.assertEquals(self.my_pipeline.outputs.all().count(), 2)

        raw_output_2 = self.my_pipeline.outputs.get(dataset_idx=2)

        self.assertEquals(raw_output_2.dataset_name, "raw_out_2")

        
class PipelineStepInputCable_tests(PipelineTestCase):

    def test_PSIC_clean_and_completely_wired_CDT_equal_no_wiring_good(self):
        # Define pipeline with mix_triplet_cdt (string, DNA, string) pipeline input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version", user=self.user)
        myPipeline_input = myPipeline.create_input(
            compounddatatype=self.mix_triplet_cdt,
            dataset_name="pipe_in",dataset_idx=1
        )

        # Define method with doublet_cdt input (string, string), add it to the pipeline, and cable it
        m = Method(revision_name="s4", revision_desc="s4", family = self.test_MF, driver = self.script_4_1_CRR,
                   user=self.user)
        m.save()
        method_input = m.create_input(compounddatatype=self.mix_triplet_cdt,dataset_name="method_in", dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=m, step_num=1)
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
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version", user=self.user)
        myPipeline_input = myPipeline.create_input(
            compounddatatype=self.mix_triplet_cdt,
            dataset_name="pipe_in",
            dataset_idx=1
        )

        # Define method with doublet_cdt input (string, string), add it to the pipeline, and cable it
        m = Method(revision_name="s4", revision_desc="s4", family=self.test_MF, driver = self.script_4_1_CRR,
                   user=self.user)
        m.save()
        method_input = m.create_input(compounddatatype=self.doublet_cdt,dataset_name="method_in",dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=m, step_num=1)
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
        wire2 = pipeline_cable.custom_wires.create(source_pin=myPipeline_input.get_cdt().members.get(column_idx=2),
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
        myPipeline = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        myPipeline_input = myPipeline.create_input(compounddatatype=self.mix_triplet_cdt, dataset_name="pipe_in",
                                                   dataset_idx=1)

        # Define method with doublet_cdt input (string, string), add it to the pipeline, and cable it
        m = Method(revision_name="s4", revision_desc="s4", family=self.test_MF, driver=self.script_4_1_CRR,
                   user=self.user)
        m.save()
        method_input = m.create_input(compounddatatype=self.doublet_cdt,dataset_name="method_in", dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=m, step_num=1)
        pipeline_cable = pipelineStep.cables_in.create(dest=method_input, source_step=0, source=myPipeline_input)

        # wire1 = string->string
        wire1 = pipeline_cable.custom_wires.create(source_pin=myPipeline_input.get_cdt().members.get(column_idx=2),
                                                   dest_pin=method_input.get_cdt().members.get(column_idx=2))

        # wire2 = DNA->string
        wire2 = pipeline_cable.custom_wires.create(source_pin=myPipeline_input.get_cdt().members.get(column_idx=2),
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
        myPipeline = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        myPipeline_input = myPipeline.create_input(
            compounddatatype=self.mix_triplet_cdt,
            dataset_name="pipe_in",
            dataset_idx=1
        )

        # Define method with triplet_cdt input (string, string, string), add it to the pipeline, and cable it
        m = Method(revision_name="s4", revision_desc="s4", family=self.test_MF, driver = self.script_4_1_CRR,
                   user=self.user)
        m.save()
        method_input = m.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        pipelineStep = myPipeline.steps.create(transformation=m, step_num=1)
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
        errorMessage = re.escape('Destination member "string: b" has no wires leading to it')
        self.assertRaisesRegexp(ValidationError,errorMessage,pipeline_cable.clean_and_completely_wired)
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelineStep.clean)
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelineStep.complete_clean)
        self.assertRaisesRegexp(ValidationError,errorMessage,myPipeline.complete_clean)

    def _make_log(self, pipeline, output_file, source):
        """
        Helper function to make an ExecLog and RSIC for a pipeline.
        """
        run = pipeline.pipeline_instances.create(user=self.user)
        pipelinestep = self.DNAcompv1_p.steps.first()
        runstep = pipelinestep.pipelinestep_instances.create(run=run)
        psic = pipelinestep.cables_in.first()
        rsic = psic.psic_instances.create(runstep=runstep)
        log = ExecLog(record=rsic, invoking_record=rsic)
        log.save()
        psic.run_cable(source, output_file, rsic, log)
        return log, rsic

    def _setup_dirs(self):
        """
        Helper function to make a temp directory and output file.
        """
        scratch_dir = tempfile.mkdtemp()
        output_file = os.path.join(scratch_dir, "output")
        return scratch_dir, output_file

    def _log_checks(self, log, rsic):
        """
        Helper function to check that an ExecLog made from an RSIC is coherent.
        """
        self.assertEqual(log.record, rsic)
        self.assertEqual(log.start_time.date(), timezone.now().date())
        self.assertEqual(log.end_time.date(), timezone.now().date())
        self.assertEqual(log.start_time < timezone.now(), True)
        self.assertEqual(log.end_time < timezone.now(), True)
        self.assertEqual(log.start_time <= log.end_time, True)
        self.assertEqual(log.is_complete(), True)
        self.assertEqual(log.complete_clean(), None)
        self.assertEqual(len(log.missing_outputs()), 0)
        self.assertEqual(log.is_successful(), True)

    def test_execlog_psic_run_cable_file(self):
        """
        Check the coherence of an ExecLog created by running a cable with a Dataset.
        """
        scratch_dir, output_file = self._setup_dirs()
        log, rsic = self._make_log(self.DNAcompv1_p, output_file, self.datafile.name)
        self._log_checks(log, rsic)
        shutil.rmtree(scratch_dir)


# August 29, 2013: reworked to handle new design for outcables.
class CustomOutputWiringTests(PipelineTestCase):

    def test_CustomOutputCableWire_clean_references_invalid_CDTM(self):
        self.my_pipeline = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version",
                                                       user=self.user)
        self.my_pipeline.create_input(compounddatatype=self.triplet_cdt, dataset_name="pipeline_in_1",
                                      dataset_idx=1)

        # Give the method self.triplet_cdt output
        method_out = self.testmethod.create_output(dataset_name="TestOut", dataset_idx=1,
                                                   compounddatatype=self.triplet_cdt)

        # Add a step
        self.my_pipeline.steps.create(transformation=self.testmethod, step_num=1)

        # Add an output cable
        outcable1 = self.my_pipeline.create_outcable(output_name="blah", output_idx=1, source_step=1, source=method_out)

        # Add custom wiring from an irrelevent CDTM
        badwire = outcable1.custom_wires.create(source_pin=self.doublet_cdt.members.first(),
                                                   dest_pin=self.triplet_cdt.members.first())

        errorMessage = re.escape('Source pin "string: x" does not come from compounddatatype '
                                 '"(string: a, string: b, string: c)"')

        self.assertRaisesRegexp(ValidationError, errorMessage, badwire.clean)
        self.assertRaisesRegexp(ValidationError, errorMessage, outcable1.clean)
        self.assertRaisesRegexp(ValidationError, errorMessage, self.my_pipeline.clean)
        

    def test_Pipeline_create_outputs_for_creation_of_output_CDT(self):
        self.my_pipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version",
                                                       user=self.user)

        self.my_pipeline.create_input(
            compounddatatype=self.triplet_cdt,
            dataset_name="pipeline_in_1",
            dataset_idx=1)

        # Give the method self.triplet_cdt output
        method_out = self.testmethod.create_output(
            dataset_name="TestOut",
            dataset_idx=1,
            compounddatatype=self.mix_triplet_cdt)

        # Add a step
        self.my_pipeline.steps.create(
            transformation=self.testmethod, step_num=1)

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
        outcable1.custom_wires.create(
            source_pin=method_out.get_cdt().members.all()[0],
            dest_pin=pin1)

        outcable1.custom_wires.create(
            source_pin=method_out.get_cdt().members.all()[1],
            dest_pin=pin2)

        outcable1.custom_wires.create(
            source_pin=method_out.get_cdt().members.all()[0],
            dest_pin=pin3)

        outcable1.custom_wires.create(
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


# June 19, 2014: for the functions that serialize and de-serialize Pipelines.
class PipelineSerializationTests(TestCase):
    """
    Tests of Pipeline serialization and deserialization.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        tools.create_sandbox_testing_tools_environment(self)

        # Set up a CDT with two elements to allow some wiring to occur.
        self.STR = Datatype.objects.get(pk=datatypes.STR_PK)

        # A CDT composed of two builtin-STR columns.
        self.string_doublet = CompoundDatatype()
        self.string_doublet.save()
        self.string_doublet.members.create(datatype=self.STR, column_name="column1", column_idx=1)
        self.string_doublet.members.create(datatype=self.STR, column_name="column2", column_idx=2)

        # A CDT composed of one builtin-STR column.
        self.string_singlet = CompoundDatatype()
        self.string_singlet.save()
        self.string_singlet.members.create(datatype=self.STR, column_name="col1", column_idx=1)

    def tearDown(self):
        tools.destroy_sandbox_testing_tools_environment(self)

    def _check_pipeline_own_members(self, dict_repr, pipeline):
        """
        Checks "internal" stuff is correct in a dictionary representation of a Pipeline.
        """
        # Check there are no extraneous keys being defined.
        self.assertSetEqual(
            set(dict_repr.keys()),
            {
                "user",
                "users_allowed",
                "groups_allowed",
                "family_pk",
                "family_name",
                "family_desc",
                "revision_number",
                "revision_name",
                "revision_desc",
                "revision_parent_pk",
                "pipeline_inputs",
                "pipeline_steps",
                "pipeline_outputs",
                "is_published_version"
            })

        self.assertEquals(dict_repr["user"], pipeline.user.pk)
        self.assertSetEqual(
            set(dict_repr["users_allowed"]),
            {u.pk for u in pipeline.users_allowed.all()}
        )
        self.assertSetEqual(
            set(dict_repr["groups_allowed"]),
            {g.pk for g in pipeline.groups_allowed.all()}
        )

        self.assertEquals(dict_repr["family_pk"], pipeline.family.pk)
        self.assertEquals(dict_repr["family_name"], pipeline.family.name)
        self.assertEquals(dict_repr["family_desc"], pipeline.family.description)
        self.assertEquals(dict_repr["revision_name"], pipeline.revision_name)
        self.assertEquals(dict_repr["revision_desc"], pipeline.revision_desc)
        self.assertEquals(dict_repr["revision_number"], pipeline.revision_number)
        self.assertEquals(dict_repr["is_published_version"], pipeline.is_published_version)

        dict_rev_parent_pk = None if pipeline.revision_parent is None else pipeline.revision_parent.pk
        self.assertEquals(dict_repr["revision_parent_pk"], dict_rev_parent_pk)

    def _check_pipeline(self, dict_repr, pipeline):
        """
        Checks correctness of a dictionary representation of a Pipeline including all children.
        """
        self._check_pipeline_own_members(dict_repr, pipeline)

        # First check the inputs.
        self.assertEquals(len(dict_repr["pipeline_inputs"]), pipeline.inputs.count())
        for input_dict in dict_repr["pipeline_inputs"]:
            corresp_input = pipeline.inputs.get(dataset_idx=input_dict["dataset_idx"])
            self._check_input(input_dict, corresp_input)

        # Next the steps.
        self.assertEquals(len(dict_repr["pipeline_steps"]), pipeline.steps.count())
        for step_dict in dict_repr["pipeline_steps"]:
            corresp_step = pipeline.steps.get(step_num=step_dict["step_num"])
            self._check_step(step_dict, corresp_step)

        # Finally the outcables.
        self.assertEquals(len(dict_repr["pipeline_outputs"]), pipeline.outcables.count())
        for outcable_dict in dict_repr["pipeline_outputs"]:
            corresp_outcable = pipeline.outcables.get(output_idx=outcable_dict["output_idx"])
            self._check_outcable(outcable_dict, corresp_outcable)

    def _check_step_own_members(self, step_dict, step):
        """
        Checks "internal" stuff is correct in a dictionary representation of a PipelineStep.
        """
        # Check for extraneous keys.
        self.assertSetEqual(
            set(step_dict.keys()),
            set([
                "transf_pk", "transf_type", "step_num", "x", "y", "name",
                 "cables_in", "outputs_to_delete", "family_pk"
            ]))

        self.assertEquals(step_dict["transf_pk"], step.transformation.definite.pk)
        transf_type_str = "Method" if type(step.transformation.definite) == Method else "Pipeline"
        self.assertEquals(step_dict["transf_type"], transf_type_str)
        self.assertEquals(step_dict["step_num"], step.step_num)
        self.assertEquals(step_dict["x"], step.x)
        self.assertEquals(step_dict["y"], step.y)
        self.assertEquals(step_dict["name"], step.name)

    def _check_step(self, step_dict, step):
        """
        Checks correctness of a dictionary representation of a PS including cabling.
        """
        self._check_step_own_members(step_dict, step)

        self.assertEquals(len(step_dict["cables_in"]), step.cables_in.count())
        for incable_dict in step_dict["cables_in"]:
            corresp_incable = step.cables_in.get(dest__dataset_name=incable_dict["dest_dataset_name"])
            self._check_incable(incable_dict, corresp_incable)

        for deleted_output_name in step_dict["outputs_to_delete"]:
            corresp_output = step.outputs.get(dataset_name=deleted_output_name)
            self.assertTrue(step.outputs_to_delete.filter(pk=corresp_output.pk).exists())

    def _check_input(self, input_dict, TI):
        """
        Checks correctness of a dictionary representation of a Pipeline input.
        """
        # Check for extraneous keys.
        self.assertSetEqual(
            set(input_dict.keys()),
            set(["CDT_pk", "dataset_name", "dataset_idx", "x", "y", "min_row", "max_row"])
        )
        my_cdt_pk = None if TI.compounddatatype is None else TI.compounddatatype.pk
        self.assertEquals(input_dict["CDT_pk"], my_cdt_pk)
        self.assertEquals(input_dict["dataset_name"], TI.dataset_name)
        self.assertEquals(input_dict["dataset_idx"], TI.dataset_idx)
        self.assertEquals(input_dict["x"], TI.x)
        self.assertEquals(input_dict["y"], TI.y)

        effective_min_row = None if TI.get_min_row() is None else TI.get_min_row()
        effective_max_row = None if TI.get_max_row() is None else TI.get_max_row()
        self.assertEquals(input_dict["min_row"], effective_min_row)
        self.assertEquals(input_dict["max_row"], effective_max_row)

    def _check_incable_own_members(self, incable_dict, input_cable):
        """
        Checks correctness of a dictionary representation of a PSIC.
        """
        # Check for extraneous keys.
        self.assertSetEqual(
            set(incable_dict.keys()),
            set(["source_dataset_name", "source_step", "dest_dataset_name", "keep_output", "wires"])
        )
        self.assertEquals(incable_dict["source_dataset_name"], input_cable.source.definite.dataset_name)
        self.assertEquals(incable_dict["source_step"], input_cable.source_step)
        self.assertEquals(incable_dict["dest_dataset_name"], input_cable.dest.definite.dataset_name)
        self.assertEquals(incable_dict["keep_output"], input_cable.keep_output)

    def _check_incable(self, incable_dict, input_cable):
        """
        Checks correctness of a PSIC's dictionary serialization including wiring.
        """
        self._check_incable_own_members(incable_dict, input_cable)

        self.assertEquals(len(incable_dict["wires"]), input_cable.custom_wires.count())
        for wire_dict in incable_dict["wires"]:
            corresp_wire = input_cable.custom_wires.get(dest_pin__column_idx=wire_dict["dest_idx"])
            self._check_wire(wire_dict, corresp_wire)

    def _check_wire(self, wire_dict, wire):
        """
        Check correctness of a dictionary representation of a CCW.
        """
        # Check for extraneous keys.
        self.assertSetEqual(
            set(wire_dict.keys()),
            set(["source_idx", "dest_idx"])
        )
        self.assertEquals(wire_dict["source_idx"], wire.source_pin.column_idx)
        self.assertEquals(wire_dict["dest_idx"], wire.dest_pin.column_idx)

    def _check_outcable_own_members(self, outcable_dict, outcable):
        """
        Checks correctness of a dictionary representation of a POC.
        """
        # Check for extraneous keys.
        self.assertSetEqual(
            set(outcable_dict.keys()),
            set(["output_idx", "output_name", "output_CDT_pk", "source_step", "source_dataset_name",
                 "x", "y", "wires"])
        )
        self.assertEquals(outcable_dict["output_idx"], outcable.output_idx)
        self.assertEquals(outcable_dict["output_name"], outcable.output_name)
        out_cdt_pk = None if outcable.output_cdt is None else outcable.output_cdt.pk
        self.assertEquals(outcable_dict["output_CDT_pk"], out_cdt_pk)
        self.assertEquals(outcable_dict["source_step"], outcable.source_step)
        self.assertEquals(outcable_dict["source_dataset_name"], outcable.source.definite.dataset_name)

    def _check_outcable(self, outcable_dict, outcable):
        """
        Checks correctness of a dictionary representation of a POC including wiring.
        """
        self._check_outcable_own_members(outcable_dict, outcable)

        self.assertEquals(len(outcable_dict["wires"]), outcable.custom_wires.count())
        for wire_dict in outcable_dict["wires"]:
            corresp_wire = outcable.custom_wires.get(dest_pin__column_idx=wire_dict["dest_idx"])
            self._check_wire(wire_dict, corresp_wire)

    def test_serialize_input(self):
        """Serializing a Pipeline input."""
        my_pipeline = tools.make_first_pipeline("serialize input", "For testing serializing input", self.user_bob)

        input_1 = my_pipeline.create_input("foo", 1, compounddatatype=self.string_doublet)
        input_raw = my_pipeline.create_input("bar", 2, compounddatatype=None, x=0.2, y=0.3)
        input_3 = my_pipeline.create_input("baz", 3, compounddatatype=self.cdt_string, min_row=5, max_row=50)

        self._check_input(input_1.represent_as_dict(), input_1)
        self._check_input(input_raw.represent_as_dict(), input_raw)
        self._check_input(input_3.represent_as_dict(), input_3)

    def test_deserialize_input(self):
        """
        Define a Pipeline input from a dictionary.
        """
        my_pipeline = tools.make_first_pipeline("deserialize input", "For testing deserializing input", self.user_bob)

        input_1_dict = {
            "dataset_name": "foo",
            "dataset_idx": 1,
            "CDT_pk": self.string_doublet.pk,
            "x": 0.2,
            "y": 0.3,
            "min_row": None,
            "max_row": None
        }

        input_raw_dict = {
            "dataset_name": "bar",
            "dataset_idx": 2,
            "CDT_pk": None,
            "x": 0.2,
            "y": 0.3,
            "min_row": None,
            "max_row": None
        }

        input_3_dict = {
            "dataset_name": "baz",
            "dataset_idx": 3,
            "CDT_pk": self.cdt_string.pk,
            "x": 0,
            "y": 0,
            "min_row": 5,
            "max_row": 50
        }

        input_1 = my_pipeline.create_input_from_dict(input_1_dict)
        self._check_input(input_1_dict, input_1)

        input_raw = my_pipeline.create_input_from_dict(input_raw_dict)
        self._check_input(input_raw_dict, input_raw)

        input_3 = my_pipeline.create_input_from_dict(input_3_dict)
        self._check_input(input_3_dict, input_3)

    def test_serialize_incable_no_wires(self):
        """Serializing PSICs with no wiring."""
        my_pipeline = tools.make_first_pipeline("serialize PSIC", "For testing serializing PSIC", self.user_bob)
        tools.create_linear_pipeline(my_pipeline, [self.method_noop, self.method_noop, self.method_noop],
                                     "input_to_not_touch", "untouched_output")

        # All of the input cables in this Pipeline are unwired.
        for step in my_pipeline.steps.all():
            curr_incable = step.cables_in.first()
            self._check_incable(curr_incable.represent_as_dict(), curr_incable)

    def test_deserialize_incable_no_wires(self):
        """Define PSICs with no wiring from dictionaries."""
        my_pipeline = tools.make_first_pipeline("deserialize PSIC", "For testing deserializing PSIC", self.user_bob)
        my_pipeline.create_input("input_to_not_touch", 1, compounddatatype=self.cdt_string)
        step_1 = my_pipeline.steps.create(step_num=1, transformation=self.method_noop)

        incable_1_dict = {
            "source_dataset_name": "input_to_not_touch",
            "source_step": 0,
            "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
            "keep_output": False,
            "wires": []
        }
        incable_1 = step_1.create_incable_from_dict(incable_1_dict)
        self._check_incable(incable_1_dict, incable_1)

        step_2 = my_pipeline.steps.create(step_num=2, transformation=self.method_noop)
        incable_2_dict = {
            "source_dataset_name": self.method_noop.outputs.first().dataset_name,
            "source_step": 1,
            "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
            "keep_output": False,
            "wires": []
        }
        incable_2 = step_2.create_incable_from_dict(incable_2_dict)
        self._check_incable(incable_2_dict, incable_2)

        step_3 = my_pipeline.steps.create(step_num=3, transformation=self.method_noop)
        incable_3_dict = {
            "source_dataset_name": self.method_noop.outputs.first().dataset_name,
            "source_step": 2,
            "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
            "keep_output": False,
            "wires": []
        }
        incable_3 = step_3.create_incable_from_dict(incable_3_dict)
        self._check_incable(incable_3_dict, incable_3)

    def test_serialize_raw_incables(self):
        """Serializing raw PSICs with no wiring."""
        my_pipeline = tools.make_first_pipeline("serialize raw PSIC", "For testing serializing raw PSIC", self.user_bob)
        tools.create_linear_pipeline(my_pipeline, [self.method_noop_raw, self.method_noop_raw, self.method_noop_raw],
                                     "input_to_not_touch", "untouched_output")

        # All of the input cables in this Pipeline are unwired.
        for step in my_pipeline.steps.all():
            curr_incable = step.cables_in.first()
            self._check_incable(curr_incable.represent_as_dict(), curr_incable)

    def test_deserialize_raw_incables(self):
        """Define raw PSICs from dictionaries."""
        my_pipeline = tools.make_first_pipeline("de-serialize raw PSIC", "For testing de-serializing raw PSIC",
                                                self.user_bob)
        my_pipeline.create_input("input_to_not_touch", 1, compounddatatype=None)
        step_1 = my_pipeline.steps.create(step_num=1, transformation=self.method_noop_raw)

        incable_1_dict = {
            "source_dataset_name": "input_to_not_touch",
            "source_step": 0,
            "dest_dataset_name": self.method_noop_raw.inputs.first().dataset_name,
            "keep_output": False,
            "wires": []
        }
        incable_1 = step_1.create_incable_from_dict(incable_1_dict)
        self._check_incable(incable_1_dict, incable_1)

        step_2 = my_pipeline.steps.create(step_num=2, transformation=self.method_noop_raw,)
        incable_2_dict = {
            "source_dataset_name": self.method_noop_raw.outputs.first().dataset_name,
            "source_step": 1,
            "dest_dataset_name": self.method_noop_raw.inputs.first().dataset_name,
            "keep_output": False,
            "wires": []
        }
        incable_2 = step_2.create_incable_from_dict(incable_2_dict)
        self._check_incable(incable_2_dict, incable_2)

        step_3 = my_pipeline.steps.create(step_num=3, transformation=self.method_noop_raw)
        incable_3_dict = {
            "source_dataset_name": self.method_noop_raw.outputs.first().dataset_name,
            "source_step": 2,
            "dest_dataset_name": self.method_noop_raw.inputs.first().dataset_name,
            "keep_output": False,
            "wires": []
        }
        incable_3 = step_3.create_incable_from_dict(incable_3_dict)
        self._check_incable(incable_3_dict, incable_3)

    def test_serialize_wire(self):
        """Serializing a CustomCableWire."""
        my_pipeline = tools.make_first_pipeline("serialize wire", "For testing serializing a wire",
                                                self.user_bob)
        input_1 = my_pipeline.create_input("pi", 1, compounddatatype=self.string_doublet)

        self.method_doublet_noop = tools.make_first_method(
            "string doublet noop",
            "a noop on a two-column input",
            self.coderev_noop,
            self.user_bob)
        tools.simple_method_io(self.method_doublet_noop, self.string_doublet, "doublets", "untouched_doublets")

        first_step = my_pipeline.steps.create(step_num=1, transformation=self.method_doublet_noop)
        first_cable = first_step.cables_in.create(source=input_1, source_step=0,
                                                  dest=self.method_doublet_noop.inputs.first())
        wire_1 = first_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=1),
                                                 dest_pin=self.string_doublet.members.get(column_idx=2))
        wire_2 = first_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=2),
                                                 dest_pin=self.string_doublet.members.get(column_idx=1))
        # Test cable from Pipeline input.
        self._check_wire(wire_1.represent_as_dict(), wire_1)
        self._check_wire(wire_2.represent_as_dict(), wire_2)

    def test_deserialize_wire(self):
        """Defining a CustomCableWire from a dictionary."""
        my_pipeline = tools.make_first_pipeline("de-serialize wire", "For testing de-serializing a wire",
                                                self.user_bob)
        input_1 = my_pipeline.create_input("pi", 1, compounddatatype=self.string_doublet)

        self.method_doublet_noop = tools.make_first_method(
            "string doublet noop",
            "a noop on a two-column input",
            self.coderev_noop,
            self.user_bob)
        tools.simple_method_io(self.method_doublet_noop, self.string_doublet, "doublets", "untouched_doublets")

        first_step = my_pipeline.steps.create(step_num=1, transformation=self.method_doublet_noop)
        first_cable = first_step.cables_in.create(source=input_1, source_step=0,
                                                  dest=self.method_doublet_noop.inputs.first())

        wire_1_dict = {"source_idx": 1, "dest_idx": 2}
        wire_2_dict = {"source_idx": 2, "dest_idx": 1}
        wire_1 = first_cable.create_wire_from_dict(wire_1_dict)
        wire_2 = first_cable.create_wire_from_dict(wire_2_dict)

        # Test cable from Pipeline input.
        self._check_wire(wire_1_dict, wire_1)
        self._check_wire(wire_2_dict, wire_2)

    def test_serialize_incable_non_trivial(self):
        """Serializing a PSIC with wiring."""
        my_pipeline = tools.make_first_pipeline("serialize non-trivial PSIC",
                                                "For testing serializing non-trivial PSIC",
                                                self.user_bob)
        input_1 = my_pipeline.create_input("pi", 1, compounddatatype=self.string_doublet)

        self.method_doublet_noop = tools.make_first_method(
            "string doublet noop",
            "a noop on a two-column input",
            self.coderev_noop,
            self.user_bob)
        tools.simple_method_io(self.method_doublet_noop, self.string_doublet, "doublets", "untouched_doublets")

        first_step = my_pipeline.steps.create(step_num=1, transformation=self.method_doublet_noop)
        first_cable = first_step.cables_in.create(source=input_1, source_step=0,
                                                  dest=self.method_doublet_noop.inputs.first())
        first_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=1),
                                        dest_pin=self.string_doublet.members.get(column_idx=2))
        first_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=2),
                                        dest_pin=self.string_doublet.members.get(column_idx=1))
        # Test cable from Pipeline input.
        self._check_incable(first_cable.represent_as_dict(), first_cable)

        second_step = my_pipeline.steps.create(step_num=2, transformation=self.method_doublet_noop)
        second_cable = second_step.cables_in.create(source=self.method_doublet_noop.outputs.first(),
                                                    source_step=1,
                                                    dest=self.method_doublet_noop.inputs.first())
        second_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=1),
                                         dest_pin=self.string_doublet.members.get(column_idx=2))
        second_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=2),
                                         dest_pin=self.string_doublet.members.get(column_idx=1))
        # Test cable from PipelineStep.
        self._check_incable(second_cable.represent_as_dict(), second_cable)

    def test_deserialize_incable_non_trivial(self):
        """Defining PSICs with wiring from dictionaries."""
        my_pipeline = tools.make_first_pipeline("deserialize non-trivial PSIC",
                                               "For testing deserializing non-trivial PSIC",
                                               self.user_bob)
        input_1 = my_pipeline.create_input("pi", 1, compounddatatype=self.string_doublet)

        self.method_doublet_noop = tools.make_first_method(
            "string doublet noop",
            "a noop on a two-column input",
            self.coderev_noop,
            self.user_bob)
        tools.simple_method_io(self.method_doublet_noop, self.string_doublet, "doublets", "untouched_doublets")

        first_step = my_pipeline.steps.create(step_num=1, transformation=self.method_doublet_noop)

        first_cable_dict = {
            "source_dataset_name": input_1.dataset_name,
            "source_step": 0,
            "dest_dataset_name": self.method_doublet_noop.inputs.first().dataset_name,
            "keep_output": False,
            "wires": [
                {"source_idx": 1, "dest_idx": 2},
                {"source_idx": 2, "dest_idx": 1}
            ]
        }
        first_cable = first_step.create_incable_from_dict(first_cable_dict)
        self._check_incable(first_cable_dict, first_cable)

        second_step = my_pipeline.steps.create(step_num=2, transformation=self.method_doublet_noop)

        second_cable_dict = {
            "source_dataset_name": self.method_doublet_noop.outputs.first().dataset_name,
            "source_step": 1,
            "dest_dataset_name": self.method_doublet_noop.inputs.first().dataset_name,
            "keep_output": False,
            "wires": [
                {"source_idx": 1, "dest_idx": 2},
                {"source_idx": 2, "dest_idx": 1}
            ]
        }
        second_cable = second_step.create_incable_from_dict(second_cable_dict)
        self._check_incable(second_cable_dict, second_cable)

    def test_serialize_incable_non_trivial_deleted(self):
        """Serializing a PSIC with wiring that keeps its output."""
        my_pipeline = tools.make_first_pipeline("serialize non-trivial PSIC",
                                                "For testing serializing non-trivial PSIC",
                                                self.user_bob)
        input_1 = my_pipeline.create_input("pi", 1, compounddatatype=self.string_doublet)

        self.method_doublet_noop = tools.make_first_method(
            "string doublet noop",
            "a noop on a two-column input",
            self.coderev_noop,
            self.user_bob)
        tools.simple_method_io(self.method_doublet_noop, self.string_doublet, "doublets", "untouched_doublets")

        first_step = my_pipeline.steps.create(step_num=1, transformation=self.method_doublet_noop)
        first_cable = first_step.cables_in.create(source=input_1, source_step=0,
                                                  dest=self.method_doublet_noop.inputs.first())
        first_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=1),
                                        dest_pin=self.string_doublet.members.get(column_idx=2))
        first_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=2),
                                        dest_pin=self.string_doublet.members.get(column_idx=1))
        first_cable.keep_output = True
        first_cable.save()
        # Test cable from Pipeline input.
        self._check_incable(first_cable.represent_as_dict(), first_cable)

        second_step = my_pipeline.steps.create(step_num=2, transformation=self.method_doublet_noop)
        second_cable = second_step.cables_in.create(source=self.method_doublet_noop.outputs.first(),
                                                    source_step=1,
                                                    dest=self.method_doublet_noop.inputs.first())
        second_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=1),
                                         dest_pin=self.string_doublet.members.get(column_idx=2))
        second_cable.custom_wires.create(source_pin=self.string_doublet.members.get(column_idx=2),
                                         dest_pin=self.string_doublet.members.get(column_idx=1))
        second_cable.keep_output = True
        second_cable.save()
        # Test cable from PipelineStep.
        self._check_incable(second_cable.represent_as_dict(), second_cable)

    def test_deserialize_incable_non_trivial_deleted(self):
        """Defining a PSIC with wiring that keeps its output using a dictionary."""
        my_pipeline = tools.make_first_pipeline("de-serialize non-trivial PSIC",
                                                "For testing de-serializing non-trivial PSIC",
                                                self.user_bob)
        my_pipeline.create_input("pi", 1, compounddatatype=self.string_doublet)

        self.method_doublet_noop = tools.make_first_method(
            "string doublet noop",
            "a noop on a two-column input",
            self.coderev_noop,
            self.user_bob)
        tools.simple_method_io(self.method_doublet_noop, self.string_doublet, "doublets", "untouched_doublets")

        first_step = my_pipeline.steps.create(step_num=1, transformation=self.method_doublet_noop)
        first_cable_dict = {
            "source_dataset_name": "pi",
            "source_step": 0,
            "dest_dataset_name": self.method_doublet_noop.inputs.first().dataset_name,
            "keep_output": True,
            "wires": [
                {"source_idx": 1, "dest_idx": 2},
                {"source_idx": 2, "dest_idx": 1}
            ]
        }
        first_cable = first_step.create_incable_from_dict(first_cable_dict)
        # Test cable from Pipeline input.
        self._check_incable(first_cable_dict, first_cable)

        second_step = my_pipeline.steps.create(step_num=2, transformation=self.method_doublet_noop)
        second_cable_dict = {
            "source_dataset_name": self.method_doublet_noop.outputs.first().dataset_name,
            "source_step": 1,
            "dest_dataset_name": self.method_doublet_noop.inputs.first().dataset_name,
            "keep_output": True,
            "wires": [
                {"source_idx": 1, "dest_idx": 2},
                {"source_idx": 2, "dest_idx": 1}
            ]
        }
        second_cable = second_step.create_incable_from_dict(second_cable_dict)
        # Test cable from PipelineStep.
        self._check_incable(second_cable_dict, second_cable)

    def test_serialize_step_one_input_no_deletions(self):
        """Serializing a PS."""
        my_pipeline = tools.make_first_pipeline(
            "serialize PS", "For testing serializing PSs with one input and no deletions", self.user_bob
        )
        input_1 = my_pipeline.create_input("pi", 1, compounddatatype=self.cdt_string)

        first_step = my_pipeline.steps.create(step_num=1, transformation=self.method_noop)
        first_step.cables_in.create(source=input_1, source_step=0,
                                                  dest=self.method_noop.inputs.first())

        second_step = my_pipeline.steps.create(step_num=2, transformation=self.method_noop,
                                               name="noop2", x=50, y=200)
        second_step.cables_in.create(source=self.method_noop.outputs.first(),
                                                    source_step=1,
                                                    dest=self.method_noop.inputs.first())

        self._check_step(first_step.represent_as_dict(), first_step)
        self._check_step(second_step.represent_as_dict(), second_step)

    def test_deserialize_step_one_input_no_deletions(self):
        """Defining a PS from a dictionary."""
        my_pipeline = tools.make_first_pipeline(
            "de-serialize PS", "For testing de-serializing PSs with one input and no deletions", self.user_bob
        )
        my_pipeline.create_input("pi", 1, compounddatatype=self.cdt_string)

        first_step_dict = {
            "transf_pk": self.method_noop.pk,
            "family_pk": self.method_noop.family.pk,
            "transf_type": "Method",
            "step_num": 1,
            "x": 0,
            "y": 0,
            "name": "",
            "cables_in": [
                {
                    "source_dataset_name": "pi",
                    "source_step": 0,
                    "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
                    "keep_output": False,
                    "wires": []
                }
            ],
            "outputs_to_delete": []
        }
        first_step = my_pipeline.create_PS_from_dict(first_step_dict)
        self._check_step(first_step_dict, first_step)

        second_step_dict = {
            "transf_pk": self.method_noop.pk,
            "family_pk": self.method_noop.family.pk,
            "transf_type": "Method",
            "step_num": 2,
            "x": 50,
            "y": 200,
            "name": "noop2",
            "cables_in": [
                {
                    "source_dataset_name": self.method_noop.outputs.first().dataset_name,
                    "source_step": 1,
                    "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
                    "keep_output": False,
                    "wires": []
                }
            ],
            "outputs_to_delete": []
        }
        second_step = my_pipeline.create_PS_from_dict(second_step_dict)
        self._check_step(second_step_dict, second_step)

    def test_serialize_step_subpipeline(self):
        """Serializing a PS whose Transformation is a Pipeline."""
        # First, define a simple sub-Pipeline.
        sub_pipeline = tools.make_first_pipeline(
            "sub-Pipeline", "For use as a sub-Pipeline", self.user_bob
        )
        tools.create_linear_pipeline(sub_pipeline, [self.method_noop, self.method_noop], "sp_in1", "sp_out1")

        my_pipeline = tools.make_first_pipeline(
            "serialize PS with deletion", "For testing serializing PSs with one input and only output deleted",
            self.user_bob
        )
        input_1 = my_pipeline.create_input("pi", 1, compounddatatype=self.cdt_string)

        first_step = my_pipeline.steps.create(step_num=1, transformation=sub_pipeline)
        first_step.cables_in.create(source=input_1, source_step=0,
                                                  dest=sub_pipeline.inputs.first())
        self._check_step(first_step.represent_as_dict(), first_step)

    def test_deserialize_step_subpipeline(self):
        """Define a PS whose Transformation is a Pipeline from a dictionary."""
        # First, define a simple sub-Pipeline as above.
        sub_pipeline = tools.make_first_pipeline(
            "sub-Pipeline", "For use as a sub-Pipeline", self.user_bob
        )
        tools.create_linear_pipeline(sub_pipeline, [self.method_noop, self.method_noop], "sp_in1", "sp_out1")

        my_pipeline = tools.make_first_pipeline(
            "de-serialize PS with deletion", "For testing de-serializing PSs with one input and only output deleted",
            self.user_bob
        )
        my_pipeline.create_input("pi", 1, compounddatatype=self.cdt_string)

        first_step_dict = {
            "transf_pk": sub_pipeline.pk,
            "transf_type": "Pipeline",
            "family_pk": sub_pipeline.family.pk,
            "step_num": 1,
            "x": 0,
            "y": 0,
            "name": "",
            "cables_in": [
                {
                    "source_dataset_name": "pi",
                    "source_step": 0,
                    "dest_dataset_name": "sp_in1",
                    "keep_output": False,
                    "wires": []
                }
            ],
            "outputs_to_delete": []
        }
        first_step = my_pipeline.create_PS_from_dict(first_step_dict)
        self._check_step(first_step_dict, first_step)

    def test_serialize_step_one_input_output_deleted(self):
        """Serializing a PS with one input and one output (which is deleted)."""
        my_pipeline = tools.make_first_pipeline(
            "serialize PS with deletion", "For testing serializing PSs with one input and only output deleted",
            self.user_bob
        )
        input_1 = my_pipeline.create_input("pi", 1, compounddatatype=self.cdt_string)

        first_step = my_pipeline.steps.create(step_num=1, transformation=self.method_noop)
        first_step.cables_in.create(source=input_1, source_step=0,
                                                  dest=self.method_noop.inputs.first())
        first_step.add_deletion(self.method_noop.outputs.first())

        second_step = my_pipeline.steps.create(step_num=2, transformation=self.method_noop)
        second_step.cables_in.create(source=self.method_noop.outputs.first(),
                                                    source_step=1,
                                                    dest=self.method_noop.inputs.first())
        second_step.add_deletion(self.method_noop.outputs.first())

        self._check_step(first_step.represent_as_dict(), first_step)
        self._check_step(second_step.represent_as_dict(), second_step)

    def test_deserialize_step_one_input_output_deleted(self):
        """Defining a PS with one input and one output (which is deleted) from a dictionary."""
        my_pipeline = tools.make_first_pipeline(
            "de-serialize PS with deletion", "For testing de-serializing PSs with one input and only output deleted",
            self.user_bob
        )
        my_pipeline.create_input("pi", 1, compounddatatype=self.cdt_string)

        first_step_dict = {
            "transf_pk": self.method_noop.pk,
            "family_pk": self.method_noop.family.pk,
            "transf_type": "Method",
            "step_num": 1,
            "x": 50,
            "y": 200,
            "name": "noop1",
            "cables_in": [
                {
                    "source_dataset_name": "pi",
                    "source_step": 0,
                    "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
                    "keep_output": False,
                    "wires": []
                }
            ],
            "outputs_to_delete": [self.method_noop.outputs.first().dataset_name]
        }
        first_step = my_pipeline.create_PS_from_dict(first_step_dict)
        self._check_step(first_step_dict, first_step)

        second_step_dict = {
            "transf_pk": self.method_noop.pk,
            "family_pk": self.method_noop.family.pk,
            "transf_type": "Method",
            "step_num": 2,
            "x": 150,
            "y": 200,
            "name": "noop1",
            "cables_in": [
                {
                    "source_dataset_name": self.method_noop.outputs.first().dataset_name,
                    "source_step": 1,
                    "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
                    "keep_output": False,
                    "wires": []
                }
            ],
            "outputs_to_delete": [self.method_noop.outputs.first().dataset_name]
        }
        second_step = my_pipeline.create_PS_from_dict(second_step_dict)
        self._check_step(second_step_dict, second_step)

    def test_serialize_step_several_inputs_outputs(self):
        """Serializing a PS with several inputs and outputs."""
        # Define a simple CodeResource and first revision for a method with multiple inputs and outputs.
        coderev_3cat = tools.make_first_revision(
            "threeintwoout",
            "Sticks together two compatible CSV files and passes through a third file",
            "threeintwoout.sh",
            """#!/bin/bash -e
cat "$1" > "$4"
tail -n +2 "$2" >> "$4"
cat "$3" >> "$5"
""",
            self.user_bob)

        # The corresponding method.
        method_threetwo_string_doublet = tools.make_first_method(
            "string doublet three-in two-out",
            "appends two compatible CSV files and passes through a third",
            coderev_3cat,
            self.user_bob)
        method_threetwo_string_doublet.create_input("firstfile", 1, compounddatatype=self.string_doublet)
        method_threetwo_string_doublet.create_input("secondfile", 2, compounddatatype=self.string_doublet)
        method_threetwo_string_doublet.create_input("third_file", 3, compounddatatype=self.cdt_string)
        mo_1 = method_threetwo_string_doublet.create_output("combinedfile", 1, compounddatatype=self.string_doublet)
        mo_2 = method_threetwo_string_doublet.create_output("passedfile", 2, compounddatatype=self.cdt_string)

        my_pipeline = tools.make_first_pipeline(
            "serialize multi-input PS", "For testing serializing PSs with several inputs",
            self.user_bob
        )
        input_1 = my_pipeline.create_input("pi_1", 1, compounddatatype=self.string_doublet)
        input_2 = my_pipeline.create_input("pi_2", 2, compounddatatype=self.string_doublet)
        input_3 = my_pipeline.create_input("pi_3", 3, compounddatatype=self.cdt_string)

        first_step = my_pipeline.steps.create(step_num=1, transformation=method_threetwo_string_doublet)
        first_step.cables_in.create(source=input_1, source_step=0,
                                              dest=method_threetwo_string_doublet.inputs.get(dataset_idx=1))
        first_step.cables_in.create(source=input_2, source_step=0,
                                              dest=method_threetwo_string_doublet.inputs.get(dataset_idx=2))
        first_step.cables_in.create(source=input_3, source_step=0,
                                              dest=method_threetwo_string_doublet.inputs.get(dataset_idx=3))

        self._check_step(first_step.represent_as_dict(), first_step)

        # Try it with deletions:
        first_step.add_deletion(mo_1)
        self._check_step(first_step.represent_as_dict(), first_step)

        first_step.outputs_to_delete.remove(mo_1)
        self.assertEquals(first_step.outputs_to_delete.count(), 0)
        first_step.add_deletion(mo_2)
        self.assertEquals(first_step.outputs_to_delete.count(), 1)
        self._check_step(first_step.represent_as_dict(), first_step)

        first_step.add_deletion(mo_1)
        self.assertEquals(first_step.outputs_to_delete.count(), 2)
        self._check_step(first_step.represent_as_dict(), first_step)

    def test_deserialize_step_several_inputs_outputs(self):
        """Defining a PS with several inputs and outputs from a dictionary."""
        # Define a simple CodeResource and first revision for a method with multiple inputs and outputs.
        coderev_3cat = tools.make_first_revision(
            "threeintwoout",
            "Sticks together two compatible CSV files and passes through a third file",
            "threeintwoout.sh",
            """#!/bin/bash -e
cat "$1" > "$4"
tail -n +2 "$2" >> "$4"
cat "$3" >> "$5"
""",
            self.user_bob)

        # The corresponding method.
        method_threetwo_string_doublet = tools.make_first_method(
            "string doublet three-in two-out",
            "appends two compatible CSV files and passes through a third",
            coderev_3cat,
            self.user_bob)
        method_threetwo_string_doublet.create_input("firstfile", 1, compounddatatype=self.string_doublet)
        method_threetwo_string_doublet.create_input("secondfile", 2, compounddatatype=self.string_doublet)
        method_threetwo_string_doublet.create_input("third_file", 3, compounddatatype=self.cdt_string)
        mo_1 = method_threetwo_string_doublet.create_output("combinedfile", 1, compounddatatype=self.string_doublet)
        mo_2 = method_threetwo_string_doublet.create_output("passedfile", 2, compounddatatype=self.cdt_string)

        my_pipeline = tools.make_first_pipeline(
            "de-serialize multi-input PS", "For testing de-serializing PSs with several inputs",
            self.user_bob
        )
        my_pipeline.create_input("pi_1", 1, compounddatatype=self.string_doublet)
        my_pipeline.create_input("pi_2", 2, compounddatatype=self.string_doublet)
        my_pipeline.create_input("pi_3", 3, compounddatatype=self.cdt_string)

        first_step_dict = {
            "transf_pk": method_threetwo_string_doublet.pk,
            "family_pk": method_threetwo_string_doublet.family.pk,
            "transf_type": "Method",
            "step_num": 1,
            "x": 50,
            "y": 200,
            "name": "first step (defined from dictionary)",
            "cables_in": [
                {
                    "source_dataset_name": "pi_1",
                    "source_step": 0,
                    "dest_dataset_name": method_threetwo_string_doublet.inputs.get(dataset_idx=1).dataset_name,
                    "keep_output": False,
                    "wires": []
                },
                {
                    "source_dataset_name": "pi_2",
                    "source_step": 0,
                    "dest_dataset_name": method_threetwo_string_doublet.inputs.get(dataset_idx=2).dataset_name,
                    "keep_output": False,
                    "wires": []
                },
                {
                    "source_dataset_name": "pi_3",
                    "source_step": 0,
                    "dest_dataset_name": method_threetwo_string_doublet.inputs.get(dataset_idx=3).dataset_name,
                    "keep_output": False,
                    "wires": []
                }
            ],
            "outputs_to_delete": []
        }
        first_step = my_pipeline.create_PS_from_dict(first_step_dict)
        self._check_step(first_step_dict, first_step)

        # Try it with deletions:
        first_step.delete()
        first_step_dict["outputs_to_delete"] = [mo_1.dataset_name]
        first_step = my_pipeline.create_PS_from_dict(first_step_dict)
        self._check_step(first_step_dict, first_step)

        first_step.delete()
        first_step_dict["outputs_to_delete"] = [mo_2.dataset_name]
        first_step = my_pipeline.create_PS_from_dict(first_step_dict)
        self._check_step(first_step_dict, first_step)

        first_step.delete()
        first_step_dict["outputs_to_delete"] = [mo_1.dataset_name, mo_2.dataset_name]
        first_step = my_pipeline.create_PS_from_dict(first_step_dict)
        self._check_step(first_step_dict, first_step)

    def test_serialize_outcable_no_wires(self):
        """Serializing a POC with no custom wiring."""
        my_pipeline = tools.make_first_pipeline("two-step noop", "Double no-op", self.user_bob)
        tools.create_linear_pipeline(my_pipeline, [self.method_noop, self.method_noop],
                                     "input_to_not_touch", "untouched_output")

        for outcable in my_pipeline.outcables.all():
            self._check_outcable(outcable.represent_as_dict(), outcable)

    def test_deserialize_outcable_no_wires(self):
        """Serializing a POC with no custom wiring."""
        my_pipeline = tools.make_first_pipeline("two-step noop", "Double no-op", self.user_bob)
        tools.create_linear_pipeline(my_pipeline, [self.method_noop, self.method_noop],
                                     "input_to_not_touch", "untouched_output")

        # Throw out the outcables and define my own.
        for outcable in my_pipeline.outcables.all():
            outcable.delete()
        for output in my_pipeline.outputs.all():
            output.delete()

        outcable_dict = {
            "output_idx": 1,
            "output_name": "untouched_output",
            "output_CDT_pk": self.cdt_string.pk,
            "source_step": 2,
            "source_dataset_name": self.method_noop.outputs.get(dataset_idx=1).dataset_name,
            "x": 500,
            "y": 500,
            "wires": []
        }
        outcable = my_pipeline.create_outcable_from_dict(outcable_dict)
        self._check_outcable(outcable_dict, outcable)

    def test_serialize_raw_outcable(self):
        """Serializing a raw POC."""
        my_pipeline = tools.make_first_pipeline("two-step raw noop", "Double raw no-op", self.user_bob)
        tools.create_linear_pipeline(my_pipeline, [self.method_noop_raw, self.method_noop_raw],
                                     "input_to_not_touch", "untouched_output")

        for outcable in my_pipeline.outcables.all():
            self._check_outcable(outcable.represent_as_dict(), outcable)

    def test_deserialize_raw_outcable(self):
        """Defining a raw POC from a dictionary."""
        my_pipeline = tools.make_first_pipeline("two-step raw noop", "Double raw no-op", self.user_bob)
        tools.create_linear_pipeline(my_pipeline, [self.method_noop_raw, self.method_noop_raw],
                                     "input_to_not_touch", "untouched_output")

        # Throw out the outcables and define my own.
        for outcable in my_pipeline.outcables.all():
            outcable.delete()
        for output in my_pipeline.outputs.all():
            output.delete()

        raw_outcable_dict = {
            "output_idx": 1,
            "output_name": "untouched_output",
            "output_CDT_pk": None,
            "source_step": 2,
            "source_dataset_name": self.method_noop_raw.outputs.get(dataset_idx=1).dataset_name,
            "x": 500,
            "y": 500,
            "wires": []
        }
        raw_outcable = my_pipeline.create_outcable_from_dict(raw_outcable_dict)
        self._check_outcable(raw_outcable_dict, raw_outcable)

    def test_serialize_outcable_custom_wires(self):
        """Serializing a POC with custom wiring."""
        my_pipeline = tools.make_first_pipeline(
            "custom-wired outcable", "For testing serialization of a POC with custom wiring", self.user_bob
        )

        # self.method_noop takes cdt_string, which is built on datatype_str, which is not the same as self.STR.
        method_builtin_STR_noop = tools.make_first_method(
            "STR noop", "a method to do nothing to builtin-STRs", self.coderev_noop, self.user_bob
        )
        tools.simple_method_io(method_builtin_STR_noop, self.string_singlet, "strings", "same_strings")

        my_pipeline.create_input("foo", 1, compounddatatype=self.string_singlet)

        my_pipeline.steps.create(step_num=1, transformation=method_builtin_STR_noop)

        outcable = my_pipeline.outcables.create(
            output_name="bar", output_idx=1, source_step=1, source=method_builtin_STR_noop.outputs.first(),
            output_cdt=self.string_doublet
        )
        # This requires wiring.
        outcable.custom_wires.create(source_pin=self.string_singlet.members.first(),
                                     dest_pin=self.string_doublet.members.get(column_idx=1))
        outcable.custom_wires.create(source_pin=self.string_singlet.members.first(),
                                     dest_pin=self.string_doublet.members.get(column_idx=2))
        my_pipeline.create_outputs()

        self._check_outcable(outcable.represent_as_dict(), outcable)

    def test_deserialize_outcable_custom_wires(self):
        """Defining a POC with custom wiring using a dictionary."""
        my_pipeline = tools.make_first_pipeline(
            "custom-wired outcable", "For testing de-serialization of a POC with custom wiring", self.user_bob
        )

        # self.method_noop takes cdt_string, which is built on datatype_str, which is not the same as self.STR.
        method_builtin_STR_noop = tools.make_first_method(
            "STR noop", "a method to do nothing to builtin-STRs", self.coderev_noop, self.user_bob
        )
        tools.simple_method_io(method_builtin_STR_noop, self.string_singlet, "strings", "same_strings")

        my_pipeline.create_input("foo", 1, compounddatatype=self.string_singlet)

        my_pipeline.steps.create(step_num=1, transformation=method_builtin_STR_noop)

        wired_outcable_dict = {
            "output_idx": 1,
            "output_name": "bar",
            "output_CDT_pk": self.string_doublet.pk,
            "source_step": 1,
            "source_dataset_name": method_builtin_STR_noop.outputs.get(dataset_idx=1).dataset_name,
            "x": 750,
            "y": 550,
            "wires": [
                {"source_idx": 1, "dest_idx": 1},
                {"source_idx": 1, "dest_idx": 2}
            ]
        }
        wired_outcable = my_pipeline.create_outcable_from_dict(wired_outcable_dict)

        self._check_outcable(wired_outcable_dict, wired_outcable)

    def _setup_pipeline_dict(self, state):
        """
        Dictionaries that define a Pipeline in varying stages of completion.
        """
        pipeline_dict = {
            "user": self.user_bob.pk,
            "users_allowed": [],
            "groups_allowed": [],

            "family_pk": None,
            "family_name": "test",
            "family_desc": "Test family",

            "revision_name": "v1",
            "revision_desc": "first version",
            "revision_number": 1,
            "revision_parent_pk": None,

            "pipeline_inputs": [],
            "pipeline_steps": [],
            "pipeline_outputs": [],
            "is_published_version": False
        }

        if state == "empty":
            return pipeline_dict

        # Add an input and two steps.
        pipeline_dict["pipeline_inputs"] = [
                {
                    "CDT_pk": self.cdt_string.pk,
                    "dataset_name": "input_to_not_touch",
                    "dataset_idx": 1,
                    "x": 0.05,
                    "y": 0.5,
                    "min_row": None,
                    "max_row": None
                }
            ]
        pipeline_dict["pipeline_steps"] = [
                {
                    "transf_pk": self.method_noop.pk,
                    "transf_type": "Method",
                    "family_pk": self.method_noop.family.pk,
                    "step_num": 1,
                    "x": 0.2,
                    "y": 0.5,
                    "name": "step 1",
                    "cables_in": [
                        {
                            "source_dataset_name": "input_to_not_touch",
                            "source_step": 0,
                            "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
                            "keep_output": False,
                            "wires": []
                        }
                    ],
                    "outputs_to_delete": []
                },
                {
                    "transf_pk": self.method_noop.pk,
                    "family_pk": self.method_noop.family.pk,
                    "transf_type": "Method",
                    "step_num": 2,
                    "x": 0.4,
                    "y": 0.5,
                    "name": "step 2",
                    "cables_in": [
                        {
                            "source_dataset_name": self.method_noop.outputs.first().dataset_name,
                            "source_step": 1,
                            "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
                            "keep_output": False,
                            "wires": []
                        }
                    ],
                    "outputs_to_delete": []
                }
            ]

        if state == "incomplete":
            return pipeline_dict

        # Add the last step and an output cable.
        pipeline_dict["pipeline_steps"].append(
            {
                "transf_pk": self.method_noop.pk,
                "transf_type": "Method",
                "family_pk": self.method_noop.family.pk,
                "step_num": 3,
                "x": 0.6,
                "y": 0.5,
                "name": "step 3",
                "cables_in": [
                    {
                        "source_dataset_name": self.method_noop.outputs.first().dataset_name,
                        "source_step": 2,
                        "dest_dataset_name": self.method_noop.inputs.first().dataset_name,
                        "keep_output": False,
                        "wires": []
                    }
                ],
                "outputs_to_delete": []
            })
        pipeline_dict["pipeline_outputs"] = [
            {
                "output_idx": 1,
                "output_name": "untouched_output",
                "output_CDT_pk": self.cdt_string.pk,
                "source_step": 3,
                "source_dataset_name": self.method_noop.outputs.first().dataset_name,
                "x": 0.85,
                "y": 0.5,
                "wires": []
            }
        ]

        return pipeline_dict

    def test_serialize_empty_pipeline(self):
        """Serializing an empty Pipeline."""
        my_pipeline = tools.make_first_pipeline("test", "Test family", self.user_bob)

        my_dict = my_pipeline.represent_as_dict()

        self._check_pipeline_own_members(my_dict, my_pipeline)
        self.assertListEqual(my_dict["pipeline_inputs"], [])
        self.assertListEqual(my_dict["pipeline_steps"], [])
        self.assertListEqual(my_dict["pipeline_outputs"], [])

    def test_deserialize_empty_pipeline(self):
        """Defining an empty Pipeline from a dictionary."""
        empty_pipeline_dict = self._setup_pipeline_dict("empty")
        my_pipeline = Pipeline.create_from_dict(empty_pipeline_dict)
        # This created a PipelineFamily, so we update pipeline_dict["family_pk"]
        # to match.
        empty_pipeline_dict["family_pk"] = my_pipeline.family.pk
        self._check_pipeline(empty_pipeline_dict, my_pipeline)

    def test_serialize_incomplete_pipeline(self):
        """Serializing an incomplete Pipeline."""
        my_pipeline = tools.make_first_pipeline("three-step noop", "Triple no-op", self.user_bob)
        tools.create_linear_pipeline(my_pipeline, [self.method_noop, self.method_noop, self.method_noop],
                                     "input_to_not_touch", "untouched_output")

        for outcable in my_pipeline.outcables.all():
            outcable.delete()

        # Delete step 3.
        my_pipeline.steps.get(step_num=3).delete()

        my_dict = my_pipeline.represent_as_dict()

        self._check_pipeline(my_dict, my_pipeline)

    def test_deserialize_incomplete_pipeline(self):
        """Defining an incomplete Pipeline from a dictionary."""
        incomplete_pipeline_dict = self._setup_pipeline_dict("incomplete")
        my_pipeline = Pipeline.create_from_dict(incomplete_pipeline_dict)
        # Get the PipelineFamily PK:
        incomplete_pipeline_dict["family_pk"] = my_pipeline.family.pk
        self._check_pipeline(incomplete_pipeline_dict, my_pipeline)

    def test_serialize_pipeline(self):
        """Serializing a complete Pipeline."""
        my_pipeline = tools.make_first_pipeline("three-step noop", "Triple no-op", self.user_bob)
        tools.create_linear_pipeline(my_pipeline, [self.method_noop, self.method_noop, self.method_noop],
                                     "input_to_not_touch", "untouched_output")

        self._check_pipeline(my_pipeline.represent_as_dict(), my_pipeline)

    def test_deserialize_pipeline(self):
        """Defining a complete Pipeline from a dictionary."""
        complete_pipeline_dict = self._setup_pipeline_dict("complete")
        complete_pipeline = Pipeline.create_from_dict(complete_pipeline_dict)

        # Get the PipelineFamily PK:
        complete_pipeline_dict["family_pk"] = complete_pipeline.family.pk

        self._check_pipeline(complete_pipeline_dict, complete_pipeline)

    def test_serialize_pipeline_multiple_steps_multiple_outputs(self):
        """Serializing a more complicated Pipeline."""
        coderev_twocat = tools.make_first_revision(
            "twocat",
            "Sticks together two compatible CSV files",
            "twocat.sh",
            """#!/bin/bash -e
cat "$1" > "$3"
tail -n +2 "$2" >> "$3"
""",
            self.user_bob)

        # The corresponding method.
        method_twocat_string_doublet = tools.make_first_method(
            "string doublet two-cat",
            "appends two string-doublet CSV files",
            coderev_twocat,
            self.user_bob)
        doublet_i1 = method_twocat_string_doublet.create_input("firstfile", 1, compounddatatype=self.string_doublet)
        doublet_i2 = method_twocat_string_doublet.create_input("secondfile", 2, compounddatatype=self.string_doublet)
        doublet_o1 = method_twocat_string_doublet.create_output("combinedfile", 1, compounddatatype=self.string_doublet)

        method_twocat_string_singlet = tools.make_first_method(
            "string singlet two-cat",
            "appends two string-singlet CSV files",
            coderev_twocat,
            self.user_bob)
        singlet_i1 = method_twocat_string_singlet.create_input("firstfile", 1, compounddatatype=self.string_singlet)
        singlet_i2 = method_twocat_string_singlet.create_input("secondfile", 2, compounddatatype=self.string_singlet)
        singlet_o1 = method_twocat_string_singlet.create_output("combinedfile", 1, compounddatatype=self.string_singlet)

        my_pipeline = tools.make_first_pipeline("more complicated Pipeline", "For testing serialization of Pipelines",
                                                self.user_bob)
        input_1 = my_pipeline.create_input("pi_1", 1, compounddatatype=self.string_doublet)
        input_2 = my_pipeline.create_input("pi_2", 2, compounddatatype=self.string_doublet)
        input_3 = my_pipeline.create_input("pi_3", 3, compounddatatype=self.string_singlet)

        step_1 = my_pipeline.steps.create(step_num=1, transformation=method_twocat_string_doublet)
        step_1.cables_in.create(source=input_1, source_step=0, dest=doublet_i1)
        step_1.cables_in.create(source=input_2, source_step=0, dest=doublet_i2)

        # Cable the output of step 1 to step 2's input.
        step_2 = my_pipeline.steps.create(step_num=2, transformation=method_twocat_string_doublet)
        step_2.cables_in.create(source=input_3, source_step=0, dest=singlet_i1)
        # This one needs wiring.
        s2c2 = step_2.cables_in.create(source=doublet_o1, source_step=1, dest=singlet_i2, keep_output=True)
        s2c2.custom_wires.create(source_pin=self.string_doublet.members.last(),
                                 dest_pin=self.string_singlet.members.first())

        # Create outcables, one from step 1 and another from step 2 (rewired to string_doublet).
        my_pipeline.outcables.create(
            output_name="output_1", output_idx=1, source_step=1, source=doublet_o1, output_cdt=self.string_doublet
        )
        oc2 = my_pipeline.outcables.create(
            output_name="output_2", output_idx=2, source_step=2, source=singlet_o1, output_cdt=self.string_doublet
        )
        oc2.custom_wires.create(source_pin=self.string_singlet.members.first(),
                                dest_pin=self.string_doublet.members.get(column_idx=1))
        oc2.custom_wires.create(source_pin=self.string_singlet.members.first(),
                                dest_pin=self.string_doublet.members.get(column_idx=2))

        my_pipeline.create_outputs()

        self._check_pipeline(my_pipeline.represent_as_dict(), my_pipeline)

    def test_deserialize_pipeline_multiple_steps_multiple_outputs(self):
        """Defining a more complicated Pipeline from a dictionary."""
        coderev_twocat = tools.make_first_revision(
            "twocat",
            "Sticks together two compatible CSV files",
            "twocat.sh",
            """#!/bin/bash -e
cat "$1" > "$3"
tail -n +2 "$2" >> "$3"
""",
            self.user_bob)

        # The corresponding method.
        method_twocat_string_doublet = tools.make_first_method(
            "string doublet two-cat",
            "appends two string-doublet CSV files",
            coderev_twocat,
            self.user_bob)
        doublet_i1 = method_twocat_string_doublet.create_input("firstfile", 1, compounddatatype=self.string_doublet)
        doublet_i2 = method_twocat_string_doublet.create_input("secondfile", 2, compounddatatype=self.string_doublet)
        doublet_o1 = method_twocat_string_doublet.create_output("combinedfile", 1, compounddatatype=self.string_doublet)

        method_twocat_string_singlet = tools.make_first_method(
            "string singlet two-cat",
            "appends two string-singlet CSV files",
            coderev_twocat,
            self.user_bob)
        singlet_i1 = method_twocat_string_singlet.create_input("firstsingletfile", 1,
                                                               compounddatatype=self.string_singlet)
        singlet_i2 = method_twocat_string_singlet.create_input("secondsingletfile", 2,
                                                               compounddatatype=self.string_singlet)
        singlet_o1 = method_twocat_string_singlet.create_output("combinedsingletfile", 1,
                                                                compounddatatype=self.string_singlet)

        complex_pipeline_dict = {
            "user": self.user_bob.pk,
            "users_allowed": [],
            "groups_allowed": [],

            "family_pk": None,
            "family_name": "more complicated Pipeline",
            "family_desc": "For testing de-serialization of Pipelines",

            "revision_name": "v1",
            "revision_desc": "first version",
            "revision_number": 1,
            "revision_parent_pk": None,
            "is_published_version": False,

            "pipeline_inputs": [
                {
                    "CDT_pk": self.string_doublet.pk,
                    "dataset_name": "pi_1",
                    "dataset_idx": 1,
                    "x": 0.05,
                    "y": 0.3,
                    "min_row": None,
                    "max_row": None
                },
                {
                    "CDT_pk": self.string_doublet.pk,
                    "dataset_name": "pi_2",
                    "dataset_idx": 2,
                    "x": 0.05,
                    "y": 0.5,
                    "min_row": None,
                    "max_row": None
                },
                {
                    "CDT_pk": self.string_singlet.pk,
                    "dataset_name": "pi_3",
                    "dataset_idx": 3,
                    "x": 0.05,
                    "y": 0.7,
                    "min_row": None,
                    "max_row": None
                }
            ],
            "pipeline_steps": [
                {
                    "transf_pk": method_twocat_string_doublet.pk,
                    "transf_type": "Method",
                    "family_pk": method_twocat_string_doublet.family.pk,
                    "step_num": 1,
                    "x": 0.2,
                    "y": 0.5,
                    "name": "step 1",
                    "cables_in": [
                        {
                            "source_dataset_name": "pi_1",
                            "source_step": 0,
                            "dest_dataset_name": doublet_i1.dataset_name,
                            "keep_output": False,
                            "wires": []
                        },
                        {
                            "source_dataset_name": "pi_2",
                            "source_step": 0,
                            "dest_dataset_name": doublet_i2.dataset_name,
                            "keep_output": False,
                            "wires": []
                        }
                    ],
                    "outputs_to_delete": []
                },
                {
                    "transf_pk": method_twocat_string_singlet.pk,
                    "transf_type": "Method",
                    "family_pk": method_twocat_string_singlet.family.pk,
                    "step_num": 2,
                    "x": 0.4,
                    "y": 0.5,
                    "name": "step 2",
                    "cables_in": [
                        {
                            "source_dataset_name": "pi_3",
                            "source_step": 0,
                            "dest_dataset_name": singlet_i1.dataset_name,
                            "keep_output": False,
                            "wires": []
                        },
                        {
                            "source_dataset_name": doublet_o1.dataset_name,
                            "source_step": 1,
                            "dest_dataset_name": singlet_i2.dataset_name,
                            "keep_output": True,
                            "wires": [
                                {"source_idx": 2, "dest_idx": 1}
                            ]
                        }
                    ],
                    "outputs_to_delete": []
                }
            ],
            "pipeline_outputs": [
                {
                    "output_idx": 1,
                    "output_name": "output_1",
                    "output_CDT_pk": self.string_doublet.pk,
                    "source_step": 1,
                    "source_dataset_name": doublet_o1.dataset_name,
                    "x": 0.85,
                    "y": 0.4,
                    "wires": []
                },
                {
                    "output_idx": 2,
                    "output_name": "output_2",
                    "output_CDT_pk": self.string_doublet.pk,
                    "source_step": 2,
                    "source_dataset_name": singlet_o1.dataset_name,
                    "x": 0.85,
                    "y": 0.4,
                    "wires": [
                        {"source_idx": 1, "dest_idx": 1},
                        {"source_idx": 1, "dest_idx": 2}
                    ]
                }
            ]
        }
        complex_pipeline = Pipeline.create_from_dict(complex_pipeline_dict)

        # Get the PipelineFamily PK.
        complex_pipeline_dict["family_pk"] = complex_pipeline.family.pk

        self._check_pipeline(complex_pipeline_dict, complex_pipeline)

    def test_update_from_dict(self):
        """Testing making an update of an existing Pipeline from a dictionary."""
        # Create an incomplete Pipeline (take it from one of the previous tests).
        empty_pipeline_dict = self._setup_pipeline_dict("empty")
        my_pipeline = Pipeline.create_from_dict(empty_pipeline_dict)
        pipeline_family_just_created = my_pipeline.family

        # Update it to a half-complete state.
        incomplete_pipeline_dict = self._setup_pipeline_dict("incomplete")
        my_pipeline.update_from_dict(incomplete_pipeline_dict)
        # The updated Pipeline should still have the same family.
        incomplete_pipeline_dict["family_pk"] = pipeline_family_just_created.pk
        self._check_pipeline(incomplete_pipeline_dict, my_pipeline)

        # Update it to a complete state.
        complete_pipeline_dict = self._setup_pipeline_dict("complete")
        my_pipeline.update_from_dict(complete_pipeline_dict)
        # The PipelineFamily should still be the same.
        complete_pipeline_dict["family_pk"] = pipeline_family_just_created.pk
        self._check_pipeline(complete_pipeline_dict, my_pipeline)

    def test_update_already_used_pipeline(self):
        """Updating an already-used Pipeline from a dictionary should fail."""
        complete_pipeline_dict = self._setup_pipeline_dict("complete")
        my_pipeline = Pipeline.create_from_dict(complete_pipeline_dict)

        # Attach a dummy run.
        my_pipeline.pipeline_instances.create(user=self.user_bob, name="DummyRun",
                                              description="A run that should prohibit updating of the Pipeline")

        # Try to update the Pipeline.
        complete_pipeline_dict["pipeline_steps"][1]["outputs_to_delete"].append(
            self.method_noop.outputs.first().dataset_name)
        complete_pipeline_dict["family_pk"] = my_pipeline.family.pk

        self.assertRaisesRegexp(
            PipelineSerializationException,
            'Pipeline "{}" has been previously run so cannot be updated'.format(my_pipeline),
            lambda: my_pipeline.update_from_dict(complete_pipeline_dict)
        )

    def test_revise_pipeline(self):
        """Revising a Pipeline from a dictionary."""
        complete_pipeline_dict = self._setup_pipeline_dict("complete")
        my_pipeline = Pipeline.create_from_dict(complete_pipeline_dict)

        # Make a revision.
        updated_pipeline_dict = complete_pipeline_dict
        updated_pipeline_dict["pipeline_steps"][1]["outputs_to_delete"].append(
            self.method_noop.outputs.first().dataset_name)

        # These fields will have changed in the revision.
        updated_pipeline_dict["family_pk"] = my_pipeline.family.pk
        updated_pipeline_dict["revision_name"] = "v2"
        updated_pipeline_dict["revision_desc"] = "Second version"
        updated_pipeline_dict["revision_number"] = 2
        updated_pipeline_dict["revision_parent_pk"] = my_pipeline.pk

        new_and_improved = my_pipeline.revise_from_dict(updated_pipeline_dict)
        self._check_pipeline(updated_pipeline_dict, new_and_improved)

    def test_update_already_revised_pipeline(self):
        """Updating an already-revised Pipeline from a dictionary should fail."""
        complete_pipeline_dict = self._setup_pipeline_dict("complete")
        my_pipeline = Pipeline.create_from_dict(complete_pipeline_dict)

        # Revise the Pipeline.
        revision_pipeline_dict = complete_pipeline_dict
        revision_pipeline_dict["pipeline_steps"][1]["outputs_to_delete"].append(
            self.method_noop.outputs.first().dataset_name)
        revision_pipeline_dict["family_pk"] = my_pipeline.family.pk
        my_pipeline.revise_from_dict(revision_pipeline_dict)

        # Try to update the first one.
        self.assertRaisesRegexp(
            PipelineSerializationException,
            'Pipeline "{}" has been previously revised so cannot be updated'.format(my_pipeline),
            lambda: my_pipeline.update_from_dict(revision_pipeline_dict)
        )

    def test_create_to_existing_family(self):
        """Creating a Pipeline with an already-existing PipelineFamily from a dictionary should fail."""
        complete_pipeline_dict = self._setup_pipeline_dict("complete")
        empty_pipeline_dict = self._setup_pipeline_dict("empty")

        # First, create a complete Pipeline.
        Pipeline.create_from_dict(complete_pipeline_dict)

        # Now try to create a second to the same family.
        self.assertRaisesRegexp(
            PipelineSerializationException,
            'Duplicate pipeline family name',
            lambda: Pipeline.create_from_dict(empty_pipeline_dict)
        )
