"""
Shipyard unit tests pertaining to Pipeline and its relatives.
"""

import os.path
import re
import shutil
import tempfile
import json

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.files.base import File
from django.core.urlresolvers import resolve
from django.db.models import Count
from django.test import TestCase
from django.utils import timezone
from rest_framework import status
from rest_framework.reverse import reverse
from rest_framework.test import force_authenticate

from archive.models import ExecLog
from constants import datatypes
from kive.tests import BaseTestCases
from metadata.models import CompoundDatatype, CompoundDatatypeMember, \
    Datatype, kive_user, everyone_group
from method.models import Method, MethodFamily, CodeResource,\
    CodeResourceRevision, CodeResourceDependency
from pipeline.models import Pipeline, PipelineFamily, \
    PipelineStep, PipelineStepInputCable, \
    PipelineOutputCable
from pipeline.serializers import PipelineSerializer,\
    PipelineStepUpdateSerializer
import kive.testing_utils as tools

samplecode_path = tools.samplecode_path


class DuckRequest(object):
    """ A fake request used to test serializers. """
    def __init__(self):
        self.user = kive_user()
        
    def build_absolute_uri(self, url):
        return url


class DuckContext(dict):
    """ A fake context used to test serializers. """
    def __init__(self):
        self['request'] = DuckRequest()


class PipelineTestCase(TestCase):
    """
    Set up a database state for unit testing Pipeline.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        """Set up default database state for Pipeline unit testing."""
        tools.create_pipeline_test_environment(self)

    def tearDown(self):
        tools.destroy_pipeline_test_environment(self)


class PipelineFamilyTests(PipelineTestCase):

    def test_unicode(self):
        """
        unicode() for PipelineFamily should display it's name
        """
        self.assertEqual(unicode(self.DNAcomp_pf), "DNAcomplement")

    def test_delete_pipeline_family(self):
        """Can I delete a PipelineFamily?"""
        self.assertIsNone(PipelineFamily.objects.first().delete())

    # def test_published_version_display_name_is_none(self):
    #     family = PipelineFamily.objects.get(name='DNAcomplement')
    #     self.assertIsNone(family.published_version_display_name)

    # def test_published_version_display_name(self):
    #     family = PipelineFamily.objects.get(name='DNAcomplement')
    #     family.published_version = family.members.last() #oldest version
    #     family.clean()
    #     family.save()
    #
    #     reloaded = PipelineFamily.objects.get(pk=family.pk)
    #
    #     self.assertEqual("1: v1", reloaded.published_version_display_name)


class PipelineTests(PipelineTestCase):
    """Tests for basic Pipeline functionality."""
    
    def test_pipeline_one_valid_input_no_steps(self):
        """A Pipeline with one valid input, but no steps, is clean but not complete."""
        p = Pipeline.objects.filter(steps__isnull=True, inputs__isnull=True).first()
        p.create_input(compounddatatype=self.DNAinput_cdt, dataset_name="oneinput", dataset_idx=1)
        self.assertIsNone(p.clean())
        self.assertRaisesRegexp(
            ValidationError,
            re.escape("Pipeline {} has no steps".format(p)),
            p.complete_clean
        )

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

    def test_pipeline_trivial_cable(self):
        """
        A trivial cable should have is_trivial() = True.
        """
        outcable = self.DNAcompv1_p.outcables.first()  # only one outcable
        self.assertEqual(outcable.is_trivial(), True)


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
        unrelated_raw_output = unrelated_method.create_output(dataset_name="unrelated_raw_output",dataset_idx=1)

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


class PipelineUpdateTests(PipelineTestCase):
    def create_dependency_revision(self):
        """ Find a dependency that is used in a pipeline.
        
        It should only have a single revision.
        Add a second revision for it.
        """
        used_methods = Method.objects.filter(pipelinesteps__isnull=False)
        used_code_resource_revisions = CodeResourceRevision.objects.filter(
            methods__in=used_methods)
        dependencies = CodeResourceDependency.objects.filter(
            coderesourcerevision__in=used_code_resource_revisions)
        dependency = dependencies.earliest('id') # dependency used in a pipeline
        code_resource_revision = dependency.requirement
        fn = "GoodRNANucSeq.csv"
        with open(os.path.join(samplecode_path, fn), "rb") as f:
            new_revision = CodeResourceRevision(
                coderesource=code_resource_revision.coderesource, 
                revision_name="rna", 
                revision_desc="Switch to RNA", 
                revision_parent=code_resource_revision, 
                content_file=File(f),
                user=self.myUser)
            new_revision.full_clean()
            new_revision.save()
        new_revision.grant_everyone_access()
        
        return new_revision
 
    def create_code_revision(self):
        # Define compv2_crRev for comp_cr
        fn = "complement_v2.py"
        with open(os.path.join(samplecode_path, fn), "rb") as f:
            self.compv3_crRev = CodeResourceRevision(coderesource=self.comp_cr, 
                revision_name="v3", 
                revision_desc="Third version: rounder", 
                revision_parent=self.compv2_crRev, 
                content_file=File(f),
                user=self.myUser)
        # case.compv2_crRev.content_file.save(fn, File(f))
            self.compv3_crRev.full_clean()
            self.compv3_crRev.save()
        self.compv3_crRev.grant_everyone_access()
 
    def create_method(self):
        self.create_code_revision()
        self.DNAcompv3_m = self.DNAcomp_mf.members.create(revision_name="v3", 
            revision_desc="Third version", 
            revision_parent=self.DNAcompv2_m, 
            driver=self.compv3_crRev, 
            user=self.myUser)
        self.DNAcompv3_m.full_clean()
        self.DNAcompv3_m.save()
        self.DNAcompv3_m.grant_everyone_access()
        self.DNAcompv3_m.copy_io_from_parent()

    def test_find_update_not_found(self):
        pipeline = self.DNAcomp_pf.members.get(revision_number=2)
        update = pipeline.find_update()
        
        self.assertEqual(update, None)

    def test_find_update(self):
        pipeline = self.DNAcomp_pf.members.get(revision_number=1)
        next_pipeline = self.DNAcomp_pf.members.get(revision_number=2)
        
        update = pipeline.find_update()
        
        self.assertEqual(update, next_pipeline)

    def test_find_step_updates_none(self):
        updates = self.DNAcompv1_p.find_step_updates()
         
        self.assertListEqual(updates, [])
     
    def test_find_step_updates_method(self):
        self.create_method()
            
        updates = self.DNAcompv1_p.find_step_updates()
          
        self.assertEqual(len(updates), 1)
        update = updates[0]
        self.assertEqual(update.step_num, 1)
        self.assertEqual(update.method, self.DNAcompv3_m)
        self.assertEqual(update.code_resource_revision, None)
     
    def test_find_step_updates_code_resource(self):
        self.create_code_revision()
        
        updates = self.DNAcompv1_p.find_step_updates()
          
        self.assertEqual(len(updates), 1)
        update = updates[0]
        self.assertEqual(update.code_resource_revision, self.compv3_crRev)
     
    def test_find_step_updates_dependency(self):
        new_revision = self.create_dependency_revision()
        
        updates = self.DNAcompv1_p.find_step_updates()
          
        self.assertEqual(len(updates), 1)
        update = updates[0]
        self.assertEqual(len(update.dependencies), 1)
        self.assertEqual(update.dependencies[0], new_revision)
     
    def test_serialize_step_updates_dependency(self):
        new_revision = self.create_dependency_revision()
        updates = self.DNAcompv1_p.find_step_updates()
        
        data = PipelineStepUpdateSerializer(
            updates,
            many=True,
            context=DuckContext()).data
        
        self.assertEqual(len(data), 1)
        update = data[0]
        self.assertEqual(len(update['dependencies']), 1)
        self.assertEqual(update['dependencies'][0]['id'], new_revision.id)


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

        # It might complain about either connection, so accept either.
        self.assertRaisesRegexp(ValidationError,
            'Destination member "(b: string|c: string)" has no wires leading to it',
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
                                        description="A datatype not having anything to do with anything",
                                        user=self.user)
        self.incompatible_dt.save()
        self.incompatible_dt.grant_everyone_access()
        self.incompatible_dt.restricts.add(Datatype.objects.get(pk=datatypes.STR_PK))

        # Define 2 CDTs that are unequal: (DNA, string, string), and (string, DNA, incompatible)
        cdt_1 = CompoundDatatype(user=self.user)
        cdt_1.save()
        cdt_1.grant_everyone_access()
        cdt_1.members.create(datatype=self.DNA_dt,column_name="col_1",column_idx=1)
        cdt_1.members.create(datatype=self.string_dt,column_name="col_2",column_idx=2)
        cdt_1.members.create(datatype=self.string_dt,column_name="col_3",column_idx=3)

        cdt_2 = CompoundDatatype(user=self.user)
        cdt_2.save()
        cdt_2.grant_everyone_access()
        cdt_2.members.create(datatype=self.string_dt,column_name="col_1",column_idx=1)
        cdt_2.members.create(datatype=self.DNA_dt,column_name="col_2",column_idx=2)
        cdt_2.members.create(datatype=self.incompatible_dt,column_name="col_3",column_idx=3)

        # Define a pipeline with single pipeline input of type cdt_1
        my_pipeline = self.test_PF.members.create(revision_name="foo", revision_desc="Foo version", user=self.user)
        my_pipeline.grant_everyone_access()
        pipeline_in = my_pipeline.create_input(compounddatatype=cdt_1,dataset_name="pipe_in_1",dataset_idx=1)

        # Define method to have an input with cdt_2, add it as a step, cable it
        self.testmethod.inputs.all().delete()
        method_in = self.testmethod.create_input(dataset_name="TestIn", dataset_idx=1, compounddatatype=cdt_2)
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

        errorMessage = ('The datatype of the source pin "col_1: DNANucSeq" is incompatible with the datatype of the '
                        'destination pin "col_3: Not compatible"')
        
        self.assertRaisesRegexp(ValidationError, errorMessage, wire3_bad.clean)
        self.assertRaisesRegexp(ValidationError, errorMessage, my_cable1.clean)

    def test_CustomCableWire_clean_source_and_dest_pin_do_not_come_from_cdt_bad(self):
        # For source_pin and dest_pin, give a CDTM from an unrelated CDT

        # Define a datatype that has nothing to do with anything.
        self.incompatible_dt = Datatype(name="poop", description="poop!!", user=self.user)
        self.incompatible_dt.save()
        self.incompatible_dt.restricts.add(Datatype.objects.get(pk=datatypes.STR_PK))


        # Define 2 different CDTs: (DNA, string, string), and (string, DNA, incompatible)
        cdt_1 = CompoundDatatype(user=self.user)
        cdt_1.save()
        cdt_1.members.create(datatype=self.DNA_dt,column_name="col_1",column_idx=1)
        cdt_1.members.create(datatype=self.string_dt,column_name="col_2",column_idx=2)
        cdt_1.members.create(datatype=self.string_dt,column_name="col_3",column_idx=3)

        cdt_2 = CompoundDatatype(user=self.user)
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
        errorMessage = re.escape('Destination member "b: string" has no wires leading to it')
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
        temporary_file, safe_fn = tempfile.mkstemp(dir=self.workdir)
        os.close(temporary_file)
        datafile = open(safe_fn, "w")
        datafile.write(",".join([m.column_name for m in self.DNAinput_cdt.members.all()]))
        datafile.write("\n")
        datafile.write("ATCG\n")
        datafile.close()
        
        log, rsic = self._make_log(self.DNAcompv1_p, output_file, datafile.name)
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

        errorMessage = re.escape('Source pin "x: string" does not come from compounddatatype '
                                 '"(a: string, b: string, c: string)"')

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
        new_cdt = CompoundDatatype(user=self.user)
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


def create_pipeline_deserialization_environment(case):
    """
    Set up stuff that will help with testing Pipeline deserialization.

    The "sandbox" testing environment must be set up already, either
    by directly calling create_sandbox_testing_tools_environment or
    by including a fixture that had called it.
    """
    case.kive_user = kive_user()
    case.everyone_group = everyone_group()

    # Explicitly load objects that are defined in create_sandbox_testing...
    # in case we are using a fixture.
    case.user_bob = User.objects.get(username="bob")
    case.coderesource_noop = CodeResource.objects.get(
        user=case.user_bob,
        name="noop"
    )
    case.coderev_noop = case.coderesource_noop.revisions.get(revision_name="1")
    case.noop_mf = MethodFamily.objects.get(name="string noop")
    case.method_noop = case.noop_mf.members.get(revision_number=1)
    case.noop_raw_mf = MethodFamily.objects.get(name="raw noop", user=case.user_bob)
    case.method_noop_raw = case.noop_raw_mf.members.get(revision_number=1)

    # Retrieve the CDT defined in create_sandbox_testing_tools_environment
    # called "self.cdt_string", or an equivalent.
    bob_string_dt = Datatype.objects.get(
        user=case.user_bob,
        name="my_string",
        description="sequences of ASCII characters"
    )
    possible_cdt_string_members = CompoundDatatypeMember.objects.filter(
        column_name="word",
        column_idx=1,
        datatype=bob_string_dt
    )
    possible_cdt_strings = [x.compounddatatype for x in possible_cdt_string_members]
    case.cdt_string = possible_cdt_strings[0]

    case.duck_context = DuckContext()

    case.test_pf = PipelineFamily(
        user=case.kive_user,
        name="test",
        description="Test family"
    )
    case.test_pf.save()
    case.test_pf.groups_allowed.add(case.everyone_group)

    # Set up a CDT with two elements to allow some wiring to occur.
    case.STR = Datatype.objects.get(pk=datatypes.STR_PK)

    # A CDT composed of two builtin-STR columns.
    case.string_doublet = CompoundDatatype(user=case.user_bob)
    case.string_doublet.save()
    case.string_doublet.members.create(datatype=case.STR, column_name="column1", column_idx=1)
    case.string_doublet.members.create(datatype=case.STR, column_name="column2", column_idx=2)
    case.string_doublet.grant_everyone_access()

    # A CDT composed of one builtin-STR column.
    case.string_singlet = CompoundDatatype(user=case.user_bob)
    case.string_singlet.save()
    case.string_singlet.members.create(datatype=case.STR, column_name="col1", column_idx=1)
    case.string_singlet.grant_everyone_access()

    # Here is a dictionary that can be deserialized into a Pipeline.
    case.noop_input_name = case.method_noop.inputs.first().dataset_name
    case.noop_output_name = case.method_noop.outputs.first().dataset_name
    case.pipeline_dict = {
        "family": "test",
        "revision_name": "v1",
        "revision_desc": "first version",
        "revision_parent": None,

        "user": case.kive_user.username,
        "users_allowed": [],
        "groups_allowed": [case.everyone_group.name],

        "inputs": [
            {
                "dataset_name": "input_to_not_touch",
                "dataset_idx": 1,
                "x": 0.05,
                "y": 0.5,
                "structure": {
                    "compounddatatype": case.cdt_string.pk,
                    "min_row": None,
                    "max_row": None
                }
            }
        ],
        "steps": [
            {
                "transformation": case.method_noop.pk,
                "step_num": 1,
                "x": 0.2,
                "y": 0.5,
                "name": "step 1",
                "cables_in": [
                    {
                        # The pipeline input doesn't exist yet so we have to specify
                        # it by name.
                        "source_dataset_name": "input_to_not_touch",
                        "source_step": 0,
                        "dest_dataset_name": case.noop_input_name,
                        "custom_wires": [],
                        "keep_output": False
                    }
                ],
                "new_outputs_to_delete_names": []
            },
            {
                "transformation": case.method_noop.pk,
                "step_num": 2,
                "x": 0.4,
                "y": 0.5,
                "name": "step 2",
                "cables_in": [
                    {
                        # Here we can specify source directly.
                        "source_dataset_name": case.noop_output_name,
                        "source_step": 1,
                        "dest_dataset_name": case.noop_input_name,
                        "custom_wires": [],
                        "keep_output": False
                    }
                ],
            },
            {
                "transformation": case.method_noop.pk,
                "step_num": 3,
                "x": 0.6,
                "y": 0.5,
                "name": "step 3",
                "cables_in": [
                    {
                        "source_dataset_name": case.noop_output_name,
                        "source_step": 2,
                        "dest_dataset_name": case.noop_input_name,
                        "custom_wires": [],
                        "keep_output": False
                    }
                ],
                "new_outputs_to_delete_names": []
            }
        ],
        "outcables": [
            {
                "output_idx": 1,
                "output_name": "untouched_output",
                "output_cdt": case.cdt_string.pk,
                "source_step": 3,
                "source_dataset_name": case.noop_output_name,
                "x": 0.85,
                "y": 0.5,
                "custom_wires": []
            }
        ]
    }

    case.method_doublet_noop = tools.make_first_method(
        "string doublet noop",
        "a noop on a two-column input",
        case.coderev_noop,
        case.user_bob)
    case.method_doublet_noop.grant_everyone_access()
    case.doublet_input_name = "doublets"
    case.doublet_output_name = "untouched_doublets"
    tools.simple_method_io(
        case.method_doublet_noop,
        case.string_doublet,
        case.doublet_input_name,
        case.doublet_output_name
    )

    # This defines a pipeline with custom wiring.
    case.pipeline_cw_dict = {
        "family": "test",
        "revision_name": "v2_c2",
        "revision_desc": "Custom wiring tester",
        "revision_parent": None,

        "user": case.kive_user.username,
        "users_allowed": [case.kive_user.username],
        "groups_allowed": [],

        "inputs": [
            {
                "dataset_name": "input_to_not_touch",
                "dataset_idx": 1,
                "x": 0.05,
                "y": 0.5,
                "structure": {
                    "compounddatatype": case.cdt_string.pk,
                    "min_row": None,
                    "max_row": None
                }
            }
        ],
        "steps": [
            {
                "transformation": case.method_doublet_noop.pk,
                "step_num": 1,
                "x": 0.2,
                "y": 0.5,
                "name": "step 1",
                "cables_in": [
                    {
                        # The pipeline input doesn't exist yet so we have to specify
                        # it by name.
                        "source_dataset_name": "input_to_not_touch",
                        "source_step": 0,
                        "dest_dataset_name": case.doublet_input_name,
                        "custom_wires": [
                            {
                                "source_pin": case.cdt_string.members.first().pk,
                                "dest_pin": case.string_doublet.members.get(column_idx=1).pk
                            },
                            {
                                "source_pin": case.cdt_string.members.first().pk,
                                "dest_pin": case.string_doublet.members.get(column_idx=2).pk
                            },
                        ],
                        "keep_output": False
                    }
                ],
            },
            {
                "transformation": case.method_noop.pk,
                "step_num": 2,
                "x": 0.4,
                "y": 0.5,
                "name": "step 2",
                "cables_in": [
                    {
                        # Here we can specify source directly.
                        "source_dataset_name": case.doublet_output_name,
                        "source_step": 1,
                        "dest_dataset_name": case.noop_input_name,
                        "custom_wires": [
                            {
                                "source_pin": case.string_doublet.members.get(column_idx=1).pk,
                                "dest_pin": case.cdt_string.members.first().pk
                            }
                        ],
                        "keep_output": False
                    }
                ],
            }
        ],
        "outcables": [
            {
                "output_idx": 1,
                "output_name": "untouched_output",
                "output_cdt": case.string_doublet.pk,
                "source_step": 2,
                "source_dataset_name": case.noop_output_name,
                "x": 0.85,
                "y": 0.5,
                "custom_wires": [
                    {
                        "source_pin": case.cdt_string.members.first().pk,
                        "dest_pin": case.string_doublet.members.get(column_idx=1).pk
                    },
                    {
                        "source_pin": case.cdt_string.members.first().pk,
                        "dest_pin": case.string_doublet.members.get(column_idx=2).pk
                    },
                ]
            }
        ]
    }

    # This defines a pipeline that handles raw data.
    case.raw_input_name = case.method_noop_raw.inputs.first().dataset_name
    case.raw_output_name = case.method_noop_raw.outputs.first().dataset_name
    case.pipeline_raw_dict = {
        "family": "test",
        "revision_name": "v3_raw",
        "revision_desc": "Raw input tester",
        "revision_parent": None,

        "user": case.kive_user.username,
        "users_allowed": [case.kive_user.username],
        "groups_allowed": [case.everyone_group],

        "inputs": [
            {
                "dataset_name": "input_to_not_touch",
                "dataset_idx": 1,
                "x": 0.05,
                "y": 0.5
            }
        ],
        "steps": [
            {
                "transformation": case.method_noop_raw.pk,
                "step_num": 1,
                "x": 0.2,
                "y": 0.5,
                "name": "step 1",
                "cables_in": [
                    {
                        # The pipeline input doesn't exist yet so we have to specify
                        # it by name.
                        "source_dataset_name": "input_to_not_touch",
                        "source_step": 0,
                        "dest_dataset_name": case.raw_input_name,
                        "keep_output": False
                    }
                ],
            },
            {
                "transformation": case.method_noop_raw.pk,
                "step_num": 2,
                "x": 0.4,
                "y": 0.5,
                "name": "step 2",
                "cables_in": [
                    {
                        # Here we can specify source directly.
                        "source_dataset_name": case.raw_output_name,
                        "source_step": 1,
                        "dest_dataset_name": case.raw_input_name,
                        "keep_output": False
                    }
                ],
            }
        ],
        "outcables": [
            {
                "output_idx": 1,
                "output_name": "untouched_output",
                "source_step": 2,
                "source_dataset_name": case.raw_output_name,
                "x": 0.85,
                "y": 0.5
            }
        ]
    }


class PipelineSerializerTests(TestCase):
    """
    Tests of PipelineSerializer and its offshoots.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        tools.create_sandbox_testing_tools_environment(self)
        create_pipeline_deserialization_environment(self)

    def tearDown(self):
        tools.clean_up_all_files()

    def test_validate(self):
        ps = PipelineSerializer(data=self.pipeline_dict, context=self.duck_context)
        ps.is_valid()
        self.assertTrue(ps.is_valid())

    def test_validate_otd_good_name(self):
        """
        Validating a properly-named output_to_delete.
        """
        self.pipeline_dict["steps"][0]["new_outputs_to_delete_names"] = [self.noop_output_name]
        ps = PipelineSerializer(data=self.pipeline_dict, context=self.duck_context)
        self.assertTrue(ps.is_valid())

    def test_validate_otd_bad_name(self):
        """
        A step with a badly-named output to delete should fail.
        """
        incorrect_name = "foo"
        self.pipeline_dict["steps"][0]["new_outputs_to_delete_names"] = [incorrect_name]
        ps = PipelineSerializer(data=self.pipeline_dict, context=self.duck_context)
        self.assertFalse(ps.is_valid())
        self.assertEquals(
            ps.errors["steps"][0]["non_field_errors"][0],
            'Step {} has no output named "{}"'.format(1, incorrect_name)
        )

    def test_validate_dest_bad_name(self):
        """
        A PSIC with a destination that has a bad name should fail.
        """
        incorrect_name = "foo"
        self.pipeline_dict["steps"][0]["cables_in"][0]["dest_dataset_name"] = incorrect_name
        ps = PipelineSerializer(data=self.pipeline_dict, context=self.duck_context)
        self.assertFalse(ps.is_valid())
        self.assertEquals(
            ps.errors["steps"][0]["non_field_errors"][0],
            'Step {} has no input named "{}"'.format(1, incorrect_name)
        )

    def test_validate_pipeline_input_source_bad_name(self):
        """
        A PSIC with a Pipeline input source that has a bad name should fail.
        """
        incorrect_name = "foo"
        self.pipeline_dict["steps"][0]["cables_in"][0]["source_dataset_name"] = incorrect_name
        ps = PipelineSerializer(data=self.pipeline_dict, context=self.duck_context)
        self.assertFalse(ps.is_valid())
        self.assertEquals(
            ps.errors["non_field_errors"][0],
            'Cable input with name "{}" does not exist'.format(incorrect_name)
        )

    def test_validate_step_output_source_bad_name(self):
        """
        A PSIC with a step output source that has a bad name should fail.
        """
        incorrect_name = "foo"
        self.pipeline_dict["steps"][1]["cables_in"][0]["source_dataset_name"] = incorrect_name
        ps = PipelineSerializer(data=self.pipeline_dict, context=self.duck_context)
        self.assertFalse(ps.is_valid())
        self.assertEquals(
            ps.errors["non_field_errors"][0],
            'Step {} has no output named "{}"'.format(1, incorrect_name)
        )

    def test_validate_step_output_source_bad_source_step(self):
        """
        A PSIC with a step output source that has a bad step number should fail.
        """
        bad_step_num = 100
        self.pipeline_dict["steps"][1]["cables_in"][0]["source_step"] = bad_step_num
        ps = PipelineSerializer(data=self.pipeline_dict, context=self.duck_context)
        self.assertFalse(ps.is_valid())
        self.assertEquals(
            ps.errors["non_field_errors"][0],
            "Step {} does not exist".format(bad_step_num)
        )

    def test_validate_custom_wires(self):
        """
        Test validation of a Pipeline containing custom wires.
        """
        ps = PipelineSerializer(data=self.pipeline_cw_dict, context=self.duck_context)
        ps.is_valid()
        self.assertTrue(ps.is_valid())

    def test_validate_raw_input(self):
        """
        Test validation of a Pipeline that only consists of raw data.
        """
        ps = PipelineSerializer(data=self.pipeline_raw_dict, context=self.duck_context)
        self.assertTrue(ps.is_valid())

    def test_create(self):
        ps = PipelineSerializer(data=self.pipeline_dict,
                                context=self.duck_context)
        ps.is_valid()
        pl = ps.save()

        # Probe the Pipeline to see if things are defined correctly.
        pl_input = pl.inputs.first()
        self.assertEquals(pl_input.structure.compounddatatype, self.cdt_string)

        step_1 = pl.steps.get(step_num=1)
        step_2 = pl.steps.get(step_num=2)
        step_3 = pl.steps.get(step_num=3)

        self.assertEquals(step_1.transformation.definite, self.method_noop)

        self.assertEquals(step_1.cables_in.count(), 1)
        # Check that this cable got mapped correctly since we defined its source
        # indirectly.
        self.assertEquals(step_1.cables_in.first().source.definite, pl_input)
        self.assertEquals(step_1.cables_in.first().custom_wires.count(), 0)

        self.assertEquals(step_2.cables_in.count(), 1)
        self.assertEquals(step_2.cables_in.first().source.definite, self.method_noop.outputs.first())

        self.assertEquals(step_3.cables_in.first().source_step, 2)

        outcable = pl.outcables.first()
        self.assertEquals(pl.outcables.count(), 1)
        self.assertEquals(outcable.custom_wires.count(), 0)

        output = pl.outputs.first()
        self.assertEquals(pl.outputs.count(), 1)
        self.assertEquals(output.dataset_name, "untouched_output")
        self.assertEquals(output.dataset_idx, 1)
        self.assertEquals(output.x, 0.85)
        self.assertEquals(output.y, 0.5)

    def test_create_with_otd(self):
        self.pipeline_dict["steps"][0]["new_outputs_to_delete_names"] = [self.noop_output_name]
        ps = PipelineSerializer(data=self.pipeline_dict,
                                context=self.duck_context)
        ps.is_valid()
        pl = ps.save()

        # Probe the Pipeline to see if the output was properly registered for deletion.
        step_1 = pl.steps.get(step_num=1)
        self.assertEquals(step_1.outputs_to_delete.count(), 1)
        self.assertEquals(step_1.outputs_to_delete.first(), step_1.transformation.outputs.first())

    def test_create_with_custom_wires(self):
        """
        Test that creation works when custom wires are used.
        """
        ps = PipelineSerializer(data=self.pipeline_cw_dict,
                                context=self.duck_context)
        ps.is_valid()
        pl = ps.save()

        # Probe the Pipeline to see if things are defined correctly.
        pl_input = pl.inputs.first()
        self.assertEquals(pl_input.structure.compounddatatype, self.cdt_string)

        step_1 = pl.steps.get(step_num=1)
        step_2 = pl.steps.get(step_num=2)

        self.assertEquals(step_1.transformation.definite, self.method_doublet_noop)

        self.assertEquals(step_1.cables_in.count(), 1)
        self.assertEquals(step_1.cables_in.first().custom_wires.count(), 2)
        wire_1 = step_1.cables_in.first().custom_wires.get(dest_pin__column_idx=1)
        wire_2 = step_1.cables_in.first().custom_wires.get(dest_pin__column_idx=2)
        self.assertEquals(wire_1.source_pin, self.cdt_string.members.first())
        self.assertEquals(wire_1.dest_pin, self.string_doublet.members.get(column_idx=1))
        self.assertEquals(wire_2.source_pin, self.cdt_string.members.first())
        self.assertEquals(wire_2.dest_pin, self.string_doublet.members.get(column_idx=2))

        # Now check the wire defined on the input cable of step 2.
        self.assertEquals(step_2.cables_in.first().custom_wires.count(), 1)
        step_2_wire = step_2.cables_in.first().custom_wires.first()
        self.assertEquals(step_2_wire.source_pin, self.string_doublet.members.get(column_idx=1))
        self.assertEquals(step_2_wire.dest_pin, self.cdt_string.members.first())

        outcable = pl.outcables.first()
        self.assertEquals(outcable.custom_wires.count(), 2)
        out_wire_1 = outcable.custom_wires.get(dest_pin__column_idx=1)
        out_wire_2 = outcable.custom_wires.get(dest_pin__column_idx=2)
        self.assertEquals(out_wire_1.source_pin, self.cdt_string.members.first())
        self.assertEquals(out_wire_1.dest_pin, self.string_doublet.members.get(column_idx=1))
        self.assertEquals(out_wire_2.source_pin, self.cdt_string.members.first())
        self.assertEquals(out_wire_2.dest_pin, self.string_doublet.members.get(column_idx=2))

    def test_create_raw_input(self):
        """
        Test deserialization of a Pipeline hadnling raw data.
        """
        raw_ps = PipelineSerializer(data=self.pipeline_raw_dict,
                                    context=self.duck_context)
        raw_ps.is_valid()
        raw_pl = raw_ps.save()

        pl_input = raw_pl.inputs.first()
        self.assertTrue(pl_input.is_raw())
        pl_output = raw_pl.outputs.first()
        self.assertTrue(pl_output.is_raw())

    def test_create_publish_on_submit(self):
        """
        Testing publishing on submission.
        """
        self.pipeline_dict["published"] = True
        ps = PipelineSerializer(data=self.pipeline_dict,
                                context=self.duck_context)
        self.assertTrue(ps.is_valid())
        pl = ps.save()
        self.assertTrue(pl.published)


class PipelineApiTests(BaseTestCases.ApiTestCase):
    fixtures = ['simple_run']
    
    def setUp(self):
        super(PipelineApiTests, self).setUp()

        self.list_path = reverse("pipeline-list")
        self.detail_pk = 5
        self.detail_path = reverse("pipeline-detail",
                                   kwargs={'pk': self.detail_pk})

        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)

    def test_list(self):
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        expected_count = Pipeline.objects.count()
        self.assertEquals(len(response.data), expected_count)
        self.assertEquals(response.data[0]['family'], 'P_basic')
        self.assertEquals(response.data[0]['revision_name'], 'v1')
        self.assertEquals(response.data[0]['inputs'][0]['dataset_name'], 'basic_in')

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['revision_name'], 'pE_name')
        self.assertEquals(response.data['inputs'][0]['dataset_name'], 'E1_in')

    def test_removal_plan(self):
        removal_path = reverse("pipeline-removal-plan",
                               kwargs={'pk': self.detail_pk})
        removal_view, _, _ = resolve(removal_path)
        request = self.factory.get(removal_path)
        force_authenticate(request, user=self.kive_user)
        
        response = removal_view(request, pk=self.detail_pk)
        
        self.assertEquals(response.data['Pipelines'], 1)

    def test_removal(self):
        start_count = Pipeline.objects.count()
        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Pipeline.objects.count()
        self.assertEquals(end_count, start_count - 1)

    def test_step_updates(self):
        step_updates_path = reverse("pipeline-step-updates",
                                    kwargs={'pk': self.detail_pk})
        step_updates_view, _, _ = resolve(step_updates_path)
        request = self.factory.get(step_updates_path)
        force_authenticate(request, user=self.kive_user)
        
        response = step_updates_view(request, pk=self.detail_pk)
        
        update = response.data[0]
        self.assertEqual(update['step_num'], 1)

    def test_create(self):
        # Note that the "sandbox" testing environment has already been set
        # up in the "simple_run" fixture.
        create_pipeline_deserialization_environment(self)
        request = self.factory.post(self.list_path, self.pipeline_dict, format="json")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)

        if response.exception:
            self.fail(response.data)
        # Probe the new object.
        new_pipeline = self.test_pf.members.get(revision_name=self.pipeline_dict["revision_name"])
        self.assertEquals(new_pipeline.steps.count(), 3)
        self.assertEquals(new_pipeline.outcables.count(), 1)
        self.assertEquals(new_pipeline.outcables.first().output_name, "untouched_output")
        
    def create_new_code_revision(self, coderesource):
        contents = "print('This is the new code.')"
        with tempfile.TemporaryFile() as f:
            f.write(contents)
            revision = CodeResourceRevision(
                coderesource=coderesource,
                revision_name="new",
                revision_desc="just print a message",
                content_file=File(f),
                user=self.user_bob)
            revision.save()
            revision.clean()
        return revision

    def test_create_with_new_method(self):
        create_pipeline_deserialization_environment(self)
        revision = self.create_new_code_revision(self.coderesource_noop)
    
        step_dict = self.pipeline_dict['steps'][0]
        step_dict['new_code_resource_revision_id'] = revision.id
        request = self.factory.post(self.list_path,
                                    self.pipeline_dict,
                                    format="json")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)

        if response.exception:
            self.fail(response.data)
        # Probe the new object.
        new_pipeline = self.test_pf.members.get(
            revision_name=self.pipeline_dict["revision_name"])
        method = revision.methods.first()
        self.assertIsNotNone(method, 'method expected for new code revision')
        step = new_pipeline.steps.get(step_num=1)
        self.assertEqual(step.transformation.display_name, method.display_name)

    def test_create_with_new_method_for_dependency(self):
        create_pipeline_deserialization_environment(self)
        dependency_revision = CodeResourceRevision.objects.exclude(
            coderesource=self.coderesource_noop).earliest('id')
        new_dependency_revision = self.create_new_code_revision(
            dependency_revision.coderesource)
        self.coderev_noop.dependencies.create(requirement=dependency_revision)
        
        step_dict = self.pipeline_dict['steps'][0]
        step_dict['new_dependency_ids'] = [new_dependency_revision.id]
        request = self.factory.post(self.list_path,
                                    self.pipeline_dict,
                                    format="json")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)

        if response.exception:
            self.fail(response.data)
        # Probe the new object.
        new_pipeline = self.test_pf.members.get(
            revision_name=self.pipeline_dict["revision_name"])
        noop_revision_dependency = new_dependency_revision.needed_by.first()
        self.assertIsNotNone(noop_revision_dependency, 'noop_revision expected for new dependency')
        noop_revision = noop_revision_dependency.coderesourcerevision
        method = noop_revision.methods.first()
        self.assertIsNotNone(method, 'method expected for noop_revision')
        step = new_pipeline.steps.get(step_num=1)
        self.assertEqual(step.transformation.display_name, method.display_name)

    def test_create_calls_clean(self):
        """
        Attempting to create a Pipeline should call complete_clean.
        """
        # Note that the "sandbox" testing environment has already been set
        # up in the "simple_run" fixture.
        create_pipeline_deserialization_environment(self)

        self.pipeline_dict["steps"][0]["cables_in"] = []
        request = self.factory.post(self.list_path, self.pipeline_dict, format="json")
        force_authenticate(request, user=self.kive_user)
        # This should barf.
        response = self.list_view(request)
        
        self.assertDictEqual(
            response.data,
            { 'non_field_errors': 'Input "strings" to transformation at step 1 is not cabled' })

    def test_partial_update_published(self):
        """
        Test PATCHing a Pipeline to update its published status.
        """
        # This is defined in simple_run.
        basic_pf = PipelineFamily.objects.get(name="P_basic")

        version_to_publish = basic_pf.members.first()
        patch_data = {
            "published": "true"
        }

        patch_path = reverse("pipeline-detail",
                             kwargs={"pk": version_to_publish.pk})
        request = self.factory.patch(patch_path, patch_data, format="json")
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=version_to_publish.pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        # Probe version_to_publish to check that it's been properly updated.
        version_to_publish = Pipeline.objects.get(pk=version_to_publish.pk)
        self.assertTrue(version_to_publish.published)

        # Now unpublish it.
        patch_data["published"] = "false"

        request = self.factory.patch(patch_path, patch_data, format="json")
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=version_to_publish.pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)
        version_to_publish = Pipeline.objects.get(pk=version_to_publish.pk)
        self.assertFalse(version_to_publish.published)


class PipelineFamilyApiTests(BaseTestCases.ApiTestCase):
    fixtures = ['simple_run']
    
    def setUp(self):
        super(PipelineFamilyApiTests, self).setUp()

        self.list_path = reverse("pipelinefamily-list")
        self.detail_pk = 2
        self.detail_path = reverse("pipelinefamily-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("pipelinefamily-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def test_list(self):
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        expected_count = PipelineFamily.objects.count()
        self.assertEquals(len(response.data), expected_count)
        self.assertEquals(response.data[1]['name'], 'Pipeline_family')

        pf = PipelineFamily.objects.get(name="Pipeline_family")
        expected_revision_pks = [x.pk for x in pf.members.all()]
        actual_revision_pks = [x['id'] for x in response.data[1]['members']]
        self.assertItemsEqual(expected_revision_pks, actual_revision_pks)

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['name'], 'P_basic')

        basic_family = PipelineFamily.objects.get(name="P_basic")
        expected_revision_pks = [x.pk for x in basic_family.members.all()]
        actual_revision_pks = [x['id'] for x in response.data['members']]
        self.assertItemsEqual(expected_revision_pks, actual_revision_pks)

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['PipelineFamilies'], 1)

    def test_removal(self):
        start_count = PipelineFamily.objects.count()
        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = PipelineFamily.objects.count()
        self.assertEquals(end_count, start_count - 1)

    def test_create(self):
        pf_name = "Test PipelineFamily"
        pf_description = "For testing the creation of a PipelineFamily through the API."
        pf_data = {
            "name": pf_name,
            "description": pf_description,
            "users_allowed": [],
            "groups_allowed": [everyone_group().name]
        }

        request = self.factory.post(self.list_path, pf_data, format="json")
        force_authenticate(request, user=self.kive_user)
        self.list_view(request)

        # Probe the resulting new PipelineFamily.
        new_pf = PipelineFamily.objects.get(name=pf_name)
        self.assertEquals(new_pf.description, pf_description)
        self.assertEquals(new_pf.members.count(), 0)
