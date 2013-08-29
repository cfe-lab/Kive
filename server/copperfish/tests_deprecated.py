"""
Old unit tests for Shipyard (Copperfish).

These are ones that predate August 24, 2013 (the introduction of ExecRecord
and its relatives) and are deprecated due to changes in architecture.
"""

from django.test import TestCase;
from copperfish.models import *;
from django.core.files import File;
from django.core.exceptions import ValidationError;
import os;
import glob, os.path;
import hashlib;

from copperfish.tests_old import CopperfishMethodTests_setup, Copperfish_Raw_Setup
from copperfish.tests_old import samplecode_path

class SingleRawInput_replaced_by_uniqueness_constraints_tests(Copperfish_Raw_Setup):

    def test_transformation_rawinput_name_collides_with_non_raw_input_name_clean_bad(self):

        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        # Define colliding input name "a_b_c" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c",dataset_idx = 2)
        self.script_4_1_M.save()

        # The names conflict
        self.assertRaisesRegexp(
            ValidationError,
            "Input names overlap raw input names",
            self.script_4_1_M.check_input_names)
        
        self.assertRaisesRegexp(
            ValidationError,
            "Input names overlap raw input names",
            self.script_4_1_M.clean)
        
    def test_transformation_rawinput_index_collides_with_non_raw_index_bad(self):

        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)

        # Define input name "a_b_c_squared" of type "triplet_cdt" at colliding index = 1
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 1)
        self.script_4_1_M.save()

        # The indices conflict
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_input_indices)
        
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean) 



class SeveralRawInputs_replaced_by_uniqueness_constraints_tests(Copperfish_Raw_Setup):
    def test_transformation_several_rawinputs_several_nonraw_inputs_indices_clash_bad(self):
        # Note that this method wouldn't actually run -- inputs don't match.

        # Define raw input "a_b_c" at index = 1
        self.script_4_1_M.create_input(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define raw input "RawIn3" at index = 2
        self.script_4_1_M.create_input(dataset_name = "RawIn2",dataset_idx = 2)
        
        # Define input "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_input(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 2)

        # Define input "Input3" of type "doublet_cdt" at index = 3
        self.script_4_1_M.create_input(compounddatatype = self.doublet_cdt,dataset_name = "Input3",dataset_idx = 3)

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_input_indices);
        self.assertEquals(self.script_4_1_M.check_input_names(), None);
        self.assertEquals(self.script_4_1_M.check_output_indices(), None);
        self.assertEquals(self.script_4_1_M.check_output_names(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean);

    def test_pipeline_several_rawinputs_coexists_with_several_nonraw_inputs_indices_clash_clean_bad(self):

        # Define 1-step pipeline with conflicting input indices
        pipeline_1 = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_1.create_input(dataset_name="input_1_raw",dataset_idx=1)
        pipeline_1.create_input(compounddatatype=self.triplet_cdt,dataset_name="input_2",dataset_idx=1)
        pipeline_1.create_input(dataset_name="input_3_raw",dataset_idx=2)
        pipeline_1.create_input(compounddatatype=self.triplet_cdt,dataset_name="input_4",dataset_idx=3)

        self.assertEquals(pipeline_1.check_input_names(), None)
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            pipeline_1.check_input_indices)
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            pipeline_1.clean)

    def test_pipeline_several_rawinputs_coexists_with_several_nonraw_inputs_names_clash_clean_bad(self):

        # Define 1-step pipeline with conflicting input names
        pipeline_1 = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");
        pipeline_1.create_input(dataset_name="clashing_name",dataset_idx=1)
        pipeline_1.create_input(compounddatatype=self.triplet_cdt,dataset_name="clashing_name",dataset_idx=3)
        pipeline_1.create_input(dataset_name="input_2",dataset_idx=2)
        pipeline_1.create_input(compounddatatype=self.triplet_cdt,dataset_name="input_4",dataset_idx=4)

        self.assertRaisesRegexp(
            ValidationError,
            "Input names overlap raw input names",
            pipeline_1.check_input_names)
        self.assertRaisesRegexp(
            ValidationError,
            "Input names overlap raw input names",
            pipeline_1.clean)

class SingleRawOutput_replaced_by_uniqueness_constraints_tests(Copperfish_Raw_Setup):

    def test_transformation_rawoutput_name_collides_with_non_raw_output_name_clean_bad(self):
        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)

        # Define colliding output name "a_b_c" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c",dataset_idx = 2)
        self.script_4_1_M.save()

        # The names conflict
        self.assertRaisesRegexp(
            ValidationError,
            "Output names overlap raw output names",
            self.script_4_1_M.check_output_names) 

        self.assertRaisesRegexp(
            ValidationError,
            "Output names overlap raw output names",
            self.script_4_1_M.clean) 

    def test_transformation_rawoutput_index_collides_with_non_raw_index_bad(self):
        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)

        # Define output name "a_b_c" of type "triplet_cdt" at colliding index = 1
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 1)
        self.script_4_1_M.save()

        # The indices conflict
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_output_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean) 

class SeveralRawOutputs_replaced_by_uniqueness_constraints_tests(Copperfish_Raw_Setup):
    def test_transformation_several_rawoutputs_with_several_nonraw_outputs_clean_indices_clash_bad(self):
        # Note: the method we define here doesn't correspond to reality; the
        # script doesn't have all of these outputs.

        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define raw output "RawOutput4" at index = 2
        self.script_4_1_M.create_output(dataset_name = "RawOutput2",dataset_idx = 2)

        # Define output name "foo" of type "doublet_cdt" at index = 2
        self.script_4_1_M.create_output(compounddatatype = self.doublet_cdt,dataset_name = "Output2",dataset_idx = 2)
            
        # Define output name "a_b_c_squared" of type "triplet_cdt" at index = 3
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 3)

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None);
        self.assertEquals(self.script_4_1_M.check_input_names(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.check_output_indices);
        self.assertEquals(self.script_4_1_M.check_output_names(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.script_4_1_M.clean);
                
    def test_transformation_several_rawoutputs_coexists_with_several_nonraw_outputs_names_clash_bad(self):
        # Note: the method we define here doesn't correspond to reality; the
        # script doesn't have all of these outputs.

        # Define raw output "a_b_c" at index = 1
        self.script_4_1_M.create_output(dataset_name = "a_b_c",dataset_idx = 1)
        
        # Define raw output "RawOutput4" at index = 4
        self.script_4_1_M.create_output(dataset_name = "ClashName",dataset_idx = 4)

        # Define output name "foo" of type "doublet_cdt" at index = 3
        self.script_4_1_M.create_output(compounddatatype = self.doublet_cdt,dataset_name = "ClashName",dataset_idx = 3)
            
        # Define output name "a_b_c_squared" of type "triplet_cdt" at index = 2
        self.script_4_1_M.create_output(compounddatatype = self.triplet_cdt,dataset_name = "a_b_c_squared",dataset_idx = 2)

        # Neither the names nor the indices conflict - this should pass
        self.assertEquals(self.script_4_1_M.check_input_indices(), None);
        self.assertEquals(self.script_4_1_M.check_input_names(), None);
        self.assertEquals(self.script_4_1_M.check_output_indices(), None);
        self.assertRaisesRegexp(
            ValidationError,
            "Output names overlap raw output names",
            self.script_4_1_M.check_output_names);
        self.assertRaisesRegexp(
            ValidationError,
            "Output names overlap raw output names",
            self.script_4_1_M.clean);

 
class PipelineRawOutputCable_replaced_by_uniqueness_constraints_tests(Copperfish_Raw_Setup):

    def test_pipeline_colliding_raw_output_name_clean_bad(self):
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
            raw_output_name="COLLIDE",
            raw_output_idx=3,
            step_providing_raw_output=1,
            provider_raw_output=raw_output_4)

        self.pipeline_1.outcables.create(
            output_name="COLLIDE",
            output_idx=2,
            step_providing_output=1,
            provider_output=output_3)

        self.assertRaisesRegexp(
            ValidationError,
            "Output names overlap raw output names",
            self.pipeline_1.clean)


    def test_pipeline_colliding_raw_output_idx_clean_bad(self):
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
            raw_output_name="foo",
            raw_output_idx=2,
            step_providing_raw_output=1,
            provider_raw_output=raw_output_4)

        self.pipeline_1.outcables.create(
            output_name="bar",
            output_idx=2,
            step_providing_output=1,
            provider_output=output_3)

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            self.pipeline_1.clean)
        
class PipelineStepInputCable_replaced_by_uniqueness_constraints_tests(Copperfish_Raw_Setup):

    def test_PSIC_clean_and_completely_wired_multiply_wired_same_source(self):
        # x -> x
        # x -> x

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
        
        # wire1 = string->string
        wire2 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=1),
            dest_pin=method_input.get_cdt().members.get(column_idx=1))

        self.assertEquals(wire1.clean(), None)
        self.assertEquals(wire2.clean(), None)

        errorMessage="Destination member \"1.* has multiple wires leading to it"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipeline_cable.clean_and_completely_wired)
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelineStep.clean)
        self.assertRaisesRegexp(ValidationError,errorMessage,myPipeline.clean)

    def test_PSIC_clean_and_completely_wired_multiply_wired_internal_steps(self):
        # Sub test 1
        # x -> y
        # y -> y
        # z -> z

        # Define pipeline
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version");

        # Define method with triplet_cdt input/output (string, string, string)
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)

        # Add method as 2 steps
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        step2 = myPipeline.steps.create(transformation=self.testmethod, step_num=2)

        # Cable the 2 internal steps together
        internal_cable = step2.cables_in.create(transf_input=method_input, step_providing_input=1, provider_output=method_output)

        wire1 = internal_cable.custom_wires.create(
            source_pin=method_output.get_cdt().members.get(column_idx=1),
            dest_pin=method_input.get_cdt().members.get(column_idx=1))

        wire2 = internal_cable.custom_wires.create(
            source_pin=method_output.get_cdt().members.get(column_idx=1),
            dest_pin=method_input.get_cdt().members.get(column_idx=2))

        wire3 = internal_cable.custom_wires.create(
            source_pin=method_output.get_cdt().members.get(column_idx=2),
            dest_pin=method_input.get_cdt().members.get(column_idx=2))

        wire4 = internal_cable.custom_wires.create(
            source_pin=method_output.get_cdt().members.get(column_idx=3),
            dest_pin=method_input.get_cdt().members.get(column_idx=3))

        errorMessage = "Destination member \"2.* has multiple wires leading to it"
        self.assertRaisesRegexp(ValidationError,errorMessage,internal_cable.clean_and_completely_wired)

    def test_PSIC_clean_and_completely_wired_multiply_wired_different_source(self):
        # x -> x
        # x -> y
        # y -> y
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
        
        # wire1 = string->string
        wire2 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=1),
            dest_pin=method_input.get_cdt().members.get(column_idx=2))

        wire3 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=2),
            dest_pin=method_input.get_cdt().members.get(column_idx=2))

        wire4 = pipeline_cable.custom_wires.create(
            source_pin=myPipeline_input.get_cdt().members.get(column_idx=3),
            dest_pin=method_input.get_cdt().members.get(column_idx=3))

        self.assertEquals(wire1.clean(), None)
        self.assertEquals(wire2.clean(), None)
        self.assertEquals(wire3.clean(), None)
        self.assertEquals(wire4.clean(), None)

        errorMessage="Destination member \"2.* has multiple wires leading to it"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipeline_cable.clean_and_completely_wired)
