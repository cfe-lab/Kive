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

class CustomOutputWiring_obsolete_tests(Copperfish_Raw_Setup):
    def test_PipelineOutputCable_clean_dest_idx_must_consecutively_start_from_1(self):
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
        my_step1 = self.my_pipeline.steps.create(
            transformation=self.testmethod, step_num=1);

        # Add an output cable
        outcable1 = self.my_pipeline.create_outcable(
            output_name="blah",
            output_idx=1,
            step_providing_output=1,
            provider_output=method_out)
        
        # Add 3 wires that with dest_idx that do not consecutively increment by 1
        wire1 = outcable1.custom_outwires.create(
            source_pin=self.triplet_cdt.members.all()[0],
            dest_idx=2,
            dest_name="bad_destination")

        self.assertRaisesRegexp(
            ValidationError,
            "Columns defined by custom wiring on output cable \"Pipeline test pipeline family foo:1 \(blah\)\" are not consecutively indexed from 1",
            outcable1.clean)

        wire2 = outcable1.custom_outwires.create(
            source_pin=self.triplet_cdt.members.all()[0],
            dest_idx=3,
            dest_name="bad_destination2")

        wire3 = outcable1.custom_outwires.create(
            source_pin=self.triplet_cdt.members.all()[2],
            dest_idx=4,
            dest_name="bad_destination3")

        self.assertEquals(wire1.clean(), None)
        self.assertEquals(wire2.clean(), None)
        self.assertEquals(wire3.clean(), None)

        self.assertRaisesRegexp(
            ValidationError,
            "Columns defined by custom wiring on output cable \"Pipeline test pipeline family foo:1 \(blah\)\" are not consecutively indexed from 1",
            outcable1.clean)

class CopperfishDatasetAndDatasetStructureTests_changedbyrestructure(CopperfishExecRecordTests_setup):
    
    def test_Dataset_sourced_from_runstep_with_corresponding_ER_but_ERO_doesnt_exist(self):
        # This is now checked at the RunStep level.
        # A dataset linked with a runstep is clean if and only if a corresponding ERO must point to it

        # Define ER for pE, then a run using this ER
        pE_ER = self.pE.execrecords.create(tainted=False)
        pE_ERI_E1_in = pE_ER.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.E1_in)
        run_pE = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Define ER for mA, then a runstep using this ER
        mA_ER = self.mA.execrecords.create(tainted=False)
        ERI_A1_in = mA_ER.execrecordins.create(
            symbolicdataset=self.raw_symDS,
            generic_input=self.A1_rawin)
        runstep_stepE1 = self.step_E1.pipelinestep_instances.create(
            run=run_pE, execrecord=mA_ER, reused=False)

        # Define dataset that is generated by this this runstep
        self.runstep_symDS = SymbolicDataset()
        self.runstep_symDS.save()
        self.runstep_DS = None
        with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
            self.runstep_DS = Dataset(
                user=self.myUser, name="doublet", description="lol",
                dataset_file=File(f), runstep=runstep_stepE1,
                symbolicdataset=self.runstep_symDS)
            self.runstep_DS.save()
        self.runstep_DS_structure = DatasetStructure(dataset=self.runstep_DS,compounddatatype=self.doublet_cdt)
        self.runstep_DS_structure.save()

        errorMessage = "Dataset \".*\" comes from runstep \".*\", but has no corresponding ERO"
        self.assertRaisesRegexp(ValidationError, errorMessage, self.runstep_DS.clean)

class RunOutputCableTests_deprecated(CopperfishExecRecordTests_setup):

    # This test is deprecated (and incomplete, but why finish it?)
    # - RL, October 3, 2013
    def test_ROC_ERO_must_have_data_if_ROC_is_not_deleted(self):
        """If the POC is not marked for deletion, the ERO must have real data."""
        # Define ER for pE, then register a run.
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Create an ER for mA and its input cable, and then a corresponding RunStep
        # and RSIC.
        E03_11_ER = self.E03_11.execrecords.create()
        E03_11_ER.execrecordins.create(
            symbolicdataset=self.raw_symDS, generic_input=self.E3_rawin)
        E03_11_ER.execrecordouts.create(
            symbolicdataset=self.raw_symDS, generic_output=self.A1_rawin)
        
        mA_ER = self.mA.execrecords.create()
        mA_ER.execrecordins.create(
            symbolicdataset=self.raw_symDS, generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(
            symbolicdataset=self.doublet_symDS,
            generic_output=self.mA.outputs.get(dataset_name="A1_out"))

        step_E1_RS = self.step_E1.pipelinestep_instances.create(
            run=pE_run, execrecord=mA_ER)
        E03_11_RSIC = self.E03_11.psic_instances.create(
            runstep=step_E1_RS, execrecord=E03_11_ER)

        # Quick check: pE_run should be OK.
        self.assertEquals(pE_run.clean(), None)
        
        # Same for pD
        pD_ER = self.pD.execrecords.create()
        pD_run = self.pD.pipeline_instances.create(user=self.myUser,execrecord=pD_ER)
        
        # Create an execrecord for one of the POCs of pD.
        D11_21_ER = self.D11_21.execrecords.create()
        empty_sd = SymbolicDataset()
        empty_sd.save()
        source = D11_21_ER.execrecordins.create(
            symbolicdataset=empty_sd,
            generic_input=self.mB.outputs.get(dataset_name="B1_out"))
        dest = D11_21_ER.execrecordouts.create(
            symbolicdataset=empty_sd,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))

        # Bad case 1: sub-pipeline POC is not marked for deletion;
        # ERO does not have real data.
        D11_21_ROC = self.D11_21.poc_instances.create(
            run=pD_run, execrecord=D11_21_ER)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut .* should reference existent data",
            D11_21_ROC.clean)

        # Good case 1: sub-pipeline POC is not marked for deletion and ERO
        # has real data.
        source.symbolicdataset = self.triplet_3_rows_symDS
        source.save()
        dest.symbolicdataset = self.triplet_3_rows_symDS
        dest.save()
        self.assertEquals(D11_21_ROC.clean(), None)

        # Mark output D1_out of step 2 (pD) for deletion.
        self.step_E2.add_deletion(self.pD.outputs.get(dataset_name="D1_out"))
        
        # Good cases 2 and 3: sub-pipeline POC is marked for deletion;
        # ERO can have real data or not, either is OK.
        self.assertEquals(D11_21_ROC.clean(), None)

        source.symbolicdataset = empty_sd
        dest.symbolicdataset = empty_sd
        self.assertEquals(D11_21_ROC.clean(), None)
        

    def test_ROC_poc_belongs_to_pipeline_of_parent_Run(self):
        """POC belongs to the parent Run's Pipeline."""
        # Define ER for pE, then register a run.
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)
        
        # Same for pD
        pD_ER = self.pD.execrecords.create()
        pD_run = self.pD.pipeline_instances.create(user=self.myUser,execrecord=pD_ER)

        # Create an execrecord for one of the POCs.
        E21_41_ER = self.E21_41.execrecords.create()
        empty_sd_source = SymbolicDataset()
        empty_sd_source.save()
        empty_sd_dest = SymbolicDataset()
        empty_sd_dest.save()
        E21_41_ER.execrecordins.create(
            symbolicdataset=empty_sd_source,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        E21_41_ER.execrecordouts.create(
            symbolicdataset=empty_sd_dest,
            generic_output=self.pE.outputs.get(dataset_name="E1_out"))

        # Create an execrecord for one of the POCs of pD.
        D11_21_ER = self.D11_21.execrecords.create()
        empty_sd_source_2 = SymbolicDataset()
        empty_sd_source_2.save()
        empty_sd_dest_2 = SymbolicDataset()
        empty_sd_dest_2.save()
        D11_21_ER.execrecordins.create(
            symbolicdataset=empty_sd_source_2,
            generic_input=self.mB.outputs.get(dataset_name="B1_out"))
        D11_21_ER.execrecordouts.create(
            symbolicdataset=empty_sd_dest_2,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))

        
        # Good case: the ROC for E21_41 lists pE_run as its parent run.
        E21_41_ROC = self.E21_41.poc_instances.create(run=pE_run,
                                                      execrecord=E21_41_ER)
        self.assertEquals(E21_41_ROC.clean(), None)

        # Bad case: the ROC for E21_41 lists pD_run as its parent run.
        E21_41_ROC.run = pD_run
        self.assertRaisesRegexp(
            ValidationError,
            "POC .* does not belong to Pipeline .*",
            E21_41_ROC.clean)

    def test_ROC_reusing_ER_should_have_no_attached_data(self):
        """A ROC that reuses an ER should have no associated Dataset."""
        # Define ER for pE, then register a run.
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        E21_41_ER = self.E21_41.execrecords.create()
        empty_sd_source = SymbolicDataset()
        empty_sd_source.save()
        empty_sd_dest = SymbolicDataset()
        empty_sd_dest.save()
        E21_41_ER.execrecordins.create(
            symbolicdataset=empty_sd_source,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        E21_41_ER.execrecordouts.create(
            symbolicdataset=empty_sd_dest,
            generic_output=self.pE.outputs.get(dataset_name="E1_out"))

        # Good case: the reused ROC for E21_41 has no associated data.
        E21_41_ROC = self.E21_41.poc_instances.create(
            run=pE_run, execrecord=E21_41_ER, reused=True)
        self.assertEquals(E21_41_ROC.clean(), None)

        # Bad case: the ROC has associated data.
        self.singlet_DS.runoutputcable = E21_41_ROC
        self.singlet_DS.save()
        self.assertRaisesRegexp(
            ValidationError,
            "RunOutputCable .* reused an ExecRecord and should not have generated Dataset .*",
            E21_41_ROC.clean)




        
    def test_ROC_produces_output_so_ERO_must_be_consistent(self):
        # If a dataset comes from a ROC, the corresponding ERO should
        # have the correct data attached; if not, anything can be
        # attached to the ERO.
        
        # Define ER for pE, then register a run.
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)
        
        # Create an execrecord for one of the POCs.
        E31_42_ER = self.E31_42.execrecords.create()

        # First: check when it has no data.
        empty_sd = SymbolicDataset()
        empty_sd.save()

        source = E31_42_ER.execrecordins.create(
            symbolicdataset=empty_sd,
            generic_input=self.mC.outputs.get(dataset_name="C1_out"))
        # This is a trivial outcable so its symbolic dataset should be
        # the same as the ERI.
        dest = E31_42_ER.execrecordouts.create(
            symbolicdataset=empty_sd,
            generic_output=self.pE.outputs.get(dataset_name="E2_out"))
        
        E31_42_ROC = self.E31_42.poc_instances.create(
            run=pE_run, execrecord=E31_42_ER)
        # No associated data and ERO has no data, so this should be fine.
        self.assertEquals(E31_42_ROC.clean(), None)

        # Add real data to the ERO and do the same test (should be fine;
        # this is the case where data is added on a subsequent run of the
        # same cable).
        source.symbolicdataset = self.C1_out_symDS
        source.save()
        dest.symbolicdataset = self.C1_out_symDS
        dest.save()
        self.assertEquals(E31_42_ROC.clean(), None)

        # Register the real data we attached to the ERO with this ROC.
        # This should be fine.
        self.C1_out_symDS.dataset.runoutputcable = E31_42_ROC
        self.C1_out_symDS.dataset.save()
        self.assertEquals(E31_42_ROC.clean(), None)

        # Unregister the real data in preparation for the bad case.
        self.C1_out_symDS.dataset.runoutputcable = None
        self.C1_out_symDS.dataset.save()
        
        # Bad case: define a different dataset as generated by this ROC.
        self.singlet_DS.runoutputcable = E31_42_ROC
        self.singlet_DS.save()
        errorMessage = "Dataset \".*\" is not in an ERO of ExecRecord \".*\""
        self.assertRaisesRegexp(ValidationError, errorMessage, E31_42_ROC.clean)

    def test_ROC_ER_must_point_to_POC(self):
        # The ER must point to a POC, as method/pipelines have to do
        # with runsteps, not runs
        
        # Define a run for pE.
        pE_run = self.pE.pipeline_instances.create(user=self.myUser)

        # Create ER for mA.
        mA_ER = self.mA.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)

        # Create an execrecord for one of the POCs.
        E21_41_ER = self.E21_41.execrecords.create()
        empty_sd_source = SymbolicDataset()
        empty_sd_source.save()
        empty_sd_dest = SymbolicDataset()
        empty_sd_dest.save()
        E21_41_ER.execrecordins.create(
            symbolicdataset=empty_sd_source,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        E21_41_ER.execrecordouts.create(
            symbolicdataset=empty_sd_dest,
            generic_output=self.pE.outputs.get(dataset_name="E1_out"))
        
        # Create an execrecord for another POC.
        E31_42_ER = self.E31_42.execrecords.create()
        E31_42_ER.execrecordins.create(
            symbolicdataset=self.C1_out_symDS,
            generic_input=self.mC.outputs.get(dataset_name="C1_out"))
        E31_42_ER.execrecordouts.create(
            symbolicdataset=self.C1_out_symDS,
            generic_output=self.pE.outputs.get(dataset_name="E2_out"))
        
        # Good case: the ROC for E21_41 points to the correct ER.
        E21_41_ROC = self.E21_41.poc_instances.create(run=pE_run,
                                                      execrecord=E21_41_ER)
        self.assertEquals(E21_41_ROC.clean(), None)

        # Bad case 1: the ROC points to an ER linked to the wrong
        # thing (another POC).
        E21_41_ROC.execrecord = E31_42_ER
        error_msg = "RunOutputCable points to cable .* but corresponding ER does not"
        self.assertRaisesRegexp(ValidationError, error_msg, E21_41_ROC.clean)

        # Bad case 2: the ROC points to an ER linked to another wrong
        # thing (not a POC).
        E21_41_ROC.execrecord = mA_ER
        self.assertRaisesRegexp(ValidationError, error_msg, E21_41_ROC.clean)


class RunStepTests_deprecated(CopperfishExecRecordTests_setup):
        

    def test_runstep_ER_must_point_to_same_transformation_this_runstep_points_to(self):
        
        # Define ER + run for pE
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Define ER for mA
        mA_ER = self.mA.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)

        # Define runstep for mB
        step_E2_RS = self.step_E2.pipelinestep_instances.create(run=pE_run,execrecord=mA_ER)
        errorMessage = "RunStep points to transformation \".*\" but corresponding ER does not"
        self.assertRaisesRegexp(ValidationError,errorMessage,step_E2_RS.clean)

    def test_runstep_PS_must_belong_to_run_pipeline(self):
        # Runstep points to a PS and a run - they must be consistent wrt pipeline step

        # Define unrelated pipeline + ER + run
        self.pX = Pipeline(family=self.pf, revision_name="pX",revision_desc="X")
        self.pX.save()
        pX_ER = self.pX.execrecords.create()
        pX_run = self.pX.pipeline_instances.create(user=self.myUser,execrecord=pX_ER)

        # Define ER + runstep for step E1 (mA) - but connect it with the wrong run pX
        mA_ER = self.step_E1.transformation.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)
        step_E1_RS = self.step_E1.pipelinestep_instances.create(
            run=pX_run, execrecord=mA_ER)

        errorMessage = "PipelineStep \".*\" of RunStep \".*\" does not belong to Pipeline \".*\""
        self.assertRaisesRegexp(ValidationError,errorMessage,step_E1_RS.clean)

    def test_runsteps_that_reuse_ER_cannot_have_associated_output_datasets(self):
        # Define ER + run for pE
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Define ER for mA
        mA_ER = self.mA.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)

        # Define recycled runstep for mA
        step_E1_RS = self.step_E1.pipelinestep_instances.create(
            run=pE_run, execrecord=mA_ER, reused=True)
        step_E1_RS.clean()

        # Assign it a Dataset (which is impossible)
        self.impossible_symDS = SymbolicDataset()
        self.impossible_symDS.save()
        self.impossible_DS = None
        with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
            self.impossible_DS = Dataset(
                user=self.myUser, name="doublet", description="lol",
                dataset_file=File(f),
                runstep=step_E1_RS,symbolicdataset=self.impossible_symDS)
            self.impossible_DS.save()
        self.impossible_DS_structure = DatasetStructure(
            dataset=self.impossible_DS, compounddatatype=self.doublet_cdt)
        self.impossible_DS_structure.save()
        self.impossible_DS.clean()

        errorMessage = "RunStep \".*\" reused an ExecRecord and should not have generated any data"
        self.assertRaisesRegexp(ValidationError,errorMessage,step_E1_RS.clean)

    def test_runstep_output_datasets_from_this_RS_should_also_belong_to_ERO_of_this_ER(self):
        # Define ER + run for pE
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Define ER and runstep for mA
        mA_ER = self.mA.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)
        step_E1_RS = self.step_E1.pipelinestep_instances.create(
            run=pE_run, execrecord=mA_ER)

        # Assign it a Dataset (But do not assign the dataset to the corresponding ERO)
        self.impossible_symDS = SymbolicDataset()
        self.impossible_symDS.save()
        self.impossible_DS = None
        with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
            self.impossible_DS = Dataset(
                user=self.myUser, name="doublet", description="lol",
                dataset_file=File(f),
                runstep=step_E1_RS,symbolicdataset=self.impossible_symDS)
            self.impossible_DS.save()

        errorMessage = "Dataset \".*\" is not in an ERO of ExecRecord \".*\""
        self.assertRaisesRegexp(ValidationError,errorMessage,step_E1_RS.clean)

    def test_runstep_each_undeleted_TO_should_have_ERO_pointing_to_existent_dataset(self):
        # Define ER + run for pE
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Create a symDS (But do not give it actual dataset contents)
        self.impossible_symDS = SymbolicDataset()
        self.impossible_symDS.save()

        # Define ER and runstep for mA, along with an ERO that does not point to existent data
        mA_ER = self.mA.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.impossible_symDS,
                                    generic_output=self.A1_out)
        step_E1_RS = self.step_E1.pipelinestep_instances.create(
            run=pE_run, execrecord=mA_ER)

        errorMessage = "ExecRecordOut \".*\" should reference existent data"
        self.assertRaisesRegexp(ValidationError,errorMessage,step_E1_RS.clean)

    def test_runstep_if_runstep_PS_stores_a_method_child_run_should_not_be_set(self):

        # Define ER + run for pE
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Define ER and runstep for mA
        mA_ER = self.mA.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)
        step_E1_RS = self.step_E1.pipelinestep_instances.create(run=pE_run,execrecord=mA_ER)
        pE_run.parent_runstep = step_E1_RS
        pE_run.save()

        errorMessage = "PipelineStep is not a Pipeline but a child run exists"
        self.assertRaisesRegexp(ValidationError,errorMessage,step_E1_RS.clean)

    def test_runstep_complete_clean_PS_stores_pipeline_but_no_child_run(self):
        # Define ER + run for pE
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Define ER and runstep for pD
        pD_ER = self.pD.execrecords.create()
        pD_ER.execrecordins.create(symbolicdataset=self.D1_in_symDS,
                                   generic_input=self.D1_in)
        pD_ER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                   generic_input=self.D2_in)
        pD_ER.execrecordouts.create(
            symbolicdataset=self.triplet_3_rows_symDS,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))
        step_E2_RS = self.step_E2.pipelinestep_instances.create(
            run=pE_run, execrecord=pD_ER)

        # Define RSICs and corresponding ERs.
        E01_21_ER = self.E01_21.execrecords.create()
        E01_21_ER.execrecordins.create(symbolicdataset=self.triplet_symDS,
                                       generic_input=self.E1_in)
        E01_21_ER.execrecordouts.create(symbolicdataset=self.D1_in_symDS,
                                        generic_output=self.D1_in)
        E01_21_RSIC = self.E01_21.psic_instances.create(
            runstep=step_E2_RS, execrecord=E01_21_ER)

        E02_22_ER = self.E02_22.execrecords.create()
        E02_22_ER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                       generic_input=self.E2_in)
        E02_22_ER.execrecordouts.create(symbolicdataset=self.singlet_symDS,
                                        generic_output=self.D2_in)
        E02_22_RSIC = self.E02_22.psic_instances.create(
            runstep=step_E2_RS, execrecord=E02_22_ER)

        self.assertEqual(step_E2_RS.clean(), None)
        errorMessage = "Specified PipelineStep is a Pipeline but no child run exists"
        self.assertRaisesRegexp(ValidationError,errorMessage,step_E2_RS.complete_clean)


class RunTests_deprecated(CopperfishExecRecordTests_setup):

    def test_run_RS_must_be_consecutive(self):

        # Define ER for pE, then register a run
        pE_ER = self.pE.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # Define a complete ER for this PS's transformation, then add a runstep for this pipeline step
        mA_ER = self.step_E1.transformation.execrecords.create()
        mA_ER.execrecordins.create(symbolicdataset=self.raw_symDS,
                                   generic_input=self.A1_rawin)
        mA_ER.execrecordouts.create(symbolicdataset=self.doublet_symDS,
                                    generic_output=self.A1_out)
        step_E1_RS = self.step_E1.pipelinestep_instances.create(
            run=pE_run, execrecord=mA_ER)
        self.assertEqual(pE_run.clean(), None)

        # Do the same thing, but now add step 3
        mC_ER = self.step_E3.transformation.execrecords.create()
        mC_ER.execrecordins.create(symbolicdataset=self.C1_in_symDS,
                                   generic_input=self.C1_in)
        mC_ER.execrecordins.create(symbolicdataset=self.C2_in_symDS,
                                   generic_input=self.C2_in)
        mC_ER.execrecordouts.create(symbolicdataset=self.C1_out_symDS,
                                    generic_output=self.C1_out)
        mC_ER.execrecordouts.create(symbolicdataset=self.C2_out_symDS,
                                    generic_output=self.C2_rawout)
        mC_ER.execrecordouts.create(symbolicdataset=self.C3_out_symDS,
                                    generic_output=self.C3_rawout)
        step_E3_RS = self.step_E3.pipelinestep_instances.create(
            run=pE_run, execrecord=mC_ER)
        errorMessage = "RunSteps of Run \".*\" are not consecutively numbered starting from 1"
        self.assertRaisesRegexp(ValidationError,errorMessage,pE_run.clean)

    def test_run_ER_must_point_to_same_pipeline_this_run_points_to(self):

        # Define unrelated ER for pE's run
        ER_unrelated = self.pD.execrecords.create()
        pE_run = self.pE.pipeline_instances.create(
            user=self.myUser, execrecord=ER_unrelated)

        errorMessage = "Run points to pipeline \".*\" but corresponding ER does not"
        self.assertRaisesRegexp(ValidationError,errorMessage,pE_run.clean)

    def test_run_for_EROs_present_must_match_corresponding_RunOutputCables(self):

        # Define an ER + EROs
        pE_ER = self.pE.execrecords.create()
        E1_out_ERO = pE_ER.execrecordouts.create(
            symbolicdataset=self.C2_in_symDS,
            generic_output=self.pE.outputs.get(dataset_name="E1_out"))

        # Register it with a run
        pE_run = self.pE.pipeline_instances.create(user=self.myUser,execrecord=pE_ER)

        # If an EROs exists, a corresponding RunOutputCable must exist
        errorMessage = "ExecRecord of Run \".*\" has an entry for output \".*\" but no corresponding RunOutputCable exists"
        self.assertRaisesRegexp(ValidationError,errorMessage,pE_run.clean)
