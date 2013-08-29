"""
Old unit tests for Shipyard (Copperfish).

These are ones that predate August 24, 2013 (the introduction of ExecRecord
and its relatives) and need to be adapted to the new setting due to
deep changes in the architecture.
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


class Datasets_tests(Copperfish_Raw_Setup):
    """
    New tests to take into account raw inputs/outputs/datasets
    """

    def test_rawDataset_pipelineStep_set_pipelineStepRawOutput_also_valid_good(self):
        """ Pipeline_step is set, and pipeline_step_raw_output is also set """

        # Define a method with a raw output
        methodRawOutput = self.script_4_1_M.create_output(
            dataset_name="theOutput",
            dataset_idx=1)

        # Define a pipeline
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a raw Dataset with a defined source pipeline step and pipeline_step_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            rawdataset_1 = RawDataset(user=self.myUser,
                                      name="test dataset",
                                      dataset_file=File(f),
                                      pipeline_step=step1,
                                      pipeline_step_raw_output=methodRawOutput)

        self.assertEquals(rawdataset_1.clean(), None)

        # Generating transformation has no inputs, so complete_clean should pass
        self.assertEquals(rawdataset_1.complete_clean(), None)

    def test_rawDataset_pipelineStepRawOutput_set_but_pipeline_step_isnt_bad(self):
        # Define a method with a raw output
        methodRawOutput = self.script_4_1_M.create_output(
            dataset_name="theOutput",
            dataset_idx=1)

        # Define a pipeline
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a raw Dataset with a defined source pipeline step and pipeline_step_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            rawdataset_1 = RawDataset(user=self.myUser,
                                      name="test dataset",
                                      dataset_file=File(f),
                                      pipeline_step_raw_output=methodRawOutput)

        self.assertRaisesRegexp(ValidationError,
            "No PipelineStep specified but a raw output from a PipelineStep is",
            rawdataset_1.clean)

    def test_rawDataset_pipelineStep_set_pipelineStepRawOutput_notSet_bad(self):
        # Define a method with a raw output
        methodRawOutput = self.script_4_1_M.create_output(
            dataset_name="theOutput",
            dataset_idx=1)

        # Define a pipeline
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a raw Dataset with a defined source pipeline step and pipeline_step_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            rawdataset_1 = RawDataset(user=self.myUser,
                                      name="test dataset",
                                      dataset_file=File(f),
                                      pipeline_step=step1)

        self.assertRaisesRegexp(ValidationError,
            "PipelineStep is specified but no raw output from it is",
            rawdataset_1.clean)

    def test_rawDataset_pipelineStep_set_pipelineStepRawOutput_does_not_belong_to_specified_PS_bad(self):
        # Define a method with a raw output
        methodRawOutput = self.script_4_1_M.create_output(
            dataset_name="theOutput",
            dataset_idx=1)

        # Define a second method
        script_4_2_M = Method(revision_name="s4-2",revision_desc="s4-2",family = self.test_MF,driver = self.script_4_1_CRR)
        script_4_2_M.save()

        # Give it a raw output
        methodRawOutput2 = script_4_2_M.create_output(
            dataset_name="theOutput",
            dataset_idx=1)

        # Define a 2-step pipeline
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)
        step2 = pipeline_1.steps.create(transformation=script_4_2_M,step_num=2)

        # Define rawDataset with source pipeline step but pipelineStepRawOutput not belonging to step 1
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            rawdataset_1 = RawDataset(user=self.myUser,
                                      name="test dataset",
                                      dataset_file=File(f),
                                      pipeline_step=step1,
                                      pipeline_step_raw_output=methodRawOutput2)

        self.assertRaisesRegexp(ValidationError,
            "Specified PipelineStep does not produce specified TransformationRawOutput",
            rawdataset_1.clean)


    def test_dataset_cdt_matches_pipeline_step_CDT_good(self):
        """Link a dataset with a pipeline output and have CDTs match"""
        
        # Give a method a triplet CDT output
        methodOutput = self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt,
                                         dataset_name="theOutput",
                                         dataset_idx=1)
        
        # Define pipeline with 1 step containing the triplet CDT output
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a Dataset with a defined source pipeline step and pipeline_step_output of matching CDT
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_1 = Dataset(compounddatatype=self.triplet_cdt,
                                user=self.myUser,
                                name="test dataset",
                                dataset_file=File(f),
                                pipeline_step=step1,
                                pipeline_step_output=methodOutput)
            dataset_1.save()
            
        self.assertEquals(dataset_1.clean(), None)

    def test_dataset_cdt_doesnt_match_pipeline_step_CDT_bad(self):
        """Link a dataset with a pipeline output and have CDTs mismatch"""
        
        # Give a method a triplet CDT output
        methodOutput = self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt,
                                         dataset_name="theOutput",
                                         dataset_idx=1)
        
        # Define pipeline with 1 step containing the triplet CDT output
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define CDT "triplet_cdt_DIFFERENT" with 3 members
        self.triplet_cdt_DIFFERENT = CompoundDatatype()
        self.triplet_cdt_DIFFERENT.save()
        self.triplet_cdt_DIFFERENT.members.create(datatype=self.string_dt,column_name="c^2",column_idx=1)
        self.triplet_cdt_DIFFERENT.members.create(datatype=self.string_dt,column_name="b^2",column_idx=2)
        self.triplet_cdt_DIFFERENT.members.create(datatype=self.string_dt,column_name="a^2",column_idx=3)

        # Define a Dataset with a defined source pipeline step and pipeline_step_output but with a conflicting CDT
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_1 = Dataset(compounddatatype=self.triplet_cdt_DIFFERENT,
                                user=self.myUser,
                                name="test dataset",
                                dataset_file=File(f),
                                pipeline_step=step1,
                                pipeline_step_output=methodOutput)
            dataset_1.save()

        self.assertRaisesRegexp(ValidationError,
            "Dataset CDT does not match the CDT of the generating TransformationOutput",
            dataset_1.clean)

    def test_dataset_pipelineStep_not_set_pipelineStepOutput_set_bad(self):
        """Dataset is linked to a pipeline step output, but not a pipeline step"""

        # Give a method a triplet CDT output
        methodOutput = self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt,
            dataset_name="theOutput",
            dataset_idx=1)

        # Define a pipeline with 1 step containing the triplet CDT output
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a Dataset without a defined source pipeline step but with a pipeline_step_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_1 = Dataset(compounddatatype=self.triplet_cdt,
                                user=self.myUser,
                                name="test dataset",
                                dataset_file=File(f),
                                pipeline_step_output=methodOutput)
            dataset_1.save()

        self.assertRaisesRegexp(ValidationError,
            "No PipelineStep specified but an output from a PipelineStep is",
            dataset_1.clean)

    def test_dataset_pipelineStep_set_pipelineStepOutput_None_bad(self):
        """Dataset comes from a pipeline step but no pipeline step output specified"""

        # Give a method a triplet CDT output
        methodOutput = self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt,
                                         dataset_name="theOutput",
                                         dataset_idx=1)

        # Define a pipeline with 1 step containing the triplet CDT output
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a Dataset with a defined source pipeline step but no pipeline_step_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_1 = Dataset(compounddatatype=self.triplet_cdt,
                                user=self.myUser,
                                name="test dataset",
                                dataset_file=File(f),
                                pipeline_step=step1)
            dataset_1.save()

        self.assertRaisesRegexp(ValidationError,"PipelineStep is specified but no output from it is",dataset_1.clean)

    def test_dataset_pipelineStep_set_pipelineStepOutput_does_not_belong_to_PS_bad(self):
        """Dataset comes from a pipeline step and pipeline step output is specified but does not belong to the pipeline step """
        
        # Give a method a triplet CDT output
        methodOutput = self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt,
                                         dataset_name="theOutput",
                                         dataset_idx=1)

        self.script_4_2_M = Method(revision_name="s4-2",revision_desc="s4-2",family = self.test_MF,driver = self.script_4_1_CRR)
        self.script_4_2_M.full_clean()
        self.script_4_2_M.save()

        methodOutput2 = self.script_4_2_M.create_output(compounddatatype=self.triplet_cdt,
                                         dataset_name="theOutput",
                                         dataset_idx=1)

        # Define a pipeline with 1 step containing the triplet CDT output
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a Dataset with a defined source pipeline step but no pipeline_step_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_1 = Dataset(compounddatatype=self.triplet_cdt,
                                user=self.myUser,
                                name="test dataset",
                                dataset_file=File(f),
                                pipeline_step=step1,
                                pipeline_step_output=methodOutput2)
            dataset_1.save()

        self.assertRaisesRegexp(ValidationError,"Specified PipelineStep does not produce specified TransformationOutput",dataset_1.clean)

    def test_dataset_CSV_incoherent_header_bad(self):
        """Loads a coherent vs incoherent dataset"""

        # Give a method a triplet CDT output
        methodOutput = self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt,
                                         dataset_name="theOutput",
                                         dataset_idx=1)

        # Define a pipeline with 1 step containing the triplet CDT output
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define an INCOHERENT Dataset with a defined source pipeline step and pipeline_step_output
        with open(os.path.join(samplecode_path, "script_5_headers_reversed_incoherent.csv"), "rb") as f:
            dataset_1 = Dataset(compounddatatype=self.triplet_cdt,
                                user=self.myUser,
                                name="test dataset",
                                dataset_file=File(f),
                                pipeline_step=step1,
                                pipeline_step_output=methodOutput)
            dataset_1.save()
        
        self.assertRaisesRegexp(ValidationError,
            "Column 1 of Dataset \"test dataset \(created by john on .*\)\" is named",
            dataset_1.clean)


    def test_dataset_numrows(self):
        # Give a method a triplet CDT output
        methodOutput = self.script_4_1_M.create_output(compounddatatype=self.triplet_cdt,
                                         dataset_name="theOutput",
                                         dataset_idx=1)

        # Define a pipeline with 1 step containing the triplet CDT output
        pipeline_1 = self.test_PF.members.create(revision_name="v1",revision_desc="First version");
        step1 = pipeline_1.steps.create(transformation=self.script_4_1_M,step_num=1)

        # Define a Dataset with a defined source pipeline step and pipeline_step_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_1 = Dataset(compounddatatype=self.triplet_cdt,
                                user=self.myUser,
                                name="test dataset",
                                dataset_file=File(f),
                                pipeline_step=step1,
                                pipeline_step_output=methodOutput)
            dataset_1.save()
            dataset_1.clean()

        self.assertEquals(dataset_1.num_rows(), 5)

    def test_abstractDataset_clean_for_correct_MD5_checksum(self):

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_1 = Dataset(compounddatatype=self.triplet_cdt,
                                user=self.myUser,
                                name="test dataset",
                                dataset_file=File(f))
            dataset_1.save()

        self.assertEquals(dataset_1.MD5_checksum, '')
        dataset_1.clean()
        self.assertEquals(dataset_1.MD5_checksum, '5f1821eebedee3b3ca95cf6b25a2abb1')

    def test_Dataset_clean_num_rows_less_than_producing_PS_output_min_row_bad(self):
        # Dataset.clean(): numRows matches producing PS TransformationOutput min/max row

        # Define pipeline with method at step 1 containing min_row constrained output
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")

        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,
                                                       min_row=10,
                                                       dataset_name="method_out",
                                                       dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)

        # Define a Dataset coming from step1
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            created_dataset = Dataset(
                pipeline_step_output=method_output,
                compounddatatype=self.triplet_cdt,
                pipeline_step=step1,
                user=self.myUser,
                name="uploaded_dataset",
                description="hehe",
                dataset_file=File(f))

        errorMessage = "Dataset .* was produced by TransformationOutput .* but has too few rows"
        self.assertRaisesRegexp(ValidationError,errorMessage, created_dataset.clean)

    def test_Dataset_clean_num_rows_more_than_producing_PS_output_max_row_bad(self):
        # Dataset.clean(): numRows matches producing PS TransformationOutput min/max row

        # Define pipeline with method at step 1 containing min_row constrained output
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")

        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,
                                                       max_row=3,
                                                       dataset_name="method_out",
                                                       dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)

        # Define a Dataset coming from step1
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            created_dataset = Dataset(
                pipeline_step_output=method_output,
                compounddatatype=self.triplet_cdt,
                pipeline_step=step1,
                user=self.myUser,
                name="uploaded_dataset",
                description="hehe",
                dataset_file=File(f))

        errorMessage = "Dataset .* was produced by TransformationOutput .* but has too many rows"
        self.assertRaisesRegexp(ValidationError,errorMessage, created_dataset.clean)

    def test_Abstract_Dataset_complete_clean_producing_transformation_raw_inputs_not_quenched_by_raw_parent_bad(self):
        # If this dataset came from a producing PS transformation, then the producing
        # transformation must have had all of it's inputs quenched by it's parent

        # Define pipeline with pipeline input cabled to method at step 1
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        
        pipeline_input = myPipeline.raw_create_input(dataset_name="pipeline_in",
                                                      dataset_idx=1)
        
        method_raw_input = self.testmethod.raw_create_input(dataset_name="method_in",
                                                             dataset_idx=1)
        
        method_raw_input_2 = self.testmethod.raw_create_input(dataset_name="method_in_2",
                                                               dataset_idx=2)
        
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,
                                                       dataset_name="method_out",
                                                       dataset_idx=1)
        
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)

        # Only partially quench the method from the pipeline
        my_cable = step1.create_raw_cable(transf_raw_input=method_raw_input_2,
                                              pipeline_raw_input=pipeline_input)

        # Define raw parental Dataset
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = RawDataset(
                user=self.myUser,
                name="uploaded_dataset",
                description="hehe",
                dataset_file=File(f))
            uploaded_dataset.save()

        # Define non-raw child Dataset
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            created_dataset = Dataset(
                pipeline_step_output=method_output,
                compounddatatype=self.triplet_cdt,
                pipeline_step=step1,
                user=self.myUser,
                name="uploaded_dataset",
                description="hehe",
                dataset_file=File(f))
            created_dataset.save()
            
        self.assertEquals(uploaded_dataset.clean(), None)
        self.assertEquals(created_dataset.clean(), None)

        self.assertEquals(uploaded_dataset.complete_clean(), None)

        errorMessage = "Raw input .* of producing transformation of .* is not quenched"
        self.assertRaisesRegexp(ValidationError,errorMessage, created_dataset.complete_clean)

    def test_Abstract_Dataset_complete_clean_producing_transformation_inputs_not_quenched_by_parent_bad(self):
        # If this dataset came from a producing PS transformation, then the producing
        # transformation must have had all of it's inputs quenched by it's parent

        # Define pipeline with pipeline input cabled to method at step 1
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")

        pipeline_input = myPipeline.raw_create_input(dataset_name="pipeline_in",
                                                      dataset_idx=1)
        
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,
                                                     dataset_name="method_in",
                                                     dataset_idx=1)
        
        method_input_2 = self.testmethod.create_input(compounddatatype=self.triplet_cdt,
                                                       dataset_name="method_in_2",
                                                       dataset_idx=2)
        
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,
                                                       dataset_name="method_out",
                                                       dataset_idx=1)
        
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)

        # Only partially quench the method from the pipeline
        my_cable = step1.cables_in.create(transf_input=method_input_2,
                                          step_providing_input=0,
                                          provider_output=pipeline_input)

        # Define parental Dataset
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(
                compounddatatype=self.triplet_cdt,
                user=self.myUser,
                name="uploaded_dataset",
                description="hehe",
                dataset_file=File(f))
            uploaded_dataset.save()

        
        # Define non-raw child Dataset
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            created_dataset = Dataset(
                pipeline_step_output=method_output,
                compounddatatype=self.triplet_cdt,
                pipeline_step=step1,
                user=self.myUser,
                name="uploaded_dataset",
                description="hehe",
                dataset_file=File(f))
            created_dataset.save()

        self.assertEquals(uploaded_dataset.complete_clean(), None)
        errorMessage = "Input .* of producing transformation of .* is not quenched"
        self.assertRaisesRegexp(ValidationError,errorMessage, created_dataset.complete_clean)


class ParentDataset_DEPRACATED_tests(Copperfish_Raw_Setup):

    def test_ParentDataset_clean_nonRaw_child_good(self):
        pass

    def test_ParentDataset_clean_raw_child_good(self):
        pass

    def test_ParentDataset_clean_parent_input_does_not_belong_to_transformation_of_PS_producing_child_bad(self):
        pass

    def test_ParentDataset_clean_parent_input_does_not_belong_to_transformation_of_PS_producing_RAW_child_bad(self):
        pass

    def test_ParentDataset_clean_CDTs_match_and_specified_parent_input_min_max_rows_are_satisfied_good(self):
        pass

    def test_ParentDataset_clean_CDTs_match_but_less_rows_than_min_rows_bad(self):
        pass

    def test_ParentDataset_clean_CDTs_match_but_more_rows_than_max_rows_bad(self):
        pass

    def test_ParentDataset_clean_CDT_of_dataset_matches_cable_provider_output_good(self):
        pass

    def test_ParentDataset_clean_CDT_of_dataset_doesnt_match_cable_provider_output_bad(self):
        pass


class RunStepRawInput_tests(Copperfish_Raw_Setup):

    def test_runsteprawinput_clean_good(self):
        # Raw parents only come from a pipeline input

        # Define pipeline with raw input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_raw_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_raw_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_raw_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Define parental raw Dataset which was uploaded (Does not come from a runstep or a run)
        with open(os.path.join(samplecode_path, "script_6_raw_input.raw"), "rb") as f:
            uploaded_raw_dataset = RawDataset(
                user=self.myUser,
                name="uploaded_raw_dataset",
                description="hehe",
                dataset_file=File(f))
            uploaded_raw_dataset.save()

       # SIMULATED EXECUTION OCCURS HERE

       # Annotate execution of the pipeline (Define a run) along with step 1
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

       # Define non-raw child Dataset which comes from a runstep
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            child_dataset = Dataset(user=self.myUser,name="child_dataset",description="hehe",dataset_file=File(f),
                                    compounddatatype=self.triplet_cdt,
                                    runstep=pipelinestep_run,intermediate_output = method_output)
            child_dataset.save()

        # Annotate non-raw child with raw parental information
        raw_parental_annotation = pipelinestep_run.input_raw_datasets.create(rawdataset=uploaded_raw_dataset,raw_cable_fed_to=initial_raw_cable)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        self.assertEquals(raw_parental_annotation.clean(), None)

    def test_runsteprawinput_clean_referenced_table_does_not_belong_to_PS_raw_cables_in_bad(self):
        # Raw parents only come from a pipeline input
        
        # Define pipeline with raw input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_raw_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_raw_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_raw_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Make an unrelated step with an unrelated cable
        step_unrelated = myPipeline.steps.create(transformation=self.testmethod, step_num=2)
        cable_unrelated = step_unrelated.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Define parental raw Dataset which was uploaded (Does not come from a runstep or a run)
        with open(os.path.join(samplecode_path, "script_6_raw_input.raw"), "rb") as f:
            uploaded_raw_dataset = RawDataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f))
            uploaded_raw_dataset.save()

       # SIMULATED EXECUTION OCCURS HERE

       # Annotate execution of the pipeline (Define a run) along with step 1
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

       # Define non-raw child Dataset which comes from a runstep
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            child_dataset = Dataset(user=self.myUser,name="child_dataset",description="hehe",dataset_file=File(f),
                                    compounddatatype=self.triplet_cdt,
                                    runstep=pipelinestep_run,intermediate_output = method_output)
            child_dataset.save()

        # Annotate non-raw child with raw parental information with an INCORRECT RAW CABLE
        # that does not belong to the runstep's PS raw_cables_in
        raw_parental_annotation = pipelinestep_run.input_raw_datasets.create(rawdataset=uploaded_raw_dataset,raw_cable_fed_to=cable_unrelated)

        errorMessage = "Specified raw cable for RunStepRawInput \"Runstep RunStep object has input raw dataset uploaded_raw_dataset\(raw\) \(created by .*\) feeding into cable Pipeline test pipeline family foo step 2:method_raw_in\(raw\)\" does not belong to the corresponding PipelineStep"
        self.assertRaisesRegexp(ValidationError,errorMessage,raw_parental_annotation.clean)


class RunStepInput_tests(Copperfish_Raw_Setup):
    
    def test_runstepinput_clean_good(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Define an uploaded parental raw Dataset (Uploaded implies there is neither a runstep nor a run)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",
                                       dataset_file=File(f),compounddatatype=self.triplet_cdt)
            uploaded_dataset.save()

        # EXECUTE() WOULD OCCUR HERE

        # Annotate execution of the pipeline (A run) and step1 of that pipeline (A runstep)
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define non-raw child Dataset that comes from a runstep (IE, it has an intermediate_output + runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            child_dataset = Dataset(user=self.myUser,name="child_dataset",description="hehe",dataset_file=File(f),
                                    compounddatatype=self.triplet_cdt,
                                    runstep=pipelinestep_run,intermediate_output = method_output)
            child_dataset.save()

        # Annotate child with parental inputs into initial_cable (The cable leading into step1)
        parental_annotation = pipelinestep_run.input_datasets.create(dataset=uploaded_dataset,cable_fed_to=initial_cable)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        self.assertEquals(parental_annotation.clean(), None)

    def test_runstepinput_clean_dataset_is_wrong_CDT_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Define an uploaded parental raw Dataset (Uploaded implies there is neither a runstep nor a run)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.doublet_cdt)
            uploaded_dataset.save()

       # SIMULATED EXECUTION OCCURS HERE

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define non-raw child Dataset that comes from a runstep (IE, it has an intermediate_output + runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            child_dataset = Dataset(user=self.myUser,name="child_dataset",description="hehe",dataset_file=File(f),
                                    compounddatatype=self.triplet_cdt,
                                    runstep=pipelinestep_run, intermediate_output = method_output)
            child_dataset.save()

        # Annotate non-raw child with non-raw parental information - parent cannot fit into initial_cable due to mismatching CDT
        parental_annotation = pipelinestep_run.input_datasets.create(dataset=uploaded_dataset,cable_fed_to=initial_cable)

        errorMessage = "Dataset .* is not of the expected CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage,parental_annotation.clean)

    def test_runstepinput_clean_dataset_is_right_CDT_too_few_rows_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1,min_row=100)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Define parental raw Dataset which was uploaded (Does not come from a runstep or a run)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt)
            uploaded_dataset.save()

       # SIMULATED EXECUTION OCCURS HERE

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

       # Define non-raw child Dataset which comes from a runstep
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            child_dataset = Dataset(user=self.myUser,name="child_dataset",description="hehe",dataset_file=File(f),
                                    compounddatatype=self.triplet_cdt,
                                    runstep=pipelinestep_run,intermediate_output = method_output)
            child_dataset.save()

        # Annotate non-raw child with non-raw parental information - parent cannot fit into initial_cable due to mismatching CDT
        parental_annotation = pipelinestep_run.input_datasets.create(
            dataset=uploaded_dataset,
            cable_fed_to=initial_cable)

        errorMessage = "Dataset .* has too few rows for TransformationInput .*"
        self.assertRaisesRegexp(ValidationError,errorMessage,parental_annotation.clean)

    def test_runstepinput_clean_dataset_is_right_CDT_too_many_rows_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1,max_row=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Define parental raw Dataset which was uploaded (Does not come from a runstep or a run)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt)
            uploaded_dataset.save()

       # SIMULATED EXECUTION OCCURS HERE

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

       # Define non-raw child Dataset which comes from a runstep
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            child_dataset = Dataset(user=self.myUser,name="child_dataset",description="hehe",dataset_file=File(f),
                                    compounddatatype=self.triplet_cdt,
                                    runstep=pipelinestep_run,intermediate_output = method_output)
            child_dataset.save()

        # Annotate non-raw child with non-raw parental information - parent cannot fit into initial_cable due to mismatching CDT
        parental_annotation = pipelinestep_run.input_datasets.create(dataset=uploaded_dataset,cable_fed_to=initial_cable)

        errorMessage = "Dataset .* has too many rows for TransformationInput .*"
        self.assertRaisesRegexp(ValidationError,errorMessage,parental_annotation.clean)


    def test_runstepinput_clean_referenced_cable_does_not_belong_to_corresponding_pipelineStep_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Define some unrelated steps and cables
        step1_unrelated = myPipeline.steps.create(transformation=self.testmethod, step_num=2)
        initial_cable_unrelated = step1_unrelated.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Define parental raw Dataset which was uploaded (Does not come from a runstep or a run)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt)
            uploaded_dataset.save()

       # SIMULATED EXECUTION OCCURS HERE

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

       # Define non-raw child Dataset which comes from a runstep
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            child_dataset = Dataset(user=self.myUser,name="child_dataset",description="hehe",dataset_file=File(f),
                                    compounddatatype=self.triplet_cdt,
                                    runstep=pipelinestep_run,intermediate_output = method_output)
            child_dataset.save()

        # Annotate non-raw child with non-raw parental information - parent cannot fit into initial_cable due to mismatching CDT
        parental_annotation = pipelinestep_run.input_datasets.create(dataset=uploaded_dataset,cable_fed_to=initial_cable_unrelated)

        errorMessage = "Cable .* for RunStepInput .* feeding into cable .* does not belong to the correct PipelineStep"
        self.assertRaisesRegexp(ValidationError,errorMessage,parental_annotation.clean)


class Dataset_new_tests(Copperfish_Raw_Setup):
    
    def test_dataset_clean_runstep_specified_but_intermediate_output_not_specified_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define a Dataset with a runstep but no intermediate_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt,
                                       runstep=pipelinestep_run)

        errorMessage = "RunStep is specified but no output from it is"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)


    def test_dataset_clean_runstep_not_specified_intermediate_output_specified_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # This dataset is the output of method_output
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt,
                                       intermediate_output=method_output)

            errorMessage = "No RunStep specified but an intermediate output is"
            self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

    def test_dataset_clean_intermediate_output_TRO_is_from_incorrect_transformation_implied_by_runstep_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Establish CRR as a method within a given method family
        script_unrelated = Method(revision_name="s4",revision_desc="s4",family = self.test_MF,driver = self.script_4_1_CRR)
        script_unrelated.save()
        step2 = myPipeline.steps.create(transformation=script_unrelated, step_num=2)

        # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        pipelinestep_run_unrelated = step2.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step2)
        
        # The intermediate_output TRO is inconsistent with the pipeline step implied by the runstep
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt,
                                       runstep= pipelinestep_run_unrelated,
                                       intermediate_output=method_output)

        errorMessage = "PipelineStep of specified RunStep does not produce specified TransformationOutput"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

    def test_dataset_clean_intermediate_output_TRO_from_correct_transformation_but_is_incorrect_CDT_bad(self):
        
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.doublet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)
        
        # The intermediate_output TRO exists with respect to the runstep, but has a different CDT from the dataset
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            myDataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt,
                                       runstep= pipelinestep_run,
                                       intermediate_output=method_output)

        errorMessage = "Dataset CDT does not match the CDT of the generating TransformationOutput"
        self.assertRaisesRegexp(ValidationError,errorMessage,myDataset.clean)

    def test_dataset_clean_dataset_has_too_few_rows_to_feed_be_product_of_intermediate_output_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1,max_row=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)
        
        # The intermediate_output TRO is inconsistent with the pipeline step implied by the runstep
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt,
                                       runstep=pipelinestep_run,
                                       intermediate_output=method_output)

        errorMessage = "Dataset .* was produced by TransformationOutput .* but has too many rows"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

    def test_dataset_clean_dataset_has_too_many_rows_to_feed_be_product_of_intermediate_output_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1,min_row=100)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

        # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)
        
        # The intermediate_output TRO is inconsistent with the pipeline step implied by the runstep
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt,
                                       runstep=pipelinestep_run,
                                       intermediate_output=method_output)

        errorMessage = "Dataset .* was produced by TransformationOutput .* but has too few rows"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

    def test_dataset_clean_run_specified_but_final_output_not_specified_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt,
                                       run=pipeline_run)

        errorMessage = "Run is specified but no final output from it is"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

    def test_dataset_clean_run_unspecified_but_final_output_is_specified_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)
        pipeline_out = myPipeline.create_output(compounddatatype=self.triplet_cdt,dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            uploaded_dataset = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt,
                                       final_output=pipeline_out)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "No Run specified but a final output is"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

    def test_dataset_clean_final_output_isnt_produced_by_the_pipeline_implied_by_run_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)
        pipeline_out = myPipeline.create_output(compounddatatype=self.triplet_cdt,dataset_name="pipeline_out",dataset_idx=1)

        # Define unrelated pipeline with an output
        myPipeline_unrelated = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_out_unrelated = myPipeline_unrelated.create_output(compounddatatype=self.triplet_cdt,dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                                   compounddatatype=self.triplet_cdt,
                                                   run=pipeline_run,final_output=pipeline_out_unrelated)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "Pipeline of specified Run does not produce specified TransformationOutput"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

    def test_dataset_clean_final_output_CDTs_dont_match_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)
        pipeline_out = myPipeline.create_output(compounddatatype=self.doublet_cdt,dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                                   compounddatatype=self.triplet_cdt,
                                                   run=pipeline_run,final_output=pipeline_out)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "Dataset CDT does not match the CDT of the generating TransformationOutput"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

    def test_dataset_clean_too_few_rows_for_final_output_TRO_bad(self):
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)
        pipeline_out = myPipeline.create_output(compounddatatype=self.triplet_cdt,dataset_name="pipeline_out",dataset_idx=1,min_row=100)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                                   compounddatatype=self.triplet_cdt,
                                                   run=pipeline_run,final_output=pipeline_out)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "Dataset .* was produced by TransformationOutput .* but has too few rows"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

    def test_dataset_clean_too_many_rows_for_final_output_TRO_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)
        pipeline_out = myPipeline.create_output(compounddatatype=self.triplet_cdt,dataset_name="pipeline_out",dataset_idx=1,max_row=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=1,provider_output=pipeline_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = Dataset(user=self.myUser,name="uploaded_raw_dataset",description="hehe",dataset_file=File(f),
                                                   compounddatatype=self.triplet_cdt,
                                                   run=pipeline_run,final_output=pipeline_out)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "Dataset .* was produced by TransformationOutput .* but has too many rows"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)



class RawDataset_new_tests(Copperfish_Raw_Setup):

    def test_rawDataset_clean_intermediate_raw_output_specified_but_not_runstep_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_raw_output = self.testmethod.create_output(dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                                      intermediate_raw_output=pipeline_raw_out)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "No RunStep specified but an intermediate raw output is"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

    def test_rawDataset_clean_intermediate_raw_output_unspecified_but_runstep_is_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_raw_output = self.testmethod.create_output(dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                                      runstep=pipelinestep_run)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "RunStep is specified but no raw output from it is"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

    def test_rawDataset_clean_intermediate_raw_output_not_from_same_transformation_implied_by_run_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        script_unrelated = Method(revision_name="s4",revision_desc="s4",family = self.test_MF,driver = self.script_4_1_CRR)
        script_unrelated.save()
        method_raw_output_unrelated = script_unrelated.create_output(dataset_name="method_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_raw_output = self.testmethod.create_output(dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

       # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Dataset comes
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                                      runstep=pipelinestep_run, intermediate_raw_output=method_raw_output_unrelated)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "PipelineStep of specified RunStep does not produce specified TransformationRawOutput"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

    def test_rawDataset_clean_final_raw_output_specified_but_not_run_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_raw_output = self.testmethod.create_output(dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Dataset is produced by a pipeline (But doesn't have run specified)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                                      final_raw_output=pipeline_raw_out)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "No Run specified but a final raw output is"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

    def test_rawDataset_clean_no_final_raw_output_specified_but_run_is_specified_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_raw_output = self.testmethod.create_output(dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Dataset is produced by a pipeline (But doesn't have run specified)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                                      run=pipeline_run)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "Run is specified but no final raw output from it is"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

    def test_rawDataset_clean_final_raw_output_TRO_doesnt_belong_to_pipeline_linked_to_run_bad(self):

        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        myPipeline_unrelated = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_out_unrelated = myPipeline_unrelated.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_raw_output = self.testmethod.create_output(dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (Define a run) and step1 of the pipeline
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Dataset is produced by a pipeline (But doesn't have run specified)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_produced_by_pipeline = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                                      run=pipeline_run,final_raw_output=pipeline_raw_out_unrelated)

        # The referenced cable belongs to the runstep's PS raw_cables_in so nothing is wrong
        errorMessage = "Pipeline of specified Run does not produce specified TransformationRawOutput"
        self.assertRaisesRegexp(ValidationError,errorMessage,dataset_produced_by_pipeline.clean)

class Runstep_tests(Copperfish_Raw_Setup):

    def test_runstep_clean_good(self):
        """
        Execution of a simple 1-step pipeline without any problems.
        """
        
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_raw_output = self.testmethod.create_output(dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (a run) and step1 of the pipeline (a runstep)
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define an uploaded dataset (Neither a run nor a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_uploaded = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f))
            dataset_uploaded.save()

        # Define a dataset produced by step 1 (It has a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_created = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                         runstep=pipelinestep_run,intermediate_raw_output=method_raw_output)
            dataset_created.save()

        # Annotate the inputs that were fed into run step 1
        pipelinestep_run.input_raw_datasets.create(rawdataset=dataset_uploaded,
                                                   raw_cable_fed_to=initial_cable)

        self.assertEquals(pipelinestep_run.clean(), None)


    def test_runstep_clean_unclean_output_dataset_propagation_check_bad(self):
        
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (a run) and step1 of the pipeline (a runstep)
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define an uploaded dataset (Neither a run nor a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_uploaded = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f))
            dataset_uploaded.save()

        # Define a dataset produced by step 1 (It has a runstep)
        with open(os.path.join(samplecode_path, "three_random_columns.csv"), "rb") as f:
            dataset_created = Dataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                      compounddatatype=self.triplet_cdt,
                                      runstep=pipelinestep_run,intermediate_output=method_output)
            dataset_created.save()

        # Annotate the inputs that were fed into run step 1
        pipelinestep_run.input_raw_datasets.create(rawdataset=dataset_uploaded,
                                                   raw_cable_fed_to=initial_cable)

        errorMessage = "Column .* of Dataset .* is named .*, not .* as specified by its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelinestep_run.clean)

    def test_runstep_clean_multiple_datasets_coming_from_same_intermediate_output_TRO_bad(self):
        
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (a run) and step1 of the pipeline (a runstep)
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define an uploaded dataset (Neither a run nor a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_uploaded = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f))
            dataset_uploaded.save()

        # Define a dataset produced by step 1 (It has a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_created = Dataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                      compounddatatype=self.triplet_cdt,
                                      runstep=pipelinestep_run,intermediate_output=method_output)
            dataset_created.save()

        # Only a single dataset is registered to method_out so far
        self.assertEquals(pipelinestep_run.clean(), None)

        # Define a SECOND dataset produced by step 1 from the same intermediate_output TRO
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_created2 = Dataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                      compounddatatype=self.triplet_cdt,
                                      runstep=pipelinestep_run,intermediate_output=method_output)
            dataset_created2.save()

        errorMessage = "Output .* of RunStep .* is multiply-quenched"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelinestep_run.clean)

    def test_runstep_clean_multiple_rawdatasets_coming_from_same_intermediate_output_TRO_bad(self):
        
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_raw_out = myPipeline.create_output(dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_raw_output = self.testmethod.create_output(dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (a run) and step1 of the pipeline (a runstep)
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define an uploaded dataset (Neither a run nor a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_uploaded = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f))
            dataset_uploaded.save()

        # Define a dataset produced by step 1 (It has a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_created = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                         runstep=pipelinestep_run,intermediate_raw_output=method_raw_output)
            dataset_created.save()

        # Only a single dataset is registered to method_out so far
        self.assertEquals(pipelinestep_run.clean(), None)

        # Define a SECOND dataset produced by step 1 from the same intermediate_output TRO
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_created2 = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                          runstep=pipelinestep_run,intermediate_raw_output=method_raw_output)
            dataset_created2.save()

        errorMessage = "Raw output .* of RunStep .* is multiply-quenched"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelinestep_run.clean)


    def test_runstep_clean_unclean_runstepinput_propagation_check_bad(self):
        # Recall: runstepinput checks that the dataset can be fed into the cable
        
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_in = myPipeline.create_input(compounddatatype=self.triplet_cdt,dataset_name="pipeline_in",dataset_idx=1)
        pipeline_out = myPipeline.create_output(compounddatatype=self.triplet_cdt,dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_input = self.testmethod.create_input(compounddatatype=self.triplet_cdt,dataset_name="method_in",dataset_idx=1,max_row=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.cables_in.create(transf_input=method_input,step_providing_input=0,provider_output=pipeline_in)

        # Annotate execution of the pipeline (a run) and step1 of the pipeline (a runstep)
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define an uploaded dataset (Neither a run nor a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_uploaded = Dataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                       compounddatatype=self.triplet_cdt)
            dataset_uploaded.save()

        # Define a dataset produced by step 1 (It has a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_created = Dataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                      compounddatatype=self.triplet_cdt,
                                      runstep=pipelinestep_run,intermediate_output=method_output)
            dataset_created.save()

        # Annotate the inputs that were fed into run step 1
        pipelinestep_run.input_datasets.create(dataset=dataset_uploaded,
                                               cable_fed_to=initial_cable)

        errorMessage = "Dataset .* has too many rows for TransformationInput .*"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelinestep_run.clean)

    def test_runstep_clean_unclean_runsteprawinput_propagation_check_bad(self):
        # Recall: runsteprawinput.clean checks that the referenced cable (raw_cable_fed_to) belongs to the runstep's PS raw_cables_in
        
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_out = myPipeline.create_output(compounddatatype=self.triplet_cdt,dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Make an unrelated cable from an unrelated PS
        step_unrelated = myPipeline.steps.create(transformation=self.testmethod, step_num=2)
        initial_cable_unrelated = step_unrelated.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (a run) and step1 of the pipeline (a runstep)
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        # Define an uploaded dataset (Neither a run nor a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_uploaded = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f))
            dataset_uploaded.save()

        # Define a dataset produced by step 1 (It has a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_created = Dataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                      compounddatatype=self.triplet_cdt,
                                      runstep=pipelinestep_run,intermediate_output=method_output)
            dataset_created.save()

        # Annotate the inputs that were fed into run step 1
        pipelinestep_run.input_raw_datasets.create(rawdataset=dataset_uploaded,
                                                   raw_cable_fed_to=initial_cable_unrelated)

        errorMessage = "Specified raw cable for RunStepRawInput .* does not belong to the corresponding PipelineStep"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelinestep_run.clean)

    def test_runstep_clean_ps_is_not_a_pipeline_but_child_run_is_set_bad(self):
        # Recall: runsteprawinput.clean checks that the referenced cable (raw_cable_fed_to) belongs to the runstep's PS raw_cables_in
        
        # Define pipeline with input
        myPipeline = self.test_PF.members.create(revision_name="foo",revision_desc="Foo version")
        pipeline_raw_in = myPipeline.create_input(dataset_name="pipeline_in",dataset_idx=1)
        pipeline_out = myPipeline.create_output(compounddatatype=self.triplet_cdt,dataset_name="pipeline_out",dataset_idx=1)

        # Define method at step 1 with triplet_cdt input and output: cable the pipeline input into the step1 method
        method_raw_input = self.testmethod.create_input(dataset_name="method_in",dataset_idx=1)
        method_output = self.testmethod.create_output(compounddatatype=self.triplet_cdt,dataset_name="method_out",dataset_idx=1)
        step1 = myPipeline.steps.create(transformation=self.testmethod, step_num=1)
        initial_cable = step1.create_raw_cable(transf_raw_input=method_raw_input,pipeline_raw_input=pipeline_raw_in)

        # Annotate execution of the pipeline (a run) and step1 of the pipeline (a runstep)
        pipeline_run = myPipeline.pipeline_instances.create(user=self.myUser)
        pipelinestep_run = step1.pipelinestep_instances.create(run=pipeline_run,pipelinestep=step1)

        pipeline_run_inner = myPipeline.pipeline_instances.create(user=self.myUser,parent_runstep=pipelinestep_run)
        pipelinestep_run_inner = step1.pipelinestep_instances.create(run=pipeline_run_inner,pipelinestep=step1)

        # Define an uploaded dataset (Neither a run nor a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_uploaded = RawDataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f))
            dataset_uploaded.save()

        # Define a dataset produced by step 1 (It has a runstep)
        with open(os.path.join(samplecode_path, "script_5_input.csv"), "rb") as f:
            dataset_created = Dataset(user=self.myUser,name="blah",description="hehe",dataset_file=File(f),
                                      compounddatatype=self.triplet_cdt,
                                      runstep=pipelinestep_run,intermediate_output=method_output)
            dataset_created.save()

        # Annotate the inputs that were fed into run step 1
        pipelinestep_run.input_raw_datasets.create(rawdataset=dataset_uploaded,
                                                   raw_cable_fed_to=initial_cable)

        errorMessage = "PipelineStep is a method but a child run exists"
        self.assertRaisesRegexp(ValidationError,errorMessage,pipelinestep_run.clean)


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
            dest_idx=1,
            dest_name="not_good")

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

        # Add an output cable
        outcable1 = self.my_pipeline.create_outcable(
            output_name="blah",
            output_idx=1,
            step_providing_output=1,
            provider_output=method_out)
        
        # Add wiring
        wire1 = outcable1.custom_outwires.create(
            source_pin=method_out.get_cdt().members.all()[0],
            dest_idx=1,
            dest_name="col1_str")

        wire2 = outcable1.custom_outwires.create(
            source_pin=method_out.get_cdt().members.all()[1],
            dest_idx=2,
            dest_name="col2_DNA")

        wire3 = outcable1.custom_outwires.create(
            source_pin=method_out.get_cdt().members.all()[0],
            dest_idx=3,
            dest_name="col3_str")

        wire4 = outcable1.custom_outwires.create(
            source_pin=method_out.get_cdt().members.all()[2],
            dest_idx=4,
            dest_name="col4_str")

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
