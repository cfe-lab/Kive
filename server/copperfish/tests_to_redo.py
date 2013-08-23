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
