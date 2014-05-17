import os
import re
import sys
import tempfile

from django.core.files import File
from django.contrib.auth.models import User
from django.test import TestCase

from archive.models import MethodOutput
from librarian.models import SymbolicDataset
from metadata.models import Datatype, CompoundDatatype
from method.models import CodeResource, CodeResourceRevision, Method, MethodFamily
from pipeline.models import Pipeline, PipelineFamily
from sandbox.execute import Sandbox

from method.tests import samplecode_path
from constants import datatypes


class ExecuteTests(TestCase):

    def setUp(self):

		# Users + method/pipeline families
        self.myUser = User.objects.create_user('john', 'lennon@thebeatles.com', 'johnpassword')
        self.myUser.save()
        self.mf = MethodFamily(name="self.mf",description="self.mf desc"); self.mf.save()
        self.pf = PipelineFamily(name="self.pf", description="self.pf desc"); self.pf.save()

		# Code on file system
        self.mA_cr = CodeResource(name="mA_CR", description="self.mA_cr desc",filename="mA.py")
        self.mA_cr.save()
        self.mA_crr = CodeResourceRevision(coderesource=self.mA_cr, revision_name="v1", revision_desc="desc")
        with open(os.path.join(samplecode_path, "generic_script.py"), "rb") as f:
            self.mA_crr.content_file.save("generic_script.py",File(f))
        self.mA_crr.save()

        # Basic DTs
        self.string_dt = Datatype.objects.get(pk=datatypes.STR_PK)
        self.int_dt = Datatype.objects.get(pk=datatypes.INT_PK)

		# Basic CDTs
        self.pX_in_cdt = CompoundDatatype()
        self.pX_in_cdt.save()
        self.pX_in_cdtm_1 = self.pX_in_cdt.members.create(datatype=self.int_dt,column_name="pX_a",column_idx=1)
        self.pX_in_cdtm_2 = self.pX_in_cdt.members.create(datatype=self.int_dt,column_name="pX_b",column_idx=2)
        self.pX_in_cdtm_3 = self.pX_in_cdt.members.create(datatype=self.string_dt,column_name="pX_c",column_idx=3)

        self.mA_in_cdt = CompoundDatatype()
        self.mA_in_cdt.save()
        self.mA_in_cdtm_1 = self.mA_in_cdt.members.create(datatype=self.string_dt,column_name="a",column_idx=1)
        self.mA_in_cdtm_2 = self.mA_in_cdt.members.create(datatype=self.int_dt,column_name="b",column_idx=2)

        self.mA_out_cdt = CompoundDatatype()
        self.mA_out_cdt.save()
        self.mA_out_cdtm_1 = self.mA_out_cdt.members.create(datatype=self.int_dt,column_name="c",column_idx=1)
        self.mA_out_cdtm_2 = self.mA_out_cdt.members.create(datatype=self.string_dt,column_name="d",column_idx=2)

        self.symDS = SymbolicDataset.create_SD(os.path.join(samplecode_path,
            "input_for_test_C_twostep_with_subpipeline.csv"), user=self.myUser, name="pX_in_symDS",
            description="input to pipeline pX", cdt=self.pX_in_cdt)

        # Method + input/outputs
        self.mA = Method(revision_name="mA",revision_desc="mA_desc",family = self.mf,driver = self.mA_crr); self.mA.save()
        self.mA_in = self.mA.create_input(compounddatatype=self.mA_in_cdt,dataset_name="mA_in", dataset_idx=1)
        self.mA_out = self.mA.create_output(compounddatatype=self.mA_out_cdt,dataset_name="mA_out", dataset_idx=1)


    def tearDown(self):

        for crr in CodeResourceRevision.objects.all():
            crr.content_file.close()
            crr.content_file.delete()

        for method_out in MethodOutput.objects.all():
            method_out.output_log.close()
            method_out.output_log.delete()
            method_out.error_log.close()
            method_out.error_log.delete()


    def test_pipeline_execute_A_simple_onestep_pipeline(self):
        """Execution of a one-step pipeline."""

        # Define pipeline containing the method, and its input + outcables
        self.pX = Pipeline(family=self.pf, revision_name="pX_revision",revision_desc="X"); self.pX.save()
        self.X1_in = self.pX.create_input(compounddatatype=self.pX_in_cdt,dataset_name="pX_in",dataset_idx=1)
        self.step_X1 = self.pX.steps.create(transformation=self.mA,step_num=1)

        # Custom cable from pipeline input to method
        self.cable_X1_A1 = self.step_X1.cables_in.create(dest=self.mA_in,source_step=0,source=self.X1_in)
        self.wire1 = self.cable_X1_A1.custom_wires.create(source_pin=self.pX_in_cdtm_2,dest_pin=self.mA_in_cdtm_2)
        self.wire2 = self.cable_X1_A1.custom_wires.create(source_pin=self.pX_in_cdtm_3,dest_pin=self.mA_in_cdtm_1)

        # Pipeline outcables
        self.X1_outcable = self.pX.create_outcable(output_name="pX_out",output_idx=1,source_step=1,source=self.mA_out)
        self.pX.create_outputs()

        # Execute pipeline
        pipeline = self.pX
        inputs = [self.symDS]
        mySandbox = Sandbox(self.myUser, pipeline, inputs)
        mySandbox.execute_pipeline()

    def test_pipeline_execute_B_twostep_pipeline_with_recycling(self):
        """Two step pipeline with second step identical to the first"""

        # Define pipeline containing two steps with the same method + pipeline input
        self.pX = Pipeline(family=self.pf, revision_name="pX_revision",revision_desc="X"); self.pX.save()
        self.X1_in = self.pX.create_input(compounddatatype=self.pX_in_cdt,dataset_name="pX_in",dataset_idx=1)
        self.step_X1 = self.pX.steps.create(transformation=self.mA,step_num=1)
        self.step_X2 = self.pX.steps.create(transformation=self.mA,step_num=2)

        # Use the SAME custom cable from pipeline input to steps 1 and 2
        self.cable_X1_A1 = self.step_X1.cables_in.create(dest=self.mA_in,source_step=0,source=self.X1_in)
        self.wire1 = self.cable_X1_A1.custom_wires.create(source_pin=self.pX_in_cdtm_2,dest_pin=self.mA_in_cdtm_2)
        self.wire2 = self.cable_X1_A1.custom_wires.create(source_pin=self.pX_in_cdtm_3,dest_pin=self.mA_in_cdtm_1)
        self.cable_X1_A2 = self.step_X2.cables_in.create(dest=self.mA_in,source_step=0,source=self.X1_in)
        self.wire3 = self.cable_X1_A2.custom_wires.create(source_pin=self.pX_in_cdtm_2,dest_pin=self.mA_in_cdtm_2)
        self.wire4 = self.cable_X1_A2.custom_wires.create(source_pin=self.pX_in_cdtm_3,dest_pin=self.mA_in_cdtm_1)

        # POCs: one is trivial, the second uses custom outwires
        # Note: by default, create_outcables assumes the POC has the CDT of the source (IE, this is a TRIVIAL cable)
        self.outcable_1 = self.pX.create_outcable(output_name="pX_out_1",output_idx=1,source_step=1,source=self.mA_out)
        self.outcable_2 = self.pX.create_outcable(output_name="pX_out_2",output_idx=2,source_step=2,source=self.mA_out)

        # Define CDT for the second output (first output is defined by a trivial cable)
        self.pipeline_out2_cdt = CompoundDatatype()
        self.pipeline_out2_cdt.save()
        self.out2_cdtm_1 = self.pipeline_out2_cdt.members.create(column_name="c",column_idx=1,datatype=self.int_dt)
        self.out2_cdtm_2 = self.pipeline_out2_cdt.members.create(column_name="d",column_idx=2,datatype=self.string_dt)
        self.out2_cdtm_3 = self.pipeline_out2_cdt.members.create(column_name="e",column_idx=3,datatype=self.string_dt)

        # Second cable is not a trivial - we assign the new CDT to it
        self.outcable_2.output_cdt = self.pipeline_out2_cdt
        self.outcable_2.save()

        # Define custom outwires to the second output (Wire twice from cdtm 2)
        self.outwire1 = self.outcable_2.custom_outwires.create(source_pin=self.mA_out_cdtm_1,dest_pin=self.out2_cdtm_1)
        self.outwire2 = self.outcable_2.custom_outwires.create(source_pin=self.mA_out_cdtm_2,dest_pin=self.out2_cdtm_2)
        self.outwire3 = self.outcable_2.custom_outwires.create(source_pin=self.mA_out_cdtm_2,dest_pin=self.out2_cdtm_3)

        # Have the cables define the TOs of the pipeline
        self.pX.create_outputs()

        # Execute pipeline
        pipeline = self.pX
        inputs = [self.symDS]
        mySandbox = Sandbox(self.myUser, pipeline, inputs)
        mySandbox.execute_pipeline()

    def test_pipeline_execute_C_twostep_pipeline_with_subpipeline(self):
        """Two step pipeline with second step identical to the first"""

        # Define 2 member input and 1 member output CDTs for inner pipeline pY
        self.pY_in_cdt = CompoundDatatype()
        self.pY_in_cdt.save()
        self.pY_in_cdtm_1 = self.pY_in_cdt.members.create(column_name="pYA",column_idx=1,datatype=self.int_dt)
        self.pY_in_cdtm_2 = self.pY_in_cdt.members.create(column_name="pYB",column_idx=2,datatype=self.string_dt)

        self.pY_out_cdt = CompoundDatatype()
        self.pY_out_cdt.save()
        self.pY_out_cdt_cdtm_1 = self.pY_out_cdt.members.create(column_name="pYC",column_idx=1,datatype=self.int_dt)

        # Define 1-step inner pipeline pY
        self.pY = Pipeline(family=self.pf, revision_name="pY_revision",revision_desc="Y")
        self.pY.save()
        self.pY_in = self.pY.create_input(compounddatatype=self.pY_in_cdt,dataset_name="pY_in",dataset_idx=1)

        self.pY_step_1 = self.pY.steps.create(transformation=self.mA,step_num=1)
        self.pY_cable_in = self.pY_step_1.cables_in.create(dest=self.mA_in,source_step=0,source=self.pY_in)
        self.pY_cable_in.custom_wires.create(source_pin=self.pY_in_cdtm_1,dest_pin=self.mA_in_cdtm_2)
        self.pY_cable_in.custom_wires.create(source_pin=self.pY_in_cdtm_2,dest_pin=self.mA_in_cdtm_1)

        self.pY_cable_out = self.pY.outcables.create(output_name="pY_out",output_idx=1,source_step=1,source=self.mA_out,output_cdt=self.pY_out_cdt)
        self.pY_outwire1 = self.pY_cable_out.custom_outwires.create(source_pin=self.mA_out_cdtm_1,dest_pin=self.pY_out_cdt_cdtm_1)
        self.pY.create_outputs()

        # Define CDTs for the output of pX
        self.pX_out_cdt_1 = CompoundDatatype()
        self.pX_out_cdt_1.save()
        self.pX_out_cdt_1_cdtm_1 = self.pX_out_cdt_1.members.create(column_name="pXq",column_idx=1,datatype=self.int_dt)

        self.pX_out_cdt_2 = CompoundDatatype()
        self.pX_out_cdt_2.save()
        self.pX_out_cdt_2_cdtm_1 = self.pX_out_cdt_2.members.create(column_name="pXr",column_idx=1,datatype=self.string_dt)

        # Define outer 2-step pipeline with mA at step 1 and pY at step 2
        self.pX = Pipeline(family=self.pf, revision_name="pX_revision",revision_desc="X")
        self.pX.save()
        self.X1_in = self.pX.create_input(compounddatatype=self.pX_in_cdt,dataset_name="pX_in",dataset_idx=1)
        self.pX_step_1 = self.pX.steps.create(transformation=self.mA,step_num=1)
        self.pX_step_2 = self.pX.steps.create(transformation=self.pY,step_num=2)

        self.pX_step_1_cable = self.pX_step_1.cables_in.create(dest=self.mA_in,source_step=0,source=self.X1_in)
        self.pX_step_1_cable.custom_wires.create(source_pin=self.pX_in_cdtm_2,dest_pin=self.mA_in_cdtm_2)
        self.pX_step_1_cable.custom_wires.create(source_pin=self.pX_in_cdtm_3,dest_pin=self.mA_in_cdtm_1)

        self.pX_step_2_cable = self.pX_step_2.cables_in.create(dest=self.pY_in,source_step=1,source=self.mA_out)
        self.pX_step_2_cable.custom_wires.create(source_pin=self.mA_out_cdtm_1,dest_pin=self.pY_in_cdtm_1)
        self.pX_step_2_cable.custom_wires.create(source_pin=self.mA_out_cdtm_2,dest_pin=self.pY_in_cdtm_2)

        self.pX_outcable_1 = self.pX.outcables.create(output_name="pX_out_1",output_idx=1,source_step=1,source=self.mA_out,output_cdt=self.pX_out_cdt_2)
        self.pX_outcable_1.custom_outwires.create(source_pin=self.mA_out_cdtm_2,dest_pin=self.pX_out_cdt_2_cdtm_1)

        self.pX_outcable_2 = self.pX.outcables.create(output_name="pX_out_2",output_idx=2,source_step=2,source=self.pY.outputs.get(dataset_name="pY_out"),output_cdt=self.pX_out_cdt_1)
        self.pX_outcable_2.custom_outwires.create(source_pin=self.pY.outputs.get(dataset_name="pY_out").get_cdt().members.get(column_name="pYC"),dest_pin=self.pX_out_cdt_1_cdtm_1)

        self.pX.create_outputs()

        # Dataset for input during execution of pipeline
        input_SD = SymbolicDataset.create_SD(
            file_path=os.path.join(samplecode_path, "input_for_test_C_twostep_with_subpipeline.csv"),
            cdt=self.pX_in_cdt,
            make_dataset=True,
            user=self.myUser,
            name="input_dataset",
            description="symDS description")

        # Execute pipeline
        pipeline = self.pX
        inputs = [input_SD]
        mySandbox = Sandbox(self.myUser, pipeline, inputs)
        mySandbox.execute_pipeline()

class SandboxTests(ExecuteTests):

    def test_sandbox_no_input(self):
        """
        A Sandbox cannot be created if the pipeline has inputs but none are supplied.
        """
        p = Pipeline(family=self.pf, revision_name="blah", revision_desc="blah blah")
        p.save()
        p.create_input(compounddatatype=self.pX_in_cdt, dataset_name="in", dataset_idx=1)
        self.assertRaisesRegexp(ValueError,
                                re.escape('Pipeline "{}" expects 1 inputs, but 0 were supplied'.format(p)),
                                lambda: Sandbox(self.myUser, p, []))

    def test_sandbox_too_many_inputs(self):
        """
        A Sandbox cannot be created if the pipeline has fewer inputs than are supplied.
        """
        p = Pipeline(family=self.pf, revision_name="blah", revision_desc="blah blah")
        p.save()
        self.assertRaisesRegexp(ValueError,
                                re.escape('Pipeline "{}" expects 0 inputs, but 1 were supplied'.format(p)),
                                lambda: Sandbox(self.myUser, p, [self.symDS]))

    def test_sandbox_correct_inputs(self):
        """
        We can create a Sandbox if the supplied inputs match the pipeline inputs.
        """
        p = Pipeline(family=self.pf, revision_name="blah", revision_desc="blah blah")
        p.save()
        p.create_input(compounddatatype=self.pX_in_cdt, dataset_name="in", dataset_idx = 1,
            min_row = 8, max_row = 12)
        # Assert no ValueError raised.
        Sandbox(self.myUser, p, [self.symDS])

    def test_sandbox_raw_expected_nonraw_supplied(self):
        """
        Can't create a Sandbox if the pipeline expects raw input and we give it nonraw.
        """
        p = Pipeline(family=self.pf, revision_name="blah", revision_desc="blah blah")
        p.save()
        p.create_input(dataset_name="in", dataset_idx = 1)
        self.assertRaisesRegexp(ValueError,
                                re.escape('Pipeline "{}" expected input {} to be raw, but got one with '
                                          'CompoundDatatype "{}"'.format(p, 1, self.symDS.structure.compounddatatype)),
                                lambda: Sandbox(self.myUser, p, [self.symDS]))

    def test_sandbox_nonraw_expected_raw_supplied(self):
        """
        Can't create a Sandbox if the pipeline expects non-raw input and we give it raw.
        """
        p = Pipeline(family=self.pf, revision_name="blah", revision_desc="blah blah")
        p.save()
        p.create_input(compounddatatype=self.pX_in_cdt, dataset_name="in", dataset_idx=1)
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.write("foo")
        tf.close()
        raw_symDS = SymbolicDataset.create_SD(tf.name, user=self.myUser, name="foo", description="bar")
        self.assertRaisesRegexp(ValueError,
                                re.escape('Pipeline "{}" expected input {} to be of CompoundDatatype "{}", but got raw'
                                          .format(p, 1, self.pX_in_cdt)),
                                lambda: Sandbox(self.myUser, p, [raw_symDS]))
        os.remove(tf.name)

    def test_sandbox_cdt_mismatch(self):
        """
        Can't create a Sandbox if the pipeline expects an input with one CDT
        and we give it the wrong one.
        """
        p = Pipeline(family=self.pf, revision_name="blah", revision_desc="blah blah")
        p.save()
        p.create_input(compounddatatype=self.mA_in_cdt, dataset_name="in", dataset_idx = 1)
        self.assertRaisesRegexp(ValueError,
                                re.escape('Pipeline "{}" expected input {} to be of CompoundDatatype "{}", but got one '
                                          'with CompoundDatatype "{}"'
                                          .format(p, 1, self.mA_in_cdt, self.symDS.structure.compounddatatype)),
            lambda: Sandbox(self.myUser, p, [self.symDS]))

    def test_sandbox_too_many_rows(self):
        """
        Can't create a Sandbox if the pipeline expects an input with one CDT
        and we give it the wrong one.
        """
        p = Pipeline(family=self.pf, revision_name="blah", revision_desc="blah blah")
        p.save()
        p.create_input(compounddatatype=self.pX_in_cdt, dataset_name="in", dataset_idx = 1,
            min_row = 2, max_row = 4)
        self.assertRaisesRegexp(ValueError,
                                re.escape('Pipeline "{}" expected input {} to have between {} and {} rows, but got one '
                                'with {}'.format(p, 1, 2, 4, self.symDS.num_rows())),
            lambda: Sandbox(self.myUser, p, [self.symDS]))

    def test_sandbox_too_few_rows(self):
        """
        Can't create a Sandbox if the pipeline expects an input with one CDT
        and we give it the wrong one.
        """
        p = Pipeline(family=self.pf, revision_name="blah", revision_desc="blah blah")
        p.save()
        p.create_input(compounddatatype=self.pX_in_cdt, dataset_name="in", dataset_idx = 1,
            min_row = 20)
        self.assertRaisesRegexp(ValueError,
                                re.escape('Pipeline "{}" expected input {} to have between {} and {} rows, but got one '
                                'with {}'.format(p, 1, 20, sys.maxint, self.symDS.num_rows())),
            lambda: Sandbox(self.myUser, p, [self.symDS]))
