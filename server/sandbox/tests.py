import os

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
        self.string_dt = Datatype(name="string",description="str desc",Python_type=Datatype.STR)
        self.string_dt.save()
        self.int_dt = Datatype(name="int",description="int desc",Python_type=Datatype.INT)
        self.int_dt.save()

		# Basic CDTs
        self.tri_cdt = CompoundDatatype()
        self.tri_cdt.save()
        self.tri_cdtm_1 = self.tri_cdt.members.create(datatype=self.int_dt,column_name="a",column_idx=1)
        self.tri_cdtm_2 = self.tri_cdt.members.create(datatype=self.int_dt,column_name="b",column_idx=2)
        self.tri_cdtm_3 = self.tri_cdt.members.create(datatype=self.string_dt,column_name="c",column_idx=3)

        self.di_cdt = CompoundDatatype()
        self.di_cdt.save()
        self.di_cdtm_1 = self.di_cdt.members.create(datatype=self.string_dt,column_name="a",column_idx=1)
        self.di_cdtm_2 = self.di_cdt.members.create(datatype=self.int_dt,column_name="b",column_idx=2)

        self.output_cdt = CompoundDatatype()
        self.output_cdt.save()
        self.output_cdtm_1 = self.output_cdt.members.create(datatype=self.int_dt,column_name="c",column_idx=1)
        self.output_cdtm_2 = self.output_cdt.members.create(datatype=self.string_dt,column_name="d",column_idx=2)

        # Method + input/outputs
        self.mA = Method(revision_name="mA",revision_desc="mA_desc",family = self.mf,driver = self.mA_crr); self.mA.save()
        self.mA_in = self.mA.create_input(compounddatatype=self.di_cdt,dataset_name="mA_in", dataset_idx=1)
        self.mA_out = self.mA.create_output(compounddatatype=self.output_cdt,dataset_name="mA_out", dataset_idx=1)

        # Dataset for input during execution of pipeline
        self.symDS = SymbolicDataset.create_SD(
            file_path=os.path.join(samplecode_path, "triplet_cdt_for_pipeline_execute_test.csv"),
            cdt=self.tri_cdt,
            make_dataset=True,
            user=self.myUser,
            name="input_dataset",
            description="symDS description")

    def tearDown(self):
        for crr in CodeResourceRevision.objects.all():
            crr.content_file.close()
            crr.content_file.delete()

        for method_out in MethodOutput.objects.all():
            method_out.output_log.close()
            #method_out.output_log.delete()
            method_out.error_log.close()
            #method_out.error_log.delete()


    def test_pipeline_execute_A(self):
        """Execution of a one-step pipeline."""

        # Define pipeline containing the method, and its input + outcables
        self.pX = Pipeline(family=self.pf, revision_name="pX_revision",revision_desc="X"); self.pX.save()
        self.X1_in = self.pX.create_input(compounddatatype=self.tri_cdt,dataset_name="pX_in",dataset_idx=1)
        self.step_X1 = self.pX.steps.create(transformation=self.mA,step_num=1)

        # Custom cable from pipeline input to method
        self.cable_X1_A1 = self.step_X1.cables_in.create(dest=self.mA_in,source_step=0,source=self.X1_in)
        self.wire1 = self.cable_X1_A1.custom_wires.create(source_pin=self.tri_cdtm_2,dest_pin=self.di_cdtm_2)
        self.wire2 = self.cable_X1_A1.custom_wires.create(source_pin=self.tri_cdtm_3,dest_pin=self.di_cdtm_1)

        # Pipeline outcables
        self.X1_outcable = self.pX.create_outcable(output_name="pX_out",output_idx=1,source_step=1,source=self.mA_out)
        self.pX.create_outputs()

        # Execute pipeline
        pipeline = self.pX
        inputs = [self.symDS]
        mySandbox = Sandbox(self.myUser, pipeline, inputs)
        mySandbox.execute_pipeline()

    def test_pipeline_execute_B(self):
        """Two step pipeline with the second step identical to the first"""

        # Define pipeline containing two steps with the same method + pipeline input
        self.pX = Pipeline(family=self.pf, revision_name="pX_revision",revision_desc="X"); self.pX.save()
        self.X1_in = self.pX.create_input(compounddatatype=self.tri_cdt,dataset_name="pX_in",dataset_idx=1)
        self.step_X1 = self.pX.steps.create(transformation=self.mA,step_num=1)
        self.step_X2 = self.pX.steps.create(transformation=self.mA,step_num=2)

        # Use the SAME custom cable from pipeline input to steps 1 and 2
        self.cable_X1_A1 = self.step_X1.cables_in.create(dest=self.mA_in,source_step=0,source=self.X1_in)
        self.wire1 = self.cable_X1_A1.custom_wires.create(source_pin=self.tri_cdtm_2,dest_pin=self.di_cdtm_2)
        self.wire2 = self.cable_X1_A1.custom_wires.create(source_pin=self.tri_cdtm_3,dest_pin=self.di_cdtm_1)
        self.cable_X1_A2 = self.step_X2.cables_in.create(dest=self.mA_in,source_step=0,source=self.X1_in)
        self.wire3 = self.cable_X1_A2.custom_wires.create(source_pin=self.tri_cdtm_2,dest_pin=self.di_cdtm_2)
        self.wire4 = self.cable_X1_A2.custom_wires.create(source_pin=self.tri_cdtm_3,dest_pin=self.di_cdtm_1)

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
        self.outwire1 = self.outcable_2.custom_outwires.create(source_pin=self.output_cdtm_1 ,dest_pin=self.out2_cdtm_1)
        self.outwire2 = self.outcable_2.custom_outwires.create(source_pin=self.output_cdtm_2 ,dest_pin=self.out2_cdtm_2)
        self.outwire3 = self.outcable_2.custom_outwires.create(source_pin=self.output_cdtm_2 ,dest_pin=self.out2_cdtm_3)

        # Have the cables define the TOs of the pipeline
        self.pX.create_outputs()

        # Execute pipeline
        pipeline = self.pX
        inputs = [self.symDS]
        mySandbox = Sandbox(self.myUser, pipeline, inputs)
        mySandbox.execute_pipeline()