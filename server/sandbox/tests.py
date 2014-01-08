import os

from django.core.files import File
from django.contrib.auth.models import User
from django.test import TestCase

from librarian.models import SymbolicDataset
from metadata.models import Datatype, CompoundDatatype
from method.models import CodeResource, CodeResourceRevision, Method, MethodFamily
from pipeline.models import Pipeline, PipelineFamily
from sandbox.execute import Sandbox

from method.tests import samplecode_path

class ExecuteTests(TestCase):

    def setUp(self):
        import shutil
        shutil.rmtree('/tmp/userjohn_run1')     # Clear the file system

        self.myUser = User.objects.create_user('john', 'lennon@thebeatles.com', 'johnpassword')
        self.myUser.save()

        # Define basic DTs/CDTs
        self.string_dt = Datatype(name="string",description="str desc",Python_type=Datatype.STR)
        self.string_dt.save()
        self.int_dt = Datatype(name="int",description="int desc",Python_type=Datatype.INT)
        self.int_dt.save()
        self.tri_cdt = CompoundDatatype()
        self.tri_cdt.save()
        self.tri_cdtm_1 = self.tri_cdt.members.create(datatype=self.int_dt,column_name="a",column_idx=1)
        self.tri_cdtm_2 = self.tri_cdt.members.create(datatype=self.int_dt,column_name="b",column_idx=2)
        self.tri_cdtm_3 = self.tri_cdt.members.create(datatype=self.string_dt,column_name="c",column_idx=3)

    def tearTown(selfself):
        import shutil
        shutil.rmtree('/tmp/userjohn_run1')     # Clear the file system

    def test_cable(self):
        """Test the cable execution of a simple one-step pipeline."""

        # Define a 1-step pipeline containing a single method and a non-trivial cable
        self.mA_cr = CodeResource(name="mA_CR", description="self.mA_cr desc",filename="mA.py")
        self.mA_cr.save()
        self.mA_crr = CodeResourceRevision(coderesource=self.mA_cr, revision_name="v1", revision_desc="desc")
        with open(os.path.join(samplecode_path, "generic_script.py"), "rb") as f:
            self.mA_crr.content_file.save("generic_script.py",File(f))
        self.mA_crr.save()
        self.mf = MethodFamily(name="self.mf",description="self.mf desc"); self.mf.save()
        self.mA = Method(revision_name="mA",revision_desc="mA_desc",family = self.mf,driver = self.mA_crr); self.mA.save()
        self.A1_in = self.mA.create_input(compounddatatype=self.tri_cdt,dataset_name="mA1_in", dataset_idx=1)

        self.pf = PipelineFamily(name="self.pf", description="self.pf desc"); self.pf.save()
        self.pX = Pipeline(family=self.pf, revision_name="pX_name",revision_desc="X"); self.pX.save()
        self.X1_in = self.pX.create_input(compounddatatype=self.tri_cdt,dataset_name="pX1_in",dataset_idx=1)
        self.step_X1 = self.pX.steps.create(transformation=self.mA,step_num=1)
        self.cable_X1_A1 = self.step_X1.cables_in.create(dest=self.A1_in,source_step=0,source=self.X1_in)

        # Custom wires drop the first member and swaps the second and third member
        self.wire1 = self.cable_X1_A1.custom_wires.create(source_pin=self.tri_cdtm_2,dest_pin=self.tri_cdtm_3)
        self.wire2 = self.cable_X1_A1.custom_wires.create(source_pin=self.tri_cdtm_3,dest_pin=self.tri_cdtm_2)

        # Simulate the upload of a dataset
        self.symDS = SymbolicDataset.create_SD(
            file_path=os.path.join(samplecode_path, "triplet_cdt_for_cable_test.csv"),
            cdt=self.tri_cdt,make_dataset=True,user=self.myUser,name="triplet", description="lol")

        # Prepare a sandbox for executing the cable of this pipeline
        pipeline = self.pX
        inputs = [self.symDS]
        mySandbox = Sandbox(self.myUser, pipeline, inputs)
        mySandbox.execute_pipeline()