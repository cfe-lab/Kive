"""
Unit tests for Shipyard (Copperfish)
"""

from django.test import TestCase;
from copperfish.models import *;
from django.core.files import File;
from django.core.exceptions import ValidationError;

from tests_old import *;

class CopperfishExecRecordTests_setup(TestCase):
    
    def setUp(self):
        """
        Setup scenario to test ExecRecords.

        Method A     Inputs: Raw-A1_rawin
                     Outputs: Doublet-A1_out

        Method B     Inputs: Doublet-B1_in, Singlet-B2_in
                     Outputs: Triplet-B1_out

        Method C     Inputs: Triplet-C1_in, Doublet-C2_in
                     OutputS: Singlet-C1_out, Raw-C2_rawout, Raw-C3_rawout

        Pipeline D   Inputs: Doublet-D1_in, Singlet-D2_in
                     Outputs: Triplet-D1_out (< 5 rows)
                     Sequence: Method D

        Pipeline E   Inputs: Triplet-E1_in, Singlet-E2_in, Raw-E3_rawin
                     Outputs: Triplet-E1_out, Singlet-E2_out, Raw-E3_rawout
                     Sequence: Method A, Pipeline D, Method C
        """

        # Datatypes and CDTs
        with open(os.path.join(samplecode_path, "stringUT.py"), "rb") as f:
            self.string_dt = Datatype(name="string",description="string desc",verification_script=File(f),Python_type="str");
            self.string_dt.save()
        self.singlet_cdt = CompoundDatatype()
        self.singlet_cdt.save()
        self.singlet_cdt.members.create(datatype=self.string_dt,column_name="k",column_idx=1)
        self.doublet_cdt = CompoundDatatype()
        self.doublet_cdt.save()
        self.doublet_cdt.members.create(datatype=self.string_dt,column_name="x",column_idx=1)
        self.doublet_cdt.members.create(datatype=self.string_dt,column_name="y",column_idx=2)
        self.triplet_cdt = CompoundDatatype()
        self.triplet_cdt.save()
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="a",column_idx=1)
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="b",column_idx=2)
        self.triplet_cdt.members.create(datatype=self.string_dt,column_name="c",column_idx=3)

        # CRs and CRRs
        self.generic_cr = CodeResource(name="genericCR",description="Just a CR",filename="complement.py")
        self.generic_cr.save()
        with open(os.path.join(samplecode_path, "generic_script.py"), "rb") as f:
            self.generic_crRev = CodeResourceRevision(coderesource=self.generic_cr,revision_name="v1",revision_desc="desc",content_file=File(f))
            self.generic_crRev.save()

        # Method family, methods, and their input/outputs
        self.mf = MethodFamily(name="method family",description="Holds methods A/B/C"); self.mf.save()
        self.mA = Method(revision_name="Method A",revision_desc="A",family = self.mf,driver = self.generic_crRev); self.mA.save()
        self.A1_rawin = self.mA.create_input(dataset_name="A1_rawin", dataset_idx=1)
        self.A1_out = self.mA.create_output(compounddatatype=self.doublet_cdt,dataset_name="A1_out",dataset_idx=1)

        self.mB = Method(revision_name="Method B",revision_desc="B",family = self.mf,driver = self.generic_crRev); self.mB.save()
        self.B1_in = self.mB.create_input(compounddatatype=self.doublet_cdt,dataset_name="B1_in",dataset_idx=1)
        self.B2_in = self.mB.create_input(compounddatatype=self.singlet_cdt,dataset_name="B2_in",dataset_idx=2)
        self.B1_out = self.mB.create_output(compounddatatype=self.triplet_cdt,dataset_name="B1_out",dataset_idx=1)
        
        self.mC = Method(revision_name="Method C",revision_desc="C",family = self.mf,driver = self.generic_crRev); self.mC.save()
        self.C1_in = self.mC.create_input(compounddatatype=self.triplet_cdt,dataset_name="C1_in",dataset_idx=1)
        self.C2_in = self.mC.create_input(compounddatatype=self.doublet_cdt,dataset_name="C2_in",dataset_idx=2)
        self.C1_out = self.mC.create_output(compounddatatype=self.singlet_cdt,dataset_name="C1_out",dataset_idx=1)
        self.C2_rawout = self.mC.create_output(dataset_name="C2_rawout",dataset_idx=2)
        self.C3_rawout = self.mC.create_output(dataset_name="C3_rawout",dataset_idx=3)

        # Pipeline family, pipelines, and their input/outputs
        self.pf = PipelineFamily(name="Pipeline family", description="PF desc"); self.pf.save()
        self.pD = Pipeline(family=self.pf, revision_name="Pipeline D",revision_desc="D"); self.pD.save()
        self.D1_in = self.pD.create_input(compounddatatype=self.doublet_cdt,dataset_name="D1_in",dataset_idx=1)
        self.D2_in = self.pD.create_input(compounddatatype=self.singlet_cdt,dataset_name="D2_in",dataset_idx=2)
        self.pE = Pipeline(family=self.pf, revision_name="Pipeline E",revision_desc="E"); self.pE.save()
        self.E1_in = self.pE.create_input(compounddatatype=self.triplet_cdt,dataset_name="E1_in",dataset_idx=1)
        self.E2_in = self.pE.create_input(compounddatatype=self.singlet_cdt,dataset_name="E2_in",dataset_idx=2)
        self.E3_rawin = self.pE.create_input(dataset_name="E3_rawin",dataset_idx=3)

        # Pipeline steps
        self.step_D1 = pD.steps.create(transformation=self.mB,step_num=1)
        self.step_E1 = pE.steps.create(transformation=self.mA,step_num=1)
        self.step_E2 = pE.steps.create(transformation=self.pD,step_num=2)
        self.step_E3 = pE.steps.create(transformation=self.mC,step_num=3)

        # Pipeline cables (method_<sourceStep><index>_<targetStep><index>)
        self.D01_11 = self.step_D1.cables_in.create(transf_input=self.B1_in,step_providing_input=0,provider_output=self.D1_in)
        self.D02_12 = self.step_D1.cables_in.create(transf_input=self.B2_in,step_providing_input=0,provider_output=self.D2_in)
        self.E03_11 = self.step_E1.cables_in.create(transf_input=self.A1_rawin,step_providing_input=0,provider_output=self.E3_rawin)
        self.E01_21 = self.step_E2.cables_in.create(transf_input=self.D1_in,step_providing_input=0,provider_output=self.E1_in)
        self.E01_22 = self.step_E2.cables_in.create(transf_input=self.D2_in,step_providing_input=0,provider_output=self.E2_in)
        self.E11_32 = self.step_E3.cables_in.create(transf_input=self.C2_in,step_providing_input=1,provider_output=self.A1_out)
        self.E21_31 = self.step_E3.cables_in.create(transf_input=self.C1_in,step_providing_input=2,provider_output=self.D1_out)

        # Outcables and pipeline outputs
        self.D11_21 = self.pD.outcables.create(output_name="D1_out",output_idx=1,step_providing_output=1,provider_output=self.B1_out)
        self.pD.create_outputs()
        self.E21_41 = self.pE.outcables.create(output_name="E1_out",output_idx=1,step_providing_output=2,provider_output=self.step_E2.transformation.outputs.get(dataset_name="D1_out"))
        self.E31_42 = self.pE.outcables.create(output_name="E2_out",output_idx=2,step_providing_output=3,provider_outout=self.C1_out)
        self.E33_43 = self.pE.outcables.create(output_name="E3_rawout",output_idx=3,step_providing_output=3,provider_output=self.C3_rawout)
        self.pE.create_outputs()

        # Custom wiring
        self.E01_21.custom_wires.create(source_pin=self.triplet_cdt.members.all()[0],dest_pin=self.doublet_cdt.members.all()[1])
        self.E01_21.custom_wires.create(source_pin=self.triplet_cdt.members.all()[2],dest_pin=self.doublet_cdt.members.all()[0])
        self.E11_32.custom_wires.create(source_pin=self.doublet_cdt.members.all()[0],dest_pin=self.doublet_cdt.members.all()[1])
        self.E11_32.custom_wires.create(source_pin=self.doublet_cdt.members.all()[1],dest_pin=self.doublet_cdt.members.all()[0])

        self.E21_41.custom_wires.create(source_pin=self.triplet_cdt.members.all()[1],dest_pin=self.doublet_cdt.members.all()[0])
