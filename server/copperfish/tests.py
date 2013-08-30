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
        self.mf = MethodFamily(name="method_family",description="Holds methods A/B/C"); self.mf.save()
        self.mA = Method(revision_name="mA_name",revision_desc="A_desc",family = self.mf,driver = self.generic_crRev); self.mA.save()
        self.A1_rawin = self.mA.create_input(dataset_name="A1_rawin", dataset_idx=1)
        self.A1_out = self.mA.create_output(compounddatatype=self.doublet_cdt,dataset_name="A1_out",dataset_idx=1)

        self.mB = Method(revision_name="mB_name",revision_desc="B_desc",family = self.mf,driver = self.generic_crRev); self.mB.save()
        self.B1_in = self.mB.create_input(compounddatatype=self.doublet_cdt,dataset_name="B1_in",dataset_idx=1)
        self.B2_in = self.mB.create_input(compounddatatype=self.singlet_cdt,dataset_name="B2_in",dataset_idx=2)
        self.B1_out = self.mB.create_output(compounddatatype=self.triplet_cdt,dataset_name="B1_out",dataset_idx=1,max_row=5)

        self.mC = Method(revision_name="mC_name",revision_desc="C_desc",family = self.mf,driver = self.generic_crRev); self.mC.save()
        self.C1_in = self.mC.create_input(compounddatatype=self.triplet_cdt,dataset_name="C1_in",dataset_idx=1)
        self.C2_in = self.mC.create_input(compounddatatype=self.doublet_cdt,dataset_name="C2_in",dataset_idx=2)
        self.C1_out = self.mC.create_output(compounddatatype=self.singlet_cdt,dataset_name="C1_out",dataset_idx=1)
        self.C2_rawout = self.mC.create_output(dataset_name="C2_rawout",dataset_idx=2)
        self.C3_rawout = self.mC.create_output(dataset_name="C3_rawout",dataset_idx=3)

        # Pipeline family, pipelines, and their input/outputs
        self.pf = PipelineFamily(name="Pipeline_family", description="PF desc"); self.pf.save()
        self.pD = Pipeline(family=self.pf, revision_name="pD_name",revision_desc="D"); self.pD.save()
        self.D1_in = self.pD.create_input(compounddatatype=self.doublet_cdt,dataset_name="D1_in",dataset_idx=1)
        self.D2_in = self.pD.create_input(compounddatatype=self.singlet_cdt,dataset_name="D2_in",dataset_idx=2)
        self.pE = Pipeline(family=self.pf, revision_name="pE_name",revision_desc="E"); self.pE.save()
        self.E1_in = self.pE.create_input(compounddatatype=self.triplet_cdt,dataset_name="E1_in",dataset_idx=1)
        self.E2_in = self.pE.create_input(compounddatatype=self.singlet_cdt,dataset_name="E2_in",dataset_idx=2,min_row=10)
        self.E3_rawin = self.pE.create_input(dataset_name="E3_rawin",dataset_idx=3)

        # Pipeline steps
        self.step_D1 = self.pD.steps.create(transformation=self.mB,step_num=1)
        self.step_E1 = self.pE.steps.create(transformation=self.mA,step_num=1)
        self.step_E2 = self.pE.steps.create(transformation=self.pD,step_num=2)
        self.step_E3 = self.pE.steps.create(transformation=self.mC,step_num=3)

        # Pipeline cables and outcables
        self.D01_11 = self.step_D1.cables_in.create(transf_input=self.B1_in,step_providing_input=0,provider_output=self.D1_in)
        self.D02_12 = self.step_D1.cables_in.create(transf_input=self.B2_in,step_providing_input=0,provider_output=self.D2_in)
        self.D11_21 = self.pD.outcables.create(output_name="D1_out",output_idx=1,output_cdt=self.triplet_cdt,step_providing_output=1,provider_output=self.B1_out)
        self.pD.create_outputs()

        self.E03_11 = self.step_E1.cables_in.create(transf_input=self.A1_rawin,step_providing_input=0,provider_output=self.E3_rawin)
        self.E01_21 = self.step_E2.cables_in.create(transf_input=self.D1_in,step_providing_input=0,provider_output=self.E1_in)
        self.E01_22 = self.step_E2.cables_in.create(transf_input=self.D2_in,step_providing_input=0,provider_output=self.E2_in)
        self.E11_32 = self.step_E3.cables_in.create(transf_input=self.C2_in,step_providing_input=1,provider_output=self.A1_out)
        self.E21_31 = self.step_E3.cables_in.create(transf_input=self.C1_in,step_providing_input=2,provider_output=self.step_E2.transformation.outputs.get(dataset_name="D1_out"))
        self.E21_41 = self.pE.outcables.create(output_name="E1_out",output_idx=1,output_cdt=self.doublet_cdt,step_providing_output=2,provider_output=self.step_E2.transformation.outputs.get(dataset_name="D1_out"))
        self.E31_42 = self.pE.outcables.create(output_name="E2_out",output_idx=2,output_cdt=self.singlet_cdt,step_providing_output=3,provider_output=self.C1_out)
        self.E33_43 = self.pE.outcables.create(output_name="E3_rawout",output_idx=3,output_cdt=None,step_providing_output=3,provider_output=self.C3_rawout)
        self.pE.create_outputs()

        # Custom wiring/outwiring
        self.E01_21_wire1 = self.E01_21.custom_wires.create(source_pin=self.triplet_cdt.members.all()[0],dest_pin=self.doublet_cdt.members.all()[1])
        self.E01_21_wire2 = self.E01_21.custom_wires.create(source_pin=self.triplet_cdt.members.all()[2],dest_pin=self.doublet_cdt.members.all()[0])
        self.E11_32_wire1 = self.E11_32.custom_wires.create(source_pin=self.doublet_cdt.members.all()[0],dest_pin=self.doublet_cdt.members.all()[1])
        self.E11_32_wire2 = self.E11_32.custom_wires.create(source_pin=self.doublet_cdt.members.all()[1],dest_pin=self.doublet_cdt.members.all()[0])
        self.E21_41_wire1 = self.E21_41.custom_outwires.create(source_pin=self.triplet_cdt.members.all()[1],dest_pin=self.doublet_cdt.members.all()[0])
        self.E21_41_wire2 = self.E21_41.custom_outwires.create(source_pin=self.triplet_cdt.members.all()[2],dest_pin=self.doublet_cdt.members.all()[1])
        self.pE.clean()

        # Define a user
        self.myUser = User.objects.create_user('john', 'lennon@thebeatles.com', 'johnpassword')
        self.myUser.save()

        # Define uploaded datasets
        self.triplet_symDS = SymbolicDataset()
        self.triplet_symDS.save()
        self.triplet_DS = None
        with open(os.path.join(samplecode_path, "step_0_triplet.csv"), "rb") as f:
            self.triplet_DS = Dataset(user=self.myUser,name="triplet",description="lol",dataset_file=File(f),symbolicdataset=self.triplet_symDS)
            self.triplet_DS.save()
        self.triplet_DS_structure = DatasetStructure(dataset=self.triplet_DS,compounddatatype=self.triplet_cdt)
        self.triplet_DS_structure.save()
        self.triplet_DS.clean()

        self.singlet_symDS = SymbolicDataset()
        self.singlet_symDS.save()
        self.singlet_DS = None
        with open(os.path.join(samplecode_path, "step_0_singlet.csv"), "rb") as f:
            self.singlet_DS = Dataset(user=self.myUser,name="singlet",description="lol",dataset_file=File(f),symbolicdataset=self.singlet_symDS)
            self.singlet_DS.save()
        self.singlet_DS_structure = DatasetStructure(dataset=self.singlet_DS,compounddatatype=self.singlet_cdt)
        self.singlet_DS_structure.save()
        self.singlet_DS.clean()

        self.raw_symDS = SymbolicDataset()
        self.raw_symDS.save()
        self.raw_DS = None
        with open(os.path.join(samplecode_path, "step_0_raw.fasta"), "rb") as f:
            self.raw_DS = Dataset(user=self.myUser,name="raw",description="lol",dataset_file=File(f),symbolicdataset=self.raw_symDS)
            self.raw_DS.save()
        self.raw_DS.clean()


    def tearDown(self):
        """ Clear CodeResources, Datasets, and VerificationScripts folders"""

        for crr in CodeResourceRevision.objects.all():
            if crr.coderesource.filename != "":
                crr.content_file.delete()
                
        for ds in Datatype.objects.all():
            ds.verification_script.delete()

        for dataset in Dataset.objects.all():
            dataset.dataset_file.delete()

class CopperfishExecRecordTests(CopperfishExecRecordTests_setup):

    def test_ER_links_POC_so_ERI_must_link_TO_that_POC_gets_output_from(self):
        # ER links POC: ERI must link to the TO that the POC gets output from
        myER = self.E21_41.execrecords.create(tainted=False)

        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,generic_input=self.C1_out)
        self.assertRaisesRegexp(ValidationError,"ExecRecordIn \".*\" does not denote the TO that feeds the parent ExecRecord POC",myERI_bad.clean)

    def test_ER_doesnt_link_POC_so_ERI_musnt_link_TO(self):
        # ER doesn't link POC (So, method/pipeline): ERI must not link a TO (which would imply ER should link POC)
        myER = self.mA.execrecords.create(tainted=False)
        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,generic_input=self.C1_out)
        self.assertRaisesRegexp(ValidationError,"ExecRecordIn \".*\" denotes a PipelineOutputCable but parent ExecRecord does not",myERI_bad.clean)

    def test_ERI_linking_TI_must_be_member_of_pipeline_linked_by_ER(self):
        # ERI links TI: TI must be a member of the ER's pipeline
        myER = self.pE.execrecords.create(tainted=False)
        myERI_good = myER.execrecordins.create(symbolicdataset=self.triplet_symDS,generic_input=self.pE.inputs.get(dataset_name="E1_in"))
        self.assertEqual(myERI_good.clean(), None)
        
        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,generic_input=self.pD.inputs.get(dataset_name="D2_in"))
        self.assertRaisesRegexp(ValidationError,"Input \".*\" does not belong to Pipeline of ExecRecord \".*\"",myERI_bad.clean)

    def test_ER_links_pipelinemethod_so_ERI_must_link_cable_with_destination_TI_belonging_to_transformation(self):
        # ERI links PSIC (so input feeds a pipeline step) - destination TI of cable must belong to TI of that transformation
        myER = self.pD.execrecords.create(tainted=False)
        myERI_good = myER.execrecordins.create(symbolicdataset=self.triplet_symDS,generic_input=self.E01_21)
        self.assertEqual(myERI_good.clean(), None)
        
        myERI_bad = myER.execrecordins.create(symbolicdataset=self.triplet_symDS,generic_input=self.E21_31)
        self.assertRaisesRegexp(ValidationError,"Cable \".*\" does not feed Method/Pipeline of ExecRecord \".*\"",myERI_bad.clean)

    def test_ERI_dataset_must_match_rawunraw_state_of_generic_input_it_was_fed_into(self):
        # ERI has a dataset: it's raw/unraw state must match the raw/unraw state of the generic_input it was fed into

        myER_C = self.mC.execrecords.create(tainted=False)

        myERI_unraw_unraw = myER_C.execrecordins.create(symbolicdataset=self.triplet_symDS,generic_input=self.E21_31)
        self.assertEqual(myERI_unraw_unraw.clean(), None)

        myERI_raw_unraw_BAD = myER_C.execrecordins.create(symbolicdataset=self.raw_symDS,generic_input=self.E11_32)
        self.assertRaisesRegexp(ValidationError,"Dataset \".*\" cannot feed source \".*\"",myERI_raw_unraw_BAD.clean)

        myER_A = self.mA.execrecords.create(tainted=False)
        myERI_unraw_raw_BAD = myER_A.execrecordins.create(symbolicdataset=self.triplet_symDS,generic_input=self.E03_11)
        self.assertRaisesRegexp(ValidationError,"Dataset \".*\" cannot feed source \".*\"",myERI_unraw_raw_BAD.clean)
        myERI_unraw_raw_BAD.delete()
    
        myERI_raw_raw = myER_A.execrecordins.create(symbolicdataset=self.raw_symDS,generic_input=self.E03_11)
        myERI_raw_raw.clean()

    def test_ER_links_POC_ERI_links_TO_which_constrains_input_dataset_CDT(self):
        # ERI links with a TO (For a POC leading from source TO), the input dataset CDT is constrained by the source TO
        myER = self.E21_41.execrecords.create(tainted=False)

        # We annotate that triplet was fed into D1_out which was connected by E21_41
        myERI_wrong_CDT = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(ValidationError,"Dataset \".*\" is not of the expected CDT",myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        # Right CDT but wrong number of rows (It needs < 5, we have 10)
        myERI_too_many_rows = myER.execrecordins.create(symbolicdataset=self.triplet_symDS,generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(ValidationError,"Dataset \".*\" has too many rows to have come from TransformationOutput \".*\"",myERI_too_many_rows.clean)

    def test_ER_links_pipeline_ERI_links_TI_which_constrains_input_dataset_CDT(self):
        # ERI links with a TI (for pipeline inputs) - the dataset is constrained by the pipeline TI CDT

        myER = self.pE.execrecords.create(tainted=False)
        myERI_wrong_CDT = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,generic_input=self.E1_in)
        self.assertRaisesRegexp(ValidationError,"Dataset \".*\" is not of the expected CDT",myERI_wrong_CDT.clean)

        myERI_too_few_rows = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,generic_input=self.E2_in)
        self.assertRaisesRegexp(ValidationError,"Dataset \".*\" has too few rows to have come from TransformationInput \".*\"",myERI_too_few_rows.clean)
        

        # I don't get it - if we feed a dataset into the whole pipeline E this makes sense
        # but is a NEW dataset made when we feed one into pipeline D?

        
