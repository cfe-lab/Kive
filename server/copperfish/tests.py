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
        self.E02_22 = self.step_E2.cables_in.create(transf_input=self.D2_in,step_providing_input=0,provider_output=self.E2_in)
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

        # Define singlet, doublet, triplet, and raw uploaded datasets
        self.triplet_symDS = SymbolicDataset()
        self.triplet_symDS.save()
        self.triplet_DS = None
        with open(os.path.join(samplecode_path, "step_0_triplet.csv"), "rb") as f:
            self.triplet_DS = Dataset(user=self.myUser,name="triplet",description="lol",dataset_file=File(f),symbolicdataset=self.triplet_symDS)
            self.triplet_DS.save()
        self.triplet_DS_structure = DatasetStructure(dataset=self.triplet_DS,compounddatatype=self.triplet_cdt)
        self.triplet_DS_structure.save()
        self.triplet_DS.clean()

        self.doublet_symDS = SymbolicDataset()
        self.doublet_symDS.save()
        self.doublet_DS = None
        with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
            self.doublet_DS = Dataset(user=self.myUser,name="doublet",description="lol",dataset_file=File(f),symbolicdataset=self.doublet_symDS)
            self.doublet_DS.save()
        self.doublet_DS_structure = DatasetStructure(dataset=self.doublet_DS,compounddatatype=self.doublet_cdt)
        self.doublet_DS_structure.save()
        self.doublet_DS.clean()

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

        # Added September 30, 2013: symbolic dataset that results from E_01_21.
        self.D1_in_symDS = SymbolicDataset()
        self.D1_in_symDS.save()

        self.C1_in_symDS = SymbolicDataset()
        self.C1_in_symDS.save()
        self.C1_in_DS = None
        with open(os.path.join(samplecode_path, "C1_in_triplet.csv"), "rb") as f:
            self.C1_in_DS = Dataset(user=self.myUser,name="raw",description="lol",dataset_file=File(f),symbolicdataset=self.C1_in_symDS)
            self.C1_in_DS.save()
        self.C1_in_DS.clean()
        self.C1_in_DS_structure = DatasetStructure(dataset=self.C1_in_DS,compounddatatype=self.triplet_cdt)
        self.C1_in_DS_structure.save()
        self.C1_in_DS.clean()

        self.C2_in_symDS = SymbolicDataset()
        self.C2_in_symDS.save()
        self.C2_in_DS = None
        with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
            self.C2_in_DS = Dataset(user=self.myUser,name="raw",description="lol",dataset_file=File(f),symbolicdataset=self.C2_in_symDS)
            self.C2_in_DS.save()
        self.C2_in_DS.clean()
        self.C2_in_DS_structure = DatasetStructure(dataset=self.C2_in_DS,compounddatatype=self.doublet_cdt)
        self.C2_in_DS_structure.save()
        self.C2_in_DS.clean()

        self.C1_out_symDS = SymbolicDataset()
        self.C1_out_symDS.save()
        self.C1_out_DS = None
        with open(os.path.join(samplecode_path, "step_0_singlet.csv"), "rb") as f:
            self.C1_out_DS = Dataset(user=self.myUser,name="raw",description="lol",dataset_file=File(f),symbolicdataset=self.C1_out_symDS)
            self.C1_out_DS.save()
        self.C1_out_DS.clean()
        self.C1_out_DS_structure = DatasetStructure(dataset=self.C1_out_DS,compounddatatype=self.singlet_cdt)
        self.C1_out_DS_structure.save()
        self.C1_out_DS.clean()

        self.C2_out_symDS = SymbolicDataset()
        self.C2_out_symDS.save()
        self.C2_out_DS = None
        with open(os.path.join(samplecode_path, "step_0_raw.fasta"), "rb") as f:
            self.C2_out_DS = Dataset(user=self.myUser,name="raw",description="lol",dataset_file=File(f),symbolicdataset=self.C2_out_symDS)
            self.C2_out_DS.save()
        self.C2_out_DS.clean()

        self.C3_out_symDS = SymbolicDataset()
        self.C3_out_symDS.save()
        self.C3_out_DS = None
        with open(os.path.join(samplecode_path, "step_0_raw.fasta"), "rb") as f:
            self.C3_out_DS = Dataset(user=self.myUser,name="raw",description="lol",dataset_file=File(f),symbolicdataset=self.C3_out_symDS)
            self.C3_out_DS.save()
        self.C3_out_DS.clean()

        self.triplet_3_rows_symDS = SymbolicDataset()
        self.triplet_3_rows_symDS.save()
        self.triplet_3_rows_DS = None
        with open(os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), "rb") as f:
            self.triplet_3_rows_DS = Dataset(user=self.myUser,name="triplet",description="lol",dataset_file=File(f),symbolicdataset=self.triplet_3_rows_symDS)
            self.triplet_3_rows_DS.save()
        self.triplet_3_rows_DS_structure = DatasetStructure(dataset=self.triplet_3_rows_DS,compounddatatype=self.triplet_cdt)
        self.triplet_3_rows_DS_structure.save()
        self.triplet_3_rows_DS.clean()

    def tearDown(self):
        """ Clear CodeResources, Datasets, and VerificationScripts folders"""

        for crr in CodeResourceRevision.objects.all():
            if crr.coderesource.filename != "":
                crr.content_file.delete()
                
        for ds in Datatype.objects.all():
            ds.verification_script.delete()

        for dataset in Dataset.objects.all():
            dataset.dataset_file.delete()

class CopperfishRunStepTests(CopperfishExecRecordTests_setup):

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

        errorMessage = "RunStep's PipelineStep \".*\" does not belong to Pipeline \".*\""
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
        pD_ER.execrecordouts.create(symbolicdataset=self.C1_in_symDS,
                                    generic_output=self.pD.outputs.get(dataset_name="D1_out"))
        step_E2_RS = self.step_E2.pipelinestep_instances.create(
            run=pE_run, execrecord=pD_ER)

        self.assertEqual(step_E2_RS.clean(), None)
        errorMessage = "Specified PipelineStep is a Pipeline but no child run exists"
        self.assertRaisesRegexp(ValidationError,errorMessage,step_E2_RS.complete_clean)


class CopperfishRunTests(CopperfishExecRecordTests_setup):
    
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

class CopperfishExecRecordTests(CopperfishExecRecordTests_setup):

    def test_ER_links_POC_so_ERI_must_link_TO_that_POC_gets_output_from(self):
        # ER links POC: ERI must link to the TO that the POC gets output from
        myER = self.E21_41.execrecords.create(tainted=False)

        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO that feeds the parent ExecRecord POC",
            myERI_bad.clean)

    def test_ER_links_PSIC_so_ERI_must_link_TX_that_PSIC_is_fed_by(self):
        # ER links PSIC: ERI must link to the TO/TI that the PSIC is fed by
        myER = self.E_11_32.execrecords.create(tainted=False)

        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            myERI_bad.clean)
        
        yourER = self.E_02_22.execrecords.create(tainted=False)

        yourERI_bad = yourER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                                  generic_input=self.D2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            yourERI_bad.clean)

    def test_ER_doesnt_link_cable_so_ERI_mustnt_link_TO(self):
        # ER doesn't refer to a cable (So, method/pipeline): ERI must refer to a TI
        myER = self.mA.execrecords.create(tainted=False)
        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" must refer to a TI of the parent ExecRecord's Method/Pipeline",
            myERI_bad.clean)

    def test_ER_links_toplevel_pipeline_so_TI_of_ERI_must_be_member_of_pipeline(self):
        # ERI links TI: TI must be a member of the ER's method/pipeline
        myER = self.pE.execrecords.create(tainted=False)
        myERI_good = myER.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.pE.inputs.get(dataset_name="E1_in"))
        self.assertEqual(myERI_good.clean(), None)
        
        myERI_bad = myER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.pD.inputs.get(dataset_name="D2_in"))
        self.assertRaisesRegexp(ValidationError,"Input \".*\" does not belong to Method/Pipeline of ExecRecord \".*\"",myERI_bad.clean)

    def test_ER_links_sub_pipelinemethod_so_ERI_must_link_TI_belonging_to_transformation(self):
        # ER is a sub-pipeline/method - ERI must refer to TI of that transformation
        myER = self.pD.execrecords.create(tainted=False)
        myERI_good = myER.execrecordins.create(
            symbolicdataset=self.D1_in_symDS,
            generic_input=self.D1_in)
        self.assertEqual(myERI_good.clean(), None)
        
        myERI_bad = myER.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.D1_out)
        self.assertRaisesRegexp(ValidationError,"Cable \".*\" does not feed Method/Pipeline of ExecRecord \".*\"",myERI_bad.clean)

    def test_ERI_dataset_must_match_rawunraw_state_of_generic_input_it_was_fed_into(self):
        # ERI has a dataset: it's raw/unraw state must match the raw/unraw state of the generic_input it was fed into

        myER_C = self.mC.execrecords.create(tainted=False)

        myERI_unraw_unraw = myER_C.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.C1_in)
        self.assertEqual(myERI_unraw_unraw.clean(), None)

        myERI_raw_unraw_BAD = myER_C.execrecordins.create(
            symbolicdataset=self.raw_symDS,
            generic_input=self.C2_in)
        self.assertRaisesRegexp(ValidationError,"Dataset \".*\" cannot feed source \".*\"",myERI_raw_unraw_BAD.clean)
        myERI_raw_unraw_BAD.delete()

        myER_A = self.mA.execrecords.create(tainted=False)
        myERI_unraw_raw_BAD = myER_A.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.A1_rawin)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" cannot feed source \".*\"",
            myERI_unraw_raw_BAD.clean)
        myERI_unraw_raw_BAD.delete()
    
        myERI_raw_raw = myER_A.execrecordins.create(
            symbolicdataset=self.raw_symDS,
            generic_input=self.A1_rawin)
        self.assertEqual(myERI_raw_raw.clean(), None)

    def test_ER_links_POC_ERI_links_TO_which_constrains_input_dataset_CDT(self):
        # ERI links with a TO (For a POC leading from source TO), the input dataset CDT is constrained by the source TO
        myER = self.E21_41.execrecords.create(tainted=False)

        # We annotate that triplet was fed from D1_out into E21_41
        myERI_wrong_CDT = myER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" is not of the expected CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        # Right CDT but wrong number of rows (It needs < 5, we have 10)
        myERI_too_many_rows = myER.execrecordins.create(
            symbolicdataset=self.triplet_symDS,
            generic_input=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" has too many rows to have come from TransformationOutput \".*\"",
            myERI_too_many_rows.clean)

    def test_ER_links_pipeline_ERI_links_TI_which_constrains_input_dataset_CDT(self):
        # ERI links with a TI (for pipeline inputs) - the dataset is constrained by the pipeline TI CDT

        myER = self.pE.execrecords.create(tainted=False)
        myERI_wrong_CDT = myER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.E1_in)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" is not of the expected CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        myERI_too_few_rows = myER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.E2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" has too few rows for TransformationInput \".*\"",
            myERI_too_few_rows.clean)

        # A dataset of correct triplet CDT.
        self.triplet_large_symDS = SymbolicDataset()
        self.triplet_large_symDS.save()
        self.triplet_large_DS = None
        with open(os.path.join(samplecode_path, "triplet_cdt_large.csv"), "rb") as f:
            self.triplet_large_DS = Dataset(
                user=self.myUser, name="triplet", description="lol",
                dataset_file=File(f), symbolicdataset=self.triplet_large_symDS)
            self.triplet_large_DS.save()
        self.triplet_large_DS_structure = DatasetStructure(
            dataset=self.triplet_large_DS,
            compounddatatype=self.triplet_cdt)
        self.triplet_large_DS_structure.save()
        self.triplet_large_DS.clean()
        
        # Define dataset of correct CDT (singlet) with > 10 rows
        self.singlet_large_symDS = SymbolicDataset()
        self.singlet_large_symDS.save()
        self.singlet_large_DS = None
        with open(os.path.join(samplecode_path, "singlet_cdt_large.csv"), "rb") as f:
            self.singlet_large_DS = Dataset(
                user=self.myUser, name="singlet", description="lol",
                dataset_file=File(f), symbolicdataset=self.singlet_large_symDS)
            self.singlet_large_DS.save()
        self.singlet_large_DS_structure = DatasetStructure(
            dataset=self.singlet_large_DS,
            compounddatatype=self.singlet_cdt)
        self.singlet_large_DS_structure.save()
        self.singlet_large_DS.clean()

        myERI_right_E1 = myER.execrecordins.create(
            symbolicdataset=self.triplet_large_symDS,
            generic_input=self.E1_in)
        self.assertEqual(myERI_right_E1.clean(), None)

        myERI_right_E2 = myER.execrecordins.create(
            symbolicdataset=self.singlet_large_symDS,
            generic_input=self.E2_in)
        self.assertEqual(myERI_right_E2.clean(), None)

    def test_ER_links_pipelinestep_ERI_links_TI_which_constrains_input_CDT(self):
        # The transformation input of its PipelineStep constrains the dataset when the ER links with a method
        
        myER = self.mC.execrecords.create(tainted=False)
        myERI_wrong_CDT = myER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.C2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" is not of the expected CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        # Define dataset with correct CDT (doublet)
        self.doublet_symDS = SymbolicDataset()
        self.doublet_symDS.save()
        self.doublet_DS = None
        with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
            self.doublet_DS = Dataset(
                user=self.myUser, name="doublet", description="lol",
                dataset_file=File(f), symbolicdataset=self.doublet_symDS)
            self.doublet_DS.save()
        self.doublet_DS_structure = DatasetStructure(
            dataset=self.doublet_DS, compounddatatype=self.doublet_cdt)
        self.doublet_DS_structure.save()
        self.doublet_DS.clean()

        myERI_right_CDT = myER.execrecordins.create(
            symbolicdataset=self.doublet_symDS, generic_input=self.C2_in)
        self.assertEqual(myERI_right_CDT.clean(), None)

    def test_ER_links_with_POC_ERO_TO_must_belong_to_same_pipeline_as_ER_POC(self):
        # If the parent ER is linked with a POC, the ERO TO must belong to that pipeline

        # E31_42 belongs to pipeline E
        myER = self.E31_42.execrecords.create(tainted=False)

        # This ERO has a TO that belongs to this pipeline
        myERO_good = myER.execrecordouts.create(
            symbolicdataset=self.singlet_symDS,
            generic_output=self.pE.outputs.get(dataset_name="E2_out"))
        self.assertEqual(myERO_good.clean(), None)
        myERO_good.delete()

        # This ERO has a TO that does NOT belong to this pipeline
        myERO_bad = myER.execrecordouts.create(
            symbolicdataset=self.triplet_3_rows_symDS,
            generic_output=self.pD.outputs.get(dataset_name="D1_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut \".*\" does not belong to the same pipeline as its parent ExecRecord POC",
            myERO_bad.clean)

    def test_ER_links_with_POC_and_POC_output_name_must_match_pipeline_TO_name(self):
        # The TO must have the same name as the POC which supposedly created it

        # Make ER for POC E21_41 which defines pipeline E's TO "E1_out"
        myER = self.E21_41.execrecords.create(tainted=False)

        # Define ERO with a TO that is part of pipeline E but with the wrong name from the POC
        myERO_bad = myER.execrecordouts.create(
            symbolicdataset=self.triplet_3_rows_symDS,
            generic_output=self.pE.outputs.get(dataset_name="E2_out"))
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordOut \".*\" does not represent the same output as its parent ExecRecord POC",
            myERO_bad.clean)

    def test_ER_if_dataset_is_undeleted_it_must_be_coherent_with_output(self):
        # 1) If the data is raw, the ERO output TO must also be raw
        myER = self.mC.execrecords.create(tainted=False)

        myERO_rawDS_rawTO = myER.execrecordouts.create(
            symbolicdataset=self.raw_symDS, generic_output=self.C3_rawout)
        self.assertEqual(myERO_rawDS_rawTO.clean(), None)
        myERO_rawDS_rawTO.delete()

        myERO_rawDS_nonrawTO = myER.execrecordouts.create(
            symbolicdataset=self.raw_symDS, generic_output=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \"raw .*\" cannot have come from output \".*\"",
            myERO_rawDS_nonrawTO.clean)
        myERO_rawDS_nonrawTO.delete()

        myERO_DS_rawTO = myER.execrecordouts.create(
            symbolicdataset=self.singlet_symDS, generic_output=self.C3_rawout)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" cannot have come from output \".*\"",
            myERO_DS_rawTO.clean)
        myERO_DS_rawTO.delete()

        myERO_DS_TO = myER.execrecordouts.create(
            symbolicdataset=self.singlet_symDS, generic_output=self.C1_out)
        self.assertEqual(myERO_DS_TO.clean(), None)
        myERO_DS_TO.delete()
        
        # 2) Dataset must have the same CDT of the producing TO
        myERO_invalid_CDT = myER.execrecordouts.create(
            symbolicdataset=self.triplet_symDS, generic_output=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" cannot have come from output \".*\"",
            myERO_DS_rawTO.clean)
        myERO_invalid_CDT.delete()

        # Dataset must have num rows within the row constraints of the producing TO
        myER_2 = self.mB.execrecords.create(tainted=False)
        myERO_too_many_rows = myER_2.execrecordouts.create(
            symbolicdataset=self.triplet_symDS, generic_output=self.B1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" was produced by TransformationOutput \".*\" but has too many rows",
            myERO_too_many_rows.clean)
        myERO_too_many_rows.delete()

# FIXME Note to self (Richard) September 30, 2013: go through these tomorrow and fix them.
class CopperfishDatasetAndDatasetStructureTests(CopperfishExecRecordTests_setup):

    def test_Dataset_sourced_from_runstep_with_corresponding_ER_but_ERO_doesnt_exist(self):
        # Appears to be checked at the RunStep level.
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
        #self.assertRaisesRegexp(ValidationError,errorMessage, self.runstep_DS.clean)


    def test_Dataset_sourced_from_ROC_so_but_corresponding_ERO_doesnt_exist(self):
        # INCOMPLETE
        # If a dataset comes from a ROC, an ER should exist, with an ERO referring to it

        # Define a dataset generated by a run
        self.run_symDS = SymbolicDataset()
        self.run_symDS.save()
        self.run_DS = None
        with open(os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), "rb") as f:
            self.run_DS = Dataset(user=self.myUser,name="triplet",description="lol",dataset_file=File(f),symbolicdataset=self.run_symDS)
            self.run_DS.save()
        self.run_DS_structure = DatasetStructure(dataset=self.run_DS,compounddatatype=self.triplet_cdt)
        self.run_DS_structure.save()

        # No ERO points to it
        errorMessage = "Dataset \".*\" comes from run .*, but has no corresponding ERO"
        #self.assertRaisesRegexp(ValidationError,errorMessage, self.runstep_DS.clean)

    def test_Dataset_sourced_from_run_and_ERO_exists_but_corresponding_ER_points_to_method_or_pipeline(self):
        # Appears to be redundant; this would be checked at the RunStep level (the ER
        # must refer to the transformation of the RunStep, and not a POC).
        # If a dataset comes from a run, an ER should exist, with an ERO referring to it
        # The ER must point to a POC, as method/pipelines have to do with runsteps, not runs

        # Define a dataset generated by a runstep
        self.runstep_symDS = SymbolicDataset()
        self.runstep_symDS.save()
        self.runstep_DS = None
        with open(os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), "rb") as f:
            self.runstep_DS = Dataset(user=self.myUser,name="triplet",description="lol",dataset_file=File(f),symbolicdataset=self.runstep_symDS)
            self.runstep_DS.save()
        self.runstep_DS_structure = DatasetStructure(dataset=self.runstep_DS,compounddatatype=self.triplet_cdt)
        self.runstep_DS_structure.save()

        # Erroneously define an ER for POC D11_21 of pipeline D with the ERO to the symbolicdataset
        D11_21_ER = self.D11_21.execrecords.create(tainted = False)
        D11_21_ER.execrecordouts.create(symbolicdataset = self.runstep_symDS, generic_output=self.step_E2.transformation.outputs.get(dataset_name="D1_out"))

        errorMessage = "Dataset \".*\" comes from runstep \".*\", but corresponding ERO links with a POC"
        #self.assertRaisesRegexp(ValidationError,errorMessage, self.runstep_DS.clean)



    def test_Dataset_clean_must_be_coherent_with_structure_if_applicable(self):

        # Valid dataset - raw (No structure defined)
        self.doublet_symDS = SymbolicDataset()
        self.doublet_symDS.save()
        self.doublet_DS = None
        with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
            self.doublet_DS = Dataset(user=self.myUser,name="doublet",description="lol",dataset_file=File(f),symbolicdataset=self.doublet_symDS)
            self.doublet_DS.save()
        self.assertEqual(self.doublet_DS.clean(), None)

        # Valid dataset - doublet
        self.doublet_DS_structure_valid = DatasetStructure(dataset=self.doublet_DS,compounddatatype=self.doublet_cdt)
        self.doublet_DS_structure_valid.save()
        self.assertEqual(self.doublet_DS.clean(), None)
        self.assertEqual(self.doublet_DS_structure_valid.clean(), None)
        self.doublet_DS_structure_valid.delete()

        # Invalid: Wrong number of columns
        self.doublet_DS_structure = DatasetStructure(dataset=self.doublet_DS,compounddatatype=self.triplet_cdt)
        self.doublet_DS_structure.save()
        errorMessage = "Dataset \".*\" does not have the same number of columns as its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage, self.doublet_DS.clean)
        self.assertRaisesRegexp(ValidationError,errorMessage, self.doublet_DS_structure.clean)
        
        # Invalid: Incorrect column header
        self.doublet_wrong_header_symDS = SymbolicDataset()
        self.doublet_wrong_header_symDS.save()
        self.doublet_wrong_header_DS = None
        with open(os.path.join(samplecode_path, "doublet_cdt_incorrect_header.csv"), "rb") as f:
            self.doublet_wrong_header_DS = Dataset(user=self.myUser,name="doublet",description="lol",dataset_file=File(f),symbolicdataset=self.doublet_wrong_header_symDS)
            self.doublet_wrong_header_DS.save()
        self.doublet_wrong_header_DS_structure = DatasetStructure(dataset=self.doublet_wrong_header_DS,compounddatatype=self.doublet_cdt)
        errorMessage = "Column .* of Dataset \".*\" is named .*, not .* as specified by its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage, self.doublet_wrong_header_DS.clean)
        self.assertRaisesRegexp(ValidationError,errorMessage, self.doublet_wrong_header_DS_structure.clean)

    def test_Dataset_check_MD5(self):
        # MD5 is now stored in symbolic dataset - even after the dataset was deleted
        self.assertEqual(self.raw_DS.compute_md5(), "7dc85e11b5c02e434af5bd3b3da9938e")

        # Initially, no change to the raw dataset has occured, so the md5 check will pass
        self.assertEqual(self.raw_DS.clean(), None)

        # The contents of the file are changed, disrupting file integrity
        self.raw_DS.dataset_file.close()
        self.raw_DS.dataset_file.open(mode='w')
        self.raw_DS.dataset_file.close()
        errorMessage = "File integrity of \".*\" lost. Current checksum \".*\" does not equal expected checksum \".*\""
        self.assertRaisesRegexp(ValidationError,errorMessage, self.raw_DS.clean)

    def test_Dataset_is_raw(self):
        self.assertEqual(self.triplet_DS.is_raw(), False)
        self.assertEqual(self.raw_DS.is_raw(), True)
        
    def test_DatasetStructure_clean_check_CSV(self):

        # triplet_DS has CSV format conforming to it's CDT
        self.triplet_DS.structure.clean()

        # Define a dataset, but with the wrong number of headers
        symDS = SymbolicDataset()
        symDS.save()
        DS1 = None
        with open(os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), "rb") as f:
            DS1 = Dataset(user=self.myUser,name="DS1",description="DS1 desc",dataset_file=File(f),symbolicdataset=symDS)
            DS1.save()
        structure = DatasetStructure(dataset=DS1,compounddatatype=self.doublet_cdt)

        errorMessage = "Dataset \".*\" does not have the same number of columns as its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage, structure.clean)

        # Define a dataset with the right number of header columns, but the wrong column names
        symDS2 = SymbolicDataset()
        symDS2.save()
        DS2 = None
        with open(os.path.join(samplecode_path, "three_random_columns.csv"), "rb") as f:
            DS2 = Dataset(user=self.myUser,name="DS2",description="DS2 desc",dataset_file=File(f),symbolicdataset=symDS2)
            DS2.save()
        structure2 = DatasetStructure(dataset=DS2,compounddatatype=self.triplet_cdt)

        errorMessage = "Column 1 of Dataset \".*\" is named .*, not .* as specified by its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage, structure2.clean)

    def test_Dataset_num_rows(self):
        self.assertEqual(self.triplet_3_rows_DS.num_rows(), 3)
        self.assertEqual(self.triplet_3_rows_DS.structure.num_rows(), 3)

