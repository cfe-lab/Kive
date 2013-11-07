"""
Shipyard models pertaining to the librarian app.
"""

from django.test import TestCase

import metadata.tests

from librarian.models import *

class LibrarianTestSetup(metadata.tests.MetadataTestSetup):
    """
    Set up a database state for unit testing the librarian app.

    This extends PipelineTestSetup, which itself extended
    other stuff (follow the chain).
    """
    def setUp(self):
        """Set up default database state for librarian unit testing."""
        # Methods, CR/CRR/CRDs, DTs/CDTs, and Pipelines are set up by
        # calling this.
        super(LibrarianTestSetup, self).setUp()
        
        ####
        # This is the big pipeline Eric developed that was originally
        # used in copperfish/tests.py.
        
        # CRs and CRRs
        self.generic_cr = CodeResource(
            name="genericCR", description="Just a CR",
            filename="generic_script.py")
        self.generic_cr.save()
        self.generic_crRev = CodeResourceRevision(
            coderesource=self.generic_cr, revision_name="v1",
            revision_desc="desc")
        with open(os.path.join(samplecode_path, "generic_script.py"), "rb") as f:
            self.generic_crRev.content_file.save("generic_script.py",
                                                 File(f))
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
        self.D1_out = self.pD.outputs.get(dataset_name="D1_out")

        self.E03_11 = self.step_E1.cables_in.create(transf_input=self.A1_rawin,step_providing_input=0,provider_output=self.E3_rawin)
        self.E01_21 = self.step_E2.cables_in.create(transf_input=self.D1_in,step_providing_input=0,provider_output=self.E1_in)
        self.E02_22 = self.step_E2.cables_in.create(transf_input=self.D2_in,step_providing_input=0,provider_output=self.E2_in)
        self.E11_32 = self.step_E3.cables_in.create(transf_input=self.C2_in,step_providing_input=1,provider_output=self.A1_out)
        self.E21_31 = self.step_E3.cables_in.create(transf_input=self.C1_in,step_providing_input=2,provider_output=self.step_E2.transformation.outputs.get(dataset_name="D1_out"))
        self.E21_41 = self.pE.outcables.create(output_name="E1_out",output_idx=1,output_cdt=self.doublet_cdt,step_providing_output=2,provider_output=self.step_E2.transformation.outputs.get(dataset_name="D1_out"))
        self.E31_42 = self.pE.outcables.create(output_name="E2_out",output_idx=2,output_cdt=self.singlet_cdt,step_providing_output=3,provider_output=self.C1_out)
        self.E33_43 = self.pE.outcables.create(output_name="E3_rawout",output_idx=3,output_cdt=None,step_providing_output=3,provider_output=self.C3_rawout)
        self.pE.create_outputs()
        self.E1_out = self.pE.outputs.get(dataset_name="E1_out")
        self.E2_out = self.pE.outputs.get(dataset_name="E2_out")
        self.E3_rawout = self.pE.outputs.get(dataset_name="E3_rawout")

        # Custom wiring/outwiring
        self.E01_21_wire1 = self.E01_21.custom_wires.create(source_pin=self.triplet_cdt.members.all()[0],dest_pin=self.doublet_cdt.members.all()[1])
        self.E01_21_wire2 = self.E01_21.custom_wires.create(source_pin=self.triplet_cdt.members.all()[2],dest_pin=self.doublet_cdt.members.all()[0])
        self.E11_32_wire1 = self.E11_32.custom_wires.create(source_pin=self.doublet_cdt.members.all()[0],dest_pin=self.doublet_cdt.members.all()[1])
        self.E11_32_wire2 = self.E11_32.custom_wires.create(source_pin=self.doublet_cdt.members.all()[1],dest_pin=self.doublet_cdt.members.all()[0])
        self.E21_41_wire1 = self.E21_41.custom_outwires.create(source_pin=self.triplet_cdt.members.all()[1],dest_pin=self.doublet_cdt.members.all()[1])
        self.E21_41_wire2 = self.E21_41.custom_outwires.create(source_pin=self.triplet_cdt.members.all()[2],dest_pin=self.doublet_cdt.members.all()[0])
        self.pE.clean()
        
        # Define a user
        self.myUser = User.objects.create_user('john', 'lennon@thebeatles.com', 'johnpassword')
        self.myUser.save()
            
        # Define singlet, doublet, triplet, and raw uploaded datasets
        self.triplet_symDS = SymbolicDataset()
        self.triplet_symDS.save()
        self.triplet_DS = Dataset(
            user=self.myUser, name="triplet", description="lol",
            symbolicdataset=self.triplet_symDS)
        with open(os.path.join(samplecode_path, "step_0_triplet.csv"), "rb") as f:
            self.triplet_DS.dataset_file.save("step_0_triplet.csv", File(f))
        self.triplet_DS.save()
        self.triplet_DS_structure = DatasetStructure(dataset=self.triplet_DS,compounddatatype=self.triplet_cdt)
        self.triplet_DS_structure.save()
        self.triplet_DS.clean()

        self.doublet_symDS = SymbolicDataset()
        self.doublet_symDS.save()
        self.doublet_DS = Dataset(
            user=self.myUser, name="doublet", description="lol",
            symbolicdataset=self.doublet_symDS)
        with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
            self.doublet_DS.dataset_file.save("doublet_cdt.csv", File(f))
        self.doublet_DS.save()
        self.doublet_DS_structure = DatasetStructure(dataset=self.doublet_DS,compounddatatype=self.doublet_cdt)
        self.doublet_DS_structure.save()
        self.doublet_DS.clean()

        self.singlet_symDS = SymbolicDataset()
        self.singlet_symDS.save()
        self.singlet_DS = Dataset(
            user=self.myUser, name="singlet", description="lol",
            symbolicdataset=self.singlet_symDS)
        # Changed October 1, 2013: input E2_in requires something with >= 10 rows.
        #with open(os.path.join(samplecode_path, "step_0_singlet.csv"), "rb") as f:
        with open(os.path.join(samplecode_path, "singlet_cdt_large.csv"), "rb") as f:
            self.singlet_DS.dataset_file.save("singlet_cdt_large.csv", File(f))
        self.singlet_DS.save()
        self.singlet_DS_structure = DatasetStructure(dataset=self.singlet_DS,compounddatatype=self.singlet_cdt)
        self.singlet_DS_structure.save()
        self.singlet_DS.clean()

        # October 1, 2013: this is the same as the old singlet_symDS.
        self.singlet_3rows_symDS = SymbolicDataset()
        self.singlet_3rows_symDS.save()
        self.singlet_3rows_DS = Dataset(
            user=self.myUser, name="singlet", description="lol",
            symbolicdataset=self.singlet_3rows_symDS)
        with open(os.path.join(samplecode_path, "step_0_singlet.csv"), "rb") as f:
            self.singlet_3rows_DS.dataset_file.save("step_0_singlet.csv",
                                                    File(f))
        self.singlet_3rows_DS.save()
        self.singlet_3rows_DS_structure = DatasetStructure(dataset=self.singlet_3rows_DS,compounddatatype=self.singlet_cdt)
        self.singlet_3rows_DS_structure.save()
        self.singlet_3rows_DS.clean()

        self.raw_symDS = SymbolicDataset()
        self.raw_symDS.save()
        self.raw_DS = Dataset(
            user=self.myUser, name="raw", description="lol",
            symbolicdataset=self.raw_symDS)
        with open(os.path.join(samplecode_path, "step_0_raw.fasta"), "rb") as f:
            self.raw_DS.dataset_file.save("step_0_raw.fasta", File(f))
        self.raw_DS.save()        
        self.raw_DS.clean()

        # Added September 30, 2013: symbolic dataset that results from E01_21.
        self.D1_in_symDS = SymbolicDataset()
        self.D1_in_symDS.save()
        # These ones aren't needed as E02_22 is a trivial cable, so the
        # symbolic DS that goes into D2_in is just the same as whatever
        # goes into E2_in.
        # self.D2_in_symDS = SymbolicDataset()
        # self.D2_in_symDS.save()

        self.C1_in_symDS = SymbolicDataset()
        self.C1_in_symDS.save()
        self.C1_in_DS = Dataset(
            user=self.myUser, name="C1_in_triplet",
            description="triplet 3 rows",
            symbolicdataset=self.C1_in_symDS)
        with open(os.path.join(samplecode_path, "C1_in_triplet.csv"), "rb") as f:
            self.C1_in_DS.dataset_file.save("C1_in_triplet.csv", File(f))
        self.C1_in_DS.save()
        self.C1_in_DS.clean()
        self.C1_in_DS_structure = DatasetStructure(dataset=self.C1_in_DS,compounddatatype=self.triplet_cdt)
        self.C1_in_DS_structure.save()
        self.C1_in_DS.clean()

        self.C2_in_symDS = SymbolicDataset()
        self.C2_in_symDS.save()

        # October 16: an alternative to C2_in_symDS, which has existent data.
        self.E11_32_output_symDS = SymbolicDataset()
        self.E11_32_output_symDS.save()
        self.E11_32_output_DS = Dataset(
            user=self.myUser, name="E11_32 output doublet",
            description="result of E11_32 fed by doublet_cdt.csv",
            symbolicdataset=self.E11_32_output_symDS)
        with open(os.path.join(samplecode_path, "E11_32_output.csv"), "rb") as f:
            self.E11_32_output_DS.dataset_file.save(
                "E11_32_output.csv", File(f))
        self.E11_32_output_DS.save()
        self.E11_32_output_DS.clean()
        self.E11_32_output_DS_structure = DatasetStructure(
            dataset=self.E11_32_output_DS,
            compounddatatype=self.doublet_cdt)
        self.E11_32_output_DS_structure.save()
        self.E11_32_output_DS.clean()

        self.C1_out_symDS = SymbolicDataset()
        self.C1_out_symDS.save()
        self.C1_out_DS = Dataset(
            user=self.myUser, name="raw", description="lol",
            symbolicdataset=self.C1_out_symDS)
        with open(os.path.join(samplecode_path, "step_0_singlet.csv"), "rb") as f:
            self.C1_out_DS.dataset_file.save("step_0_singlet.csv", File(f))
        self.C1_out_DS.save()
        self.C1_out_DS.clean()
        self.C1_out_DS_structure = DatasetStructure(dataset=self.C1_out_DS,compounddatatype=self.singlet_cdt)
        self.C1_out_DS_structure.save()
        self.C1_out_DS.clean()

        self.C2_out_symDS = SymbolicDataset()
        self.C2_out_symDS.save()
        self.C2_out_DS = Dataset(
            user=self.myUser, name="raw", description="lol",
            symbolicdataset=self.C2_out_symDS)
        with open(os.path.join(samplecode_path, "step_0_raw.fasta"), "rb") as f:
            self.C2_out_DS.dataset_file.save("step_0_raw.fasta", File(f))
        self.C2_out_DS.save()
        self.C2_out_DS.clean()

        self.C3_out_symDS = SymbolicDataset()
        self.C3_out_symDS.save()
        self.C3_out_DS = Dataset(
            user=self.myUser, name="raw", description="lol",
            symbolicdataset=self.C3_out_symDS)
        with open(os.path.join(samplecode_path, "step_0_raw.fasta"), "rb") as f:
            self.C3_out_DS.dataset_file.save("step_0_raw.fasta", File(f))
        self.C3_out_DS.save()
        self.C3_out_DS.clean()

        self.triplet_3_rows_symDS = SymbolicDataset()
        self.triplet_3_rows_symDS.save()
        self.triplet_3_rows_DS = Dataset(
            user=self.myUser, name="triplet", description="lol",
            symbolicdataset=self.triplet_3_rows_symDS)
        with open(os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), "rb") as f:
            self.triplet_3_rows_DS.dataset_file.save(
                "step_0_triplet_3_rows.csv", File(f))
        self.triplet_3_rows_DS.save()
        self.triplet_3_rows_DS_structure = DatasetStructure(dataset=self.triplet_3_rows_DS,compounddatatype=self.triplet_cdt)
        self.triplet_3_rows_DS_structure.save()
        self.triplet_3_rows_DS.clean()

        # October 9, 2013: added as the result of cable E21_41.
        self.E1_out_symDS = SymbolicDataset()
        self.E1_out_symDS.save()
        self.E1_out_DS = Dataset(
            user=self.myUser, name="E1_out",
            description="doublet remuxed from triplet",
            symbolicdataset=self.E1_out_symDS)
        with open(os.path.join(samplecode_path, "doublet_remuxed_from_t3r.csv"), "rb") as f:
            self.E1_out_DS.dataset_file.save(
                "doublet_remuxed_from_t3r.csv", File(f))
        self.E1_out_DS.save()
        self.E1_out_DS_structure = DatasetStructure(
            dataset=self.E1_out_DS, compounddatatype=self.doublet_cdt)
        self.E1_out_DS_structure.save()
        self.E1_out_DS.clean()

        # October 15, 2013: SymbolicDatasets that go into and come out
        # of cable E01_21 and E21_41.
        self.DNA_triplet_symDS = SymbolicDataset()
        self.DNA_triplet_symDS.save()
        self.DNA_triplet_DS = Dataset(
            user=self.myUser, name="DNA_triplet",
            description="DNA triplet data",
            symbolicdataset=self.DNA_triplet_symDS)
        with open(os.path.join(samplecode_path, "DNA_triplet.csv"), "rb") as f:
            self.DNA_triplet_DS.dataset_file.save(
                "DNA_triplet.csv", File(f))
        self.DNA_triplet_DS.save()
        self.DNA_triplet_DS_structure = DatasetStructure(
            dataset=self.DNA_triplet_DS,
            compounddatatype=self.DNA_triplet_cdt)
        self.DNA_triplet_DS_structure.save()
        self.DNA_triplet_DS.clean()

        self.E01_21_DNA_doublet_symDS = SymbolicDataset()
        self.E01_21_DNA_doublet_symDS.save()
        self.E01_21_DNA_doublet_DS = Dataset(
            user=self.myUser, name="E01_21_DNA_doublet",
            description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E01_21",
            symbolicdataset=self.E01_21_DNA_doublet_symDS)
        with open(os.path.join(samplecode_path, "E01_21_DNA_doublet.csv"), "rb") as f:
            self.E01_21_DNA_doublet_DS.dataset_file.save(
                "E01_21_DNA_doublet.csv", File(f))
        self.E01_21_DNA_doublet_DS.save()
        self.E01_21_DNA_doublet_DS_structure = DatasetStructure(
            dataset=self.E01_21_DNA_doublet_DS,
            compounddatatype=self.DNA_doublet_cdt)
        self.E01_21_DNA_doublet_DS_structure.save()
        self.E01_21_DNA_doublet_DS.clean()

        self.E21_41_DNA_doublet_symDS = SymbolicDataset()
        self.E21_41_DNA_doublet_symDS.save()
        self.E21_41_DNA_doublet_DS = Dataset(
            user=self.myUser, name="E21_41_DNA_doublet",
            description="DNA doublet data coming from DNA_triplet.csv but remultiplexed according to cable E21_41",
            symbolicdataset=self.E21_41_DNA_doublet_symDS)
        with open(os.path.join(samplecode_path, "E21_41_DNA_doublet.csv"), "rb") as f:
            self.E21_41_DNA_doublet_DS.dataset_file.save(
                "E21_41_DNA_doublet.csv", File(f))
        self.E21_41_DNA_doublet_DS.save()
        self.E21_41_DNA_doublet_DS_structure = DatasetStructure(
            dataset=self.E21_41_DNA_doublet_DS,
            compounddatatype=self.DNA_doublet_cdt)
        self.E21_41_DNA_doublet_DS_structure.save()
        self.E21_41_DNA_doublet_DS.clean()

        

    def tearDown(self):
        """Clear CodeResources and Datasets folders."""
        super(LibrarianTestSetup, self).tearDown()

        for dataset in Dataset.objects.all():
            dataset.dataset_file.close()
            dataset.dataset_file.delete()


class SymbolicDatasetTests(LibrarianTestSetup):
    
    def test_is_raw(self):
        self.assertEqual(self.triplet_DS.is_raw(), False)
        self.assertEqual(self.raw_DS.is_raw(), True)

class DatasetStructureTests(LibrarianTestSetup):
    
    def test_clean_must_be_coherent_with_structure_if_applicable(self):
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

        
    def test_clean_check_CSV(self):

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

    def test_num_rows(self):
        self.assertEqual(self.triplet_3_rows_DS.num_rows(), 3)
        self.assertEqual(self.triplet_3_rows_DS.structure.num_rows(), 3)

    def test_dataset_clean_incorrect_number_of_CSV_header_fields_bad(self):

        uploaded_sd = SymbolicDataset()
        uploaded_sd.save()
        uploaded_dataset = None
        with open(os.path.join(samplecode_path, "script_2_output_2.csv"), "rb") as f:
            uploaded_dataset = Dataset(
                user=self.myUser,name="uploaded_dataset",
                description="hehe",dataset_file=File(f),
                symbolicdataset=uploaded_sd)
            uploaded_dataset.save()
        new_structure = DatasetStructure(dataset=uploaded_dataset,
                                         compounddatatype=self.triplet_cdt)
        new_structure.save()

        errorMessage = "Dataset .* does not have the same number of columns as its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

    def test_dataset_clean_correct_number_of_CSV_header_fields_but_incorrect_contents_bad(self):

        uploaded_sd = SymbolicDataset()
        uploaded_sd.save()
        uploaded_dataset = None
        with open(os.path.join(samplecode_path, "three_random_columns.csv"), "rb") as f:
            uploaded_dataset = Dataset(
                user=self.myUser,name="uploaded_raw_dataset",
                description="hehe",dataset_file=File(f),
                symbolicdataset=uploaded_sd)
            uploaded_dataset.save()
        new_structure = DatasetStructure(dataset=uploaded_dataset,
                                         compounddatatype=self.triplet_cdt)
        new_structure.save()

        errorMessage = "Column .* of Dataset .* is named .*, not .* as specified by its CDT"
        self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

class ExecRecordTests(LibrarianTestSetup):

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
        myER = self.E11_32.execrecords.create(tainted=False)

        myERI_bad = myER.execrecordins.create(symbolicdataset=self.singlet_symDS,
                                              generic_input=self.C1_out)
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" does not denote the TO/TI that feeds the parent ExecRecord PSIC",
            myERI_bad.clean)
        
        yourER = self.E02_22.execrecords.create(tainted=False)

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
            "ExecRecordIn \".*\" must refer to a TI of the Method/Pipeline of the parent ExecRecord",
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
            generic_input=self.pD.outputs.all()[0])
        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecordIn \".*\" must refer to a TI of the Method/Pipeline of the parent ExecRecord",
            myERI_bad.clean)

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
            "CDT of Dataset .* is not a restriction of the required CDT",
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
            "CDT of Dataset .* is not a restriction of the required CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        myERI_too_few_rows = myER.execrecordins.create(
            symbolicdataset=self.singlet_3rows_symDS,
            generic_input=self.E2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "Dataset \".*\" has too few rows for TransformationInput \".*\"",
            myERI_too_few_rows.clean)
        myERI_too_few_rows.delete()

        # A dataset of correct triplet CDT.
        self.triplet_large_symDS = SymbolicDataset()
        self.triplet_large_symDS.save()
        self.triplet_large_DS = Dataset(
            user=self.myUser, name="triplet", description="lol",
            symbolicdataset=self.triplet_large_symDS)
        with open(os.path.join(samplecode_path, "triplet_cdt_large.csv"), "rb") as f:
            self.triplet_large_DS.dataset_file.save(
                "triplet_cdt_large.csv", File(f))
        self.triplet_large_DS.save()
        self.triplet_large_DS_structure = DatasetStructure(
            dataset=self.triplet_large_DS,
            compounddatatype=self.triplet_cdt)
        self.triplet_large_DS_structure.save()
        self.triplet_large_DS.clean()
        
        # Define dataset of correct CDT (singlet) with > 10 rows
        self.singlet_large_symDS = SymbolicDataset()
        self.singlet_large_symDS.save()
        self.singlet_large_DS = Dataset(
            user=self.myUser, name="singlet", description="lol",
            symbolicdataset=self.singlet_large_symDS)
        with open(os.path.join(samplecode_path, "singlet_cdt_large.csv"), "rb") as f:
            self.singlet_large_DS.dataset_file.save(
                "singlet_cdt_large.csv", File(f))
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
            "CDT of Dataset .* is not a restriction of the required CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

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

    def test_ERI_associated_Dataset_must_be_restriction_of_input_CDT(self):
        """If the ERI has a real non-raw Dataset associated to it, the Dataset must have a CDT that is a restriction of the input it feeds."""
        mC_ER = self.mC.execrecords.create()
        mC_ER_in_1 = mC_ER.execrecordins.create(
            generic_input=self.C1_in,
            symbolicdataset=self.C1_in_symDS)

        # Good case: input Dataset has the CDT of generic_input.
        self.assertEquals(mC_ER_in_1.clean(), None)

        # Good case: input Dataset has an identical CDT of generic_input.
        other_CDT = CompoundDatatype()
        other_CDT.save()

        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="a", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="b", column_idx=2)
        col3 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="c", column_idx=3)

        self.C1_in_DS.structure.compounddatatype = other_CDT
        self.assertEquals(mC_ER_in_1.clean(), None)

        # Good case: proper restriction.
        col1.datatype = self.DNA_dt
        col2.datatype = self.RNA_dt
        self.assertEquals(mC_ER_in_1.clean(), None)

        # Bad case: a type that is not a restriction at all.
        self.C1_in_DS.structure.compounddatatype = self.doublet_cdt
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not a restriction of the required CDT",
            mC_ER_in_1.clean)
        
    def test_ERO_CDT_restrictions_Method(self):
        """ERO CDT restriction tests for the ER of a Method."""
        ####
        mA_ER = self.mA.execrecords.create()
        mA_ERO = mA_ER.execrecordouts.create(
            generic_output=self.A1_out,
            symbolicdataset=self.doublet_symDS)

        # Good case: output Dataset has the CDT of generic_output.
        self.assertEquals(mA_ERO.clean(), None)

        # Bad case: output Dataset has an identical CDT.
        other_CDT = CompoundDatatype()
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="x", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="y", column_idx=2)
        
        self.doublet_DS.structure.compounddatatype = other_CDT
        self.doublet_DS.structure.save()

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not the CDT of the TransformationOutput .* of the generating Method",
            mA_ERO.clean)

        # Bad case: output Dataset has another CDT altogether.
        mA_ERO.symbolicdataset=self.triplet_symDS

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not the CDT of the TransformationOutput .* of the generating Method",
            mA_ERO.clean)
        
    def test_ERO_CDT_restrictions_Pipeline(self):
        """ERO CDT restriction tests for the ER of a Pipeline."""
        ####
        pD_ER = self.pD.execrecords.create()
        pD_ERO = pD_ER.execrecordouts.create(
            generic_output=self.D1_out,
            symbolicdataset=self.C1_in_symDS)

        # Good case: output Dataset has the CDT of generic_output.
        self.assertEquals(pD_ERO.clean(), None)

        # Good case: output Dataset has an identical CDT.
        other_CDT = CompoundDatatype()
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="a", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="b", column_idx=2)
        col3 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="c", column_idx=3)
        
        self.C1_in_DS.structure.compounddatatype = other_CDT
        self.C1_in_DS.structure.save()
        self.assertEquals(pD_ERO.clean(), None)

        # Bad case: output Dataset has a CDT that is a restriction of
        # generic_output.
        col1.datatype = self.DNA_dt
        col1.save()
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            pD_ERO.clean)

        # Bad case: output Dataset has another CDT altogether.
        pD_ERO.symbolicdataset = self.doublet_symDS

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            pD_ERO.clean)
        
    def test_ERO_CDT_restrictions_POC(self):
        """ERO CDT restriction tests for the ER of a POC."""
        ####
        outcable_ER = self.E21_41.execrecords.create()
        outcable_ERO = outcable_ER.execrecordouts.create(
            generic_output=self.E1_out,
            symbolicdataset=self.E1_out_symDS)

        # Good case: output Dataset has the CDT of generic_output.
        self.assertEquals(outcable_ERO.clean(), None)

        # Good case: output Dataset has an identical CDT.
        other_CDT = CompoundDatatype()
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="x", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="y", column_idx=2)
        
        self.E1_out_DS.structure.compounddatatype = other_CDT
        self.E1_out_DS.structure.save()
        self.assertEquals(outcable_ERO.clean(), None)

        # Bad case: output Dataset has a CDT that is a restriction of
        # generic_output.
        col1.datatype = self.DNA_dt
        col1.save()
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            outcable_ERO.clean)

        # Bad case: output Dataset has another CDT altogether.
        outcable_ERO.symbolicdataset = self.singlet_symDS

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            outcable_ERO.clean)

    def test_ERO_CDT_restrictions_PSIC(self):
        """ERO CDT restriction tests for the ER of a PSIC."""
        ####
        cable_ER = self.E11_32.execrecords.create()
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.C2_in,
            symbolicdataset=self.doublet_symDS)

        # Good case: output Dataset has the CDT of generic_output.
        self.assertEquals(cable_ERO.clean(), None)

        # Good case: output Dataset has an identical CDT.
        other_CDT = CompoundDatatype()
        other_CDT.save()
        col1 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="x", column_idx=1)
        col2 = other_CDT.members.create(datatype=self.string_dt,
                                        column_name="y", column_idx=2)
        
        self.doublet_DS.structure.compounddatatype = other_CDT
        self.doublet_DS.structure.save()
        self.assertEquals(cable_ERO.clean(), None)

        # Good case: output Dataset has a CDT that is a restriction of
        # generic_output.
        col1.datatype = self.DNA_dt
        col1.save()
        self.assertEquals(cable_ERO.clean(), None)

        # Bad case: output Dataset has another CDT altogether.
        cable_ERO.symbolicdataset = self.singlet_symDS

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of Dataset .* is not a restriction of the CDT of the fed TransformationInput .*",
            cable_ERO.clean)

    def test_ER_trivial_PSICs_have_same_SD_on_both_sides(self):
        """ERs representing trivial PSICs must have the same SymbolicDataset on both sides."""
        cable_ER = self.E02_22.execrecords.create()
        cable_ERI = cable_ER.execrecordins.create(
            generic_input=self.E2_in,
            symbolicdataset = self.singlet_symDS)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.D2_in,
            symbolicdataset = self.singlet_symDS)

        # Good case: SDs on either side of this trivial cable match.
        self.assertEquals(cable_ER.clean(), None)

        # Bad case: SDs don't match.
        cable_ERO.symbolicdataset = self.C1_out_symDS
        cable_ERO.save()
        self.assertRaisesRegexp(
            ValidationError,
            "ER .* represents a trivial cable but its input and output do not match",
            cable_ER.clean)

    def test_ER_trivial_POCs_have_same_SD_on_both_sides(self):
        """ERs representing trivial POCs must have the same SymbolicDataset on both sides."""
        outcable_ER = self.E31_42.execrecords.create()
        outcable_ERI = outcable_ER.execrecordins.create(
            generic_input=self.C1_out,
            symbolicdataset = self.C1_out_symDS)
        outcable_ERO = outcable_ER.execrecordouts.create(
            generic_output=self.E2_out,
            symbolicdataset = self.C1_out_symDS)

        # Good case: SDs on either side of this trivial POC match.
        self.assertEquals(outcable_ER.clean(), None)

        # Bad case: SDs don't match.
        outcable_ERO.symbolicdataset = self.singlet_symDS
        outcable_ERO.save()
        self.assertRaisesRegexp(
            ValidationError,
            "ER .* represents a trivial cable but its input and output do not match",
            outcable_ER.clean)
        

    def test_ER_Datasets_passing_through_non_trivial_POCs(self):
        """Test that the Datatypes of Datasets passing through POCs are properly preserved."""
        outcable_ER = self.E21_41.execrecords.create()
        outcable_ERI = outcable_ER.execrecordins.create(
            generic_input=self.D1_out,
            symbolicdataset=self.C1_in_symDS)
        outcable_ERO = outcable_ER.execrecordouts.create(
            generic_output=self.E1_out,
            symbolicdataset=self.E1_out_symDS)

        # Good case: the Datatypes are exactly those needed.
        self.assertEquals(outcable_ER.clean(), None)

        # Good case: same as above, but with CDTs that are restrictions.
        D1_out_structure = self.D1_out.structure.all()[0]
        E1_out_structure = self.E1_out.structure.all()[0]
        D1_out_structure.compounddatatype = self.DNA_triplet_cdt
        D1_out_structure.save()
        E1_out_structure.compounddatatype = self.DNA_doublet_cdt
        E1_out_structure.save()
        
        outcable_ERI.symbolicdataset = self.DNA_triplet_symDS
        outcable_ERI.save()
        outcable_ERO.symbolicdataset = self.E21_41_DNA_doublet_symDS
        outcable_ERO.save()
        self.assertEquals(outcable_ER.clean(), None)

        # Bad case: cable does some casting.
        output_col1 = (self.E21_41_DNA_doublet_DS.structure.compounddatatype.
                       members.get(column_idx=1))
        output_col1.datatype = self.string_dt
        output_col1.save()

        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecord .* represents a cable but Datatype of destination Dataset column .* does not match its source",
            outcable_ER.clean)
        
    def test_ER_Datasets_passing_through_non_trivial_PSICs(self):
        """Test that the Datatypes of Datasets passing through PSICs are properly preserved."""
        cable_ER = self.E01_21.execrecords.create()
        cable_ERI = cable_ER.execrecordins.create(
            generic_input=self.E1_in,
            symbolicdataset=self.triplet_symDS)
        cable_ERO = cable_ER.execrecordouts.create(
            generic_output=self.D1_in,
            symbolicdataset=self.D1_in_symDS)

        # Good case: the Datatypes are exactly those needed.
        self.assertEquals(cable_ER.clean(), None)

        # Good case: same as above, but with CDTs that are restrictions.
        in_structure = self.E1_in.structure.all()[0]
        out_structure = self.D1_in.structure.all()[0]
        in_structure.compounddatatype = self.DNA_triplet_cdt
        in_structure.save()
        out_structure.compounddatatype = self.DNA_doublet_cdt
        out_structure.save()
        
        cable_ERI.symbolicdataset = self.DNA_triplet_symDS
        cable_ERI.save()
        cable_ERO.symbolicdataset = self.E01_21_DNA_doublet_symDS
        cable_ERO.save()
        self.assertEquals(cable_ER.clean(), None)

        # Bad case: cable does some casting.
        output_col1 = (self.E01_21_DNA_doublet_DS.structure.compounddatatype.
                       members.get(column_idx=1))
        output_col1.datatype = self.string_dt
        output_col1.save()

        self.assertRaisesRegexp(
            ValidationError,
            "ExecRecord .* represents a cable but Datatype of destination Dataset column .* does not match its source",
            cable_ER.clean)
