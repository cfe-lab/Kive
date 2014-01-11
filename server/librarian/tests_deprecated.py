    def test_ER_links_pipeline_ERI_links_TI_which_constrains_input_dataset_CDT(self):
        # ERI links with a TI (for pipeline inputs) - the dataset is constrained by the pipeline TI CDT

        myER = self.pE.execrecords.create(tainted=False)
        myERI_wrong_CDT = myER.execrecordins.create(
            symbolicdataset=self.singlet_symDS,
            generic_input=self.E1_in)
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not a restriction of the required CDT",
            myERI_wrong_CDT.clean)
        myERI_wrong_CDT.delete()

        myERI_too_few_rows = myER.execrecordins.create(
            symbolicdataset=self.singlet_3rows_symDS,
            generic_input=self.E2_in)
        self.assertRaisesRegexp(
            ValidationError,
            "SymbolicDataset \".*\" has too few rows for TransformationInput \".*\"",
            myERI_too_few_rows.clean)
        myERI_too_few_rows.delete()

        # A dataset of correct triplet CDT.
        self.triplet_large_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "triplet_cdt_large.csv"),
            self.triplet_cdt,
            user=self.myUser, name="triplet", description="lol")
        
        # Define dataset of correct CDT (singlet) with > 10 rows
        self.singlet_large_symDS = SymbolicDataset.create_SD(
            os.path.join(samplecode_path, "singlet_cdt_large.csv"),
            self.singlet_cdt,
            user=self.myUser, name="singlet", description="lol")

        myERI_right_E1 = myER.execrecordins.create(
            symbolicdataset=self.triplet_large_symDS,
            generic_input=self.E1_in)
        self.assertEqual(myERI_right_E1.clean(), None)

        myERI_right_E2 = myER.execrecordins.create(
            symbolicdataset=self.singlet_large_symDS,
            generic_input=self.E2_in)
        self.assertEqual(myERI_right_E2.clean(), None)
