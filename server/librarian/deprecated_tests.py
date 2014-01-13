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
        
        self.C1_in_symDS.structure.compounddatatype = other_CDT
        self.C1_in_symDS.structure.save()
        self.assertEquals(pD_ERO.clean(), None)

        # Bad case: output SymbolicDataset has a CDT that is a
        # restriction of generic_output.
        col1.datatype = self.DNA_dt
        col1.save()
        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            pD_ERO.clean)

        # Bad case: output Dataset has another CDT altogether.
        pD_ERO.symbolicdataset = self.doublet_symDS

        self.assertRaisesRegexp(
            ValidationError,
            "CDT of SymbolicDataset .* is not identical to the CDT of the TransformationOutput .* of the generating Pipeline",
            pD_ERO.clean)
        
