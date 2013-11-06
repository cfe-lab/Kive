"""
Shipyard archive application unit tests.
"""

from django.test import TestCase
from django.contrib.auth.models import User

import pipeline.tests

class ArchiveTestSetup(pipeline.tests.PipelineTestSetup):
    """
    Set up a database state for unit testing the archive application.

    This extends PipelineTestSetup, which itself extends
    other ...TestSetup stuff (follow the chain).
    """
    def setUp(self):
        """Set up default database state for archive unit testing."""
        # Needed for Datasets to be created
        self.myUser = User.objects.create_user(
            'john', 'lennon@thebeatles.com', 'johnpassword')
        self.myUser.last_name = 'Lennon'
        self.myUser.save()

class Dataset_new_tests(Copperfish_Raw_Setup):

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

