"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase
from django.contrib.auth.models import User

import shutil
import tempfile

from constants import datatypes
import metadata.models
from datachecking.models import *
from librarian.models import *
import kive.testing_utils as tools


class BlankableTestCase(TestCase):
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        self.user_doug = User.objects.create_user('doug', 'dford@deco.com', 'durrrrr')
        self.user_doug.save()
        self.user_doug.groups.add(metadata.models.everyone_group())
        self.user_doug.save()

        self.INT = metadata.models.Datatype.objects.get(pk=datatypes.INT_PK)

        self.canucks_lineup = """firstcol
22
33
17

23
8
"""


class BlankableColumn(BlankableTestCase):

    def setUp(self):
        BlankableTestCase.setUp(self)
        self.blankable_CDT = metadata.models.CompoundDatatype(user=self.user_doug)
        self.blankable_CDT.save()
        self.blankable_CDT.members.create(datatype=self.INT, column_name="firstcol", column_idx=1,
                                          blankable=True)
        self.blankable_CDT.clean()

        self.good_dataset = tools.make_dataset(
            self.canucks_lineup,
            self.blankable_CDT,
            True,
            self.user_doug,
            "Dataset with blankable column",
            "Canucks starting lineup",
            None,
            False
        )

        run_dir = tempfile.mkdtemp(prefix="dataset{}".format(self.good_dataset.pk))
        try:
            self.good_dataset.dataset.dataset_file.open("rb")
            self.ccl = self.good_dataset.check_file_contents(
                file_path_to_check=None, file_handle=self.good_dataset.dataset.dataset_file,
                summary_path=run_dir, min_row=None, max_row=None, execlog=None,
                checking_user=self.user_doug
            )
        finally:
            self.good_dataset.dataset.dataset_file.close()
        shutil.rmtree(run_dir)

    def test_blank_on_blankable_column_OK(self):
        """
        No error applied to a column that allows blanks.
        """
        self.assertTrue(self.good_dataset.is_OK())

    def test_clean_blank_on_blankable_column(self):
        """
        There should be no BlankCell attached to an entry from a blankable column.
        """
        ccl = self.good_dataset.content_checks.first()
        baddata = BadData(contentchecklog=ccl)
        baddata.save()
        cell_error = baddata.cell_errors.create(row_num=4, column=self.blankable_CDT.members.first())

        bc = BlankCell(cellerror = cell_error)
        self.assertRaisesRegexp(
            ValidationError,
            'Entry \(4,1\) of Dataset ".*" is blankable',
            bc.clean
        )


class BlankCellNonBlankable(BlankableTestCase):

    def setUp(self):
        BlankableTestCase.setUp(self)
        self.test_CDT = metadata.models.CompoundDatatype(user=self.user_doug)
        self.test_CDT.save()
        self.test_CDT.members.create(datatype=self.INT, column_name="firstcol", column_idx=1)
        self.test_CDT.clean()

        self.bad_dataset = tools.make_dataset(
            self.canucks_lineup,
            self.test_CDT,
            True,
            self.user_doug,
            "Dataset with non-blankable column",
            "Canucks starting lineup",
            None,
            False
        )

        run_dir = tempfile.mkdtemp(prefix="dataset{}".format(self.bad_dataset.pk))
        try:
            self.bad_dataset.dataset.dataset_file.open("rb")
            self.ccl = self.bad_dataset.check_file_contents(
                file_path_to_check=None, file_handle=self.bad_dataset.dataset.dataset_file,
                summary_path=run_dir, min_row=None, max_row=None,
                execlog=None, checking_user=self.user_doug
            )
        finally:
            self.bad_dataset.dataset.dataset_file.close()
        shutil.rmtree(run_dir)

    def test_blank_on_non_blankable_column_creates_baddata(self):
        """
        A blank cell causes an error when the CDT doesn't allow blanks.
        """

        self.assertFalse(self.bad_dataset.is_OK())

    def test_blank_on_non_blankable_column_creates_cellerror(self):
        """
        A blank cell creates a BlankCell object when the CDT doesn't allow blanks.
        """
        self.assertEquals(self.bad_dataset.content_checks.count(), 1)
        ccl = self.bad_dataset.content_checks.first()
        self.assertTrue(ccl.is_fail())
        baddata = ccl.baddata
        self.assertTrue(baddata.cell_errors.count(), 1)
        cell_error = baddata.cell_errors.first()
        self.assertEquals(cell_error.row_num, 4)
        self.assertEquals(cell_error.column, self.test_CDT.members.first())
        self.assertIsNone(cell_error.constraint_failed)

    def test_blank_on_non_blankable_column_creates_blankcell(self):
        """
        A blank cell creates a BlankCell object when the CDT doesn't allow blanks.
        """
        cell_error = self.bad_dataset.content_checks.first().baddata.cell_errors.first()
        self.assertTrue(cell_error.has_blank_error())