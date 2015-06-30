"""
Unit tests for Shipyard metadata models.
"""
import os
import re

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.test import TestCase, TransactionTestCase
from django.core.urlresolvers import reverse, resolve

from rest_framework import status
from rest_framework.test import APIRequestFactory, force_authenticate

from metadata.models import BasicConstraint, CompoundDatatype, Datatype, everyone_group, kive_user
from method.models import CodeResourceRevision
from archive.models import Dataset, MethodOutput
from librarian.models import SymbolicDataset
from datachecking.models import VerificationLog
from portal.models import StagedFile
from constants import datatypes, CDTs


samplecode_path = "../samplecode"


def create_metadata_test_environment(case):
    """Setup default database state from which to perform unit testing."""
    # Define a user.  This was previously in librarian/tests.py,
    # but we put it here now so all tests can use it.
    case.myUser = User.objects.create_user('john', 'lennon@thebeatles.com', 'johnpassword')
    case.myUser.save()
    case.myUser.groups.add(everyone_group())
    case.myUser.save()

    # Load up the builtin Datatypes.
    case.STR = Datatype.objects.get(pk=datatypes.STR_PK)
    case.FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
    case.INT = Datatype.objects.get(pk=datatypes.INT_PK)
    case.BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

    # Many tests use case.string_dt as a name for case.STR.
    case.string_dt = case.STR

    # Create Datatype "DNANucSeq" with a regexp basic constraint.
    case.DNA_dt = Datatype(
        name="DNANucSeq",
        description="String consisting of ACGTacgt",
        user=case.myUser)
    case.DNA_dt.save()
    # DNA_dt is a restricted type of string
    case.DNA_dt.restricts.add(case.string_dt)
    case.DNA_dt.grant_everyone_access()
    case.DNA_dt.basic_constraints.create(
        ruletype=BasicConstraint.REGEXP,
        rule="^[ACGTacgt]*$")
    case.DNA_dt.save()

    # Similarly, create Datatype "RNANucSeq".
    case.RNA_dt = Datatype(
        name="RNANucSeq",
        description="String consisting of ACGUacgu",
        user=case.myUser)
    case.RNA_dt.save()
    # RNA_dt is a restricted type of string
    case.RNA_dt.restricts.add(case.string_dt)
    case.RNA_dt.grant_everyone_access()
    case.RNA_dt.basic_constraints.create(
        ruletype=BasicConstraint.REGEXP,
        rule="^[ACGUacgu]*$")
    case.RNA_dt.save()

    # Define a new CDT with a bunch of different member
    case.basic_cdt = CompoundDatatype(user=case.myUser)
    case.basic_cdt.save()
    case.basic_cdt.grant_everyone_access()
    case.basic_cdt.save()

    case.basic_cdt.members.create(
        datatype=case.string_dt,
        column_name='label',
        column_idx=1)
    case.basic_cdt.members.create(
        datatype=case.INT,
        column_name='integer',
        column_idx=2)
    case.basic_cdt.members.create(
        datatype=case.FLOAT,
        column_name='float',
        column_idx=3)
    case.basic_cdt.members.create(
        datatype=case.BOOL,
        column_name='bool',
        column_idx=4)
    case.basic_cdt.members.create(
        datatype=case.RNA_dt,
        column_name="rna",
        column_idx=5)
    case.basic_cdt.full_clean()
    case.basic_cdt.save()

    # Define test_cdt as containing 3 members:
    # (label, PBMCseq, PLAseq) as (string,DNA,RNA)
    case.test_cdt = CompoundDatatype(user=case.myUser)
    case.test_cdt.save()
    case.test_cdt.grant_everyone_access()
    case.test_cdt.save()
    case.test_cdt.members.create(
        datatype=case.string_dt,
        column_name="label",
        column_idx=1)
    case.test_cdt.members.create(
        datatype=case.DNA_dt,
        column_name="PBMCseq",
        column_idx=2)
    case.test_cdt.members.create(
        datatype=case.RNA_dt,
        column_name="PLAseq",
        column_idx=3)
    case.test_cdt.full_clean()
    case.test_cdt.save()

    # Define DNAinput_cdt (1 member)
    case.DNAinput_cdt = CompoundDatatype(user=case.myUser)
    case.DNAinput_cdt.save()
    case.DNAinput_cdt.members.create(
        datatype=case.DNA_dt,
        column_name="SeqToComplement",
        column_idx=1)
    case.DNAinput_cdt.grant_everyone_access()
    case.DNAinput_cdt.full_clean()
    case.DNAinput_cdt.save()

    # Define DNAoutput_cdt (1 member)
    case.DNAoutput_cdt = CompoundDatatype(user=case.myUser)
    case.DNAoutput_cdt.save()
    case.DNAoutput_cdt.members.create(
        datatype=case.DNA_dt,
        column_name="ComplementedSeq",
        column_idx=1)
    case.DNAoutput_cdt.grant_everyone_access()
    case.DNAoutput_cdt.full_clean()
    case.DNAoutput_cdt.save()

    # Define RNAinput_cdt (1 column)
    case.RNAinput_cdt = CompoundDatatype(user=case.myUser)
    case.RNAinput_cdt.save()
    case.RNAinput_cdt.members.create(
        datatype=case.RNA_dt,
        column_name="SeqToComplement",
        column_idx=1)
    case.RNAinput_cdt.grant_everyone_access()
    case.RNAinput_cdt.full_clean()
    case.RNAinput_cdt.save()

    # Define RNAoutput_cdt (1 column)
    case.RNAoutput_cdt = CompoundDatatype(user=case.myUser)
    case.RNAoutput_cdt.save()
    case.RNAoutput_cdt.members.create(
        datatype=case.RNA_dt,
        column_name="ComplementedSeq",
        column_idx=1)
    case.RNAoutput_cdt.grant_everyone_access()
    case.RNAoutput_cdt.full_clean()
    case.RNAoutput_cdt.save()

    ####
    # Everything above this point is used in metadata.tests.
    # This next bit is used in method.tests.

    # Define "tuple" CDT containing (x,y): members x and y exist at index 1 and 2
    case.tuple_cdt = CompoundDatatype(user=case.myUser)
    case.tuple_cdt.save()
    case.tuple_cdt.members.create(datatype=case.string_dt, column_name="x", column_idx=1)
    case.tuple_cdt.members.create(datatype=case.string_dt, column_name="y", column_idx=2)
    case.tuple_cdt.grant_everyone_access()

    # Define "singlet" CDT containing CDT member (a) and "triplet" CDT with members (a,b,c)
    case.singlet_cdt = CompoundDatatype(user=case.myUser)
    case.singlet_cdt.save()
    case.singlet_cdt.members.create(
        datatype=case.string_dt, column_name="k", column_idx=1)
    case.singlet_cdt.grant_everyone_access()

    case.triplet_cdt = CompoundDatatype(user=case.myUser)
    case.triplet_cdt.save()
    case.triplet_cdt.members.create(datatype=case.string_dt, column_name="a", column_idx=1)
    case.triplet_cdt.members.create(datatype=case.string_dt, column_name="b", column_idx=2)
    case.triplet_cdt.members.create(datatype=case.string_dt, column_name="c", column_idx=3)
    case.triplet_cdt.grant_everyone_access()

    ####
    # This next bit is used for pipeline.tests.

    # Define CDT "triplet_squares_cdt" with 3 members for use as an input/output
    case.triplet_squares_cdt = CompoundDatatype(user=case.myUser)
    case.triplet_squares_cdt.save()
    case.triplet_squares_cdt.members.create(datatype=case.string_dt, column_name="a^2", column_idx=1)
    case.triplet_squares_cdt.members.create(datatype=case.string_dt, column_name="b^2", column_idx=2)
    case.triplet_squares_cdt.members.create(datatype=case.string_dt, column_name="c^2", column_idx=3)
    case.triplet_squares_cdt.grant_everyone_access()

    # A CDT with mixed Datatypes
    case.mix_triplet_cdt = CompoundDatatype(user=case.myUser)
    case.mix_triplet_cdt.save()
    case.mix_triplet_cdt.members.create(datatype=case.string_dt, column_name="StrCol1", column_idx=1)
    case.mix_triplet_cdt.members.create(datatype=case.DNA_dt, column_name="DNACol2", column_idx=2)
    case.mix_triplet_cdt.members.create(datatype=case.string_dt, column_name="StrCol3", column_idx=3)
    case.mix_triplet_cdt.grant_everyone_access()

    # Define CDT "doublet_cdt" with 2 members for use as an input/output
    case.doublet_cdt = CompoundDatatype(user=case.myUser)
    case.doublet_cdt.save()
    case.doublet_cdt.members.create(datatype=case.string_dt, column_name="x", column_idx=1)
    case.doublet_cdt.members.create(datatype=case.string_dt, column_name="y", column_idx=2)
    case.doublet_cdt.grant_everyone_access()

    ####
    # Stuff from this point on is used in librarian and archive
    # testing.

    # October 15: more CDTs.
    case.DNA_triplet_cdt = CompoundDatatype(user=case.myUser)
    case.DNA_triplet_cdt.save()
    case.DNA_triplet_cdt.members.create(datatype=case.DNA_dt, column_name="a", column_idx=1)
    case.DNA_triplet_cdt.members.create(datatype=case.DNA_dt, column_name="b", column_idx=2)
    case.DNA_triplet_cdt.members.create(datatype=case.DNA_dt, column_name="c", column_idx=3)
    case.DNA_triplet_cdt.grant_everyone_access()

    case.DNA_doublet_cdt = CompoundDatatype(user=case.myUser)
    case.DNA_doublet_cdt.save()
    case.DNA_doublet_cdt.members.create(datatype=case.DNA_dt, column_name="x", column_idx=1)
    case.DNA_doublet_cdt.members.create(datatype=case.DNA_dt, column_name="y", column_idx=2)
    case.DNA_doublet_cdt.grant_everyone_access()


def clean_up_all_files():
    """
    Delete all files that have been put into the database as FileFields.
    """
    for crr in CodeResourceRevision.objects.all():
        # Remember that this can be empty.
        # if crr.content_file != None:
        #     crr.content_file.delete()
        # Weirdly, if crr.content_file == None,
        # it still entered the above.  This seems to be a bug
        # in Django!
        if crr.coderesource.filename != "":
            crr.content_file.close()
            crr.content_file.delete()

        crr.delete()

    # Also clear all datasets.  This was previously in librarian.tests
    # but we move it here.
    for dataset in Dataset.objects.all():
        dataset.dataset_file.close()
        dataset.dataset_file.delete()
        dataset.delete()

    for mo in MethodOutput.objects.all():
        mo.output_log.close()
        mo.output_log.delete()
        mo.error_log.close()
        mo.error_log.delete()
        mo.delete()

    for vl in VerificationLog.objects.all():
        vl.output_log.close()
        vl.output_log.delete()
        vl.error_log.close()
        vl.error_log.delete()
        vl.delete()

    for sf in StagedFile.objects.all():
        sf.uploaded_file.close()
        sf.uploaded_file.delete()
        sf.delete()


class MetadataTestCase(TestCase):
    """
    Set up a database state for unit testing.
    
    Other test classes that require this state can extend this one.
    """
    # fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        create_metadata_test_environment(self)

    def tearDown(self):
        clean_up_all_files()


class MetadataTransactionTestCase(TransactionTestCase):
    """
    Set up a database state for unit testing.

    Other test classes that require this state can extend this one.
    """
    # fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        create_metadata_test_environment(self)

    def tearDown(self):
        clean_up_all_files()


class DatatypeTests(MetadataTestCase):

    def setUp(self):
        """Add some DTs used to check circular restrictions."""
        MetadataTestCase.setUp(self)

        # Datatypes used to test circular restrictions.
        self.dt_1 = Datatype(
            name="dt_1",
            description="A string (1)",
            user=self.myUser)
        self.dt_1.save()
        self.dt_1.restricts.add(self.string_dt)

        self.dt_2 = Datatype(
            name="dt_2",
            description="A string (2)",
            user=self.myUser)
        self.dt_2.save()
        self.dt_2.restricts.add(self.string_dt)

        self.dt_3 = Datatype(
            name="dt_3",
            description="A string (3)",
            user=self.myUser)
        self.dt_3.save()
        self.dt_3.restricts.add(self.string_dt)

        self.dt_4 = Datatype(
            name="dt_4",
            description="A string (4)",
            user=self.myUser)
        self.dt_4.save()
        self.dt_4.restricts.add(self.string_dt)

        self.dt_5 = Datatype(
            name="dt_5",
            description="A string (5)",
            user=self.myUser)
        self.dt_5.save()
        self.dt_5.restricts.add(self.string_dt)

    def test_datatype_unicode(self):
        """
        Unicode representation must be the instance's name.

        """
        my_datatype = Datatype(name="fhqwhgads", user=self.myUser)
        self.assertEqual(unicode(my_datatype), "fhqwhgads")

    ### Unit tests for datatype.clean (Circular restrictions) ###

    # Direct circular cases: start, middle, end
    # Start   dt1 restricts dt1, dt3, dt4
    # Middle  dt1 restricts dt3, dt1, dt4
    # End     dt1 restricts dt3, dt4, dt1
    # Good    dt1 restricts dt2, dt3, dt4

    def test_datatype_circular_direct_start_clean_bad(self):
        """
        Circular, direct, start
        dt1 restricts dt1, dt3, dt4
        """
        self.dt_1.restricts.add(self.dt_1)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()

        self.assertRaisesRegexp(ValidationError, 
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_circular_direct_middle_clean_bad(self):
        """
        Circular, direct, middle
        dt1 restricts dt3, dt1, dt4
        """
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_1)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_circular_direct_end_clean_bad(self):
        """
        Circular, direct, middle
        dt1 restricts dt3, dt4, dt1
        """
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.restricts.add(self.dt_1)
        self.dt_1.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_circular_direct_clean_good(self):
        """
        dt1 restricts dt2, dt3, dt4
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.assertEqual(self.dt_1.clean(), None)

    def test_datatype_circular_recursive_begin_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()

        self.dt_2.restricts.add(self.dt_1)
        self.dt_2.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_circular_recursive_middle_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()

        self.dt_3.restricts.add(self.dt_1)
        self.dt_3.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_circular_recursive_end_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_4.restricts.add(self.dt_1)
        self.dt_4.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_circular_recursive_clean_good1(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_2.restricts.add(self.dt_5)
        self.dt_2.save()
        self.assertEqual(self.dt_1.clean(), None)

    def test_datatype_circular_recursive_clean_good2(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_3.restricts.add(self.dt_5)
        self.dt_3.save()
        self.assertEqual(self.dt_1.clean(), None)

    def test_datatype_circular_recursive_clean_good3(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_4.restricts.add(self.dt_5)
        self.dt_4.save()
        self.assertEqual(self.dt_1.clean(), None)

    def test_datatype_circular_recursive_clean_good4(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt4
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_2.restricts.add(self.dt_4)
        self.dt_2.save()
        self.assertEqual(self.dt_1.clean(), None)

    def test_datatype_circular_recursive_clean_good5(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt4
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_3.restricts.add(self.dt_4)
        self.dt_3.save()
        self.assertEqual(self.dt_1.clean(), None)

    def test_datatype_circular_recursive_clean_good6(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt2
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_4.restricts.add(self.dt_2)
        self.dt_4.save()
        self.assertEqual(self.dt_1.clean(), None)

    def test_datatype_direct_is_restricted_by_1(self):
        """
        dt1 restricts dt2
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - TRUE
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.save()

        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False)
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), True)

    def test_datatype_direct_is_restricted_by_2(self):
        """
        dt1 and dt2 exist but do not restrict each other
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - FALSE
        """
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False)
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), False)

    def test_datatype_recursive_is_restricted_by_1(self):
        """
        dt1 restricts dt2, dt2 restricts dt3

        dt1.is_restricted_by(dt3) - FALSE
        dt3.is_restricted_by(dt1) - TRUE
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - TRUE
        """

        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.save()
        self.dt_2.restricts.add(self.dt_3)
        self.dt_2.save()

        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False)
        self.assertEqual(self.dt_3.is_restricted_by(self.dt_1), True)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False)
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), True)

    def test_datatype_recursive_is_restricted_by_2(self):
        """
        dt1 restricts dt[2,3,4]
        dt2 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_2.restricts.add(self.dt_5)
        self.dt_2.save()
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False)
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True)

    def test_datatype_recursive_is_restricted_by_3(self):
        """
        dt1 restricts dt[2,3,4]
        dt3 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_3.restricts.add(self.dt_5)
        self.dt_3.save()
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False)
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True)

    def test_datatype_recursive_is_restricted_by_4(self):
        """
        dt1 restricts dt[2,3,4]
        dt4 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.restricts.add(self.dt_4)
        self.dt_1.save()
        self.dt_4.restricts.add(self.dt_5)
        self.dt_4.save()
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False)
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False)
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True)

    def test_datatype_no_restriction_clean_good(self):
        """
        Datatype without any restrictions.
        """
        self.assertEqual(self.dt_1.clean(), None)

    def test_datatype_nested_valid_restrictions_clean_good(self):
        """
        Datatypes such that A restricts B, and B restricts C
        """
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.save()
        self.dt_2.restricts.add(self.dt_3)
        self.dt_2.save()
        self.assertEqual(self.dt_1.clean(), None)
        self.assertEqual(self.dt_2.clean(), None)
        self.assertEqual(self.dt_3.clean(), None)

    def test_datatype_nested_invalid_restrictions_scrambled_clean_bad(self):
        """
        Datatypes are restricted to constrain execution order such that:
        A restricts C
        A restricts B
        B restricts C
        C restricts A
        """

        self.dt_1.restricts.add(self.dt_3)
        self.dt_1.save()
        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.save()
        self.dt_2.restricts.add(self.dt_3)
        self.dt_2.save()
        self.dt_3.restricts.add(self.dt_1)
        self.dt_3.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_direct_circular_restriction_clean_bad(self):
        """
        Datatype directly restricts itself: A restricts A
        """

        self.dt_1.restricts.add(self.dt_1)
        self.dt_1.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_circular_restriction_indirect_clean(self):
        """
        Datatype restricts itself through intermediary:
        A restricts B
        B restricts A
        """

        self.dt_1.restricts.add(self.dt_2)
        self.dt_1.save()
        self.dt_2.restricts.add(self.dt_1)
        self.dt_2.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape('Datatype "{}" has a circular restriction'.format(self.dt_1)),
                                self.dt_1.clean)

    def test_datatype_clean_no_restricts(self):
        """
        Clean on a Datatype with no restrictions should pass.
        """
        datatype = Datatype(
            name="squeaky",
            description="a clean, new datatype",
            user=self.myUser)
        # Note that this passes if the next line is uncommented.
        #datatype.save()
        self.assertEqual(datatype.clean(), None)

    ########
    # New tests to check the new functionality in Datatype.clean()
    # that checks BasicConstraints, the prototype Dataset, etc.

    def __test_clean_restrict_same_builtin_multiply_good_h(self, builtin_type):
        """
        Helper for testing clean() on cases where a Datatype restricts several supertypes with the same builtin type.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype 1", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(builtin_type)

        super2_DT = Datatype(name="SuperDT2", description="Supertype 2", user=self.myUser)
        super2_DT.full_clean()
        super2_DT.save()
        super2_DT.restricts.add(builtin_type)

        my_DT = Datatype(name="MyDT", description="Datatype with two built-in supertypes", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(builtin_type, builtin_type)

        your_DT = Datatype(name="YourDT", description="Datatype with two supertypes", user=self.myUser)
        your_DT.full_clean()
        your_DT.save()
        your_DT.restricts.add(super_DT, super2_DT)

        self.assertEquals(my_DT.clean(), None)
        self.assertEquals(your_DT.clean(), None)

    def test_clean_restrict_several_str_good(self):
        """
        Testing clean() on the case where a Datatype restricts several string supertypes.
        """
        self.__test_clean_restrict_same_builtin_multiply_good_h(self.STR)

    def test_clean_restrict_several_int_good(self):
        """
        Testing clean() on the case where a Datatype restricts several integer supertypes.
        """
        self.__test_clean_restrict_same_builtin_multiply_good_h(self.INT)

    def test_clean_restrict_several_float_good(self):
        """
        Testing clean() on the case where a Datatype restricts several float supertypes.
        """
        self.__test_clean_restrict_same_builtin_multiply_good_h(self.FLOAT)

    def test_clean_restrict_several_bool_good(self):
        """
        Testing clean() on the case where a Datatype restricts several Boolean supertypes.
        """
        self.__test_clean_restrict_same_builtin_multiply_good_h(self.BOOL)

    def test_clean_restrict_int_float_good(self):
        """
        Testing clean() on the case where a Datatype restricts both integer and float supertypes.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype 1", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.INT)

        super2_DT = Datatype(name="SuperDT2", description="Supertype 2", user=self.myUser)
        super2_DT.full_clean()
        super2_DT.save()
        super2_DT.restricts.add(self.FLOAT)

        my_DT = Datatype(name="MyDT", description="Datatype with two built-in supertypes", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.INT, self.FLOAT)

        your_DT = Datatype(name="YourDT", description="Datatype with two supertypes", user=self.myUser)
        your_DT.full_clean()
        your_DT.save()
        your_DT.restricts.add(super_DT, super2_DT)

        self.assertEquals(my_DT.clean(), None)
        self.assertEquals(your_DT.clean(), None)

    ####
    def __test_clean_restrict_several_builtins_bad_h(self, builtin_type_1, builtin_type_2):
        """
        Helper for testing clean() on cases where a Datatype restricts supertypes with non-compatible builtin types.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype 1", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(builtin_type_1)

        super2_DT = Datatype(name="SuperDT2", description="Supertype 2", user=self.myUser)
        super2_DT.full_clean()
        super2_DT.save()
        super2_DT.restricts.add(builtin_type_2)

        my_DT = Datatype(name="MyDT", description="Datatype with two built-in supertypes", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(builtin_type_1, builtin_type_2)

        your_DT = Datatype(name="YourDT", description="Datatype with two supertypes", user=self.myUser)
        your_DT.full_clean()
        your_DT.save()
        your_DT.restricts.add(super_DT, super2_DT)

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" restricts multiple built-in, non-numeric types'
                                           .format(my_DT))),
                                my_DT.clean)

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" restricts multiple built-in, non-numeric types'
                                           .format(your_DT))),
                                your_DT.clean)

    def test_clean_restricts_str_int_bad(self):
        """
        Tests clean() on the case of a Datatype restricting both STR and INT.
        """
        self.__test_clean_restrict_several_builtins_bad_h(self.STR, self.INT)

    def test_clean_restricts_str_float_bad(self):
        """
        Tests clean() on the case of a Datatype restricting both STR and FLOAT.
        """
        self.__test_clean_restrict_several_builtins_bad_h(self.STR, self.FLOAT)

    def test_clean_restricts_str_bool_bad(self):
        """
        Tests clean() on the case of a Datatype restricting both STR and BOOL.
        """
        self.__test_clean_restrict_several_builtins_bad_h(self.STR, self.BOOL)

    def test_clean_restricts_float_bool_bad(self):
        """
        Tests clean() on the case of a Datatype restricting both FLOAT and BOOL.
        """
        self.__test_clean_restrict_several_builtins_bad_h(self.FLOAT, self.BOOL)

    def test_clean_restricts_int_bool_bad(self):
        """
        Tests clean() on the case of a Datatype restricting both INT and BOOL.
        """
        self.__test_clean_restrict_several_builtins_bad_h(self.BOOL, self.INT)

    ####
    def test_clean_prototype_good(self):
        """
        Testing clean() on a Datatype whose prototype is well-defined.
        """
        # Make a Dataset for the prototype CSV file.
        PROTOTYPE_CDT = CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK)
        DNA_prototype = SymbolicDataset.create_SD(os.path.join(samplecode_path, "DNAprototype.csv"),
                                                  user=self.myUser, cdt=PROTOTYPE_CDT, name="DNAPrototype",
                                                  description="Prototype for the DNA Datatype")

        self.DNA_dt.prototype = DNA_prototype.dataset

        self.assertEquals(self.DNA_dt.clean(), None)

    def test_clean_raw_prototype_bad(self):
        """
        Testing clean() on a Datatype whose prototype is raw.
        """
        DNA_raw_prototype = SymbolicDataset.create_SD(os.path.join(samplecode_path, "DNAprototype.csv"),
                                                      user=self.myUser, cdt=None, name="RawPrototype",
                                                      description="Prototype that is raw")

        self.DNA_dt.prototype = DNA_raw_prototype.dataset
        PROTOTYPE_CDT = CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK)
        self.assertRaisesRegexp(ValidationError, 
                                re.escape('Prototype Dataset for Datatype "{}" should have CompoundDatatype "{}", '
                                          'but it is raw'.format(self.DNA_dt, PROTOTYPE_CDT)),
                                self.DNA_dt.clean)

    def test_clean_prototype_wrong_CDT_bad(self):
        """
        Testing clean() on a Datatype whose prototype has the incorrect CDT.
        """
        wrong_CDT = CompoundDatatype(user=self.myUser)
        wrong_CDT.save()
        wrong_CDT.members.create(datatype=self.STR, column_name="example", column_idx=1,
                                 blankable=True)
        wrong_CDT.members.create(datatype=self.BOOL, column_name="thisshouldbesomethingelse", column_idx=2)
        wrong_CDT.clean()

        DNA_prototype_bad_CDT = SymbolicDataset.create_SD(os.path.join(samplecode_path, "DNAprototype_bad_CDT.csv"),
                                                          user=self.myUser, cdt=wrong_CDT, name="BadCDTPrototype",
                                                          description="Prototype with a bad CDT")

        self.DNA_dt.prototype = DNA_prototype_bad_CDT.dataset

        PROTOTYPE_CDT = CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK)
        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Prototype Dataset for Datatype "{}" should have CompoundDatatype "{}", '
                                           'but it has "{}"'.format(self.DNA_dt, PROTOTYPE_CDT, wrong_CDT))),
                                self.DNA_dt.clean)

    # Propagation of BasicConstraint errors is checked thoroughly in the BasicConstraint
    # tests.  Let's just quickly check two cases.
    def test_clean_BC_clean_propagation_good(self):
        """
        Testing to confirm that BasicConstraint.clean() is called from Datatype.clean(): good case.
        """
        constr_DT = Datatype(name="ConstrainedDatatype", description="Datatype with good BasicConstraint",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.FLOAT)

        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="7")

        self.assertEquals(constr_DT.clean(), None)

    def test_clean_BC_clean_propagation_bad(self):
        """
        Testing to confirm that BasicConstraint.clean() is called from Datatype.clean(): bad case.
        """
        constr_DT = Datatype(name="BadlyConstrainedDatatype", description="Datatype with bad BasicConstraint",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.FLOAT)

        constr = constr_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT, rule="%Y %b %d")

        self.assertRaisesRegexp(ValidationError,
                                re.escape((('BasicConstraint "{}" specifies a date/time format, but its parent '
                                            'Datatype "{}" has builtin type "{}"')
                                            .format(constr, constr_DT, self.FLOAT))),
                                constr_DT.clean)

    # Cases where a Datatype has a good BasicConstraint associated to it are well-tested in the
    # BasicConstraint tests.  Again we quickly check a couple of cases.
    def test_clean_has_good_regexp_good(self):
        """
        Testing clean() on a Datatype with a good REGEXP attached.
        """
        constr_DT = Datatype(name="ConstrainedDatatype", description="Datatype with good REGEXP",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.FLOAT)

        constr_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule=".*")

        self.assertEquals(constr_DT.clean(), None)

    def test_clean_has_good_min_val_good(self):
        """
        Testing clean() on a Datatype with a good MIN_VAL attached.
        """
        constr_DT = Datatype(name="ConstrainedDatatype", description="Datatype with good MIN_VAL",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.INT)

        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="-22.3")

        self.assertEquals(constr_DT.clean(), None)

    # Cases where a Datatype has several good BCs attached.
    def test_clean_float_has_several_good_BCs_good(self):
        """
        Testing clean() on a Datatype with several good BCs attached.
        """
        constr_DT = Datatype(name="ConstrainedDatatype", description="FLOAT with good BCs",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.FLOAT)

        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="1000")
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="1.7")
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="[1-9]*")

        self.assertEquals(constr_DT.clean(), None)

    def test_clean_string_has_several_good_BCs_good(self):
        """
        Testing clean() on a string Datatype with several good BCs attached.
        """
        constr_DT = Datatype(name="ConstrainedDatatype", description="STR with good BCs",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.STR)

        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="6")
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT, rule="%Y %b %d")

        self.assertEquals(constr_DT.clean(), None)

    ####
    # Cases where a Datatype has multiple BasicConstraints of the same type.
    def __test_clean_multiple_same_BCs_bad_h(self, builtin_type, rules, multiple_BC_type):
        """
        Helper for the case where a Datatype has multiple BasicConstraints of the same type.

        rules is a list of tuples of the form (ruletype, rule).
        multiple_BC_type is one of BasicConstraint.(MIN|MAX)_(LENGTH|VAL) or
        BasicConstraint.DATETIMEFORMAT.
        """
        constr_DT = Datatype(name="MultiplyConstrainedDatatype",
                             description="Datatype with several BCs of the same type",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(builtin_type)

        counts = {}
        bad_ruletypes = set()
        for curr_ruletype, curr_rule in rules:
            try:
                counts[curr_ruletype] += 1
                bad_ruletypes.add(curr_ruletype)
            except KeyError:
                counts[curr_ruletype] = 1
            constr_DT.basic_constraints.create(ruletype=curr_ruletype, rule="{}".format(curr_rule))

        possible_matches = [re.escape('Datatype "{}" has {} constraints of type {}, but should have at most one'.
                                      format(constr_DT, counts[x], x))
                            for x in bad_ruletypes]
        match_pattern = "|".join(possible_matches)

        self.assertRaisesRegexp(ValidationError, match_pattern, constr_DT.clean)

    def test_clean_int_multiple_min_val_bad(self):
        """
        Testing clean() on an integer Datatype with multiple MIN_VAL restrictions.
        """
        self.__test_clean_multiple_same_BCs_bad_h(
            self.INT,
            [(BasicConstraint.MIN_VAL, 6), (BasicConstraint.MIN_VAL, 8)],
            BasicConstraint.MIN_VAL
        )

    def test_clean_float_multiple_max_val_bad(self):
        """
        Testing clean() on a float Datatype with multiple MAX_VAL restrictions.
        """
        self.__test_clean_multiple_same_BCs_bad_h(
            self.FLOAT,
            [(BasicConstraint.MAX_VAL, 1220), (BasicConstraint.MAX_VAL, 6)],
            BasicConstraint.MAX_VAL
        )

    def test_clean_str_multiple_min_length_bad(self):
        """
        Testing clean() on a string Datatype with multiple MIN_LENGTH restrictions.
        """
        self.__test_clean_multiple_same_BCs_bad_h(
            self.STR,
            [(BasicConstraint.MIN_LENGTH, 1), (BasicConstraint.MIN_LENGTH, 2)],
            BasicConstraint.MIN_LENGTH
        )

    def test_clean_str_multiple_max_length_bad(self):
        """
        Testing clean() on a string Datatype with multiple MAX_LENGTH restrictions.
        """
        self.__test_clean_multiple_same_BCs_bad_h(
            self.STR,
            [(BasicConstraint.MAX_LENGTH, 7), (BasicConstraint.MAX_LENGTH, 4), (BasicConstraint.MAX_LENGTH, 7)],
            BasicConstraint.MAX_LENGTH
        )

    def test_clean_str_multiple_dtf_bad(self):
        """
        Testing clean() on a string Datatype with multiple DATETIMEFORMAT restrictions.
        """
        self.__test_clean_multiple_same_BCs_bad_h(
            self.STR,
            [(BasicConstraint.DATETIMEFORMAT, "%Y"), (BasicConstraint.DATETIMEFORMAT, "foo"),
             (BasicConstraint.MAX_LENGTH, 7)],
            BasicConstraint.DATETIMEFORMAT
        )

    def test_clean_float_some_good_some_multiple_BCs_bad(self):
        """
        Testing clean() on a float Datatype with several BCs but with at least one multiply-defined.
        """
        # Note that here, both MIN_VAL and MAX_VAL are multiply-defined,
        # so either one could fail.  That's why we pass a regexp for
        # multiple_BC_type.
        self.__test_clean_multiple_same_BCs_bad_h(
            self.FLOAT,
            [(BasicConstraint.MIN_VAL, "7"), (BasicConstraint.MAX_VAL, "15"),
             (BasicConstraint.MIN_VAL, "13"), (BasicConstraint.REGEXP, "[1-9]+"),
             (BasicConstraint.MAX_VAL, "19")],
            "(?:{}|{})".format(BasicConstraint.MIN_VAL, BasicConstraint.MAX_VAL)
        )

    def _setup_datatype(self, name, desc, rules, restricts):
        """
        Helper function to create a Datatype. Rules is a list of tuples (ruletype, rule),
        and restricts is a list of supertypes.
        """
        dt = Datatype(name=name, description=desc, user=self.myUser)
        dt.full_clean()
        dt.save()
        for supertype in restricts:
            dt.restricts.add(supertype)
        for ruletype, rule in rules:
            if ruletype:
                dt.basic_constraints.create(ruletype=ruletype, rule=rule)
        return dt

    def _setup_inheriting_datatype(self, 
                                   super_name, super_desc, super_ruletype, super_rule, super_builtin,
                                   cnstr_name, cnstr_desc, cnstr_ruletype, cnstr_rule):
        """
        Helper function to create a pair of Datatypes, one inheriting 
        from the other.
        """
        super_DT = self._setup_datatype(super_name, super_desc, [(super_ruletype, super_rule)], [super_builtin])
        constr_DT = self._setup_datatype(cnstr_name, cnstr_desc, [(cnstr_ruletype, cnstr_rule)], [super_DT])
        return (super_DT, constr_DT)

    def _setup_inheriting_datatype2(self,
                                    super1_name, super1_desc, super1_ruletype, super1_rule, super1_builtin,
                                    super2_name, super2_desc, super2_ruletype, super2_rule, super2_builtin,
                                    constr_name, constr_desc, constr_ruletype, constr_rule):
        """
        Helper function to create three Datatypes, the first two being
        supertypes of the third.
        """
        super1_DT, constr_DT = self._setup_inheriting_datatype(
                super1_name, super1_desc, super1_ruletype, super1_rule, super1_builtin,
                constr_name, constr_desc, constr_ruletype, constr_rule)
        super2_DT = self._setup_datatype(super2_name, super2_desc, [(super2_ruletype, super2_rule)], [super2_builtin])
        constr_DT.restricts.add(super2_DT)
        return (super1_DT, super2_DT, constr_DT)

    ####
    def __test_clean_num_constraint_conflicts_with_supertypes_h(self, builtin_type, BC_type, constr_val,
                                                                supertype_constr_val):
        """
        Helper to test cases where numerical constraints conflict with those of the supertypes.
        """
        super_DT, constr_DT = self._setup_inheriting_datatype("ParentDT", "Parent with constraint",
                BC_type, supertype_constr_val, builtin_type, "ConstrDT", 
                "Datatype whose constraint conflicts with parent",
                BC_type, constr_val)

        if BC_type == BasicConstraint.MIN_LENGTH:
            error_msg = 'Datatype "{}" has MIN_LENGTH {}, but its supertype "{}" has a longer or equal MIN_LENGTH of {}'
        elif BC_type == BasicConstraint.MAX_LENGTH:
            error_msg = 'Datatype "{}" has MAX_LENGTH {}, but its supertype "{}" has a shorter or equal MAX_LENGTH of {}'
        elif BC_type == BasicConstraint.MIN_VAL:
            error_msg = 'Datatype "{}" has MIN_VAL {}, but its supertype "{}" has a larger or equal MIN_VAL of {}'
        elif BC_type == BasicConstraint.MAX_VAL:
            error_msg = 'Datatype "{}" has MAX_VAL {}, but its supertype "{}" has a smaller or equal MAX_VAL of {}'

        self.assertRaisesRegexp(ValidationError,
                                re.escape(error_msg.format(constr_DT, constr_val, super_DT, supertype_constr_val)),
                                constr_DT.clean)

    def test_clean_int_min_val_supertype_conflict_bad(self):
        """
        Testing clean() on an integer whose MIN_VAL conflicts with its supertypes'.
        """
        self.__test_clean_num_constraint_conflicts_with_supertypes_h(
            self.INT,
            BasicConstraint.MIN_VAL,
            7, 9
        )

    def test_clean_float_max_val_supertype_conflict_bad(self):
        """
        Testing clean() on an integer whose MIN_VAL conflicts with its supertypes'.
        """
        self.__test_clean_num_constraint_conflicts_with_supertypes_h(
            self.FLOAT,
            BasicConstraint.MAX_VAL,
            11, 10.7
        )

    def test_clean_str_min_length_supertype_conflict_bad(self):
        """
        Testing clean() on an integer whose MIN_VAL conflicts with its supertypes'.
        """
        self.__test_clean_num_constraint_conflicts_with_supertypes_h(
            self.STR,
            BasicConstraint.MIN_LENGTH,
            9, 10
        )

    def test_clean_str_max_length_supertype_conflict_bad(self):
        """
        Testing clean() on an integer whose MAX_VAL conflicts with its supertypes'.
        """
        self.__test_clean_num_constraint_conflicts_with_supertypes_h(
            self.STR,
            BasicConstraint.MAX_LENGTH,
            223, 20
        )

    def test_clean_dtf_conflict_with_supertype_bad(self):
        """
        Testing clean() on the case where a Datatype has a DATETIMEFORMAT but so does its supertype.
        """
        _super_DT, constr_DT = self._setup_inheriting_datatype("DateTimeDT", "String with a DATETIMEFORMAT",
                BasicConstraint.DATETIMEFORMAT, "%Y %b %d", self.STR, "OverwritingDateTimeDT",
                "String with a DATETIMEFORMAT whose parent also has one", 
                BasicConstraint.DATETIMEFORMAT, "%Y-%b-%d")

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" should have only one DATETIMEFORMAT restriction acting on '
                                           'it, but it has {}'.format(constr_DT, 2))),
                                constr_DT.clean)

    def test_clean_several_supertypes_have_dtfs_bad(self):
        """
        Testing clean() on the case where a Datatype has several supertypes with DATETIMEFORMATs.
        """
        dtf = BasicConstraint.DATETIMEFORMAT
        _super_DT, _second_DT, constr_DT = self._setup_inheriting_datatype2(
                "DateTimeDT", "String with a DATETIMEFORMAT", dtf, "%Y %b %d", self.STR, 
                "OverwritingDateTimeDT", "Second string with a DATETIMEFORMAT", dtf, "%Y %b %d", self.STR,
                "OverwritingDateTimeChildDT", "String with a DATETIMEFORMAT whose parent also has one", dtf, "%Y %b %d")

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" should have only one DATETIMEFORMAT restriction acting on '
                                           'it, but it has {}'.format(constr_DT, 3))),
                                constr_DT.clean)

    def test_clean_dtf_several_supertypes_one_has_dtf_bad(self):
        """
        Testing clean() on the case where a Datatype has a DATETIMEFORMAT and several supertypes, one which has one.
        """
        dtf = BasicConstraint.DATETIMEFORMAT
        _super_DT, _second_DT, constr_DT = self._setup_inheriting_datatype2(
                "DateTimeDT", "String with a DATETIMEFORMAT", dtf, "%Y %b %d", self.STR, 
                "OtherDT", "String by a different name", None, None, self.STR,
                "OverwritingDateTimeDT", "String with a DATETIMEFORMAT whose parent also has one", dtf, "%Y %d")

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" should have only one DATETIMEFORMAT restriction acting on '
                                           'it, but it has {}'.format(constr_DT, 2))),
                                constr_DT.clean)

    def test_clean_dtf_several_supertypes_one_has_dtf_other_is_builtin_bad(self):
        """
        Testing clean() on a DATETIMEFORMATted Datatype with two supertypes: STR and another DTFd Datatype.
        """
        dtf = BasicConstraint.DATETIMEFORMAT
        _super_DT, constr_DT = self._setup_inheriting_datatype(
            "DateTimeDT", "String with a DATETIMEFORMAT", dtf, "%Y %b %d", self.STR,
            "OverwritingDateTimeDT", "String with a DATETIMEFORMAT whose parent also has one", dtf, "%Y %d")
        constr_DT.restricts.add(self.STR)
        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" should have only one DATETIMEFORMAT restriction acting on '
                                           'it, but it has {}'.format(constr_DT, 2))),
                                constr_DT.clean)

    def test_clean_float_conflicting_min_max_val_bad(self):
        """
        Testing clean() on a float Datatype with conflicting MIN|MAX_VAL defined directly.
        """
        constr_DT = self._setup_datatype("ConflictingBoundsDT", "Float with conflicting MIN|MAX_VAL",
                [(BasicConstraint.MIN_VAL, "15"), (BasicConstraint.MAX_VAL, "5")], [self.FLOAT])

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" has effective MIN_VAL {} exceeding its effective MAX_VAL {}'
                                           .format(constr_DT, 15, 5))),
                                constr_DT.clean)

    def test_clean_int_conflicting_inherited_min_max_val_bad(self):
        """
        Testing clean() on an int Datatype with conflicting MIN|MAX_VAL defined on its supertypes.
        """
        _, _, constr_DT = self._setup_inheriting_datatype2(
            "BoundedFloatDT", "Float with a MIN_VAL", BasicConstraint.MIN_VAL, "20", self.FLOAT,
            "BoundedIntDT", "Int with a MAX_VAL", BasicConstraint.MAX_VAL, "18.2", self.INT,
            "InheritingBadBoundsDT", "Datatype inheriting conflicting MIN|MAX_VAL", None, None)

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" has effective MIN_VAL {} exceeding its effective MAX_VAL {}'
                                           .format(constr_DT, 20, 18.2))),
                                constr_DT.clean)


    def test_clean_float_conflicting_half_inherited_min_max_val_bad(self):
        """
        Testing clean() on a float Datatype with conflicting MIN|MAX_VAL, one inherited and one directly.
        """
        super_DT = Datatype(name="BoundedDT", description="Float with a MIN_VAL", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.FLOAT)
        super_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="17.7")

        constr_DT = Datatype(name="ConflictingBoundsDT",
                             description="Float with half-inherited conflicting MIN|MAX_VAL",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super_DT)
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="6")

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" has effective MIN_VAL {} exceeding its effective MAX_VAL {}'
                                           .format(constr_DT, 17.7, 6))),
                                constr_DT.clean)

    def test_clean_int_min_max_val_too_narrow_bad(self):
        """
        Testing clean() on an integer Datatype whose MIN|MAX_VAL do not admit any integers.
        """
        constr_DT = Datatype(name="ConflictingBoundsDT",
                             description="INT with MIN|MAX_VAL too narrow",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.INT)
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="15.7")
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="15.9")

        self.assertRaisesRegexp(ValidationError,
                                re.escape((('Datatype "{}" has built-in type INT, but there are no integers between its '
                                            'effective MIN_VAL {} and its effective MAX_VAL {}')
                                            .format(constr_DT, 15.7, 15.9))),
                                constr_DT.clean)

    def test_clean_int_inherited_min_max_val_too_narrow_bad(self):
        """
        Testing clean() on an integer Datatype whose inherited MIN|MAX_VAL do not admit any integers.
        """
        super_DT = Datatype(name="BoundedFloatDT", description="Float with a MIN_VAL",
                            user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.FLOAT)
        super_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="20.2")

        second_DT = Datatype(name="BoundedIntDT", description="Int with a MAX_VAL",
                             user=self.myUser)
        second_DT.full_clean()
        second_DT.save()
        second_DT.restricts.add(self.INT)
        second_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="20.55")

        constr_DT = Datatype(name="InheritingBadBoundsDT",
                             description="Datatype inheriting too-narrow MIN|MAX_VAL",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super_DT)
        constr_DT.restricts.add(second_DT)

        self.assertRaisesRegexp(ValidationError,
                                re.escape((('Datatype "{}" has built-in type INT, but there are no integers between its '
                                            'effective MIN_VAL {} and its effective MAX_VAL {}')
                                            .format(constr_DT, 20.2, 20.55))),
                                constr_DT.clean)


    def test_clean_int_half_inherited_min_max_val_too_narrow_bad(self):
        """
        Testing clean() on a float Datatype with half-inherited MIN|MAX_VAL that are too narrow.
        """
        super_DT = Datatype(name="BoundedDT", description="Float with a MIN_VAL", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.FLOAT)
        super_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="17.1")

        constr_DT = Datatype(name="NarrowBoundsDT",
                             description="INT with half-inherited too-narrow MIN|MAX_VAL",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super_DT)
        constr_DT.restricts.add(self.INT)
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="17.8")

        self.assertRaisesRegexp(ValidationError,
                                re.escape((('Datatype "{}" has built-in type INT, but there are no integers between its '
                                            'effective MIN_VAL {} and its effective MAX_VAL {}')
                                            .format(constr_DT, 17.1, 17.8))),
                                constr_DT.clean)

    def test_clean_str_conflicting_min_max_length_bad(self):
        """
        Testing clean() on a string Datatype with conflicting MIN|MAX_LENGTH defined directly.
        """
        constr_DT = Datatype(name="ConflictingBoundsDT",
                             description="String with conflicting MIN|MAX_LENGTH",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.STR)
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="2234")
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="6")

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" has effective MIN_LENGTH {} exceeding its effective MAX_LENGTH {}'
                                           .format(constr_DT, 2234, 6))),
                                constr_DT.clean)

    def test_clean_str_conflicting_inherited_min_max_length_bad(self):
        """
        Testing clean() on a string Datatype with conflicting MIN|MAX_LENGTH defined on its supertypes.
        """
        super_DT = Datatype(name="BoundedMinDT", description="String with a MIN_LENGTH", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)
        super_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="44")

        second_DT = Datatype(name="BoundedMaxDT", description="String with a MAX_LENGTH", user=self.myUser)
        second_DT.full_clean()
        second_DT.save()
        second_DT.restricts.add(self.STR)
        second_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="22")

        constr_DT = Datatype(name="InheritingBadBoundsDT",
                             description="Datatype inheriting conflicting MIN|MAX_LENGTH",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super_DT)
        constr_DT.restricts.add(second_DT)

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" has effective MIN_LENGTH {} exceeding its effective MAX_LENGTH {}'
                                           .format(constr_DT, 44, 22))),
                                constr_DT.clean)

    def test_clean_str_conflicting_half_inherited_min_max_length_bad(self):
        """
        Testing clean() on a string Datatype with conflicting MIN|MAX_LENGTH, one inherited and one direct.
        """
        super_DT = Datatype(name="BoundedDT", description="String with a MIN_LENGTH",
                            user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)
        super_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="20")

        constr_DT = Datatype(name="HalfInheritingBadBoundsDT",
                             description="Datatype inheriting conflicting MIN|MAX_LENGTH",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super_DT)
        constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="30")

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" has effective MIN_LENGTH {} exceeding its effective MAX_LENGTH {}'
                                           .format(constr_DT, 30, 20))),
                                constr_DT.clean)

    # FIXME: add some tests here when CustomConstraints are fully-coded.

    ####
    # Tests of is_complete() and complete_clean().
    def test_is_complete_unsaved(self):
        """
        Tests is_complete() on an unsaved Datatype (returns False).
        """
        my_DT = Datatype(name="IncompleteDT", description="Non-finished Datatype",
                         user=self.myUser)
        my_DT.full_clean()

        self.assertEquals(my_DT.is_complete(), False)

    def test_is_complete_incomplete(self):
        """
        Tests is_complete() on a saved but incomplete Datatype (returns False).
        """
        my_DT = Datatype(name="IncompleteDT", description="Non-finished Datatype",
                         user=self.myUser)
        my_DT.full_clean()
        my_DT.save()

        self.assertEquals(my_DT.is_complete(), False)

    def test_is_complete_restricts_string(self):
        """
        Tests is_complete() on a complete Datatype that restricts STR (returns True).
        """
        my_DT = Datatype(name="IncompleteDT", description="Non-finished Datatype",
                         user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.STR)

        self.assertEquals(my_DT.is_complete(), True)

    def test_is_complete_restricts_others(self):
        """
        Tests is_complete() on a complete Datatype that restricts other Datatypes (returns True).
        """
        super_DT = Datatype(name="SuperDT", description="Supertype", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)

        middle_DT = Datatype(name="MiddleDT", description="Middle type", user=self.myUser)
        middle_DT.full_clean()
        middle_DT.save()
        middle_DT.restricts.add(super_DT)

        my_DT = Datatype(name="SubDT", description="Subtype", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(middle_DT, self.INT)

        self.assertEquals(middle_DT.is_complete(), True)
        self.assertEquals(my_DT.is_complete(), True)

        self.assertEquals(my_DT.is_complete(), True)

    def test_complete_clean_unsaved_bad(self):
        """
        Tests complete_clean() on an unsaved Datatype.
        """
        my_DT = Datatype(name="IncompleteDT", description="Non-finished Datatype", user=self.myUser)
        my_DT.full_clean()

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" does not restrict any of the Shipyard atomic Datatypes'
                                          .format(my_DT))),
                                my_DT.complete_clean)

    def test_complete_clean_incomplete(self):
        """
        Tests complete_clean() on a saved but incomplete Datatype.
        """
        my_DT = Datatype(name="IncompleteDT", description="Non-finished Datatype", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" does not restrict any of the Shipyard atomic Datatypes'
                                          .format(my_DT))),
                                my_DT.complete_clean)


    def test_complete_clean_restricts_string(self):
        """
        Tests complete_clean() on a complete Datatype that restricts STR.
        """
        my_DT = Datatype(name="IncompleteDT", description="Non-finished Datatype", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.STR)

        self.assertEquals(my_DT.complete_clean(), None)

    def test_complete_clean_restricts_others(self):
        """
        Tests complete_clean() on a complete Datatype that restricts other Datatypes (returns True).
        """
        super_DT = Datatype(name="SuperDT", description="Supertype", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)

        middle_DT = Datatype(name="MiddleDT", description="Middle type", user=self.myUser)
        middle_DT.full_clean()
        middle_DT.save()
        middle_DT.restricts.add(super_DT)

        my_DT = Datatype(name="SubDT", description="Subtype", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(middle_DT, self.STR)

        self.assertEquals(middle_DT.complete_clean(), None)
        self.assertEquals(my_DT.complete_clean(), None)

        self.assertEquals(my_DT.complete_clean(), None)

    # Quick check of propagation.
    def test_complete_clean_propagate_from_clean(self):
        """
        Testing complete_clean() on a string Datatype with conflicting MIN|MAX_LENGTH defined on its supertypes.
        """
        super_DT = Datatype(name="BoundedMinDT", description="String with a MIN_LENGTH", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)
        super_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="44")

        second_DT = Datatype(name="BoundedMaxDT", description="String with a MAX_LENGTH", user=self.myUser)
        second_DT.full_clean()
        second_DT.save()
        second_DT.restricts.add(self.STR)
        second_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="22")

        constr_DT = Datatype(name="InheritingBadBoundsDT",
                             description="Datatype inheriting conflicting MIN|MAX_LENGTH",
                             user=self.myUser)
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super_DT)
        constr_DT.restricts.add(second_DT)

        self.assertRaisesRegexp(ValidationError,
                                re.escape(('Datatype "{}" has effective MIN_LENGTH {} exceeding its effective '
                                          'MAX_LENGTH {}').format(constr_DT, 44, 22)),
                                constr_DT.complete_clean)


class DatatypeGetBuiltinTypeTests(MetadataTestCase):
    """
    Tests of the Datatype.get_builtin_type() function.
    """
    def test_on_builtins(self):
        """
        Testing on the built-in Shipyard types.
        """
        self.assertEquals(self.STR.get_builtin_type(), self.STR)
        self.assertEquals(self.INT.get_builtin_type(), self.INT)
        self.assertEquals(self.FLOAT.get_builtin_type(), self.FLOAT)
        self.assertEquals(self.BOOL.get_builtin_type(), self.BOOL)

    ########
    def __test_on_direct_builtin_descendant_h(self, builtin_type):
        """
        Helper for testing on direct descendants on the builtins.
        """
        my_DT = Datatype(name="DescendantDT", description="Descendant of builtin DT", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(builtin_type)

        self.assertEquals(my_DT.get_builtin_type(), builtin_type)

    def test_direct_descendant_int(self):
        """
        Testing case where Datatype is a descendant of INT.
        """
        self.__test_on_direct_builtin_descendant_h(self.INT)

    def test_direct_descendant_str(self):
        """
        Testing case where Datatype is a descendant of STR.
        """
        self.__test_on_direct_builtin_descendant_h(self.STR)

    def test_direct_descendant_float(self):
        """
        Testing case where Datatype is a descendant of FLOAT.
        """
        self.__test_on_direct_builtin_descendant_h(self.FLOAT)

    def test_direct_descendant_bool(self):
        """
        Testing case where Datatype is a descendant of BOOL.
        """
        self.__test_on_direct_builtin_descendant_h(self.BOOL)

    ########
    def __test_supertype_precedence_h(self, builtin_types_to_restrict, most_restrictive_type):
        """
        Helper for testing appropriate supertype precedence.
        """
        my_DT = Datatype(name="InheritingDT", description="Datatype with several supertypes", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()

        for to_restrict in builtin_types_to_restrict:
            my_DT.restricts.add(to_restrict)

        self.assertEquals(my_DT.get_builtin_type(), most_restrictive_type)

    def test_supertype_precedence_float_over_str(self):
        """
        FLOAT should take precedence over STR.
        """
        self.__test_supertype_precedence_h([self.STR, self.FLOAT], self.FLOAT)

    def test_supertype_precedence_int_over_str(self):
        """
        INT should take precedence over STR.
        """
        self.__test_supertype_precedence_h([self.STR, self.INT], self.INT)

    def test_supertype_precedence_bool_over_str(self):
        """
        BOOL should take precedence over STR.
        """
        self.__test_supertype_precedence_h([self.BOOL, self.STR], self.BOOL)

    def test_supertype_precedence_int_over_float(self):
        """
        INT should take precedence over FLOAT.
        """
        self.__test_supertype_precedence_h([self.INT, self.FLOAT], self.INT)

    def test_supertype_precedence_bool_over_float(self):
        """
        BOOL should take precedence over FLOAT.
        """
        self.__test_supertype_precedence_h([self.FLOAT, self.BOOL], self.BOOL)

    def test_supertype_precedence_bool_over_int(self):
        """
        BOOL should take precedence over INT.
        """
        self.__test_supertype_precedence_h([self.INT, self.BOOL], self.BOOL)

    def test_supertype_precedence_multiple(self):
        """
        Testing precendence when there are several builtins restricted.
        """
        self.__test_supertype_precedence_h([self.INT, self.BOOL, self.STR], self.BOOL)

    ########
    def test_multiple_supertypes(self):
        """
        Testing case where Datatype has multiple supertypes of varying generations.
        """
        super_DT = Datatype(name="SuperDT", description="Super DT", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.FLOAT)

        super2_DT = Datatype(name="SuperDT2", description="Super DT 2", user=self.myUser)
        super2_DT.full_clean()
        super2_DT.save()
        super2_DT.restricts.add(self.STR)

        super3_DT = Datatype(name="SuperDT3", description="Super DT 3", user=self.myUser)
        super3_DT.full_clean()
        super3_DT.save()
        super3_DT.restricts.add(super_DT)

        my_DT = Datatype(name="DescendantDT", description="Descendant of several supertypes", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(super2_DT)
        my_DT.restricts.add(super3_DT)

        self.assertEquals(my_DT.get_builtin_type(), self.FLOAT)

    def test_multiple_supertypes_2(self):
        """
        Another testing case where Datatype has multiple supertypes of varying generations.
        """
        super_DT = Datatype(name="SuperDT", description="Super DT", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.FLOAT)

        super2_DT = Datatype(name="SuperDT2", description="Super DT 2", user=self.myUser)
        super2_DT.full_clean()
        super2_DT.save()
        super2_DT.restricts.add(self.BOOL)

        super3_DT = Datatype(name="SuperDT3", description="Super DT 3", user=self.myUser)
        super3_DT.full_clean()
        super3_DT.save()
        super3_DT.restricts.add(super_DT)

        my_DT = Datatype(name="DescendantDT", description="Descendant of several supertypes", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(super2_DT)
        my_DT.restricts.add(super3_DT)

        self.assertEquals(my_DT.get_builtin_type(), self.BOOL)


class DatatypeCheckBasicConstraints(MetadataTestCase):
    """
    Tests of Datatype.check_basic_constraints().
    """
    def __test_builtin_type_good_h(self, builtin_type, string_to_check):
        """
        Helper for testing good cases where the input conforms to the appropriate built-in type.
        """
        my_DT = Datatype(name="MyDT", description="Non-builtin datatype", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(builtin_type)

        # Check builtin type too.
        self.assertEquals(builtin_type.check_basic_constraints(string_to_check), [])
        self.assertEquals(my_DT.check_basic_constraints(string_to_check), [])

        my_DT.delete()

    def test_str_good(self):
        """
        Testing case of a string with no constraints.
        """
        self.__test_builtin_type_good_h(self.STR, "foo")

    def test_float_good(self):
        """
        Testing case of a float with no constraints.
        """
        self.__test_builtin_type_good_h(self.FLOAT, "3.14")

    def test_int_good(self):
        """
        Testing case of an int with no constraints.
        """
        self.__test_builtin_type_good_h(self.INT, "-8")

    def test_bool_good(self):
        """
        Testing case of an int with no constraints.
        """
        self.__test_builtin_type_good_h(self.BOOL, "True")
        self.__test_builtin_type_good_h(self.BOOL, "TRUE")
        self.__test_builtin_type_good_h(self.BOOL, "true")
        self.__test_builtin_type_good_h(self.BOOL, "T")
        self.__test_builtin_type_good_h(self.BOOL, "t")
        self.__test_builtin_type_good_h(self.BOOL, "1")
        self.__test_builtin_type_good_h(self.BOOL, "False")
        self.__test_builtin_type_good_h(self.BOOL, "FALSE")
        self.__test_builtin_type_good_h(self.BOOL, "false")
        self.__test_builtin_type_good_h(self.BOOL, "F")
        self.__test_builtin_type_good_h(self.BOOL, "f")
        self.__test_builtin_type_good_h(self.BOOL, "0")

    def __test_builtin_type_bad_h(self, builtin_type, string_to_check):
        """
        Helper for testing cases where the input does not conform to the appropriate built-in type.
        """
        my_DT = Datatype(name="MyDT", description="Non-builtin datatype", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(builtin_type)

        builtin_type_error = ""
        if builtin_type == self.FLOAT:
            builtin_type_error = "Was not float"
        elif builtin_type == self.INT:
            builtin_type_error = "Was not integer"
        elif builtin_type == self.BOOL:
            builtin_type_error = "Was not Boolean"

        # Check builtin type too.
        self.assertEquals(builtin_type.check_basic_constraints(string_to_check), [builtin_type_error])
        self.assertEquals(my_DT.check_basic_constraints(string_to_check), [builtin_type_error])

    def test_float_error(self):
        """
        Testing case where string cannot be cast to a float.
        """
        self.__test_builtin_type_bad_h(self.FLOAT, "foo")

    def test_int_error(self):
        """
        Testing case where string cannot be cast to an int.
        """
        self.__test_builtin_type_bad_h(self.INT, "1.72")

    def test_bool_error(self):
        """
        Testing case where string cannot be cast to a Boolean.
        """
        self.__test_builtin_type_bad_h(self.BOOL, "maybe")

    # Test that "Was not [builtin type]" overrules other constraints.
    def __test_builtin_type_with_constraint_bad_h(self, builtin_type, BC_type, constr_val, string_to_check):
        """
        Helper for testing cases where the input does not conform to the appropriate built-in type.
        """
        my_DT = Datatype(name="MyDT", description="Non-builtin datatype", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(builtin_type)
        my_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))

        builtin_type_error = ""
        if builtin_type == self.FLOAT:
            builtin_type_error = "Was not float"
        elif builtin_type == self.INT:
            builtin_type_error = "Was not integer"
        elif builtin_type == self.BOOL:
            builtin_type_error = "Was not Boolean"

        self.assertEquals(my_DT.check_basic_constraints(string_to_check), [builtin_type_error])

    def test_float_error_with_constraint(self):
        """
        Testing case where string cannot be cast to a float and the Datatype has a constraint.
        """
        self.__test_builtin_type_with_constraint_bad_h(self.FLOAT, BasicConstraint.MIN_VAL, 8, "foo")

    def test_int_error_with_constraint(self):
        """
        Testing case where string cannot be cast to an integer and the Datatype has a constraint.
        """
        self.__test_builtin_type_with_constraint_bad_h(self.INT, BasicConstraint.MAX_VAL, 17, "1.2")

    def test_bool_error_with_constraint(self):
        """
        Testing case where string cannot be cast to an integer and the Datatype has a constraint.
        """
        self.__test_builtin_type_with_constraint_bad_h(self.BOOL, BasicConstraint.REGEXP, ".*", "what")

    ########
    def __test_numerical_constraint_h(self, builtin_type, BC_type, constr_val, string_to_check,
                                      passes_constraint=True):
        """
        Helper to test strings against numerical constraints.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with numerical BC", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(builtin_type)

        my_BC = my_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))

        if passes_constraint:
            self.assertEquals(my_DT.check_basic_constraints(string_to_check), [])
        else:
            self.assertEquals(my_DT.check_basic_constraints(string_to_check), [my_BC])

    ####
    def test_min_length_pass(self):
        """
        Testing case where a string passes the MIN_LENGTH restriction.
        """
        self.__test_numerical_constraint_h(self.STR, BasicConstraint.MIN_LENGTH, 5, "foobar",
                                           passes_constraint=True)

    def test_min_length_edge_pass(self):
        """
        Testing case where a string just passes (edge-condition) the MIN_LENGTH restriction.
        """
        self.__test_numerical_constraint_h(self.STR, BasicConstraint.MIN_LENGTH, 11, "hello world",
                                           passes_constraint=True)

    def test_min_length_fail(self):
        """
        Testing case where a string fails the MIN_LENGTH restriction.
        """
        self.__test_numerical_constraint_h(self.STR, BasicConstraint.MIN_LENGTH, 100, "short string",
                                           passes_constraint=False)

    def test_min_length_edge_fail(self):
        """
        Testing case where a string just fails (edge-condition) the MIN_LENGTH restriction.
        """
        self.__test_numerical_constraint_h(self.STR, BasicConstraint.MIN_LENGTH, 8, "bye all",
                                           passes_constraint=False)

    ####
    def test_max_length_pass(self):
        """
        Testing case where a string passes the MAX_LENGTH restriction.
        """
        self.__test_numerical_constraint_h(self.STR, BasicConstraint.MAX_LENGTH, 2, "Hi",
                                           passes_constraint=True)

    def test_max_length_edge_pass(self):
        """
        Testing case where a string just passes (edge-condition) the MAX_LENGTH restriction.
        """
        self.__test_numerical_constraint_h(self.STR, BasicConstraint.MAX_LENGTH, 27, "onetwothreefourfive and six",
                                           passes_constraint=True)

    def test_max_length_fail(self):
        """
        Testing case where a string fails the MAX_LENGTH restriction.
        """
        self.__test_numerical_constraint_h(self.STR, BasicConstraint.MAX_LENGTH, 10, "Hello everyone",
                                           passes_constraint=False)

    def test_max_length_edge_fail(self):
        """
        Testing case where a string just fails (edge-condition) the MAX_LENGTH restriction.
        """
        self.__test_numerical_constraint_h(self.STR, BasicConstraint.MAX_LENGTH, 10, "Hello world",
                                           passes_constraint=False)

    ####
    def test_min_val_float_pass(self):
        """
        Testing case where a float passes the MIN_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL, 17, "100",
                                           passes_constraint=True)

    def test_min_val_float_edge_pass(self):
        """
        Testing case where a float just passes (edge-condition) the MIN_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL, -1722.4, "-1722.4",
                                           passes_constraint=True)

    def test_min_val_float_fail(self):
        """
        Testing case where a float fails the MIN_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL, 17, "14",
                                           passes_constraint=False)

    # Note that there isn't an "edge fail" case here.

    ####
    def test_max_val_float_pass(self):
        """
        Testing case where a float passes the MAX_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MAX_VAL, -100090, "-111117.445",
                                           passes_constraint=True)

    def test_max_val_float_edge_pass(self):
        """
        Testing case where a float just passes (edge-condition) the MIN_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MAX_VAL, 42.77, "42.77",
                                           passes_constraint=True)

    def test_max_val_float_fail(self):
        """
        Testing case where a float fails the MAX_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MAX_VAL, -17, "-1",
                                           passes_constraint=False)
    # As above there is no "edge fail" here.

    ####
    def test_min_val_int_pass(self):
        """
        Testing case where an integer passes the MIN_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.INT, BasicConstraint.MIN_VAL, -4, "6",
                                           passes_constraint=True)

    def test_min_val_int_edge_pass(self):
        """
        Testing case where an integer just passes (edge-condition) the MIN_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.INT, BasicConstraint.MIN_VAL, 165, "165",
                                           passes_constraint=True)

    def test_min_val_int_fail(self):
        """
        Testing case where an integer fails the MIN_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL, 3, "-2",
                                           passes_constraint=False)

    def test_min_val_int_edge_fail(self):
        """
        Testing case where an integer just fails (edge-condition) the MIN_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL, 7, "6",
                                           passes_constraint=False)

    ####
    def test_max_val_int_pass(self):
        """
        Testing case where an integer passes the MAX_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.INT, BasicConstraint.MAX_VAL, 85, "3",
                                           passes_constraint=True)

    def test_max_val_int_edge_pass(self):
        """
        Testing case where an integer just passes (edge-condition) the MAX_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.INT, BasicConstraint.MAX_VAL, -92, "-92",
                                           passes_constraint=True)

    def test_max_val_int_fail(self):
        """
        Testing case where an integer fails the MAX_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MAX_VAL, 3, "44",
                                           passes_constraint=False)

    def test_max_val_int_edge_fail(self):
        """
        Testing case where an integer just fails (edge-condition) the MAX_VAL restriction.
        """
        self.__test_numerical_constraint_h(self.FLOAT, BasicConstraint.MAX_VAL, 7, "8",
                                           passes_constraint=False)

    ####
    def __test_regexp_h(self, builtin_type, constr_val, string_to_check,
                        passes_constraint=True):
        """
        Helper to test strings against a REGEXP constraints.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with REGEXP BC", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(builtin_type)

        my_BC = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="{}".format(constr_val))

        if passes_constraint:
            self.assertEquals(my_DT.check_basic_constraints(string_to_check), [])
        else:
            self.assertEquals(my_DT.check_basic_constraints(string_to_check), [my_BC])

    def test_str_regexp_pass(self):
        """
        Test a string against a REGEXP it satisfies.
        """
        self.__test_regexp_h(self.STR, "[a-z]+", "123abc", passes_constraint=True)

    def test_str_regexp_fail(self):
        """
        Test a string against a REGEXP it does not satisfy.
        """
        self.__test_regexp_h(self.STR, "foo|bar", "123abc", passes_constraint=False)

    def test_float_regexp_pass(self):
        """
        Test a float against a REGEXP it satisfies.
        """
        self.__test_regexp_h(self.FLOAT, "[1-9]+\.663", "1325.663", passes_constraint=True)

    def test_float_regexp_fail(self):
        """
        Test a float against a REGEXP it doesn't satisfy.
        """
        self.__test_regexp_h(self.FLOAT, "1065[0-9]+", "132544", passes_constraint=False)

    def test_int_regexp_pass(self):
        """
        Test an int against a REGEXP it satisfies.
        """
        self.__test_regexp_h(self.INT, ".+", "4444", passes_constraint=True)

    def test_int_regexp_fail(self):
        """
        Test an int against a REGEXP it doesn't satisfy.
        """
        self.__test_regexp_h(self.INT, "[1-9]{4}", "-1000", passes_constraint=False)

    def test_bool_regexp_pass(self):
        """
        Test a Boolean against a REGEXP it satisfies.
        """
        self.__test_regexp_h(self.BOOL, "True|TRUE|true|t|1", "True", passes_constraint=True)

    def test_bool_regexp_fail(self):
        """
        Test a Boolean against a REGEXP it doesn't satisfy.
        """
        self.__test_regexp_h(self.STR, "False", "True", passes_constraint=False)

    ####
    # Some test cases with combined restrictions.
    def test_str_multiple_restrictions_pass(self):
        """
        Test a string against several restrictions.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with several restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.STR)

        my_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="4")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="7")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="foo...")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="...bar")

        self.assertEquals(my_DT.check_basic_constraints("foobar"), [])

    def test_str_multiple_restrictions_fail(self):
        """
        Test a string against several restrictions, some of which fail.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with several restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.STR)

        my_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="4")
        my_max_length = my_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="5")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="foo...")
        my_regexp_2 = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="...baz")

        constr_fail = my_DT.check_basic_constraints("foobar")
        self.assertEquals(len(constr_fail), 2)
        self.assertEquals(my_max_length in constr_fail, True)
        self.assertEquals(my_regexp_2 in constr_fail, True)

    def test_float_multiple_restrictions_pass(self):
        """
        Test a float against several restrictions, all of which pass.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with several restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.FLOAT)

        my_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="1999")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="^....$")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="..14")

        self.assertEquals(my_DT.check_basic_constraints("2014"), [])

    def test_float_multiple_restrictions_fail(self):
        """
        Test a float against several restrictions, some of which fail.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with several restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.FLOAT)

        my_min_val = my_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="1999")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="^....$")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="..14")

        self.assertEquals(my_DT.check_basic_constraints("2014"), [my_min_val])

    def test_int_multiple_restrictions_pass(self):
        """
        Test an int against several restrictions, all of which pass.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with several restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.INT)

        my_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="2099")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="^....$")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="..35")

        self.assertEquals(my_DT.check_basic_constraints("2035"), [])

    def test_int_multiple_restrictions_fail(self):
        """
        Test an int against several restrictions, some of which fail.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with several restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.INT)

        my_min_val = my_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="2099")
        my_regexp = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="^....$")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="35")

        constr_fail = my_DT.check_basic_constraints("935")
        self.assertEquals(len(constr_fail), 2)
        self.assertEquals(my_regexp in constr_fail, True)
        self.assertEquals(my_min_val in constr_fail, True)

    def test_bool_multiple_restrictions_pass(self):
        """
        Test a Boolean against several restrictions, all of which pass.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with several restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.BOOL)

        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="T...")
        my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="rue|RUE")

        self.assertEquals(my_DT.check_basic_constraints("True"), [])

    def test_bool_multiple_restrictions_fail(self):
        """
        Test a Boolean against several restrictions, some of which fail.
        """
        my_DT = Datatype(name="MyDT", description="Datatype with several restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.BOOL)

        my_regexp = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="T...")
        my_regexp_2 = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="rue|RUE")

        constr_fail = my_DT.check_basic_constraints("False")
        self.assertEquals(len(constr_fail), 2)
        self.assertEquals(my_regexp in constr_fail, True)
        self.assertEquals(my_regexp_2 in constr_fail, True)

    ####
    # A couple of test cases for inherited constraints.

    # FIXME we need to think on this further!

    def test_str_inherit_restrictions(self):
        """
        Testing a string against some inherited restrictions.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)
        my_regexp = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="Hello t....")

        my_DT = Datatype(name="MyDT", description="Datatype inheriting a restriction", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(super_DT)
        my_max_length = my_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="12")

        self.assertEquals(my_DT.check_basic_constraints("Hello there"), [])
        self.assertEquals(my_DT.check_basic_constraints("Hello theremin"), [my_max_length])
        self.assertEquals(my_DT.check_basic_constraints("Hello"), [my_regexp])

        constr_fail = my_DT.check_basic_constraints("Goodbye everyone")
        self.assertEquals(len(constr_fail), 2)
        self.assertEquals(my_regexp in constr_fail, True)
        self.assertEquals(my_max_length in constr_fail, True)

    def test_float_inherit_restrictions(self):
        """
        Testing a float against some inherited restrictions.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)
        super_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="2")

        my_DT = Datatype(name="MyDT", description="Datatype inheriting a restriction", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(super_DT)
        my_DT.restricts.add(self.FLOAT)
        my_max_val = my_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="95")

        self.assertEquals(my_DT.check_basic_constraints("82"), [])
        self.assertEquals(my_DT.check_basic_constraints(".7"), [])
        self.assertEquals(my_DT.check_basic_constraints("99"), [my_max_val])

        # Note that since my_DT is no longer a STR, only my_max_val applies.
        self.assertEquals(my_DT.check_basic_constraints("114"), [my_max_val])

    def test_int_inherit_restrictions(self):
        """
        Testing an integer against some inherited restrictions.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)
        my_regexp = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="1000...")

        super2_DT = Datatype(name="SuperDT2", description="Supertype 2", user=self.myUser)
        super2_DT.full_clean()
        super2_DT.save()
        super2_DT.restricts.add(self.INT)
        my_min_val = super2_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="1000100")

        my_DT = Datatype(name="MyDT", description="Datatype inheriting restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(super_DT)
        my_DT.restricts.add(super2_DT)

        self.assertEquals(my_DT.check_basic_constraints("1000107"), [])
        self.assertEquals(my_DT.check_basic_constraints("1000004"), [my_min_val])
        self.assertEquals(my_DT.check_basic_constraints("1099999"), [my_regexp])

        constr_fail = my_DT.check_basic_constraints("99999")
        self.assertEquals(len(constr_fail), 2)
        self.assertEquals(my_regexp in constr_fail, True)
        self.assertEquals(my_min_val in constr_fail, True)

    def test_int_inherit_overridden_restriction(self):
        """
        Testing an integer against an overridden inherited restriction.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.INT)
        super_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="999")

        my_DT = Datatype(name="MyDT", description="Datatype inheriting restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(super_DT)
        my_max_val = my_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="899")

        self.assertEquals(my_DT.check_basic_constraints("0"), [])
        self.assertEquals(my_DT.check_basic_constraints("950"), [my_max_val])
        # super_max_val is overridden so only my_max_val should fail.
        self.assertEquals(my_DT.check_basic_constraints("1055"), [my_max_val])

    def test_bool_inherit_restrictions(self):
        """
        Testing a Boolean against some inherited restrictions.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype", user=self.myUser)
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.BOOL)
        my_regexp = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule="T.+")
        my_regexp2 = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP, rule=".rue")

        my_DT = Datatype(name="MyDT", description="Datatype inheriting restrictions", user=self.myUser)
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(super_DT)
        my_DT.restricts.add(self.BOOL)

        self.assertEquals(my_DT.check_basic_constraints("True"), [])
        self.assertEquals(my_DT.check_basic_constraints("true"), [my_regexp])
        self.assertEquals(my_DT.check_basic_constraints("TRUE"), [my_regexp2])

        self.assertEquals(set(my_DT.check_basic_constraints("False")), set([my_regexp, my_regexp2]))


class CompoundDatatypeMemberTests(MetadataTestCase):
    def test_cdtMember_unicode(self):
        """
        Unicode of compoundDatatypeMember should return
        (column index, datatype name, column name)
        """
        self.assertEqual(
            unicode(self.test_cdt.members.get(column_idx=1)),
            "label: string"
        )
        self.assertEqual(
            unicode(self.test_cdt.members.get(column_idx=2)),
            "PBMCseq: DNANucSeq"
        )
        self.assertEqual(
            unicode(self.test_cdt.members.get(column_idx=3)),
            "PLAseq: RNANucSeq"
        )


class CompoundDatatypeTests(MetadataTestCase):

    def test_cdt_zero_member_unicode(self):
        """
        Unicode of empty CompoundDatatype should be empty.
        """
        empty_cdt = CompoundDatatype(user=self.myUser)
        empty_cdt.save()
        self.assertEqual(unicode(empty_cdt), "[empty CompoundDatatype]")

    def test_cdt_single_member_unicode(self):
        """
        Unicode on single-member cdt returns its member.
        """
        self.assertEqual(unicode(self.DNAinput_cdt),
                         "(SeqToComplement: DNANucSeq)")

    def test_cdt_multiple_members_unicode(self):
        """
        Unicode returns a list of its Datatype members.

        Each member is in the form of unicode(CompoundDatatypeMember).
        """
        self.assertEqual(
            unicode(self.test_cdt),
            "(label: string, PBMCseq: DNANucSeq, PLAseq: RNANucSeq)")

    def test_cdt_four_members_short_name(self):
        self.basic_cdt.members.get(column_idx=5).delete()
        self.assertEqual(
            self.basic_cdt.short_name,
            "(label: string, integer: integer, float: float, bool: boolean)")

    def test_cdt_five_members_short_name(self):
        self.assertEqual(
            self.basic_cdt.short_name,
            "(label: string, integer: integer, float: float, plus 2 others)")

    def test_clean_single_index_good(self):
        """
        CompoundDatatype with single index equalling 1.
        """
        sad_cdt = CompoundDatatype(user=self.myUser)
        sad_cdt.save()
        sad_cdt.members.create(datatype=self.RNA_dt,
                               column_name="ColumnTwo",
                               column_idx=1)
        self.assertEqual(sad_cdt.clean(), None)

    def test_clean_single_index_bad(self):
        """
        CompoundDatatype with single index not equalling 1.
        """
        sad_cdt = CompoundDatatype(user=self.myUser)
        sad_cdt.save()
        sad_cdt.members.create(datatype=self.RNA_dt,
                               column_name="ColumnTwo",
                               column_idx=3)

        self.assertRaisesRegexp(ValidationError,
            re.escape(('Column indices of CompoundDatatype "{}" are not consecutive starting from 1'.format(sad_cdt))),
            sad_cdt.clean)

    def test_clean_consecutive_member_indices_correct(self):
        """
        A CompoundDatatype with consecutive member indices passes clean.
        """
        self.assertEqual(self.test_cdt.clean(), None)

        good_cdt = CompoundDatatype(user=self.myUser)
        good_cdt.save()
        good_cdt.members.create(datatype=self.RNA_dt, column_name="ColumnTwo", column_idx=2)
        good_cdt.members.create(datatype=self.DNA_dt, column_name="ColumnOne", column_idx=1)
        self.assertEqual(good_cdt.clean(), None)

    def test_clean_catches_consecutive_member_indices(self):
        """
        A CompoundDatatype without consecutive member indices throws a ValidationError.
        """
        bad_cdt = CompoundDatatype(user=self.myUser)
        bad_cdt.save()
        bad_cdt.members.create(datatype=self.RNA_dt, column_name="ColumnOne", column_idx=3)
        bad_cdt.members.create(datatype=self.DNA_dt, column_name="ColumnTwo", column_idx=1)

        self.assertRaisesRegexp(ValidationError,
            re.escape(('Column indices of CompoundDatatype "{}" are not consecutive starting from 1'.format(bad_cdt))),
            bad_cdt.clean)

    def test_clean_members_no_column_names(self):
        """
        Datatype members must have column names.
        """
        cdt = CompoundDatatype(user=self.myUser)
        cdt.save()
        cdt.members.create(datatype=self.RNA_dt, column_idx=1)
        self.assertRaisesRegexp(ValidationError,
                                "{'column_name': \[u'This field cannot be blank.'\]}",
                                cdt.clean)

    def test_create_SD_raw(self):
        """
        Creating a raw SD should pass clean
        """
        path = os.path.join(samplecode_path, "doublet_cdt.csv")
        raw_SD = SymbolicDataset.create_SD(path, user=self.myUser, make_dataset=True, name="something",
                                           description="desc")
        self.assertEqual(raw_SD.clean(), None)
        self.assertEqual(raw_SD.dataset.clean(), None)

    def test_create_SD_valid(self):
        """
        Creating an SD with a CDT, where the file conforms, should be OK.
        """
        path = os.path.join(samplecode_path, "doublet_cdt.csv")
        doublet_SD = SymbolicDataset.create_SD(path, user=self.myUser, cdt=self.doublet_cdt,
                                               make_dataset=True, name="something", description="desc")
        self.assertEqual(doublet_SD.clean(), None)
        self.assertEqual(doublet_SD.structure.clean(), None)
        self.assertEqual(doublet_SD.dataset.clean(), None)

    def test_create_SD_bad_num_cols(self):
        # Define a dataset, but with the wrong number of headers
        path = os.path.join(samplecode_path, "step_0_triplet_3_rows.csv")
        self.assertRaisesRegexp(ValueError,
                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                          .format(path, self.doublet_cdt)),
                lambda: SymbolicDataset.create_SD(path, user=self.myUser,
                                                  cdt=self.doublet_cdt, name="DS1", description="DS1 desc"))

    def test_create_SD_bad_col_names(self):
        # Define a dataset with the right number of header columns, but the wrong column names
        path = os.path.join(samplecode_path, "three_random_columns.csv")
        self.assertRaisesRegexp(ValueError,
                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                          .format(path, self.triplet_cdt)),
                lambda: SymbolicDataset.create_SD(path, user=self.myUser,
                                                  cdt=self.triplet_cdt, name="DS1", description="DS1 desc"))

    def test_type_constraints_row(self):

        # The cdt schema is (string, int, float, bool, rna)
        t1 = self.basic_cdt.check_constraints(['Once', 'upon', 'a', 'time', 'there'])
        t2 = self.basic_cdt.check_constraints(['was', '1', 'young', 'lazy', 'dev'])
        t3 = self.basic_cdt.check_constraints(['that', 'needed', '2', 'test', 'his'])
        t4 = self.basic_cdt.check_constraints(['datatype', 'as', 'a', 'True', 'which'])
        t5 = self.basic_cdt.check_constraints(['often', 'made', 'him', 'scream', 'UGGGG'])

        int_fail = u'Was not integer'
        float_fail = u'Was not float'
        bool_fail = u'Was not Boolean'
        rna_fail = u"Failed check 'regexp=^[ACGUacgu]*$'"

        self.assertEqual(t1, [[], [int_fail], [float_fail], [bool_fail], [rna_fail]])
        self.assertEqual(t2, [[], [], [float_fail], [bool_fail], [rna_fail]])
        self.assertEqual(t3, [[], [int_fail], [], [bool_fail], [rna_fail]])
        self.assertEqual(t4, [[], [int_fail], [float_fail], [], [rna_fail]])
        self.assertEqual(t5, [[], [int_fail], [float_fail], [bool_fail], []])


class DatatypeApiTests(TestCase):

    def setUp(self):
        self.factory = APIRequestFactory()
        self.kive_user = kive_user()

        self.list_path = reverse("datatype-list")
        self.detail_pk = 7
        self.detail_path = reverse("datatype-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("datatype-removal-plan",
                                   kwargs={'pk': self.detail_pk})

        # This should equal metadata.ajax.CompoundDatatypeViewSet.as_view({"get": "list"}).
        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def test_auth(self):
        # First try to access while not logged in.
        request = self.factory.get(self.list_path)
        response = self.list_view(request)
        self.assertEquals(response.data["detail"],
                          "Authentication credentials were not provided.")

        # Now log in and check that "detail" is not passed in the response.
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)
        self.assertNotIn('detail', response.data)

    def test_list(self):
        """
        Test the CompoundDatatype API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        # There are four CDTs loaded into the Database by default.
        self.assertEquals(len(response.data), 7)
        self.assertEquals(response.data[0]['id'], 1)
        self.assertEquals(response.data[2]['name'], 'float')

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['name'], 'nucleotide sequence')

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['Datatypes'], 1)

    def test_removal(self):
        start_count = Datatype.objects.all().count()
        
        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Datatype.objects.all().count()
        self.assertEquals(end_count, start_count - 1)


class CompoundDatatypeApiTests(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.kive_user = kive_user()

        self.list_path = reverse("compounddatatype-list")
        self.detail_pk = 3
        self.detail_path = reverse("compounddatatype-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("compounddatatype-removal-plan",
                                   kwargs={'pk': self.detail_pk})

        # This should equal metadata.ajax.CompoundDatatypeViewSet.as_view({"get": "list"}).
        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def test_auth(self):
        # First try to access while not logged in.
        request = self.factory.get(self.list_path)
        response = self.list_view(request)
        self.assertEquals(response.data["detail"],
                          "Authentication credentials were not provided.")

        # Now log in and check that "detail" is not passed in the response.
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)
        self.assertNotIn('detail', response.data)

    def test_list(self):
        """
        Test the CompoundDatatype API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        # There are four CDTs loaded into the Database by default.
        self.assertEquals(len(response.data), 4)
        self.assertEquals(response.data[0]['id'], 1)
        self.assertEquals(response.data[2]['representation'],
                          '(example: string?, valid: boolean)')

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['representation'],
                          '(example: string?, valid: boolean)')

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['CompoundDatatypes'], 1)

    def test_removal(self):
        start_count = CompoundDatatype.objects.all().count()
        
        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = CompoundDatatype.objects.all().count()
        self.assertEquals(end_count, start_count - 1)
