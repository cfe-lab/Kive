"""
Unit tests for Shipyard metadata models.
"""
from django.test import TestCase
from django.core.exceptions import ValidationError

from metadata.models import *
from method.models import CodeResourceRevision


class MetadataTestSetup(TestCase):
    """
    Set up a database state for unit testing.
    
    Other test classes that require this state can extend this one.
    """

    def setUp(self):
        """Setup default database state from which to perform unit testing."""
        # Create Datatype "string".
        self.string_dt = Datatype(
            name="string",
            description="String (basically anything)",
            Python_type=Datatype.STR)
        self.string_dt.save()

        # Create Datatype "DNANucSeq" with a regexp basic constraint.
        self.DNA_dt = Datatype(
            name="DNANucSeq",
            description="String consisting of ACGTacgt",
            Python_type=Datatype.STR)
        self.DNA_dt.save()
        # DNA_dt is a restricted type of string
        self.DNA_dt.restricts.add(self.string_dt);
        self.DNA_dt.basic_constraints.create(
            ruletype=BasicConstraint.REGEXP,
            rule="^[ACGTacgt]*$")
        self.DNA_dt.save()

        # Similarly, create Datatype "RNANucSeq".
        self.RNA_dt = Datatype(
            name="RNANucSeq",
            description="String consisting of ACGUacgu",
            Python_type=Datatype.STR)
        self.RNA_dt.save()
        # RNA_dt is a restricted type of string
        self.RNA_dt.restricts.add(self.string_dt)
        self.RNA_dt.basic_constraints.create(
            ruletype=BasicConstraint.REGEXP,
            rule="^[ACGUacgu]*$")
        self.RNA_dt.save()

        # Define test_cdt as containing 3 members:
        # (label, PBMCseq, PLAseq) as (string,DNA,RNA)
        self.test_cdt = CompoundDatatype()
        self.test_cdt.save()

        self.test_cdt.members.create(
            datatype=self.string_dt,
            column_name="label",
            column_idx=1)
        self.test_cdt.members.create(
            datatype=self.DNA_dt,
            column_name="PBMCseq",
            column_idx=2)
        self.test_cdt.members.create(
            datatype=self.RNA_dt,
            column_name="PLAseq",
            column_idx=3)
        self.test_cdt.full_clean()
        self.test_cdt.save()

        # Define DNAinput_cdt (1 member)
        self.DNAinput_cdt = CompoundDatatype();
        self.DNAinput_cdt.save();
        self.DNAinput_cdt.members.create(
            datatype=self.DNA_dt,
            column_name="SeqToComplement",
            column_idx=1);
        self.DNAinput_cdt.full_clean();
        self.DNAinput_cdt.save();

        # Define DNAoutput_cdt (1 member)
        self.DNAoutput_cdt = CompoundDatatype();
        self.DNAoutput_cdt.save();
        self.DNAoutput_cdt.members.create(
            datatype=self.DNA_dt,
            column_name="ComplementedSeq",
            column_idx=1);
        self.DNAoutput_cdt.full_clean();
        self.DNAoutput_cdt.save();

        # Define RNAinput_cdt (1 column)
        self.RNAinput_cdt = CompoundDatatype();
        self.RNAinput_cdt.save();
        self.RNAinput_cdt.members.create(
            datatype=self.RNA_dt,
            column_name="SeqToComplement",
            column_idx=1);
        self.RNAinput_cdt.full_clean();
        self.RNAinput_cdt.save();

        # Define RNAoutput_cdt (1 column)
        self.RNAoutput_cdt = CompoundDatatype();
        self.RNAoutput_cdt.save();
        self.RNAoutput_cdt.members.create(
            datatype=self.RNA_dt,
            column_name="ComplementedSeq",
            column_idx=1);
        self.RNAoutput_cdt.full_clean();
        self.RNAoutput_cdt.save();

        ####
        # Everything above this point is used in metadata.tests.
        # This next bit is used in method.tests.

        # Define "tuple" CDT containing (x,y): members x and y exist at index 1 and 2
        self.tuple_cdt = CompoundDatatype()
        self.tuple_cdt.save()
        self.tuple_cdt.members.create(datatype=self.string_dt, column_name="x", column_idx=1)
        self.tuple_cdt.members.create(datatype=self.string_dt, column_name="y", column_idx=2)

        # Define "singlet" CDT containing CDT member (a) and "triplet" CDT with members (a,b,c)
        self.singlet_cdt = CompoundDatatype()
        self.singlet_cdt.save()
        self.singlet_cdt.members.create(
            datatype=self.string_dt, column_name="k", column_idx=1)

        self.triplet_cdt = CompoundDatatype()
        self.triplet_cdt.save()
        self.triplet_cdt.members.create(
            datatype=self.string_dt, column_name="a", column_idx=1)
        self.triplet_cdt.members.create(
            datatype=self.string_dt, column_name="b", column_idx=2)
        self.triplet_cdt.members.create(
            datatype=self.string_dt, column_name="c", column_idx=3)

        ####
        # This next bit is used for pipeline.tests.

        # Define CDT "triplet_squares_cdt" with 3 members for use as an input/output
        self.triplet_squares_cdt = CompoundDatatype()
        self.triplet_squares_cdt.save()
        self.triplet_squares_cdt.members.create(
            datatype=self.string_dt, column_name="a^2",
            column_idx=1)
        self.triplet_squares_cdt.members.create(
            datatype=self.string_dt, column_name="b^2",
            column_idx=2)
        self.triplet_squares_cdt.members.create(
            datatype=self.string_dt, column_name="c^2",
            column_idx=3)

        # A CDT with mixed Datatypes
        self.mix_triplet_cdt = CompoundDatatype()
        self.mix_triplet_cdt.save()
        self.mix_triplet_cdt.members.create(
            datatype=self.string_dt, column_name="StrCol1",
            column_idx=1)
        self.mix_triplet_cdt.members.create(
            datatype=self.DNA_dt, column_name="DNACol2",
            column_idx=2)
        self.mix_triplet_cdt.members.create(
            datatype=self.string_dt, column_name="StrCol3",
            column_idx=3)

        # Define CDT "doublet_cdt" with 2 members for use as an input/output
        self.doublet_cdt = CompoundDatatype()
        self.doublet_cdt.save();
        self.doublet_cdt.members.create(
            datatype=self.string_dt, column_name="x",
            column_idx=1)
        self.doublet_cdt.members.create(
            datatype=self.string_dt, column_name="y",
            column_idx=2)

        #### 
        # Stuff from this point on is used in librarian and archive
        # testing.

        # October 15: more CDTs.
        self.DNA_triplet_cdt = CompoundDatatype()
        self.DNA_triplet_cdt.save()
        self.DNA_triplet_cdt.members.create(
            datatype=self.DNA_dt, column_name="a", column_idx=1)
        self.DNA_triplet_cdt.members.create(
            datatype=self.DNA_dt, column_name="b", column_idx=2)
        self.DNA_triplet_cdt.members.create(
            datatype=self.DNA_dt, column_name="c", column_idx=3)

        self.DNA_doublet_cdt = CompoundDatatype()
        self.DNA_doublet_cdt.save()
        self.DNA_doublet_cdt.members.create(
            datatype=self.DNA_dt, column_name="x", column_idx=1)
        self.DNA_doublet_cdt.members.create(
            datatype=self.DNA_dt, column_name="y", column_idx=2)


    def tearDown(self):
        """Delete any files that have been put into the database."""
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


class DatatypeTests(MetadataTestSetup):
    def setUp(self):
        """Add some DTs used to check circular restrictions."""
        super(DatatypeTests, self).setUp()

        # Datatypes used to test circular restrictions.
        self.dt_1 = Datatype(
            name="dt_1",
            description="A string (1)",
            Python_type=Datatype.STR)
        self.dt_1.save()

        self.dt_2 = Datatype(
            name="dt_2",
            description="A string (2)",
            Python_type=Datatype.STR)
        self.dt_2.save()

        self.dt_3 = Datatype(
            name="dt_3",
            description="A string (3)",
            Python_type=Datatype.STR);
        self.dt_3.save()

        self.dt_4 = Datatype(
            name="dt_4",
            description="A string (4)",
            Python_type=Datatype.STR);
        self.dt_4.save()

        self.dt_5 = Datatype(
            name="dt_5",
            description="A string (5)",
            Python_type=Datatype.STR);
        self.dt_5.save()


    def test_datatype_unicode(self):
        """
        Unicode representation must be the instance's name.

        """
        my_datatype = Datatype(name="fhqwhgads");
        self.assertEqual(unicode(my_datatype), "fhqwhgads");

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
        self.dt_1.restricts.add(self.dt_1);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);

    def test_datatype_circular_direct_middle_clean_bad(self):
        """
        Circular, direct, middle
        dt1 restricts dt3, dt1, dt4
        """
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_1);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);

    def test_datatype_circular_direct_end_clean_bad(self):
        """
        Circular, direct, middle
        dt1 restricts dt3, dt4, dt1
        """
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.restricts.add(self.dt_1);
        self.dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);

    def test_datatype_circular_direct_clean_good(self):
        """
        dt1 restricts dt2, dt3, dt4
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_begin_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();

        self.dt_2.restricts.add(self.dt_1);
        self.dt_2.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);

    def test_datatype_circular_recursive_middle_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();

        self.dt_3.restricts.add(self.dt_1);
        self.dt_3.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);

    def test_datatype_circular_recursive_end_clean_bad(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt1
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_4.restricts.add(self.dt_1);
        self.dt_4.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);


    def test_datatype_circular_recursive_clean_good1(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_5);
        self.dt_2.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good2(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_3.restricts.add(self.dt_5);
        self.dt_3.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good3(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt5
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_4.restricts.add(self.dt_5);
        self.dt_4.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good4(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt2 restricts dt4
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_4);
        self.dt_2.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good5(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt3 restricts dt4
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_3.restricts.add(self.dt_4);
        self.dt_3.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_circular_recursive_clean_good6(self):
        """
        dt1 restricts dt2, dt3, dt4
        dt4 restricts dt2
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_4.restricts.add(self.dt_2);
        self.dt_4.save();
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_direct_is_restricted_by_1(self):
        """
        dt1 restricts dt2
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - TRUE
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();

        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), True);

    def test_datatype_direct_is_restricted_by_2(self):
        """
        dt1 and dt2 exist but do not restrict each other
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - FALSE
        """
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), False);

    def test_datatype_recursive_is_restricted_by_1(self):
        """
        dt1 restricts dt2, dt2 restricts dt3

        dt1.is_restricted_by(dt3) - FALSE
        dt3.is_restricted_by(dt1) - TRUE
        dt1.is_restricted_by(dt2) - FALSE
        dt2.is_restricted_by(dt1) - TRUE
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_3);
        self.dt_2.save();

        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False);
        self.assertEqual(self.dt_3.is_restricted_by(self.dt_1), True);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_2.is_restricted_by(self.dt_1), True);

    def test_datatype_recursive_is_restricted_by_2(self):
        """
        dt1 restricts dt[2,3,4]
        dt2 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_5);
        self.dt_2.save();
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False);
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True);

    def test_datatype_recursive_is_restricted_by_3(self):
        """
        dt1 restricts dt[2,3,4]
        dt3 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_3.restricts.add(self.dt_5);
        self.dt_3.save();
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False);
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True);

    def test_datatype_recursive_is_restricted_by_4(self):
        """
        dt1 restricts dt[2,3,4]
        dt4 restricts dt5
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.restricts.add(self.dt_4);
        self.dt_1.save();
        self.dt_4.restricts.add(self.dt_5);
        self.dt_4.save();
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_2), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_3), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_4), False);
        self.assertEqual(self.dt_1.is_restricted_by(self.dt_5), False);
        self.assertEqual(self.dt_5.is_restricted_by(self.dt_1), True);

    def test_datatype_no_restriction_clean_good(self):
        """
        Datatype without any restrictions.
        """
        self.assertEqual(self.dt_1.clean(), None);

    def test_datatype_nested_valid_restrictions_clean_good(self):
        """
        Datatypes such that A restricts B, and B restricts C
        """
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_3);
        self.dt_2.save();
        self.assertEqual(self.dt_1.clean(), None);
        self.assertEqual(self.dt_2.clean(), None);
        self.assertEqual(self.dt_3.clean(), None);

    def test_datatype_nested_invalid_restrictions_scrambled_clean_bad(self):
        """
        Datatypes are restricted to constrain execution order such that:
        A restricts C
        A restricts B
        B restricts C
        C restricts A
        """

        self.dt_1.restricts.add(self.dt_3);
        self.dt_1.save();
        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_3);
        self.dt_2.save();
        self.dt_3.restricts.add(self.dt_1);
        self.dt_3.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);


    def test_datatype_direct_circular_restriction_clean_bad(self):
        """
        Datatype directly restricts itself: A restricts A
        """

        self.dt_1.restricts.add(self.dt_1);
        self.dt_1.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);

    def test_datatype_circular_restriction_indirect_clean(self):
        """
        Datatype restricts itself through intermediary:
        A restricts B
        B restricts A
        """

        self.dt_1.restricts.add(self.dt_2);
        self.dt_1.save();
        self.dt_2.restricts.add(self.dt_1);
        self.dt_2.save();

        self.assertRaisesRegexp(ValidationError,
                                "Datatype \"dt_1\" has a circular restriction",
                                self.dt_1.clean);


class CompoundDatatypeMemberTests(MetadataTestSetup):
    def test_cdtMember_unicode(self):
        """
        Unicode of compoundDatatypeMember should return
        (column index, datatype name, column name)
        """
        self.assertEqual(unicode(self.test_cdt.members.all()[0]),
                         "1: <string> [label]");
        self.assertEqual(unicode(self.test_cdt.members.all()[1]),
                         "2: <DNANucSeq> [PBMCseq]");
        self.assertEqual(unicode(self.test_cdt.members.all()[2]),
                         "3: <RNANucSeq> [PLAseq]");


class CompoundDatatypeTests(MetadataTestSetup):
    def test_cdt_zero_member_unicode(self):
        """
        Unicode of empty CompoundDatatype should be empty.
        """
        empty_cdt = CompoundDatatype();
        empty_cdt.save();
        self.assertEqual(unicode(empty_cdt), "[empty CompoundDatatype]");

    def test_cdt_single_member_unicode(self):
        """
        Unicode on single-member cdt returns its member.
        """
        self.assertEqual(unicode(self.DNAinput_cdt),
                         "(1: <DNANucSeq> [SeqToComplement])");

    def test_cdt_multiple_members_unicode(self):
        """
        Unicode returns a list of it's Datatype members.

        Each member is in the form of unicode(CompoundDatatypeMember).
        """
        self.assertEqual(unicode(self.test_cdt),
                         "(1: <string> [label], 2: <DNANucSeq> [PBMCseq], " +
                         "3: <RNANucSeq> [PLAseq])");

    def test_clean_single_index_good(self):
        """
        CompoundDatatype with single index equalling 1.
        """
        sad_cdt = CompoundDatatype();
        sad_cdt.save();
        sad_cdt.members.create(datatype=self.RNA_dt,
                               column_name="ColumnTwo",
                               column_idx=1);
        self.assertEqual(sad_cdt.clean(), None);

    def test_clean_single_index_bad(self):
        """
        CompoundDatatype with single index not equalling 1.
        """
        sad_cdt = CompoundDatatype();
        sad_cdt.save();
        sad_cdt.members.create(datatype=self.RNA_dt,
                               column_name="ColumnTwo",
                               column_idx=3);

        self.assertRaisesRegexp(
            ValidationError,
            "Column indices are not consecutive starting from 1",
            sad_cdt.clean);

    def test_clean_catches_consecutive_member_indices(self):
        """
        CompoundDatatype must have consecutive indices from 1 to n.
        
        Otherwise, throw a ValidationError.
        """
        self.assertEqual(self.test_cdt.clean(), None);

        good_cdt = CompoundDatatype();
        good_cdt.save();
        good_cdt.members.create(datatype=self.RNA_dt,
                                column_name="ColumnTwo",
                                column_idx=2);
        good_cdt.members.create(datatype=self.DNA_dt,
                                column_name="ColumnOne",
                                column_idx=1);
        self.assertEqual(good_cdt.clean(), None);

        bad_cdt = CompoundDatatype();
        bad_cdt.save();
        bad_cdt.members.create(datatype=self.RNA_dt,
                               column_name="ColumnOne",
                               column_idx=3);

        bad_cdt.members.create(datatype=self.DNA_dt,
                               column_name="ColumnTwo",
                               column_idx=1);

        self.assertRaisesRegexp(
            ValidationError,
            "Column indices are not consecutive starting from 1",
            bad_cdt.clean);

    def test_clean_members_no_column_names(self):
        """
        Datatype members must have column names.
        """
        cdt = CompoundDatatype();
        cdt.save()
        cdt.members.create(datatype=self.RNA_dt, column_idx=1)
        self.assertRaisesRegexp(ValidationError,
                                "{'column_name': \[u'This field cannot be blank.'\]}",
                                cdt.clean)

        # The following tests were previously tests on
        # DatasetStructure.clean(), but now they must be adapted as
        # tests on summarize_CSV.

        # def test_clean_must_be_coherent_with_structure_if_applicable(self):
        #     # Valid dataset - raw (No structure defined)
        #     self.doublet_symDS = SymbolicDataset()
        #     self.doublet_symDS.save()
        #     self.doublet_DS = None
        #     with open(os.path.join(samplecode_path, "doublet_cdt.csv"), "rb") as f:
        #         self.doublet_DS = Dataset(user=self.myUser,name="doublet",description="lol",dataset_file=File(f),symbolicdataset=self.doublet_symDS)
        #         self.doublet_DS.save()
        #     self.assertEqual(self.doublet_DS.clean(), None)

        #     # Valid dataset - doublet
        #     self.doublet_DS_structure_valid = DatasetStructure(dataset=self.doublet_DS,compounddatatype=self.doublet_cdt)
        #     self.doublet_DS_structure_valid.save()
        #     self.assertEqual(self.doublet_DS.clean(), None)
        #     self.assertEqual(self.doublet_DS_structure_valid.clean(), None)
        #     self.doublet_DS_structure_valid.delete()

        #     # Invalid: Wrong number of columns
        #     self.doublet_DS_structure = DatasetStructure(dataset=self.doublet_DS,compounddatatype=self.triplet_cdt)
        #     self.doublet_DS_structure.save()
        #     errorMessage = "Dataset \".*\" does not have the same number of columns as its CDT"
        #     self.assertRaisesRegexp(ValidationError,errorMessage, self.doublet_DS.clean)
        #     self.assertRaisesRegexp(ValidationError,errorMessage, self.doublet_DS_structure.clean)

        #     # Invalid: Incorrect column header
        #     self.doublet_wrong_header_symDS = SymbolicDataset()
        #     self.doublet_wrong_header_symDS.save()
        #     self.doublet_wrong_header_DS = None
        #     with open(os.path.join(samplecode_path, "doublet_cdt_incorrect_header.csv"), "rb") as f:
        #         self.doublet_wrong_header_DS = Dataset(user=self.myUser,name="doublet",description="lol",dataset_file=File(f),symbolicdataset=self.doublet_wrong_header_symDS)
        #         self.doublet_wrong_header_DS.save()
        #     self.doublet_wrong_header_DS_structure = DatasetStructure(dataset=self.doublet_wrong_header_DS,compounddatatype=self.doublet_cdt)
        #     errorMessage = "Column .* of Dataset \".*\" is named .*, not .* as specified by its CDT"
        #     self.assertRaisesRegexp(ValidationError,errorMessage, self.doublet_wrong_header_DS.clean)
        #     self.assertRaisesRegexp(ValidationError,errorMessage, self.doublet_wrong_header_DS_structure.clean)

        # def test_clean_check_CSV(self):

        #     # triplet_DS has CSV format conforming to it's CDT
        #     self.triplet_symDS.structure.clean()

        #     # Define a dataset, but with the wrong number of headers
        #     symDS = SymbolicDataset()
        #     symDS.save()
        #     DS1 = None
        #     with open(os.path.join(samplecode_path, "step_0_triplet_3_rows.csv"), "rb") as f:
        #         DS1 = Dataset(user=self.myUser,name="DS1",description="DS1 desc",dataset_file=File(f),symbolicdataset=symDS)
        #         DS1.save()
        #     structure = DatasetStructure(dataset=DS1,compounddatatype=self.doublet_cdt)

        #     errorMessage = "Dataset \".*\" does not have the same number of columns as its CDT"
        #     self.assertRaisesRegexp(ValidationError,errorMessage, structure.clean)

        #     # Define a dataset with the right number of header columns, but the wrong column names
        #     symDS2 = SymbolicDataset()
        #     symDS2.save()
        #     DS2 = None
        #     with open(os.path.join(samplecode_path, "three_random_columns.csv"), "rb") as f:
        #         DS2 = Dataset(user=self.myUser,name="DS2",description="DS2 desc",dataset_file=File(f),symbolicdataset=symDS2)
        #         DS2.save()
        #     structure2 = DatasetStructure(dataset=DS2,compounddatatype=self.triplet_cdt)

        #     errorMessage = "Column 1 of Dataset \".*\" is named .*, not .* as specified by its CDT"
        #     self.assertRaisesRegexp(ValidationError,errorMessage, structure2.clean)



        # def test_dataset_clean_incorrect_number_of_CSV_header_fields_bad(self):

        #     uploaded_sd = SymbolicDataset.create_SD(
        #         os.path.join(samplecode_path, "script_2_output_2.csv"),
        #         self.triplet_cdt,
        #         make_dataset=False)

        #     new_structure = uploaded_sd.structure

        #     # Attach a file with the wrong number of columns.
        #     uploaded_dataset = Dataset(
        #         user=self.myUser,name="uploaded_dataset",
        #         description="hehe",
        #         symbolicdataset=uploaded_sd)
        #     with open(os.path.join(samplecode_path, "script_2_output_2.csv"), "rb") as f:
        #         uploaded_dataset.dataset_file.save("script_2_output_2.csv", File(f))
        #         uploaded_dataset.save()

        #     errorMessage = "Dataset .* does not have the same number of columns as its CDT"
        #     self.assertRaisesRegexp(
        #         ValidationError, errorMessage,
        #         new_structure.clean)

        # def test_dataset_clean_correct_number_of_CSV_header_fields_but_incorrect_contents_bad(self):

        #     uploaded_sd = SymbolicDataset()
        #     uploaded_sd.save()
        #     uploaded_dataset = None
        #     with open(os.path.join(samplecode_path, "three_random_columns.csv"), "rb") as f:
        #         uploaded_dataset = Dataset(
        #             user=self.myUser,name="uploaded_raw_dataset",
        #             description="hehe",dataset_file=File(f),
        #             symbolicdataset=uploaded_sd)
        #         uploaded_dataset.save()
        #     new_structure = DatasetStructure(dataset=uploaded_dataset,
        #                                      compounddatatype=self.triplet_cdt)
        #     new_structure.save()

        #     errorMessage = "Column .* of Dataset .* is named .*, not .* as specified by its CDT"
        #     self.assertRaisesRegexp(ValidationError,errorMessage,uploaded_dataset.clean)

