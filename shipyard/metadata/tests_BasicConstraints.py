"""
Unit tests for Shipyard's BasicConstraint class and functionality relating to it.
"""
from django.test import TestCase
from django.core.exceptions import ValidationError

from metadata.models import *
from method.models import CodeResourceRevision

from constants import datatypes, error_messages

class BasicConstraintTestSetup(TestCase):

    def setUp(self):
        """
        General setup for BasicConstraint testing.
        """
        # The built-in Shipyard atomic Datatypes.
        self.STR = Datatype.objects.get(pk=datatypes.STR_PK)
        self.INT = Datatype.objects.get(pk=datatypes.INT_PK)
        self.FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
        self.BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

class BasicConstraintCleanTests(BasicConstraintTestSetup):

    def __test_clean_numeric_constraint_good_h(self, builtin_type, BC_type, constr_val):
        """
        Helper for testing clean() on a well-defined (MIN|MAX)_(VAL|LENGTH) constraint.
        """
        constr_DT = Datatype(name="ConstrDT", description="Constrained Datatype")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(builtin_type)
        constr = constr_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))

        self.assertEquals(constr.clean(), None)
        # Propagation check
        self.assertEquals(constr_DT.clean(), None)

    def test_clean_min_val_int_good(self):
        """
        Testing clean() on a well-defined MIN_VAL constraint on an integer.
        """
        self.__test_clean_numeric_constraint_good_h(self.INT, BasicConstraint.MIN_VAL, -7.5)

    def test_clean_max_val_int_good(self):
        """
        Testing clean() on a well-defined MAX_VAL constraint on an integer.
        """
        self.__test_clean_numeric_constraint_good_h(self.INT, BasicConstraint.MAX_VAL, -92)

    def test_clean_min_val_float_good(self):
        """
        Testing clean() on a well-defined MIN_VAL constraint on a float.
        """
        self.__test_clean_numeric_constraint_good_h(self.FLOAT, BasicConstraint.MIN_VAL, 987)

    def test_clean_max_val_float_good(self):
        """
        Testing clean() on a well-defined MAX_VAL constraint on a float.
        """
        self.__test_clean_numeric_constraint_good_h(self.FLOAT, BasicConstraint.MAX_VAL, -7.2)

    def test_clean_min_length_good(self):
        """
        Testing clean() on a well-defined MIN_LENGTH constraint on a string.
        """
        self.__test_clean_numeric_constraint_good_h(self.STR, BasicConstraint.MIN_LENGTH, 8)

    def test_clean_max_length_good(self):
        """
        Testing clean() on a well-defined MAX_LENGTH constraint on a string.
        """
        self.__test_clean_numeric_constraint_good_h(self.STR, BasicConstraint.MAX_LENGTH, 8)

    def test_clean_min_length_good_edge(self):
        """
        Testing clean() on a minimal (1) well-defined MIN_LENGTH constraint on a string.

        Note that MIN_LENGTH should not be 0, as that's the default constraint on any string.
        """
        self.__test_clean_numeric_constraint_good_h(self.STR, BasicConstraint.MIN_LENGTH, 1)

    def test_clean_max_length_good_edge(self):
        """
        Testing clean() on a minimal (1) well-defined MAX_LENGTH constraint on a string.
        """
        self.__test_clean_numeric_constraint_good_h(self.STR, BasicConstraint.MAX_LENGTH, 1)

    ########
    def __create_bad_numeric_constraint_h(self, builtin_type, BC_type, constr_val):
        """
        Helper for testing clean() on bad numeric constraints.
        """
        constr_DT = Datatype(name="ConstrDT", description="Constrained Datatype")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(builtin_type)
        constr = constr_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))

        return constr, constr_DT

    def test_clean_min_val_int_bad(self):
        """
        Testing clean() on a badly-defined MIN_VAL constraint (integer).
        """
        constr, constr_DT = self.__create_bad_numeric_constraint_h(
            self.INT, BasicConstraint.MIN_VAL, "foo")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr.clean)

        # Propagation check.
        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr_DT.clean)

    def test_clean_max_val_int_bad(self):
        """
        Testing clean() on a badly-defined MAX_VAL constraint (integer).
        """
        constr, constr_DT = self.__create_bad_numeric_constraint_h(
            self.INT, BasicConstraint.MAX_VAL, "foo")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr.clean)

        # Propagation check.
        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr_DT.clean)

    def test_clean_min_val_float_bad(self):
        """
        Testing clean() on a badly-defined MIN_VAL constraint (float).
        """
        constr, constr_DT = self.__create_bad_numeric_constraint_h(
            self.FLOAT, BasicConstraint.MIN_VAL, "foo")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr.clean)

        # Propagation check.
        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr_DT.clean)

    def test_clean_max_val_float_bad(self):
        """
        Testing clean() on a badly-defined MAX_VAL constraint (float).
        """
        constr, constr_DT = self.__create_bad_numeric_constraint_h(
            self.FLOAT, BasicConstraint.MAX_VAL, "foo")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr.clean)

        # Propagation check.
        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr_DT.clean)

    def test_clean_min_val_str_bad(self):
        """
        Testing clean() on a badly-defined MIN_VAL constraint (string).
        """
        constr, constr_DT = self.__create_bad_numeric_constraint_h(
            self.STR, BasicConstraint.MIN_VAL, "300")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_parent_non_numeric"].format(constr, constr_DT),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_parent_non_numeric"].format(constr, constr_DT),
                                constr_DT.clean)


    def test_clean_max_val_str_bad(self):
        """
        Testing clean() on a badly-defined MAX_VAL constraint (string).
        """
        constr, constr_DT = self.__create_bad_numeric_constraint_h(
            self.STR, BasicConstraint.MAX_VAL, "300")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_parent_non_numeric"].format(constr, constr_DT),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_parent_non_numeric"].format(constr, constr_DT),
                                constr_DT.clean)


    def test_clean_min_val_bool_bad(self):
        """
        Testing clean() on a badly-defined MIN_VAL constraint (Boolean).
        """
        constr, constr_DT = self.__create_bad_numeric_constraint_h(
            self.BOOL, BasicConstraint.MIN_VAL, "300")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_parent_non_numeric"].format(constr, constr_DT),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_parent_non_numeric"].format(constr, constr_DT),
                                constr_DT.clean)

    def test_clean_max_val_bool_bad(self):
        """
        Testing clean() on a badly-defined MAX_VAL constraint (Boolean).
        """
        constr, constr_DT = self.__create_bad_numeric_constraint_h(
            self.BOOL, BasicConstraint.MAX_VAL, "300")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_parent_non_numeric"].format(constr, constr_DT),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_parent_non_numeric"].format(constr, constr_DT),
                                constr_DT.clean)

    ########
    def __test_clean_length_constraint_non_string_h(self, builtin_type, BC_type, constr_val):
        """
        Helper for defining tests on (MIN|MAX)_LENGTH constraints wrongly applied to non-string types.
        """
        constr_DT = Datatype(name="NumericalWithLengthConstraint",
                             description="Incorrectly length-constrained Datatype")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(builtin_type)
        constr = constr_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))

        err_msg_key = "BC_length_constraint_on_non_string"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, constr_DT),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, constr_DT),
                                constr_DT.clean)

    def test_clean_min_length_int_bad(self):
        """
        Testing clean() on a badly-defined MIN_LENGTH constraint (int).
        """
        self.__test_clean_length_constraint_non_string_h(self.INT, BasicConstraint.MIN_LENGTH, 50)

    def test_clean_min_length_float_bad(self):
        """
        Testing clean() on a badly-defined MIN_LENGTH constraint (float).
        """
        self.__test_clean_length_constraint_non_string_h(self.FLOAT, BasicConstraint.MIN_LENGTH, 5)

    def test_clean_min_length_bool_bad(self):
        """
        Testing clean() on a badly-defined MIN_LENGTH constraint (float).
        """
        self.__test_clean_length_constraint_non_string_h(self.BOOL, BasicConstraint.MIN_LENGTH, 12)

    def test_clean_max_length_int_bad(self):
        """
        Testing clean() on a badly-defined MAX_LENGTH constraint (int).
        """
        self.__test_clean_length_constraint_non_string_h(self.INT, BasicConstraint.MAX_LENGTH, 10000)

    def test_clean_max_length_float_bad(self):
        """
        Testing clean() on a badly-defined MAX_LENGTH constraint (float).
        """
        self.__test_clean_length_constraint_non_string_h(self.FLOAT, BasicConstraint.MAX_LENGTH, 1)

    def test_clean_max_length_bool_bad(self):
        """
        Testing clean() on a badly-defined MAX_LENGTH constraint (bool).
        """
        self.__test_clean_length_constraint_non_string_h(self.BOOL, BasicConstraint.MAX_LENGTH, 47)

    ########
    def __test_clean_length_constraint_non_integer_h(self, BC_type, constr_val):
        """
        Helper for defining tests on (MIN|MAX)_LENGTH constraints with non-integer values.
        """
        constr_DT = Datatype(name="NonIntegerLengthConstraint",
                             description="String with poorly-formed length constraint")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.STR)
        constr = constr_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))

        err_msg_key = "BC_length_constraint_non_integer"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, constr_val),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, constr_val),
                                constr_DT.clean)

    def test_clean_float_min_length_bad(self):
        """
        Testing clean() on a badly-defined (float) MIN_LENGTH constraint.
        """
        self.__test_clean_length_constraint_non_integer_h(BasicConstraint.MIN_LENGTH, 4.7)

    def test_clean_str_min_length_bad(self):
        """
        Testing clean() on a badly-defined (str) MIN_LENGTH constraint.
        """
        self.__test_clean_length_constraint_non_integer_h(BasicConstraint.MIN_LENGTH, "foo")

    def test_clean_float_max_length_bad(self):
        """
        Testing clean() on a badly-defined (float) MAX_LENGTH constraint.
        """
        self.__test_clean_length_constraint_non_integer_h(BasicConstraint.MAX_LENGTH, 66.25)

    def test_clean_str_max_length_bad(self):
        """
        Testing clean() on a badly-defined (str) MIN_LENGTH constraint.
        """
        self.__test_clean_length_constraint_non_integer_h(BasicConstraint.MAX_LENGTH, "bar")

    ########
    def __test_clean_length_constraint_too_small_h(self, BC_type, constr_val):
        """
        Helper for defining tests on (MIN|MAX)_LENGTH constraints whose values are too small.
        """
        constr_DT = Datatype(name="TooSmallLengthConstraint",
                             description="String with too-small length constraint")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.STR)
        constr = constr_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))

        err_msg_key = "BC_length_constraint_non_positive"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, constr_val),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, constr_val),
                                constr_DT.clean)

    def test_clean_min_length_non_positive_edge(self):
        """
        Testing clean() on an edge-case negative (0) MIN_LENGTH constraint.
        """
        self.__test_clean_length_constraint_too_small_h(BasicConstraint.MIN_LENGTH, 0)

    def test_clean_min_length_non_positive_regular(self):
        """
        Testing clean() on a non-edge non-positive MIN_LENGTH constraint.
        """
        self.__test_clean_length_constraint_too_small_h(BasicConstraint.MIN_LENGTH, -15)

    def test_clean_max_length_non_positive_edge(self):
        """
        Testing clean() on an edge-case non-positive (0) MAX_LENGTH constraint.
        """
        self.__test_clean_length_constraint_too_small_h(BasicConstraint.MAX_LENGTH, 0)

    def test_clean_max_length_non_positive_regular(self):
        """
        Testing clean() on a non-edge non-positive MAX_LENGTH constraint.
        """
        self.__test_clean_length_constraint_too_small_h(BasicConstraint.MAX_LENGTH, -20)

    ########
    def __test_clean_regexp_good_h(self, builtin_type, pattern):
        """
        Helper to create good REGEXP-constraint test cases.
        """
        regexped_DT = Datatype(name="RegexpedDT",
                               description="Datatype with good REGEXP attached")
        regexped_DT.full_clean()
        regexped_DT.save()
        regexped_DT.restricts.add(builtin_type)
        regexp_constr = regexped_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                             rule="{}".format(pattern))

        self.assertEquals(regexp_constr.clean(), None)
        # Propagation check.
        self.assertEquals(regexped_DT.clean(), None)

    def test_clean_regexp_str_good(self):
        """
        Testing clean() on a string with a good REGEXP attached.
        """
        self.__test_clean_regexp_good_h(self.STR, "foo")

    def test_clean_regexp_float_good(self):
        """
        Testing clean() on a float with a good REGEXP attached.
        """
        self.__test_clean_regexp_good_h(self.STR, "1e.+")

    def test_clean_regexp_int_good(self):
        """
        Testing clean() on an int with a good REGEXP attached.
        """
        # Note that this would be a pretty dumb regexp to put on an integer!
        self.__test_clean_regexp_good_h(self.STR, "bar")

    def test_clean_regexp_bool_good(self):
        """
        Testing clean() on a Boolean with a good REGEXP attached.
        """
        # Note that this would be a pretty dumb regexp to put on an integer!
        self.__test_clean_regexp_good_h(self.STR, "T|F")

    ####
    def __test_clean_regexp_bad_h(self, builtin_type, pattern):
        """
        Helper to create bad REGEXP-constraint test cases.
        """
        regexped_DT = Datatype(name="RegexpedDT",
                               description="Datatype with bad REGEXP attached")
        regexped_DT.full_clean()
        regexped_DT.save()
        regexped_DT.restricts.add(builtin_type)
        regexp_constr = regexped_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                             rule="{}".format(pattern))

        err_msg_key = "BC_bad_RE"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(regexp_constr, re.escape(pattern)),
                                regexp_constr.clean)
        # Propagation check.
        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(regexp_constr, re.escape(pattern)),
                                regexped_DT.clean)

    def test_clean_regexp_str_bad(self):
        """
        Testing clean() on a string with a bad REGEXP attached.
        """
        self.__test_clean_regexp_bad_h(self.STR, "(.+")

    def test_clean_regexp_float_bad(self):
        """
        Testing clean() on a float with a bad REGEXP attached.
        """
        self.__test_clean_regexp_bad_h(self.FLOAT, "[a-z")

    def test_clean_regexp_int_bad(self):
        """
        Testing clean() on an int with a bad REGEXP attached.
        """
        self.__test_clean_regexp_bad_h(self.INT, "1)")

    def test_clean_regexp_bool_bad(self):
        """
        Testing clean() on a Boolean with a bad REGEXP attached.
        """
        self.__test_clean_regexp_bad_h(self.BOOL, "1919)")

    ####
    def __test_clean_dtf_good_h(self, format_string):
        """
        Helper for testing clean() on good DATETIMEFORMATs.
        """
        dtf_DT = Datatype(name="GoodDTF", description="String with a DTF constraint attached")
        dtf_DT.full_clean()
        dtf_DT.save()
        dtf_DT.restricts.add(self.STR)
        dtf = dtf_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT,
                                              rule=format_string)

        self.assertEquals(dtf.clean(), None)
        # Propagation check.
        self.assertEquals(dtf_DT.clean(), None)

    def test_clean_dtf_good(self):
        """
        Testing clean() on a good DATETIMEFORMAT BasicConstraint.
        """
        self.__test_clean_dtf_good_h("%Y %b %d")

    def test_clean_dtf_good_2(self):
        """
        Testing clean() on a second good DATETIMEFORMAT BasicConstraint.
        """
        self.__test_clean_dtf_good_h("%A, %Y-%m-%d %H:%M:%S %z")

    def test_clean_dtf_good_3(self):
        """
        Testing clean() on a third good DATETIMEFORMAT BasicConstraint.
        """
        self.__test_clean_dtf_good_h("FOOBAR")

    def __test_clean_dtf_bad_h(self, builtin_type, format_string):
        """
        Helper for testing clean() on DATETIMEFORMATs applied to non-strings.
        """
        dtf_DT = Datatype(name="BadDTF", description="Non-string with a DTF constraint attached")
        dtf_DT.full_clean()
        dtf_DT.save()
        dtf_DT.restricts.add(builtin_type)
        dtf = dtf_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT,
                                              rule=format_string)

        err_msg_key = "BC_datetimeformat_non_string"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(dtf, dtf_DT),
                                dtf.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(dtf, dtf_DT),
                                dtf_DT.clean)

    def test_clean_dtf_float_bad(self):
        """
        Testing clean() on a DATETIMEFORMAT applied to a float.
        """
        self.__test_clean_dtf_bad_h(self.FLOAT, "%Y %b %d")

    def test_clean_dtf_int_bad(self):
        """
        Testing clean() on a DATETIMEFORMAT applied to an int.
        """
        self.__test_clean_dtf_bad_h(self.INT, "FOOBAR")

    def test_clean_dtf_bool_bad(self):
        """
        Testing clean() on a DATETIMEFORMAT applied to a Boolean.
        """
        self.__test_clean_dtf_bad_h(self.FLOAT, "2014-%m-%d %H:%M:%S %z")

    ########
    def __test_clean_incomplete_parent_bad_h(self, BC_type, constr_val):
        """
        Helper for clean() on a BasicConstraint attached to an incomplete Datatype.
        """
        incomplete_DT = Datatype(name="IncompleteDT", description="Datatype that does not restrict any builtin")
        incomplete_DT.full_clean()
        incomplete_DT.save()

        constr = incomplete_DT.basic_constraints.create(ruletype=BC_type,
                                                        rule="{}".format(constr_val))

        err_msg_key = "BC_DT_not_complete"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(incomplete_DT, constr),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(incomplete_DT, constr),
                                incomplete_DT.clean)

    def test_clean_incomplete_parent_regexp_bad(self):
        """
        Testing clean() on a REGEXP BasicConstraint attached to an incomplete Datatype.
        """
        self.__test_clean_incomplete_parent_bad_h(BasicConstraint.REGEXP, ".*")

    def test_clean_incomplete_parent_dtf_bad(self):
        """
        Testing clean() on a DATETIMEFORMAT BasicConstraint attached to an incomplete Datatype.
        """
        self.__test_clean_incomplete_parent_bad_h(BasicConstraint.DATETIMEFORMAT, "%Y %b %d")

    def test_clean_incomplete_parent_min_val_bad(self):
        """
        Testing clean() on a MIN_VAL BasicConstraint attached to an incomplete Datatype.
        """
        self.__test_clean_incomplete_parent_bad_h(BasicConstraint.MIN_VAL, 16)

    def test_clean_incomplete_parent_max_val_bad(self):
        """
        Testing clean() on a MAX_VAL BasicConstraint attached to an incomplete Datatype.
        """
        self.__test_clean_incomplete_parent_bad_h(BasicConstraint.MAX_VAL, 333333)

    def test_clean_incomplete_parent_min_length_bad(self):
        """
        Testing clean() on a MIN_LENGTH BasicConstraint attached to an incomplete Datatype.
        """
        self.__test_clean_incomplete_parent_bad_h(BasicConstraint.MIN_LENGTH, 2)

    def test_clean_incomplete_parent_max_length_bad(self):
        """
        Testing clean() on a MAX_LENGTH BasicConstraint attached to an incomplete Datatype.
        """
        self.__test_clean_incomplete_parent_bad_h(BasicConstraint.MAX_LENGTH, 27)

    ########
    # Some "greatest hits" from the above testing cases where the
    # parent Datatype does not directly inherit from a builtin.

    def test_clean_second_gen_min_val_int_good(self):
        """
        Testing clean() on a well-defined MIN_VAL constraint on a second-generation integer.
        """
        parent_DT = Datatype(name="Middleman DT", description="Middleman DT")
        parent_DT.full_clean()
        parent_DT.save()
        parent_DT.restricts.add(self.INT)

        constr_DT = Datatype(name="ConstrDT", description="Constrained Datatype")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(parent_DT)
        constr = constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="{}".format(-7.5))

        self.assertEquals(constr.clean(), None)
        # Propagation check
        self.assertEquals(constr_DT.clean(), None)

    def test_clean_second_gen_max_val_float_bad(self):
        """
        Testing clean() on a badly-defined MAX_VAL constraint (second-gen float).
        """
        parent_DT = Datatype(name="Middleman DT", description="Middleman DT")
        parent_DT.full_clean()
        parent_DT.save()
        parent_DT.restricts.add(self.FLOAT)

        constr_DT = Datatype(name="ConstrDT", description="Constrained Datatype")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(parent_DT)
        constr = constr_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_VAL, rule="foo")

        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr.clean)

        # Propagation check.
        self.assertRaisesRegexp(ValidationError,
                                error_messages["BC_val_constraint_rule_non_numeric"].format(constr, "foo"),
                                constr_DT.clean)

    def test_clean_second_gen_min_length_bool_bad(self):
        """
        Testing clean() on a badly-defined MIN_LENGTH constraint (second-gen Boolean).
        """
        parent_DT = Datatype(name="Middleman DT", description="Middleman DT")
        parent_DT.full_clean()
        parent_DT.save()
        parent_DT.restricts.add(self.BOOL)

        constr_DT = Datatype(name="BooleanWithLengthConstraint",
                             description="Incorrectly length-constrained Datatype")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(parent_DT)
        constr = constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="{}".format(12))

        err_msg_key = "BC_length_constraint_on_non_string"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, constr_DT),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, constr_DT),
                                constr_DT.clean)

    def test_clean_second_gen_str_max_length_bad(self):
        """
        Testing clean() on a badly-defined (str) MIN_LENGTH constraint (second-gen Datatype).
        """
        parent_DT = Datatype(name="Middleman DT", description="Middleman DT")
        parent_DT.full_clean()
        parent_DT.save()
        parent_DT.restricts.add(self.STR)

        constr_DT = Datatype(name="NonIntegerLengthConstraint",
                             description="String with poorly-formed length constraint")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(parent_DT)
        constr = constr_DT.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="bar")

        err_msg_key = "BC_length_constraint_non_integer"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, "bar"),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, "bar"),
                                constr_DT.clean)

    def test_clean_second_gen_min_length_non_positive_edge(self):
        """
        Testing clean() on an edge-case negative (0) MIN_LENGTH constraint (second-gen Datatype).
        """
        parent_DT = Datatype(name="Middleman DT", description="Middleman DT")
        parent_DT.full_clean()
        parent_DT.save()
        parent_DT.restricts.add(self.STR)

        constr_DT = Datatype(name="TooSmallLengthConstraint",
                             description="String with too-small length constraint")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(parent_DT)
        constr = constr_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="{}".format(0))

        err_msg_key = "BC_length_constraint_non_positive"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, 0),
                                constr.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(constr, 0),
                                constr_DT.clean)

    def test_clean_second_gen_regexp_good(self):
        """
        Testing clean() on a second-gen Datatype with good REGEXP attached.
        """
        mother_DT = Datatype(name="Mother", description="Mother")
        mother_DT.full_clean()
        mother_DT.save()
        mother_DT.restricts.add(self.STR)

        father_DT = Datatype(name="Father", description="Father")
        father_DT.full_clean()
        father_DT.save()
        father_DT.restricts.add(self.STR)

        milkman_DT = Datatype(name="Milkman", description="Milkman")
        milkman_DT.full_clean()
        milkman_DT.save()
        milkman_DT.restricts.add(self.FLOAT)

        regexped_DT = Datatype(name="RegexpedDT",
                               description="Datatype with good REGEXP attached")
        regexped_DT.full_clean()
        regexped_DT.save()
        regexped_DT.restricts.add(mother_DT)
        regexped_DT.restricts.add(father_DT)
        regexped_DT.restricts.add(milkman_DT)
        regexp_constr = regexped_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                             rule="foo")

        self.assertEquals(regexp_constr.clean(), None)
        # Propagation check.
        self.assertEquals(regexped_DT.clean(), None)

    def test_clean_second_gen_regexp_bad(self):
        """
        Testing clean() on a second-gen Datatype with a bad REGEXP constraint.
        """
        Danny_DT = Datatype(name="Bob Saget", description="Ostensible father")
        # Danny_DT.full_house()
        Danny_DT.full_clean()
        Danny_DT.save()
        Danny_DT.restricts.add(self.BOOL)

        Joey_DT = Datatype(name="Dave Coulier", description="Popeye imitator")
        Joey_DT.full_clean()
        Joey_DT.save()
        Joey_DT.restricts.add(self.INT)

        Jesse_DT = Datatype(name="John Stamos", description="Mercy-haver")
        Jesse_DT.full_clean()
        Jesse_DT.save()
        Jesse_DT.restricts.add(self.FLOAT)

        # The bad regexp pattern.
        pattern = "(.+"

        regexped_DT = Datatype(name="RegexpedDT",
                               description="Datatype with bad REGEXP attached")
        regexped_DT.full_clean()
        regexped_DT.save()
        regexped_DT.restricts.add(Danny_DT)
        regexped_DT.restricts.add(Joey_DT)
        regexped_DT.restricts.add(Jesse_DT)
        regexp_constr = regexped_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                             rule=pattern)

        err_msg_key = "BC_bad_RE"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(regexp_constr, re.escape(pattern)),
                                regexp_constr.clean)
        # Propagation check.
        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(regexp_constr, re.escape(pattern)),
                                regexped_DT.clean)

    def test_clean_second_gen_dtf_good(self):
        """
        Testing clean() on a good DATETIMEFORMAT (second-gen Datatype).
        """
        parent_DT = Datatype(name="Middleman DT", description="Middleman DT")
        parent_DT.full_clean()
        parent_DT.save()
        parent_DT.restricts.add(self.STR)

        dtf_DT = Datatype(name="GoodDTF", description="String with a DTF constraint attached")
        dtf_DT.full_clean()
        dtf_DT.save()
        dtf_DT.restricts.add(parent_DT)
        dtf = dtf_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT,
                                              rule="%Y %b %d")

        self.assertEquals(dtf.clean(), None)
        # Propagation check.
        self.assertEquals(dtf_DT.clean(), None)

    def test_clean_second_gen_dtf_bad_h(self):
        """
        Testing clean() on a DATETIMEFORMATs applied to a float (second-gen).
        """
        parent_DT = Datatype(name="Middleman DT", description="Middleman DT")
        parent_DT.full_clean()
        parent_DT.save()
        parent_DT.restricts.add(self.FLOAT)

        dtf_DT = Datatype(name="BadDTF", description="Float with a DTF constraint attached")
        dtf_DT.full_clean()
        dtf_DT.save()
        dtf_DT.restricts.add(parent_DT)
        dtf = dtf_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT,
                                              rule="%Y %b %d")

        err_msg_key = "BC_datetimeformat_non_string"

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(dtf, dtf_DT),
                                dtf.clean)

        self.assertRaisesRegexp(ValidationError,
                                error_messages[err_msg_key].format(dtf, dtf_DT),
                                dtf_DT.clean)

class BasicConstraintGetEffectiveNumConstraintTests(BasicConstraintTestSetup):

    def test_get_effective_min_val_builtins(self):
        """
        get_effective_num_constraint, when used to retrieve MIN_VAL restrictions,
        should give (None, -float("Inf")) for all builtins.
        """
        self.assertEquals(self.STR.get_effective_num_constraint(BasicConstraint.MIN_VAL), (None, -float("inf")))
        self.assertEquals(self.INT.get_effective_num_constraint(BasicConstraint.MIN_VAL), (None, -float("inf")))
        self.assertEquals(self.FLOAT.get_effective_num_constraint(BasicConstraint.MIN_VAL), (None, -float("inf")))
        self.assertEquals(self.BOOL.get_effective_num_constraint(BasicConstraint.MIN_VAL), (None, -float("inf")))

    def test_get_effective_max_val_builtins(self):
        """
        get_effective_num_constraint, when used to retrieve MAX_VAL restrictions,
        should give (None, float("Inf")) for all builtins.
        """
        self.assertEquals(self.STR.get_effective_num_constraint(BasicConstraint.MAX_VAL), (None, float("inf")))
        self.assertEquals(self.INT.get_effective_num_constraint(BasicConstraint.MAX_VAL), (None, float("inf")))
        self.assertEquals(self.FLOAT.get_effective_num_constraint(BasicConstraint.MAX_VAL), (None, float("inf")))
        self.assertEquals(self.BOOL.get_effective_num_constraint(BasicConstraint.MAX_VAL), (None, float("inf")))

    def test_get_effective_min_length_builtins(self):
        """
        get_effective_num_constraint, when used to retrieve MIN_LENGTH restrictions,
        should give (None, 0) for all builtins.
        """
        self.assertEquals(self.STR.get_effective_num_constraint(BasicConstraint.MIN_LENGTH), (None, 0))
        self.assertEquals(self.INT.get_effective_num_constraint(BasicConstraint.MIN_LENGTH), (None, 0))
        self.assertEquals(self.FLOAT.get_effective_num_constraint(BasicConstraint.MIN_LENGTH), (None, 0))
        self.assertEquals(self.BOOL.get_effective_num_constraint(BasicConstraint.MIN_LENGTH), (None, 0))

    def test_get_effective_max_length_builtins(self):
        """
        get_effective_num_constraint, when used to retrieve MAX_LENGTH restrictions,
        should give (None, float("Inf")) for all builtins.
        """
        self.assertEquals(self.STR.get_effective_num_constraint(BasicConstraint.MAX_LENGTH), (None, float("inf")))
        self.assertEquals(self.INT.get_effective_num_constraint(BasicConstraint.MAX_LENGTH), (None, float("inf")))
        self.assertEquals(self.FLOAT.get_effective_num_constraint(BasicConstraint.MAX_LENGTH), (None, float("inf")))
        self.assertEquals(self.BOOL.get_effective_num_constraint(BasicConstraint.MAX_LENGTH), (None, float("inf")))


    ########
    def __test_get_effective_num_constraint_no_constraint_h(self, builtin_type, BC_type):
        """
        Helper to test get_effective_num_constraint for several different builtin types
        and constraint types in the no-constraint case
        """
        no_constr_set = Datatype(name="NoConstrSet", description="No constraint set")
        no_constr_set.clean()
        no_constr_set.save()
        no_constr_set.restricts.add(builtin_type)

        restriction_val = None
        if BC_type == BasicConstraint.MIN_VAL:
            restriction_val = -float("inf")
        elif BC_type in (BasicConstraint.MAX_VAL, BasicConstraint.MAX_LENGTH):
            restriction_val = float("inf")
        elif BC_type == BasicConstraint.MIN_LENGTH:
            restriction_val = 0
        else:
            # Pathological case: should never happen.
            print("WTF this shouldn't happen")

        self.assertEquals(no_constr_set.get_effective_num_constraint(BC_type), (None, restriction_val))

    def test_get_effective_min_val_int_no_constraint(self):
        """
        Datatype (integer) with no MIN_VAL set should have -\infty as its effective min val.
        """
        self.__test_get_effective_num_constraint_no_constraint_h(self.INT, BasicConstraint.MIN_VAL)

    def test_get_effective_min_val_float_no_constraint(self):
        """
        Datatype (float) with no MIN_VAL set should have -\infty as its effective min val.
        """
        self.__test_get_effective_num_constraint_no_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL)

    def test_get_effective_max_val_int_no_constraint(self):
        """
        Datatype (integer) with no MAX_VAL set should have \infty as its effective max val.
        """
        self.__test_get_effective_num_constraint_no_constraint_h(self.INT, BasicConstraint.MAX_VAL)

    def test_get_effective_max_val_float_no_constraint(self):
        """
        Datatype (float) with no MAX_VAL set should have \infty as its effective max val.
        """
        self.__test_get_effective_num_constraint_no_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL)

    def test_get_effective_min_length_no_constraint(self):
        """
        Datatype (string) with no MIN_LENGTH set should have 0 as its effective min length.
        """
        self.__test_get_effective_num_constraint_no_constraint_h(self.STR, BasicConstraint.MIN_LENGTH)

    def test_get_effective_max_length_no_constraint(self):
        """
        Datatype (string) with no MAX_LENGTH set should have \infty as its effective max length.
        """
        self.__test_get_effective_num_constraint_no_constraint_h(self.STR, BasicConstraint.MAX_LENGTH)

    ########
    def __test_get_effective_num_constraint_with_constraint_h(self, builtin_type, BC_type, constr_val):
        """
        Helper to check retrieving constraints set directly on a Datatype.
        """
        constr_DT = Datatype(name="Constrained DT", description="Datatype with numerical constraint")
        constr_DT.clean()
        constr_DT.save()
        constr_DT.restricts.add(builtin_type)

        constr = constr_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))
        constr.full_clean()

        self.assertEquals(constr_DT.get_effective_num_constraint(BC_type), (constr, constr_val))

    def test_get_effective_min_val_int_with_constraint(self):
        """
        MIN_VAL constraint set directly on the (integer) Datatype.
        """
        self.__test_get_effective_num_constraint_with_constraint_h(self.INT, BasicConstraint.MIN_VAL, -5)

    def test_get_effective_min_val_float_with_constraint(self):
        """
        MIN_VAL constraint set directly on the (float) Datatype.
        """
        self.__test_get_effective_num_constraint_with_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL, 2.5)

    def test_get_effective_max_val_int_with_constraint(self):
        """
        MAX_VAL constraint set directly on the (integer) Datatype.
        """
        self.__test_get_effective_num_constraint_with_constraint_h(self.INT, BasicConstraint.MAX_VAL, 133.7)

    def test_get_effective_max_val_float_with_constraint(self):
        """
        MAX_VAL constraint set directly on the (float) Datatype.
        """
        self.__test_get_effective_num_constraint_with_constraint_h(self.FLOAT, BasicConstraint.MAX_VAL, -3)

    def test_get_effective_min_length_with_constraint(self):
        """
        MIN_LENGTH constraint set directly on the (string) Datatype.
        """
        self.__test_get_effective_num_constraint_with_constraint_h(self.STR, BasicConstraint.MIN_LENGTH, 4)

    def test_get_effective_max_length_with_constraint(self):
        """
        MAX_LENGTH constraint set directly on the (string) Datatype.
        """
        self.__test_get_effective_num_constraint_with_constraint_h(self.STR, BasicConstraint.MAX_LENGTH, 4)

    ########
    def __test_get_effective_num_constraint_inherits_constraint_h(self, builtin_type, BC_type, constr_val):
        """
        Helper for testing the inheritance of numerical constraints from a single parent.
        """
        constr_parent = Datatype(name="ConstrParent", description="Constrained parent")
        constr_parent.clean()
        constr_parent.save()
        constr_parent.restricts.add(builtin_type)

        constr = constr_parent.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))
        constr.full_clean()

        heir = Datatype(name="Heir", description="Inherits BC from parent")
        heir.clean()
        heir.save()
        heir.restricts.add(constr_parent)
        heir.complete_clean()

        self.assertEquals(heir.get_effective_num_constraint(BC_type), (constr, constr_val))

    def test_get_effective_min_val_int_inherits_constraint(self):
        """
        Datatype (integer) with no MIN_VAL of its own but whose parent has one should inherit it.
        """
        self.__test_get_effective_num_constraint_inherits_constraint_h(self.INT, BasicConstraint.MIN_VAL, 4)

    def test_get_effective_min_val_float_inherits_constraint(self):
        """
        Datatype (float) with no MIN_VAL of its own but whose parent has one should inherit it.
        """
        self.__test_get_effective_num_constraint_inherits_constraint_h(self.FLOAT, BasicConstraint.MIN_VAL, 7.5)

    def test_get_effective_max_val_int_inherits_constraint(self):
        """
        Datatype (integer) with no MAX_VAL of its own but whose parent has one should inherit it.
        """
        self.__test_get_effective_num_constraint_inherits_constraint_h(self.INT, BasicConstraint.MAX_VAL, 7.5)

    def test_get_effective_max_val_float_inherits_constraint(self):
        """
        Datatype (float) with no MAX_VAL of its own but whose parent has one should inherit it.
        """
        self.__test_get_effective_num_constraint_inherits_constraint_h(self.FLOAT, BasicConstraint.MAX_VAL, 4)

    def test_get_effective_min_length_inherits_constraint(self):
        """
        Datatype (string) with no MIN_LENGTH of its own but whose parent has one should inherit it.
        """
        self.__test_get_effective_num_constraint_inherits_constraint_h(self.STR, BasicConstraint.MIN_LENGTH, 4)

    def test_get_effective_max_length_inherits_constraint(self):
        """
        Datatype (string) with no MAX_LENGTH of its own but whose parent has one should inherit it.
        """
        self.__test_get_effective_num_constraint_inherits_constraint_h(self.STR, BasicConstraint.MAX_LENGTH, 4)

    ########
    def __test_get_effective_num_constraint_inherits_several_constraints_h(
            self, dominant_builtin, other_builtin, BC_type, dominant_constr_val, other_constr_val=None):
        """
        Helper for testing the inheritance of constraints from several supertypes.
        """
        dominant_parent = Datatype(name="DominantParent", description="Parent with dominant constraint")
        dominant_parent.full_clean()
        dominant_parent.save()
        dominant_parent.restricts.add(dominant_builtin)
        dominant_constr = dominant_parent.basic_constraints.create(
            ruletype=BC_type, rule="{}".format(dominant_constr_val))

        other_parent = Datatype(name="OtherParent", description="Parent whose constraint is overruled")
        other_parent.full_clean()
        other_parent.save()
        other_parent.restricts.add(other_builtin)
        other_constr = None
        if other_constr_val != None:
            other_constr = other_parent.basic_constraints.create(ruletype=BC_type, rule="{}".format(other_constr_val))

        heir = Datatype(name="Heir", description="Inherits from two parents")
        heir.full_clean()
        heir.save()
        heir.restricts.add(dominant_parent)
        heir.restricts.add(other_parent)

        self.assertEquals(heir.get_effective_num_constraint(BC_type), (dominant_constr, dominant_constr_val))

        # Try swapping the order....
        heir.restricts.remove(dominant_parent)
        heir.restricts.remove(other_parent)
        heir.restricts.add(other_parent)
        heir.restricts.add(dominant_parent)

        self.assertEquals(heir.get_effective_num_constraint(BC_type), (dominant_constr, dominant_constr_val))

    def test_get_effective_min_val_inherits_several_constraints_int_int(self):
        """
        Datatype (integer) inheriting several MIN_VALs should inherit the largest one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.INT, self.INT, BasicConstraint.MIN_VAL, 5, 3.2)

    def test_get_effective_max_val_inherits_several_constraints_int_float(self):
        """
        Datatype (integer) inheriting several MIN_VALs should inherit the largest one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.INT, self.FLOAT, BasicConstraint.MAX_VAL, 3.2, 7)

    def test_get_effective_min_val_inherits_several_constraints_float_int(self):
        """
        Datatype (integer) inheriting several MIN_VALs should inherit the largest one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.FLOAT, self.INT, BasicConstraint.MIN_VAL, 19, 18)

    def test_get_effective_max_val_inherits_several_constraints_float_float(self):
        """
        Datatype (float) inheriting several MAX_VALs should inherit the largest one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.FLOAT, self.FLOAT, BasicConstraint.MAX_VAL, 100, 180)

    def test_get_effective_min_val_inherits_from_several_with_one_trivial_int_int(self):
        """
        Datatype (integer) inheriting from several supertypes but with only one MIN_VAL should inherit that one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.INT, self.INT, BasicConstraint.MIN_VAL, 5, None)

    def test_get_effective_max_val_inherits_from_several_with_one_trivial_int_float(self):
        """
        Datatype (integer) inheriting from several supertypes but with only one MIN_VAL should inherit that one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.INT, self.FLOAT, BasicConstraint.MAX_VAL, 5, None)

    def test_get_effective_min_val_inherits_from_several_with_one_trivial_float_int(self):
        """
        Datatype (integer) inheriting from several supertypes but with only one MIN_VAL should inherit that one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.FLOAT, self.INT, BasicConstraint.MIN_VAL, 5, None)

    def test_get_effective_max_val_inherits_from_several_with_one_trivial_float_float(self):
        """
        Datatype (float) inheriting from several supertypes but with only one MIN_VAL should inherit that one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.FLOAT, self.FLOAT, BasicConstraint.MAX_VAL, 5, None)

    def test_get_effective_min_length_inherits_from_several(self):
        """
        Datatype (string) inheriting from several supertypes with MIN_LENGTHs should inherit the largest.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.STR, self.STR, BasicConstraint.MIN_LENGTH, 50, 2)

    def test_get_effective_max_length_inherits_from_several(self):
        """
        Datatype (string) inheriting from several supertypes with MAX_LENGTHs should inherit the smallest.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.STR, self.STR, BasicConstraint.MAX_LENGTH, 2, 50)

    def test_get_effective_min_length_inherits_from_several_with_one_trivial(self):
        """
        Datatype (string) inheriting from several supertypes, only one of which has a MIN_LENGTH, inherits that one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.STR, self.STR, BasicConstraint.MIN_LENGTH, 2, None)

    def test_get_effective_max_length_inherits_from_several_with_one_trivial(self):
        """
        Datatype (string) inheriting from several supertypes, only one of which has a MAX_LENGTH, inherits that one.
        """
        self.__test_get_effective_num_constraint_inherits_several_constraints_h(
            self.STR, self.STR, BasicConstraint.MAX_LENGTH, 20, None)

    ########

    def test_get_effective_min_val_on_bool(self):
        """
        Datatype that inherits from BOOL should not have an effective MIN_VAL.
        """
        min_zero = Datatype(name="MinZero", description="Integer >= 0")
        min_zero.full_clean()
        min_zero.save()
        min_zero.restricts.add(self.INT)
        min_zero.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="0")

        heir = Datatype(name="Heir", description="Inherits from MinZero and BOOL")
        heir.full_clean()
        heir.save()
        heir.restricts.add(min_zero)
        heir.restricts.add(self.BOOL)

        self.assertEquals(heir.get_effective_num_constraint(BasicConstraint.MIN_VAL), (None, -float("inf")))

    def test_get_effective_max_length_on_float(self):
        """
        Datatype that inherits from FLOAT should not have an effective MAX_LENGTH.
        """
        max_50 = Datatype(name="Max50", description="String of length <= 50")
        max_50.full_clean()
        max_50.save()
        max_50.restricts.add(self.STR)
        max_50.basic_constraints.create(ruletype=BasicConstraint.MAX_LENGTH, rule="50")

        heir = Datatype(name="Heir", description="Inherits from Max50 and FLOAT")
        heir.full_clean()
        heir.save()
        heir.restricts.add(max_50)
        heir.restricts.add(self.FLOAT)

        self.assertEquals(heir.get_effective_num_constraint(BasicConstraint.MAX_LENGTH), (None, float("inf")))

    def test_get_effective_min_length_on_bool(self):
        """
        Datatype that inherits from BOOL should not have an effective MIN_LENGTH.
        """
        min_50 = Datatype(name="Min50", description="String of length <= 50")
        min_50.full_clean()
        min_50.save()
        min_50.restricts.add(self.STR)
        min_50.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="50")

        heir = Datatype(name="Heir", description="Inherits from Min50 and BOOL")
        heir.full_clean()
        heir.save()
        heir.restricts.add(min_50)
        heir.restricts.add(self.BOOL)

        self.assertEquals(heir.get_effective_num_constraint(BasicConstraint.MIN_LENGTH), (None, 0))

    ########
    def __test_get_effective_num_constraint_BC_overrides_inherited_h(self, builtin_type, supertype_builtin_type,
                                                                     BC_type, constr_val, supertype_constr_val):
        """
        Helper for testing cases where a Datatype overrides its supertypes' constraints.
        """
        super_DT = Datatype(name="SuperDT", description="Supertype with constraint")
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(supertype_builtin_type)
        super_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(supertype_constr_val))

        heir_DT = Datatype(name="Heir", description="Heir of supertype with overriding constraint")
        heir_DT.full_clean()
        heir_DT.save()
        heir_DT.restricts.add(builtin_type)
        override = heir_DT.basic_constraints.create(ruletype=BC_type, rule="{}".format(constr_val))

        self.assertEquals(heir_DT.get_effective_num_constraint(BC_type), (override, constr_val))

    # We just pick a few cases to test for this situation.
    def test_get_effective_min_val_float_overrides_inherited(self):
        """
        Get MIN_VAL from Datatype that overrides its inherited MIN_VAL.
        """
        self.__test_get_effective_num_constraint_BC_overrides_inherited_h(
            self.FLOAT, self.FLOAT, BasicConstraint.MIN_VAL, 33, 30
        )

    def test_get_effective_max_val_int_overrides_inherited(self):
        """
        Get MAX_VAL from Datatype that overrides its inherited MAX_VAL.
        """
        self.__test_get_effective_num_constraint_BC_overrides_inherited_h(
            self.INT, self.FLOAT, BasicConstraint.MAX_VAL, 22, 37
        )

    def test_get_effective_min_length_overrides_inherited(self):
        """
        Get MIN_LENGTH from Datatype that overrides its inherited MIN_LENGTH.
        """
        self.__test_get_effective_num_constraint_BC_overrides_inherited_h(
            self.STR, self.STR, BasicConstraint.MIN_LENGTH, 30, 5
        )

    def test_get_effective_max_length_overrides_inherited(self):
        """
        Get MAX_LENGTH from Datatype that overrides its inherited MAX_LENGTH.
        """
        self.__test_get_effective_num_constraint_BC_overrides_inherited_h(
            self.STR, self.STR, BasicConstraint.MAX_LENGTH, 16, 17
        )

class BasicConstraintGetAllRegexpTests(BasicConstraintTestSetup):

    # There should be no distinction on what builtin types a Datatype
    # inherits from, so we just shuffle through them.
    def test_no_regexps(self):
        """
        Case where Datatype has no regexps defined on it.
        """
        my_DT = Datatype(name="NoRegexpDT", description="Unfettered DT")
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.STR)

        self.assertEquals(my_DT.get_all_regexps(), [])

    def test_no_regexps_second_gen(self):
        """
        Case where Datatype has no regexps defined on it and neither do its supertypes.
        """
        super_DT = Datatype(name="SuperDT", description="Unfettered FLOAT")
        super_DT.save()
        super_DT.restricts.add(self.FLOAT)

        second_DT = Datatype(name="SecondDT", description="Unfettered INT")
        second_DT.save()
        second_DT.restricts.add(self.INT)

        my_DT = Datatype(name="NoRegexpDT", description="Unfettered DT")
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(second_DT)

        self.assertEquals(second_DT.get_all_regexps(), [])
        self.assertEquals(my_DT.get_all_regexps(), [])

    def test_one_direct_regexp(self):
        """
        Case where Datatype has one regexp defined on it.
        """
        my_DT = Datatype(name="RegexpedDT", description="Regexped Boolean")
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.BOOL)
        regexp_BC = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                   rule="T|F")

        self.assertEquals(my_DT.get_all_regexps(), [regexp_BC])

    def test_several_direct_regexps(self):
        """
        Case where Datatype has several regexps defined on it.
        """
        my_DT = Datatype(name="RegexpedDT", description="Regexped Boolean")
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(self.BOOL)
        regexp_BC = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                   rule="T|F")
        regexp2_BC = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                    rule="T")
        regexp3_BC = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                    rule=".*")

        self.assertEquals(my_DT.get_all_regexps(), [regexp_BC, regexp2_BC, regexp3_BC])

    def test_one_inherited_regexp(self):
        """
        Case where Datatype has no regexps defined on it but its supertypes do.
        """
        super_DT = Datatype(name="SuperDT", description="Regexped STR")
        super_DT.save()
        super_DT.restricts.add(self.STR)
        regexp_BC = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                      rule="1e.+")

        second_DT = Datatype(name="SecondDT", description="FLOAT inheriting a REGEXP")
        second_DT.save()
        second_DT.restricts.add(super_DT)
        second_DT.restricts.add(self.FLOAT)

        my_DT = Datatype(name="InheritingDT", description="Third-gen inheriting DT")
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(second_DT)

        self.assertEquals(second_DT.get_all_regexps(), [regexp_BC])
        self.assertEquals(my_DT.get_all_regexps(), [regexp_BC])

    def test_several_inherited_regexps(self):
        """
        Case where Datatype inherits several regexps and has none of its own.
        """
        super_DT = Datatype(name="SuperDT", description="Regexped FLOAT")
        super_DT.save()
        super_DT.restricts.add(self.FLOAT)
        regexp_BC = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                      rule="1999393939.....")

        second_DT = Datatype(name="SecondDT", description="FLOAT inheriting a REGEXP")
        second_DT.save()
        second_DT.restricts.add(super_DT)
        regexp2_BC = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                      rule="[1-9]+")

        my_DT = Datatype(name="InheritingDT", description="Third-gen inheriting DT")
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(second_DT)

        self.assertEquals(second_DT.get_all_regexps(), [regexp_BC, regexp2_BC])
        self.assertEquals(my_DT.get_all_regexps(), [regexp_BC, regexp2_BC])

    def test_several_once_removed_inherited_regexps(self):
        """
        Case where Datatype inherits several regexps from direct ancestors and has none of its own.
        """
        super_DT = Datatype(name="SuperDT", description="Regexped FLOAT")
        super_DT.save()
        super_DT.restricts.add(self.FLOAT)
        regexp_BC = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                      rule="1999393939.....")

        second_DT = Datatype(name="SecondDT", description="FLOAT inheriting a REGEXP")
        second_DT.save()
        second_DT.restricts.add(self.FLOAT)
        regexp2_BC = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                      rule="[1-9]+")

        my_DT = Datatype(name="InheritingDT", description="Third-gen inheriting DT")
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(super_DT)
        my_DT.restricts.add(second_DT)

        self.assertEquals(my_DT.get_all_regexps(), [regexp_BC, regexp2_BC])

    def test_several_regexps_multiple_sources(self):
        """
        Case where Datatype inherits several regexps from ancestors and has some of its own.
        """
        super_DT = Datatype(name="SuperDT", description="Regexped FLOAT")
        super_DT.save()
        super_DT.restricts.add(self.STR)
        regexp_BC = super_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                      rule=".*")

        second_DT = Datatype(name="SecondDT", description="STR inheriting a REGEXP")
        second_DT.save()
        second_DT.restricts.add(super_DT)
        regexp2_BC = second_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                       rule="[0-9]*")
        regexp3_BC = second_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                       rule="[1-7]*")

        third_DT = Datatype(name="ThirdDT", description="STR inheriting a REGEXP")
        third_DT.save()
        third_DT.restricts.add(self.STR)
        regexp4_BC = third_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                       rule=".+")

        my_DT = Datatype(name="InheritingDT", description="Third-gen inheriting DT")
        my_DT.full_clean()
        my_DT.save()
        my_DT.restricts.add(second_DT)
        my_DT.restricts.add(third_DT)
        regexp5_BC = my_DT.basic_constraints.create(ruletype=BasicConstraint.REGEXP,
                                                       rule="[4-7]+")

        second_DT_regexps = second_DT.get_all_regexps()
        self.assertEquals(len(second_DT_regexps), 3)
        self.assertEquals(regexp_BC in second_DT_regexps, True)
        self.assertEquals(regexp2_BC in second_DT_regexps, True)
        self.assertEquals(regexp3_BC in second_DT_regexps, True)

        my_DT_regexps = my_DT.get_all_regexps()
        self.assertEquals(len(my_DT_regexps), 5)
        self.assertEquals(regexp_BC in my_DT_regexps, True)
        self.assertEquals(regexp2_BC in my_DT_regexps, True)
        self.assertEquals(regexp3_BC in my_DT_regexps, True)
        self.assertEquals(regexp4_BC in my_DT_regexps, True)
        self.assertEquals(regexp5_BC in my_DT_regexps, True)

class BasicConstraintGetEffectiveDatetimeformatTests(BasicConstraintTestSetup):

    def test_on_builtins(self):
        """
        Test on the builtin types.
        """
        self.assertEquals(self.STR.get_effective_datetimeformat(), None)
        self.assertEquals(self.INT.get_effective_datetimeformat(), None)
        self.assertEquals(self.FLOAT.get_effective_datetimeformat(), None)
        self.assertEquals(self.BOOL.get_effective_datetimeformat(), None)

    def __test_no_dtf_h(self, builtin_type):
        """
        Helper to test the cases where a non-builtin Datatype has no DTF defined.
        """
        constr_DT = Datatype(name="DTwithoutDTF", description="Datatype with no DTF")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(builtin_type)

        self.assertEquals(constr_DT.get_effective_datetimeformat(), None)

    def test_no_dtf_str(self):
        """
        Test case where a non-builtin string Datatype has no DATETIMEFORMAT.
        """
        self.__test_no_dtf_h(self.STR)

    def test_no_dtf_int(self):
        """
        Test case where a non-builtin integer Datatype has no DATETIMEFORMAT.
        """
        self.__test_no_dtf_h(self.INT)

    def test_no_dtf_float(self):
        """
        Test case where a non-builtin float Datatype has no DATETIMEFORMAT.
        """
        self.__test_no_dtf_h(self.FLOAT)

    def test_no_dtf_bool(self):
        """
        Test case where a non-builtin Boolean Datatype has no DATETIMEFORMAT.
        """
        self.__test_no_dtf_h(self.BOOL)

    def test_direct_dtf_str(self):
        """
        Testing the case where a string has a direct DTF defined.
        """
        constr_DT = Datatype(name="DTwithDTF", description="Datatype with a DTF")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(self.STR)

        new_DTF = constr_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT,
                                                     rule="%Y %m %d %H:%M:%S")

        self.assertEquals(constr_DT.get_effective_datetimeformat(), new_DTF)

    def test_inherited_dtf(self):
        """
        Testing the case where a string has one supertype and inherits its DTF.
        """
        super_DT = Datatype(name="DTwithDTF", description="Datatype with a DTF")
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)
        new_DTF = super_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT,
                                                    rule="%Y %m %d %H:%M:%S %z")

        constr_DT = Datatype(name="InheritingDT", description="Datatype with inherited DTF")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super_DT)

        self.assertEquals(constr_DT.get_effective_datetimeformat(), new_DTF)

    def test_inherited_dtf_non_str(self):
        """
        Testing the case where a non-string has a supertype with a DTF.
        """
        super_DT = Datatype(name="DTwithDTF", description="Datatype with a DTF")
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)
        new_DTF = super_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT,
                                                    rule="%Y")

        constr_DT = Datatype(name="InheritingDT", description="Non-string Datatype with inherited DTF")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super_DT)
        constr_DT.restricts.add(self.INT)

        self.assertEquals(constr_DT.get_effective_datetimeformat(), None)

    def test_distantly_inherited_dtf(self):
        """
        Testing the case where a non-string has several supertypes and inherits a DTF from an indirect ancestor.
        """
        super_DT = Datatype(name="AncestorDT", description="Ancestor Datatype with no DTF")
        super_DT.full_clean()
        super_DT.save()
        super_DT.restricts.add(self.STR)

        super2_DT = Datatype(name="DTwithDTF", description="Datatype with a DTF")
        super2_DT.full_clean()
        super2_DT.save()
        super2_DT.restricts.add(self.STR)
        new_DTF = super2_DT.basic_constraints.create(ruletype=BasicConstraint.DATETIMEFORMAT,
                                                     rule="%Y")

        super3_DT = Datatype(name="DTwithREGEXP", description="Datatype with a REGEXP but no DTF")
        super3_DT.full_clean()
        super3_DT.save()
        super3_DT.restricts.add(self.STR)
        super3_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_LENGTH, rule="4")

        super4_DT = Datatype(name="DirectAncestor", description="Datatype with no DTF")
        super4_DT.full_clean()
        super4_DT.save()
        super4_DT.restricts.add(super2_DT)

        constr_DT = Datatype(name="InheritingDT", description="Non-string Datatype with inherited DTF")
        constr_DT.full_clean()
        constr_DT.save()
        constr_DT.restricts.add(super2_DT)
        constr_DT.restricts.add(super4_DT)

        self.assertEquals(constr_DT.get_effective_datetimeformat(), new_DTF)
