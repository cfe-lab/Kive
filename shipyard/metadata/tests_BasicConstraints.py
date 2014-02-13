"""
Unit tests for Shipyard's BasicConstraint class.
"""
from django.test import TestCase
from django.core.exceptions import ValidationError

from metadata.models import *
from method.models import CodeResourceRevision

from constants import datatypes, error_messages

class BasicConstraintTests(TestCase):

    def setUp(self):
        """
        General setup for BasicConstraint testing.
        """
        # The built-in Shipyard atomic Datatypes.
        self.STR = Datatype.objects.get(pk=datatypes.STR_PK)
        self.INT = Datatype.objects.get(pk=datatypes.INT_PK)
        self.FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
        self.BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

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

    def test_clean_min_val_float_good(self):
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


    # FIXME continue from here!
