"""
Unit tests for Shipyard's BasicConstraint class.
"""
from django.test import TestCase
from django.core.exceptions import ValidationError

from metadata.models import *
from method.models import CodeResourceRevision

from constants import datatypes

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

    def test_get_effective_min_val_int_no_constraint(self):
        """
        Datatype with no MIN_VAL set should have -\infty as its effective min val.
        """
        no_min_set = Datatype(
            name="NoMinSet",
            description="No minimum set")
        self.assertEquals(no_min_set.clean(), None)
        no_min_set.save()

        # Right now, the appropriate MIN_VAL restriction is -\infty.
        self.assertEquals(no_min_set.get_effective_num_constraint(BasicConstraint.MIN_VAL), (None, -float("Inf")))

    def test_get_effective_min_val_int_with_constraint(self):
        """
        MIN_VAL constraint set directly on the Datatype.
        """
        min_minus_5 = Datatype(
            name="MinMinus5",
            description="Integer >= -5")
        self.assertEquals(min_minus_5.clean(), None)
        min_minus_5.save()
        min_minus_5.restricts.add(self.INT)

        geq_minus_5 = min_minus_5.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="-5")
        self.assertEquals(geq_minus_5.full_clean(), None)

        # # The appropriate MIN_VAL restriction is the one we just added.
        # print("min_minus_5 restricts FLOAT: {}".format(min_minus_5.is_restriction(self.FLOAT)))
        # print("min_minus_5 restricts BOOL: {}".format(min_minus_5.is_restriction(self.BOOL)))
        # print("min_minus_5 first BasicConstraint ruletype: {}".
        #       format(min_minus_5.basic_constraints.all().first().ruletype))

        self.assertEquals(min_minus_5.get_effective_num_constraint(BasicConstraint.MIN_VAL), (geq_minus_5, -5))
