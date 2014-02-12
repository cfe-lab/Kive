"""
Unit tests for Shipyard's BasicConstraint class.
"""
from django.test import TestCase
from django.core.exceptions import ValidationError

from metadata.models import *
from method.models import CodeResourceRevision


class BasicConstraintTests(TestCase):

    def test_get_effective_min_val_int_no_constraint(self):
        """
        Datatype with no MIN_VAL set should have -\infty as its effective min val.
        """
        min_minus_5 = Datatype(
            name="MinMinus5",
            description="Integer >= -5",
            Python_type=Datatype.INT)
        self.assertEquals(min_minus_5.clean())
        min_minus_5.save()

        # Right now, the appropriate MIN_VAL restriction is -\infty.
        self.assertEquals(min_minus_5.get_effective_min_val(), (None, -float("Inf")))

    def test_get_effective_min_val_int_with_constraint(self):
        """
        MIN_VAL constraint set directly on the Datatype.
        """
        geq_minus_5 = min_minus_5.basic_constraints.create(rule=BasicConstraint.MIN_VAL, ruletype="-5")
        self.assertEquals(geq_minus_5.clean())

        # The appropriate MIN_VAL restriction is the one we just added.
        self.assertEquals(min_minus_5.get_effective_min_val(), (geq_minus_5, -5))

        min_1 = Datatype(
            name="Min1",
            description="Integer >= 1",
            Python_type=Datatype.INT
        )
        self.assertEquals(min_1.clean())
        min_1.save()
        min_1.restricts.add(min_minus_5)

        # Right now, the appropriate MIN_VAL

        geq_1 = min_1.basic_constraints.create(rule=BasicConstraint.MIN_VAL, ruletype="1")
        self.assertEquals(geq_1.clean())