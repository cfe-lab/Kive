"""
metadata.models

Shipyard data models relating to metadata: Datatypes and their related
paraphernalia, CompoundDatatypes, etc.

FIXME get all the models pointing at each other correctly!
"""

from django.db import models
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, RegexValidator
from django.core.files import File
from django.utils import timezone

import operator
import re
import csv
import os
import sys
from datetime import datetime

from file_access_utils import set_up_directory
from constants import datatypes, CDTs, error_messages
from datachecking.models import VerificationLog

import logging

class Datatype(models.Model):
    """
    Abstract definition of a semantically atomic type of data.
    Related to :model:`copperfish.CompoundDatatype`
    """
    # print(__name__)
    name = models.CharField(
        "Datatype name",
        max_length=64,
        help_text="The name for this Datatype");

    # auto_now_add: set to now on instantiation (editable=False)
    date_created = models.DateTimeField(
        'Date created',
        auto_now_add = True,
        help_text="Date Datatype was defined");

    description = models.TextField(
        "Datatype description",
        help_text="A description for this Datatype");

    # Admissible "Shipyard atomic" types.
    INT = "int"
    STR = "str"
    FLOAT = "float"
    BOOL = "bool"

    restricts = models.ManyToManyField(
        'self',
        symmetrical=False,
        related_name="restricted_by",
        null=True,
        blank=True,
        help_text="Captures hierarchical is-a classifications among Datatypes");

    def get_restricts (self):
        return ','.join([dt['name'] for dt in self.restricts.values()])
    restricts_str = property(get_restricts)

    prototype = models.OneToOneField(
        "archive.Dataset",
        null=True,
        blank=True,
        related_name="datatype_modelled")

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def is_restricted_by(self, possible_restrictor_datatype):
        """
        Determine if this datatype is ever *properly* restricted, directly or indirectly,
        by a given datatype.
        
        PRE: there is no circular restriction in the possible restrictor
        datatype (this would cause an infinite recursion).
        """
        # The default is that self is not restricted by
        # possible_restrictor_datatype; toggle to True if it turns out
        # that it is.
        is_restricted = False
        restrictions = possible_restrictor_datatype.restricts.all()

        for restrictedDataType in restrictions:

            # Case 1: If restrictions restrict self, return true
            if restrictedDataType == self:
                is_restricted = True

            # Case 2: Check if any restricted Datatypes themselves restrict self
            else:
                theValue = self.is_restricted_by(restrictedDataType)

                # If any restricted Datatypes themselves restrict self, propagate
                # this information to the parent Datatype as restricting self
                if theValue == True:
                    is_restricted = True

        # Return False if Case 1 is never encountered
        return is_restricted

    def is_restriction(self, possible_restricted_datatype):
        """
        True if this Datatype restricts the parameter, directly or indirectly.

        This induces a partial ordering A <= B if A is a restriction of B.
        For example, a DNA sequence is a restriction of a string.
        """
        return (self == possible_restricted_datatype or
                possible_restricted_datatype.is_restricted_by(self))

    def get_effective_num_constraint(self, BC_type):
        """
        Gets the 'effective' BasicConstraint of the specified type.

        That is, the most restrictive BasicConstraint of this type
        acting on this Datatype or its supertypes.  Returns a tuple
        with the BC in the first position and its value in the
        second.

        If this instance cannot have a numerical constraint (it does
        not restrict either INT or FLOAT or it restricts BOOL) then
        return (None, [appropriate value]).

        PRE: all of this instance's supertypes are clean (in the
        Django sense), this instance has been properly defined as
        restricting at least one of the Shipyard builtin Datatypes,
        and this instance has at most one BasicConstraint of the
        specified type (and it is clean).  If this instance has such
        a BasicConstraint, it exceeds the maximum of those of its
        supertypes.
        """
        # The Shipyard builtins.
        STR = Datatype.objects.get(pk=datatypes.STR_PK)
        INT = Datatype.objects.get(pk=datatypes.INT_PK)
        FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
        BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

        # TODO: make these functions, which we call below, not strings
        min_or_max = "min" if BC_type in (BasicConstraint.MIN_LENGTH, BasicConstraint.MIN_VAL) else "max"
        val_or_len = "val" if BC_type in (BasicConstraint.MIN_VAL, BasicConstraint.MAX_VAL) else "len"

        effective_BC = None
        # Default value if this is a 'max' constraint.
        effective_val = float("inf")
        if BC_type == BasicConstraint.MIN_VAL:
            effective_val = -float("inf")
        elif BC_type == BasicConstraint.MIN_LENGTH:
            effective_val = 0

        # Base case: this Datatype does not restrict INT or FLOAT,
        # or it restricts BOOL, and this is a (MIN|MAX)_VAL restriction.
        if val_or_len == "val" and self.get_builtin_type() in (STR, BOOL):
            # self.logger.debug("Datatype \"{}\" with pk={} is not one that should have a numerical constraint".
            #                   format(self, self.pk))
            return (None, effective_val)

        # Base case 2: this Datatype restricts any of FLOAT, INT, or
        # BOOL, and this is a (MIN|MAX)_LENGTH restriction.
        if (val_or_len == "len") and self.get_builtin_type() != STR:
            return (None, effective_val)

        # Base case 2: this instance has a constraint of this type already.
        my_BC = self.basic_constraints.filter(ruletype=BC_type)
        if my_BC.exists():
            if val_or_len == "val":
                effective_val = float(my_BC.first().rule)
            else:
                effective_val = int(my_BC.first().rule)
            effective_BC = my_BC.first()

        else:
            # Base case 3: this instance has no supertypes, in which case we don't touch effective_BC or
            # effective_val.
            if hasattr(self, "restricts"):
                for supertype in self.restricts.all():
                    # self.logger.debug("Checking supertype \"{}\" with pk={} for BasicConstraints of form \"{}\"".
                    #                   format(supertype, supertype.pk, BC_type))
                    # Recursive case: go through all of the supertypes and take the maximum.
                    supertype_BC, supertype_val = supertype.get_effective_num_constraint(BC_type)

                    if ((min_or_max == "min" and supertype_val > effective_val) or
                            (min_or_max == "max" and supertype_val < effective_val)):
                        effective_BC = supertype_BC
                        effective_val = supertype_val

        return (effective_BC, effective_val)

    def get_all_regexps(self):
        """
        Retrieves all of the REGEXP BasicConstraints acting on this instance.

        PRE: all of this instance's supertypes are clean (in the Django
        sense), as are this instance's REGEXP BasicConstraints.
        """
        all_regexp_BCs = []

        for regexp_BC in self.basic_constraints.filter(ruletype=BasicConstraint.REGEXP):
            all_regexp_BCs.append(regexp_BC)

        for supertype in self.restricts.all():
            for regexp_BC in supertype.basic_constraints.filter(ruletype=BasicConstraint.REGEXP):
                all_regexp_BCs.append(regexp_BC)

        return all_regexp_BCs

    def get_effective_datetimeformat(self):
        """
        Retrieves the date-time format string effective for this instance.

        There can only be one such format string acting on this or its supertypes.
        Moreover, this returns None if this instance restricts any other atomic
        type than STR.

        PRE: this instance has at most one DATETIMEFORMAT BasicConstraint (and it is clean if it exists),
        and all its supertypes are clean (in the Django sense).
        """
        # The Shipyard builtins.
        STR = Datatype.objects.get(pk=datatypes.STR_PK)
        INT = Datatype.objects.get(pk=datatypes.INT_PK)
        FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
        BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

        if self.get_builtin_type() != STR:
            return None

        my_dtf = self.basic_constraints.filter(ruletype=BasicConstraint.DATETIMEFORMAT)
        if my_dtf.exists():
            return my_dtf.first()

        for supertype in self.restricts.all():
            curr_dtf = supertype.basic_constraints.filter(ruletype=BasicConstraint.DATETIMEFORMAT)
            if curr_dtf.exists():
                return curr_dtf.first()

        # If we reach this point, there is no effective datetimeformat constraint.
        return None

    def get_builtin_type(self):
        """
        Get the Shipyard builtin type restricted by this Datatype.

        This retrieves the most restrictive one under the ordering:
        BOOL < INT < FLOAT < STR

        PRE: this Datatype restricts at least one clean Datatype
        (thus at least restricts STR).
        """
        # The Shipyard builtins.
        STR = Datatype.objects.get(pk=datatypes.STR_PK)
        INT = Datatype.objects.get(pk=datatypes.INT_PK)
        FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
        BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

        builtin_type = STR
        if self.is_restriction(BOOL):
            builtin_type = BOOL
        elif self.is_restriction(INT):
            builtin_type = INT
        elif self.is_restriction(FLOAT):
            builtin_type = FLOAT

        return builtin_type


    # Clean: If prototype is specified, it must have a CDT with
    # 2 columns: column 1 is a string "example" field,
    # column 2 is a bool "valid" field.  This CDT will be hard-coded
    # and loaded into the database on creation.
    def clean(self):
        """
        Checks coherence of this Datatype.

        Note that a Datatype must be saved into the database before it's complete,
        as a complete Datatype must be a restriction of a Shipyard atomic Datatype
        (STR, INT, FLOAT, or BOOL).  This necessitates a complete_clean() routine.
        """
        # The Shipyard builtins.
        STR = Datatype.objects.get(pk=datatypes.STR_PK)
        INT = Datatype.objects.get(pk=datatypes.INT_PK)
        FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
        BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

        if hasattr(self, "restricts") and self.is_restricted_by(self):
            raise ValidationError(error_messages["DT_circular_restriction"].format(self))

        if self.prototype is not None:
            if self.prototype.symbolicdataset.is_raw():
                raise ValidationError(error_messages["DT_prototype_raw"].format(self))

            PROTOTYPE_CDT = CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK)

            if not self.prototype.symbolicdataset.get_cdt().is_identical(PROTOTYPE_CDT):
                raise ValidationError(error_messages["DT_prototype_wrong_CDT"].format(self))

        # Clean all BasicConstraints.
        for bc in self.basic_constraints.all():
            bc.clean()

        # Check numerical constraints for coherence against the supertypes' constraints.
        for constr_type in (BasicConstraint.MIN_VAL, BasicConstraint.MAX_VAL,
                            BasicConstraint.MIN_LENGTH, BasicConstraint.MAX_LENGTH):
            # Extract details on what kind of constraint this is.
            min_or_max = "min" if constr_type in (BasicConstraint.MIN_VAL, BasicConstraint.MIN_LENGTH) else "max"
            val_or_len = "val" if constr_type in (BasicConstraint.MIN_VAL, BasicConstraint.MAX_VAL) else "length"

            all_curr_type = self.basic_constraints.filter(ruletype=constr_type)

            if all_curr_type.count() > 1:
                # Check that there is at most one BasicConstraint of types (MIN|MAX)_(VAL|LENGTH)
                # directly associated to this instance.
                raise ValidationError(error_messages["DT_several_same_constraint"].format(self, constr_type))

            elif all_curr_type.count() == 1:
                my_constr_val = None
                if val_or_len == "val":
                    my_constr_val = float(all_curr_type.first().rule)
                else:
                    my_constr_val = int(all_curr_type.first().rule)

                # Default constraint value for the supertypes if the constraint is a max-type.
                supertypes_val = float("inf")
                if constr_type == BasicConstraint.MIN_VAL:
                    supertypes_val = -float("inf")
                elif constr_type == BasicConstraint.MIN_LENGTH:
                    supertypes_val = 0

                if hasattr(self, "restricts"):
                    if min_or_max == "min":
                        supertypes_val = max(
                            supertype.get_effective_num_constraint(constr_type)[1]
                            for supertype in self.restricts.all())
                    else:
                        supertypes_val = min(
                            supertype.get_effective_num_constraint(constr_type)[1]
                            for supertype in self.restricts.all())

                if min_or_max == "min" and my_constr_val <= supertypes_val:
                    if val_or_len == "val":
                        raise ValidationError(error_messages["DT_min_val_smaller_than_supertypes"].format(self))
                    else:
                        raise ValidationError(error_messages["DT_min_length_smaller_than_supertypes"].format(self))
                elif min_or_max == "max" and my_constr_val >= supertypes_val:
                    if val_or_len == "val":
                        raise ValidationError(error_messages["DT_max_val_larger_than_supertypes"].format(self))
                    else:
                        raise ValidationError(error_messages["DT_max_length_larger_than_supertypes"].format(self))

        # Check that there is only one DATETIMEFORMAT between this Datatype and all of its supertypes.
        my_dtf_count = self.basic_constraints.filter(ruletype=BasicConstraint.DATETIMEFORMAT).count()
        supertype_dtf_count = 0 if not hasattr(self, "restricts") else sum(
            [supertype.basic_constraints.filter(ruletype=BasicConstraint.DATETIMEFORMAT).count()
             for supertype in self.restricts.all()])
        if my_dtf_count + supertype_dtf_count > 1:
            raise ValidationError(error_messages["DT_too_many_datetimeformats"].format(self))

        # Check that effective min_length <= max_length, min_val <= max_val if possible;
        # i.e. if this Datatype restricts only STR, or restricts INT/FLOAT and not BOOL, respectively.
        # These checks don't happen if this Datatype hasn't already been saved into the database
        # # (or else there is no way that they can have effective numerical constraints).
        if hasattr(self, "restricts") and self.get_builtin_type() in (FLOAT, INT):
            if (self.get_effective_num_constraint(BasicConstraint.MIN_VAL)[1] >
                    self.get_effective_num_constraint(BasicConstraint.MAX_VAL)[1]):
                raise ValidationError(error_messages["DT_min_val_exceeds_max_val"].format(self))

        if hasattr(self, "restricts") and self.get_builtin_type() == STR:
            if (self.get_effective_num_constraint(BasicConstraint.MIN_LENGTH)[1] >
                    self.get_effective_num_constraint(BasicConstraint.MAX_LENGTH)[1]):
                raise ValidationError(error_messages["DT_min_length_exceeds_max_length"].format(self))

        # Clean the CustomConstraint if it exists.
        if self.has_custom_constraint():
            self.custom_constraint.clean()

    def is_complete(self):
        """
        Returns whether this Datatype has a complete definition; i.e. restricts a Shipyard atomic.
        """
        # The hard-coded Shipyard atomic string Datatype.  We check that this
        # instance restricts it.
        STR = Datatype.objects.get(pk=datatypes.STR_PK)
        if hasattr(self, "restricts") and self.is_restriction(STR):
            return True
        return False

    def complete_clean(self):
        """
        Checks completeness and coherence of this Datatype.

        First calls clean; then confirms that this Datatype restricts
        a Shipyard atomic Datatype.
        """
        self.clean()

        if not self.is_complete():
            raise ValidationError(error_messages["DT_does_not_restrict_atomic"].format(self))

    def get_absolute_url(self):
        return '/datatypes/%i' % self.id

    def __unicode__(self):
        """Describe Datatype by name"""
        return self.name

    def has_custom_constraint(self):
        """Tells whether this Datatype has a CustomConstraint."""
        return hasattr(self, "custom_constraint")

    def check_basic_constraints(self, string_to_check):
        """
        Check the specified string against basic constraints.

        This includes both whether or not the string can be 
        interpreted as all of the Shipyard atomic types it inherits from, but also
        whether it then checks out against all BasicConstraints.

        Return a list of BasicConstraints that it failed (hopefully it's
        empty!).

        PRE: this Datatype and by extension all of its BasicConstraints
        are clean.  That means that only the appropriate BasicConstraints
        for this Datatype are applied.
        """
        ####
        # First, determine what Shipyard atomic datatypes this restricts.
        # Then, check it against any type-specific BasicConstraints
        # (MIN|MAX_LENGTH or DATETIMEFORMAT for strings,
        # MIN|MAX_VAL for numerical types).

        # The hard-coded Shipyard atomic types.
        STR = Datatype.objects.get(pk=datatypes.STR_PK)
        INT = Datatype.objects.get(pk=datatypes.INT_PK)
        FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
        BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

        constraints_failed = []
        if self.is_restriction(STR) and not self.is_restriction(FLOAT) and not self.is_restriction(BOOL):
            # string_to_check is, by definition, a string, so we skip
            # to checking the BasicConstraints.
            eff_min_length_BC, eff_min_length = self.get_effective_num_constraint(BasicConstraint.MIN_LENGTH)
            if eff_min_length_BC is not None and len(string_to_check) < eff_min_length:
                constraints_failed.append(eff_min_length_BC)
            else:
                eff_max_length_BC, eff_max_length = self.get_effective_num_constraint(BasicConstraint.MAX_LENGTH)
                if eff_max_length_BC is not None and len(string_to_check) > eff_max_length:
                    constraints_failed.append(eff_max_length_BC)

            eff_dtf_BC = self.get_effective_datetimeformat()
            # Attempt to make a datetime object using this format
            # string.
            if eff_dtf_BC is not None:
              try:
                  datetime.strptime(string_to_check, eff_dtf_BC.rule)
              except ValueError:
                  constraints_failed.append(eff_dtf_BC)

        # Next, check the numeric (and non-Boolean) cases.
        elif self.is_restriction(INT) and not self.is_restriction(BOOL):
            try:
                int(string_to_check)
            except ValueError:
                return ["Was not integer"]

            # Check the numeric-type BasicConstraints.
            eff_min_val_BC, eff_min_val = self.get_effective_num_constraint(BasicConstraint.MIN_VAL)
            if eff_min_val_BC is not None and int(string_to_check) < eff_min_val:
                constraints_failed.append(eff_min_val_BC)
            else:
                eff_max_val_BC, eff_max_val = self.get_effective_num_constraint(BasicConstraint.MAX_VAL)
                if eff_max_val_BC is not None and int(string_to_check) > eff_max_val:
                    constraints_failed.append(eff_max_val_BC)

        elif self.is_restriction(FLOAT) and not self.is_restriction(BOOL):
            try:
                float(string_to_check)
            except ValueError:
                return ["Was not float"]

            # Same as for the int case.
            eff_min_val_BC, eff_min_val = self.get_effective_num_constraint(BasicConstraint.MIN_VAL)
            if eff_min_val_BC is not None and float(string_to_check) < eff_min_val:
                constraints_failed.append(eff_min_val_BC)
            else:
                eff_max_val_BC, eff_max_val = self.get_effective_num_constraint(BasicConstraint.MAX_VAL)
                if eff_max_val_BC is not None and float(string_to_check) > eff_max_val:
                    constraints_failed.append(eff_max_val_BC)

        elif self.is_restriction(BOOL):
            bool_RE = re.compile("^(True)|(False)|(true)|(false)|(TRUE)|(FALSE)|T|F|t|f|0|1$")
            if not bool_RE.match(string_to_check):
                return ["Was not boolean"]

        ####
        # Check all REGEXP-type BasicConstraints.
        constraints_failed = []

        for re_BC in self.get_all_regexps():
            constraint_re = re.compile(re_BC.rule)
            if not constraint_re.search(string_to_check):
                constraints_failed.append(re_BC)

        return constraints_failed

    # Note that checking the CustomConstraint requires a place to run.
    # As such, it's debatable whether to put it here or as a Sandbox
    # method.  We'll probably put it here.

class BasicConstraint(models.Model):
    """
    Basic (level 1) constraint on a Datatype.

    The admissible constraints are:
     - (min|max)len (string)
     - (min|max)val (numeric)
     - (min|max)prec (float)
     - regexp (this will work on anything)
     - datetimeformat (string -- this is a special case)
    """
    datatype = models.ForeignKey(
        Datatype,
        related_name="basic_constraints")

    # Define the choices for rule type.
    MIN_LENGTH = "minlen"
    MAX_LENGTH = "maxlen"
    MIN_VAL = "minval"
    MAX_VAL = "maxval"
    # MIN_PREC = "minprec"
    # MAX_PREC = "maxprec"
    REGEXP = "regexp"
    DATETIMEFORMAT = "datetimeformat"
    
    CONSTRAINT_TYPES = (
        (MIN_LENGTH, "minimum string length"),
        (MAX_LENGTH, "maximum string length"),
        (MIN_VAL, "minimum numeric value"),
        (MAX_VAL, "maximum numeric value"),
        (REGEXP, "Perl regular expression"),
        (DATETIMEFORMAT, "date format string (1989 C standard)")
    )

    # Added the validator here to ensure that the value of ruletype is one of the allowable choices.
    ruletype = models.CharField(
        "Type of rule",
        max_length=32,
        choices=CONSTRAINT_TYPES,
        validators=[
            RegexValidator(
                re.compile("{}|{}|{}|{}|{}|{}".format(MIN_LENGTH, MAX_LENGTH, MIN_VAL, MAX_VAL, REGEXP, DATETIMEFORMAT)
                )
            )])

    rule = models.CharField(
        "Rule specification",
        max_length = 100)

    # TO DO: write a clean function handling the above.
    def clean(self):
        """
        Check coherence of the specified rule and rule type.

        First check: the parent Datatype must restrict at least one of our Shipyard atomic types, or else
        there is no sense in creating a BasicConstraint at all.

        The rule types must satisfy:
         - MIN_LENGTH: rule must be castable to a non-negative integer;
           parent DT inherits from Shipyard type 'STR' and not 'FLOAT', 'INT', or 'BOOL'
         - MAX_LENGTH: as for MIN_LENGTH but rule must be castable to a positive integer
         - (MIN|MAX)_VAL: rule must be castable to a float; parent DT
           must inherit from Shipyard type 'FLOAT' or 'INT'
         - REGEXP: rule must be a valid Perl-style RE
         - DATETIMEFORMAT: rule can be anything (note that it's up to you
           to define something *useful* here); parent DT inherits from Shipyard type 'STR'
           and not 'FLOAT', 'INT', or 'BOOL'
        """
        if not self.datatype.is_complete():
            raise ValidationError(error_messages["BC_DT_not_complete"].format(self.datatype, self))

        # The hard-coded Shipyard atomic types.
        STR = Datatype.objects.get(pk=datatypes.STR_PK)
        INT = Datatype.objects.get(pk=datatypes.INT_PK)
        FLOAT = Datatype.objects.get(pk=datatypes.FLOAT_PK)
        BOOL = Datatype.objects.get(pk=datatypes.BOOL_PK)

        error_msg = ""
        is_error = False

        # Check the rule for coherence.
        if self.ruletype in (BasicConstraint.MIN_LENGTH, BasicConstraint.MAX_LENGTH):
            # MIN/MAX_LENGTH should not apply to anything that restricts INT, FLOAT, or BOOL.  Note that INT <= FLOAT.
            if self.datatype.get_builtin_type() != STR:
                error_msg = error_messages["BC_length_constraint_on_non_string"].format(self, self.datatype)
                is_error = True
            try:
                length_constraint = int(self.rule)
                if length_constraint < 1:
                    error_msg = error_messages["BC_length_constraint_non_positive"].format(self, self.rule)
                    is_error = True
            except ValueError:
                error_msg = error_messages["BC_length_constraint_non_integer"].format(self, self.rule)
                is_error = True

        elif self.ruletype in (BasicConstraint.MAX_VAL, BasicConstraint.MIN_VAL):
            # This should not apply to a non-numeric.
            if self.datatype.get_builtin_type() not in (FLOAT, INT):
                error_msg = error_messages["BC_val_constraint_parent_non_numeric"].format(self, self.datatype)
                is_error = True
            try:
                val_bound = float(self.rule)
            except ValueError:
                error_msg = error_messages["BC_val_constraint_rule_non_numeric"].format(self, self.rule)
                is_error = True
        
        elif self.ruletype == BasicConstraint.REGEXP:
            try:
                re.compile(self.rule)
            except re.error:
                error_msg = error_messages["BC_bad_RE"].format(self, self.rule)
                is_error = True

        elif self.ruletype == BasicConstraint.DATETIMEFORMAT:
            # This should not apply to a boolean or a numeric.
            if self.datatype.get_builtin_type() != STR:
                error_msg = error_messages["BC_datetimeformat_non_string"].format(self, self.datatype)
                is_error = True

        if is_error:
            raise ValidationError(error_msg)

class CustomConstraint(models.Model):
    """
    More complex (level 2) verification of Datatypes.

    These will be specified in the form of Methods that
    take a CSV of strings (which is the parent of all
    Datatypes) and return T/F for 
    """
    datatype = models.OneToOneField(
        Datatype,
        related_name="custom_constraint")

    verification_method = models.ForeignKey(
        "method.Method",
        related_name="custom_constraints")

    # Clean: Methods which function as CustomConstraints must take in
    # a column of strings named "to_test" and returns a column of
    # positive integers named "failed_row".  We thus need to
    # hard-code in at least two Datatypes and two CDTs (string,
    # PositiveInteger (and probably int) so that PositiveInteger can
    # restrict it), and a CDT for each).  We'll probably need more
    # later anyway.  Such CDTs will be pre-loaded into the database.
    def clean(self):
        """
        Checks coherence of this CustomConstraint.

        The method used for verification must accept as input
        a CDT looking like (string to_test); it must return
        as output a CDT looking like (bool is_valid).
        """
        # Pre-defined CDTs that the verification method must use.
        VERIF_IN = CompoundDatatype.objects.get(pk=CDTs.VERIF_IN_PK)
        VERIF_OUT = CompoundDatatype.objects.get(pk=CDTs.VERIF_OUT_PK)
        
        verif_method_in = self.verification_method.inputs.all()
        verif_method_out = self.verification_method.outputs.all()
        if verif_method_in.count() != 1 or verif_method_out.count() != 1:
            raise ValidationError("CustomConstraint \"{}\" verification method does not have exactly one input and one output".
                                  format(self))
        if not verif_method_in[0].get_cdt().is_identical(VERIF_IN):
            raise ValidationError(
                "CustomConstraint \"{}\" verification method does not have an input CDT identical to VERIF_IN".
                format(self))

        if not verif_method_out[0].get_cdt().is_identical(VERIF_OUT):
            raise ValidationError(
                "CustomConstraint \"{}\" verification method does not have an output CDT identical to VERIF_OUT".
                format(self))



class CompoundDatatypeMember(models.Model):
    """
    A data type member of a particular CompoundDatatype.
    Related to :model:`archive.models.Dataset`
    Related to :model:`metadata.models.CompoundDatatype`
    """

    compounddatatype = models.ForeignKey(
        "CompoundDatatype",
        related_name="members",
        help_text="Links this DataType member to a particular CompoundDataType");

    datatype = models.ForeignKey(
        Datatype,
        help_text="Specifies which DataType this member is");

    column_name = models.CharField(
        "Column name",
        blank=False,
        max_length=128,
        help_text="Gives datatype a 'column name' as an alternative to column index");

    # MinValueValidator(1) constrains column_idx to be >= 1
    column_idx = models.PositiveIntegerField(
        validators=[MinValueValidator(1)],
        help_text="The column number of this DataType");

    # Define database indexing rules to ensure tuple uniqueness
    # A compoundDataType cannot have 2 member definitions with the same column name or column number
    class Meta:
        unique_together = (("compounddatatype", "column_name"),
                           ("compounddatatype", "column_idx"));

    def __unicode__(self):
        """Describe a CompoundDatatypeMember with it's column number, datatype name, and column name"""

        returnString = u"{}: <{}> [{}]".format(self.column_idx,
                                               unicode(self.datatype),
                                               self.column_name);

        return returnString

class CompoundDatatype(models.Model):
    """
    A definition of a structured collection of datatypes,
    the resultant data structure serving as inputs or outputs
    for a Transformation.

    Related to :model:`copperfish.CompoundDatatypeMember`
    Related to :model:`copperfish.Dataset`
    """

    # Implicitly defined:
    #   members (CompoundDatatypeMember/ForeignKey)
    #   Conforming_datasets (Dataset/ForeignKey)

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def __unicode__(self):
        """ Represent CompoundDatatype with a list of it's members """

        string_rep = u"(";

        # Get the members for this compound data type
        all_members = self.members.all();

        # A) Get the column index for each member
        member_indices = [member.column_idx for member in all_members];

        # B) Get the column index of each Datatype member, along with the Datatype member itself
        members_with_indices = [ (member_indices[i], all_members[i]) for i in range(len(all_members))];
        # Can we do this?
        # members_with_indices = [ (all_members[i].column_idx, all_members[i])
        #                          for i in range(len(all_members))];

        # Sort members using column index as a basis (operator.itemgetter(0))
        members_with_indices = sorted(  members_with_indices,
                                        key=operator.itemgetter(0));

        # Add sorted Datatype members to the string representation
        for i, colIdx_and_member in enumerate(members_with_indices):
            colIdx, member = colIdx_and_member;
            string_rep += unicode(member);

            # Add comma if not at the end of member list
            if i != len(members_with_indices) - 1:
                string_rep += ", ";

        string_rep += ")";

        if string_rep == "()":
            string_rep = "[empty CompoundDatatype]";

        return string_rep;

    # clean() is executed prior to save() to perform model validation
    def clean(self):
        """Check if Datatype members have consecutive indices from 1 to n"""
        column_indices = [];

        # += is shorthand for extend() - concatenate a list with another list
        for member in self.members.all():
            member.full_clean()
            column_indices += [member.column_idx]

        # Check if the sorted list is exactly a sequence from 1 to n
        if sorted(column_indices) != range(1, self.members.count()+1):
            raise ValidationError("Column indices are not consecutive starting from 1");

    def is_restriction(self, other_CDT):
        """
        True if this CDT is a column-wise restriction of its parameter.

        This is trivially true if they are the same CDT; otherwise
        the column names must be exactly the same and each column
        of this CDT is a restriction of the corresponding column
        of the parameter CDT.

        Note that this induces a partial order on CDTs.

        PRE: this CDT and other_CDT are clean.
        """
        if self == other_CDT:
            return True
        
        # Make sure they have the same number of columns.
        if self.members.count() != other_CDT.members.count():
            return False

        # Since they have the same number of columns at this point,
        # and we have enforced that the numbering of members is
        # consecutive starting from one, we can go through all of this
        # CDT's members and look for the matching one.
        for member in self.members.all():
            counterpart = other_CDT.members.get(
                column_idx=member.column_idx)
            if (member.column_name != counterpart.column_name or
                    not member.datatype.is_restriction(
                        counterpart.datatype)):
                return False
        
        # Having reached this point, this CDT must be a restriction
        # of other_CDT.
        return True

    def is_identical(self, other_CDT):
        """
        True if this CDT is identical with its parameter; False otherwise.
        
        This is trivially true if they are the same CDT; otherwise
        the column names and column types must be exactly the same.

        PRE: this CDT and other_CDT are clean.
        """
        return (self.is_restriction(other_CDT) and
                other_CDT.is_restriction(self))


    def _check_header(self, header):
        """
        SYNOPSIS
        Verify that a list of field names (which we presumably read from a file) 
        matches the anticipated header for this CompoundDatatype. This is a helper
        function for summarize_CSV.

        INPUTS
        header  list of fields forming a header, to check against this
                CompoundDatatype's expected header

        OUTPUTS
        A dictionary with keys indicating header errors. Possible key: value
        pairs are the following.

            - bad_num_cols: length of fieldnames, which does not match number
              of members of this CompoundDatatype.
            - bad_col_indices: list of column indices which do not have the same
              name as the corresponding CompoundDatatypeMember. Will only be
              present if the number of columns is correct.

        """
        summary = {}
        if len(header) != self.members.count():
            summary["bad_num_cols"] = len(header)
            self.logger.debug("number of CSV columns must match number of CDT members")
            return summary
    
        # The ith cdt member must have the same name as the ith CSV header.
        bad_col_indices = []
        for cdtm in self.members.all():
            if cdtm.column_name != header[cdtm.column_idx-1]:
                bad_col_indices.append(cdtm.column_idx)
                self.logger.debug("Incorrect header for column {}".format(cdtm.column_idx))

        if bad_col_indices:
            summary["bad_col_indices"] = bad_col_indices
        
        return summary

    def _columns_with_cc(self):
        """
        SYNOPSIS
        Return a list of the column indices of this CompoundDatatype which have
        a custom constraint. This is a helper function for summarize_CSV.
        """
        return [m.column_idx for m in self.members.all() if m.datatype.has_custom_constraint()]

    def _setup_verification_path(self, column_index, summary_path):
        """
        Set up a path on the file system where we will run the verification
        method for the column of this CompoundDatatype with the index
        column_index. This is a helper function for summarize_CSV.

        INPUTS
        column_index        index of the column which we are going to verify.
        summary_path        top-level directory in which the checks are happening,
                            where we are going to make subdirectories to do the
                            verification.

        OUTPUTS
        input_file_path     a file name where the data to verify should be written to.
        """
        verif_in = CompoundDatatype.objects.get(pk=CDTs.VERIF_IN_PK)
        column_test_path = os.path.join(summary_path, "col{}".format(column_index))
    
        # Set up the paths
        # [testing path]/col[colnum]/
        # [testing path]/col[colnum]/input_data/
        # [testing path]/col[colnum]/output_data/
        # [testing path]/col[colnum]/logs/
        
        # We will use the first to actually run the script; the input file will
        # go into the second; the output will go into the third; output and
        # error logs go into the fourth.

        input_data = os.path.join(column_test_path, "input_data")
        output_data = os.path.join(column_test_path, "output_data")
        logs = os.path.join(column_test_path, "logs")
        for workdir in [input_data, output_data, logs]:
            set_up_directory(workdir)

        input_file_path = os.path.join(column_test_path, "input_data", "to_test.csv")
        
        # Write a CSV header.
        with open(input_file_path, "wb") as f:
            verif_in_header = [m.column_name for m in verif_in.members.all()]
            writer = csv.DictWriter(f, fieldnames=verif_in_header)
            writer.writeheader()

        return input_file_path

    def _check_basic_constraints(self, data_reader, out_handles):
        """
        Check the basic constraints on a CSV file, and copy the contents of
        each column to the file handle indicated in out_handles. Return the
        number of rows processed, and a dictionary of cells where a
        BasicConstraint was not satisfied. Outputs a tuple (num_rows,
        failing_cells). This is a helper function for summarize_CSV.
        TODO: Make out_handles CSV writers or DictWriters, not file handles.

        INPUTS
        data_reader     csv.DictReader object, open on the CSV file we wish
                        to check.
        out_handles     dictionary of file handles, keyed by column index,
                        where the column should be copied to. If the column
                        index is not present in the dictionary, don't copy the
                        column anywhere.

        OUTPUTS
        num_rows        the number of rows which were processed.
        failing_cells   a dictionary of failed BasicContraints for cells in
                        the CSV. Key is (row, column), and value is a list of
                        BasicConstraints which the cell failed.
        """
        failing_cells = {}
        rownum = 0
        for i, row in enumerate(data_reader):
            rownum = i+1
                
            for cdtm in self.members.all():
                colnum = cdtm.column_idx
                curr_cell_value = row[cdtm.column_name]
                test_result = cdtm.datatype.check_basic_constraints(curr_cell_value)
                    
                # Note that i is 0-based, but our rows should be 1-based.
                if test_result:
                    failing_cells[(rownum, colnum)] = test_result
    
                if colnum in out_handles:
                    out_handles[colnum].write(curr_cell_value + "\n")
    
        return (rownum, failing_cells)

    def _check_verification_output(self, column_index, output_path, num_rows):
        """
        Check the one-column CSV file, contained at output_path, which was output
        by a verification method for the Datatype member with index column_index.
        This is a helper function for summarize_CSV.

        INPUTS
        output_path     the CSV file to check, which was output by a verification method
        column_index    index of the CompoundDatatypeMember for which a verification was
                        run, resulting in the file at output_path
        num_rows        the number of rows in the CSV which was verified

        OUTPUTS
        failing_cells   a dictionary of CustomConstraints which were failed in the
                        original CSV, as indicated by the verification method's output.
                        Keys are (row, column), and values are lists of failed custom
                        constraints (currently, these lists may only be of length 1, since
                        a Datatype may only have one CustomConstraint and we do not check
                        them recursively).
        """
        VERIF_OUT = CompoundDatatype.objects.get(pk=CDTs.VERIF_OUT_PK)
        corresp_DT = self.members.get(column_idx=column_index).datatype
        summary_path = os.path.split(output_path)[0]
        for i in range(2):
            summary_path = os.path.split(summary_path)[0]

        if not os.path.exists(output_path):
            raise ValueError(error_messages["verification_no_output"].
                    format(column_index, self))

        # Now: open the resulting file, which is at output_path, and make sure
        # it's OK.  We're going to have to call summarize_CSV on this resulting
        # file, but that's OK because it must have a CDT (NaturalNumber
        # failed_row), and we will define NaturalNumber to have no
        # CustomConstraint, so that no deeper recursion will happen.
        with open(output_path, "rb") as test_out:
            output_summary = VERIF_OUT.summarize_CSV(test_out, 
                os.path.join(summary_path, "SHOULDNEVERBEWRITTENTO"))

        if output_summary.has_key("bad_num_cols"):
            raise ValueError(
                "Output of verification method for Datatype \"{}\" had the wrong number of columns".
                format(corresp_DT))

        if output_summary.has_key("bad_col_indices"):
            raise ValueError(
                "Output of verification method for Datatype \"{}\" had a malformed header".
                format(corresp_DT))

        if output_summary.has_key("failing_cells"):
            raise ValueError(
                "Output of verification method for Datatype \"{}\" had malformed entries".
                format(corresp_DT))

        # This should really never happen.
        # Should this really be a value error? The previous checks are for
        # problems with the user's code, but this one is for ours. Seems
        # inconsistent. -RM
        if os.path.exists(os.path.join(summary_path, "SHOULDNEVERBEWRITTENTO")):
            raise ValueError(
                "Verification output CDT \"{}\" has been corrupted".
                format(VERIF_OUT))

        # Collect the row numbers of incorrect entries in this column.
        failing_cells = {}
        with open(output_path, "rb") as test_out:
            test_out_csv = csv.reader(test_out)
            next(test_out_csv) # skip header
            for row in test_out_csv:
                if int(row[0]) > num_rows:
                    raise ValueError(error_messages["verification_large_row"].
                            format(corresp_DT, row[0], self, num_rows))
                failing_cells[(int(row[0]), column_index)] = [corresp_DT.custom_constraint]

        return failing_cells

    def _check_custom_constraint(self, column_index, input_path,
            content_check_log, num_rows):
        """
        SYNOPSIS
        Check the one-column CSV file file stored at input_path against the
        CustomConstraint of the column_index column of this CompoundDatatype.
        Create a new VerificationLog, which records the running of the
        verification method, pointing to the provided ContentCheckLog.
        This is a helper function for summarize_CSV.

        INPUTS
        column_index        index of the column whose CustomConstraint we will
                            verify on the file
        input_path          one-column CSV to be checked
        content_check_log   this function is called during a check of the
                            contents of a CSV file - this parameter is the log
                            created for that check
        num_rows            the number of rows in the CSV to be checked

        OUTPUTS
        failing_cells       a dictionary of cells which failed a custom
                            constraint (see summarize_CSV)

        ASSUMPTIONS 
        1) input_path has been returned from _setup_verification_path for this
        column index. This means it is in the folder 
        [testing path]/col[colnum]/input_data.
        """
        # We need to invoke the verification method using run_code.
        # All of our inputs are in place.
        corresp_DTM = self.members.get(column_idx=column_index)
        verif_method = corresp_DTM.datatype.custom_constraint.verification_method

        # Go up two levels.
        dir_to_run = os.path.split(os.path.split(input_path)[0])[0]
        output_path = os.path.join(dir_to_run, "output_data", "is_valid.csv")
        stdout_path = os.path.join(dir_to_run, "logs", "stdout.txt")
        stderr_path = os.path.join(dir_to_run, "logs", "stderr.txt")

        # TODO: There is still a bit of duplication here, namely in filling
        # out the log. Perhaps put it into run_code_with_streams.
        with open(stdout_path, "wb") as out, open(stderr_path, "wb") as err:
            verif_log = VerificationLog(contentchecklog=content_check_log,
                    CDTM = corresp_DTM)
            verif_log.save()
            return_code = verif_method.run_code_with_streams(dir_to_run, 
                    [input_path], [output_path], 
                    [out, sys.stdout], [err, sys.stderr])
            verif_log.end_time = timezone.now()
            verif_log.return_code = return_code

        with open(stdout_path, "rb") as out, open(stderr_path, "rb") as err:
            verif_log.output_log.save(stdout_path, File(out))
            verif_log.error_log.save(stderr_path, File(err))

        verif_log.complete_clean()

        return self._check_verification_output(column_index, output_path,
                num_rows)
    
    def summarize_CSV(self, file_to_check, summary_path, content_check_log=None):
        """
        SYNOPSIS
        Give metadata on the CSV: number of rows, and any deviations
        from the CDT (defects).

        INPUTS
        file_to_check       open file object set to the beginning
        summary_path        if any column of this CompoundDatatype has
                            CustomConstraints, checking a CSV file will require
                            running a verification method. summary_path is the
                            work directory where we will do that
        content_check_log   summarize_CSV is called as part of a content check
                            on a SymbolicDataset; this is the log of that check

        OUTPUT
        summary             a dict containing metadata about the CSV, whose keys
                            may be any of the following:

        - bad_num_cols: set if header has wrong number of columns;
          if so, returns number of columns in the header.
        - bad_col_indices: set if header has improperly named columns;
          if so, returns list of indices of bad columns
        - num_rows: number of rows
        - failing_cells: dict of non-conforming cells in the file.
          Entries keyed by (rownum, colnum) contain list of tests failed.

        ASSUMPTIONS
        1) content_check_log may only be None if this function is being called
        to check the output of a verification method (ie. we are verifying that
        file_to_check matches VERIF_OUT). 
        """
        # A CSV reader which we will use to check individual 
        # cells in the file, as well as creating external CSVs
        # for columns whose DT has a CustomConstraint.
        data_csv = csv.DictReader(file_to_check)
        if data_csv.fieldnames is None:
            self.logger.warning("file is empty")
            return {}
    
        # CHECK HEADER
        self.logger.debug("Checking header")
        header = data_csv.fieldnames
        summary = self._check_header(header)
        summary["header"] = header

        # If the header was malformed, just return the summary. We don't keep
        # checking constraints.
        if summary.has_key("bad_num_cols") or summary.has_key("bad_col_indices"):
            return summary

        # CHECK CONSTRAINTS
        # Check if any columns have CustomConstraints.  We will use this lookup
        # table while we're reading through the CSV file to see which columns
        # need to be copied out for checking against CustomConstraints.
        self.logger.debug("Retrieving columns with custom constraints")
        cols_with_cc = dict.fromkeys(self._columns_with_cc())
        self.logger.debug("{} columns with custom constrains found".format(len(cols_with_cc)))

        # Each column with custom constraints gets a file handle where 
        # the results of the verification method will be written.
        try:
            for column in cols_with_cc:
                self.logger.debug("Setting up verification path for column {}".
                        format(summary_path, column))
                input_file_path = self._setup_verification_path(column, summary_path)
                self.logger.debug("Verification path was set up, column will be written to {}".
                        format(input_file_path))
                cols_with_cc[column] = open(input_file_path, "ab")

            # CHECK BASIC CONSTRAINTS AND COUNT ROWS
            self.logger.debug("Checking basic constraints")
            num_rows, failing_cells = self._check_basic_constraints(data_csv, cols_with_cc)
            summary["num_rows"] = num_rows
            self.logger.debug("Checked basic constraints for {} rows".
                    format(num_rows))
    
        finally:
            for col in cols_with_cc:
                cols_with_cc[col].close()

        # CHECK CUSTOM CONSTRAINTS
        # Now: any column that had a CustomConstraint must be checked 
        # using the specified verification method. The handles in cols_with_cc
        # are all closed.
        if cols_with_cc:
            self.logger.debug("Checking custom constraints")
        for col in cols_with_cc:
            for k, v in self._check_custom_constraint(col, cols_with_cc[col].name,
                    content_check_log, summary["num_rows"]).items():
                if k in failing_cells:
                    failing_cells[k].extend(v)
                else:
                    failing_cells[k] = v
        self.logger.debug("{} cells failed constraints".format(len(failing_cells)))
    
        # If there are any failing cells, then add the dict to summary.
        if failing_cells:
            summary["failing_cells"] = failing_cells
    
        return summary

    def count_conforming_datasets (self):
        """
        Returns the number of Datasets that conform to this CompoundDatatype.
        Is this even possible?
        """
        return 0

    num_conforming_datasets = property(count_conforming_datasets)
