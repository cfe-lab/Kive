"""
metadata.models

Shipyard data models relating to metadata: Datatypes and their related
paraphernalia, CompoundDatatypes, etc.
"""
from __future__ import unicode_literals

from django.db import models, transaction
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.core.validators import MinValueValidator, RegexValidator
from django.http import Http404
from django.utils.encoding import python_2_unicode_compatible
from django.contrib.auth.models import User, Group
from django.db.models import Q
from django.core.urlresolvers import reverse
import six

import re
import csv
import math
from datetime import datetime
import json
import itertools

from constants import datatypes, CDTs, maxlengths, groups, users

import logging
from portal.views import admin_check
from archive.exceptions import SandboxActiveException, RunNotFinished

LOGGER = logging.getLogger(__name__)  # Module level logger.


# We delete objects in this order:
deletion_order = [
    "ExecRecords", "Datasets", "Runs", "Pipelines", "PipelineFamilies", "Methods",
    "MethodFamilies", "CompoundDatatypes", "Datatypes",
    "CodeResourceRevisions", "CodeResources", "DockerImages", "Containers",
    "ContainerFamilies"
]


# A helper for remove_helper and redact_helper, in archive.models.
def any_runs_in_progress(plan_of_attack):

    for sd in plan_of_attack["Datasets"]:
        for rtp_input in sd.runinputs.all():
            curr_run = rtp_input.run
            if not curr_run.is_complete() and not curr_run.is_pending():
                return True

    for er in plan_of_attack["ExecRecords"]:
        for affected_rc in er.used_by_components.all():
            curr_run = affected_rc.top_level_run
            if not curr_run.is_complete() and not curr_run.is_pending():
                return True

    if "Pipelines" in plan_of_attack:  # Redaction plans don't have this key
        for pipeline in plan_of_attack["Pipelines"]:
            for run in pipeline.pipeline_instances.all():
                if not run.is_complete() and not run.is_pending():
                    return True

    return False


@transaction.atomic
def remove_helper(removal_plan):
    # If we're affecting anything that's currently running, stop immediately.

    if any_runs_in_progress(removal_plan):
        raise RunNotFinished("Cannot remove: an affected run is still in progress")

    # Redact any sandboxes tied to Runs that we're removing.
    for run in removal_plan["Runs"]:
        try:
            if not run.purged:
                run.collect_garbage()
        except ObjectDoesNotExist:
            # There's no associated RunToProcess, and therefore no sandbox path.
            pass
        except (SandboxActiveException, OSError) as e:
            LOGGER.warning(e)

    for execrecord in removal_plan["ExecRecords"]:
        execlog = execrecord.generator
        methodoutput = getattr(execlog, 'methodoutput', None)
        if methodoutput is not None:
            for f in (methodoutput.output_log, methodoutput.error_log):
                if f is not None:
                    f.delete()

    for class_name in deletion_order:
        if class_name in removal_plan:
            for obj_to_delete in removal_plan[class_name]:
                try:
                    obj_to_delete.refresh_from_db()
                    obj_to_delete.delete()
                except ObjectDoesNotExist:
                    pass


def update_removal_plan(orig_dict, updating_dict):
    """
    Helper to update dictionaries of sets of objects to remove/redact.
    """
    for key in updating_dict.keys():
        if key in orig_dict:
            orig_dict[key].update(updating_dict[key])
        else:
            orig_dict[key] = updating_dict[key]

    return orig_dict


def empty_removal_plan():
    removal_plan = {}
    for key in deletion_order:
        removal_plan[key] = set()
    # Track any Datasets associated with external files.
    removal_plan["ExternalFiles"] = set()
    return removal_plan


def kive_user():
    return User.objects.get(pk=users.KIVE_USER_PK)


def everyone_group():
    return Group.objects.get(pk=groups.EVERYONE_PK)


def get_builtin_types(source_datatypes):
    """
    Retrieves the built-in types of all source_datatypes passed as input.

    Returns a set (to remove duplicates) with all of the built-in
    types represented in the inputs.

    INPUTS
    source_datatypes    iterable of Datatypes

    OUTPUT
    builtins            set of built-in Datatypes represented by source_datatypes

    ASSUMPTIONS
    All Datatypes in source_datatypes are clean and complete.
    """
    return set([datatype.get_builtin_type() for datatype in source_datatypes])


def summarize_csv(columns, data_csv):
    """
    SYNOPSIS
    Inspect a CSV file, whose columns are expected to contain instances
    of the provided datatypes. Report any deviations from the datatypes,
    as well as the number of rows and the header.

    INPUTS
    columns             list of either Datatypes or CompoundDatatypeMembers
                        to which the columns of the file are supposed to conform
    data_csv            csv.reader object set to the first line of data (NOT
                        the header)
    summary_path        working directory to run verification methods in FIXME get rid of this
    content_check_log   should be provided if this function is called as part
                        of a check on a Dataset

    OUTPUT
    summary             a dict containing metadata about the CSV, whose keys
                        may be any of the following:

    - num_rows: number of rows
    - failing_cells: dict of non-conforming cells in the file,
      entries keyed by (row_num, colnum) contain list of tests failed.

    ASSUMPTIONS
    1) content_check_log may only be None if this function is being called
    with Datatypes as columns, not CompoundDatatypeMembers.
    2) the file has the correct number of columns (ie. the same number as
    the length of the columns parameter).
    """
    summary = {}

    # Check basic constraints and count rows.
    failing_cells = {}
    row_num = 0
    for row_num, row in enumerate(data_csv, start=1):
        for col_num, col in enumerate(columns, start=1):
            curr_cell_value = row[col_num-1] if col_num <= len(row) else ''
            test_result = col.check_basic_constraints(curr_cell_value)

            if test_result:
                LOGGER.debug('Value "{}" failed basic constraints'.format(curr_cell_value))
                failing_cells[(row_num, col_num)] = test_result

    summary["num_rows"] = row_num
    plural = "" if row_num == 1 else "s"
    LOGGER.debug("Checked basic constraints for {} row{}".format(row_num, plural))

    # If there are any failing cells, then add the dict to summary.
    if failing_cells:
        plural = "" if len(failing_cells) == 1 else "s"
        LOGGER.debug("{} cell{} failed constraints".format(len(failing_cells), plural))
        summary["failing_cells"] = failing_cells

    return summary


def who_cannot_access(user, users_allowed, groups_allowed, acs):
    """
    Tells which of the specified users and groups cannot access all
    of the AccessControl objects specified.

    user: a User
    users_allowed: an iterable of Users
    groups_allowed: an iterable of Groups
    acs: a list of AccessControl instances.

    NOTE: This routine returns subsets of users_allowed and groups_allowed only.
    E.g. if these are empty sets, then empty sets will be returned as well.
    """
    allowed_users = {user} | set(users_allowed)
    allowed_groups = set(groups_allowed)
    all_defined_users = frozenset(User.objects.all())
    all_defined_groups = frozenset(Group.objects.all())
    ok_user = all_defined_users
    ok_group = all_defined_groups
    for ac in acs:
        has_everyone = ac.groups_allowed.filter(pk=groups.EVERYONE_PK).exists()
        cur_user = all_defined_users if has_everyone else {ac.user} | (set(ac.users_allowed.all()))
        cur_group = all_defined_groups if has_everyone else set(ac.groups_allowed.all())
        ok_user &= cur_user
        ok_group &= cur_group
    # Special case: everyone is allowed access to all of the elements of acs.
    if everyone_group() in ok_group:
        return set(), set()
    else:
        return allowed_users - ok_user, allowed_groups - ok_group


class KiveUser(User):
    """
    Proxy model that has some convenience functions for Users.
    """
    class Meta:
        proxy = True

    @classmethod
    def kiveify(cls, user):
        return KiveUser.objects.get(pk=user.pk)

    def access_query(self):
        query_object = (Q(user=self) | Q(users_allowed=self) | Q(groups_allowed=groups.EVERYONE_PK) |
                        Q(groups_allowed__in=self.groups.all()))
        return query_object


class AccessControl(models.Model):
    """
    Represents anything that belongs to a certain user.
    """
    user = models.ForeignKey(User)
    users_allowed = models.ManyToManyField(
        User,
        related_name="%(app_label)s_%(class)s_has_access_to",
        help_text="Which users have access?",
        blank=True
    )
    groups_allowed = models.ManyToManyField(
        Group,
        related_name="%(app_label)s_%(class)s_has_access_to",
        help_text="What groups have access?",
        blank=True
    )

    # Avoid PyCharm warnings. These get overwritten later with the real thing.
    objects = None
    DoesNotExist = None

    class Meta:
        abstract = True

    @property
    def shared_with_everyone(self):
        return self.groups_allowed.filter(pk=groups.EVERYONE_PK).exists()

    def can_be_accessed(self, user):
        """
        True if user can access this object; False otherwise.
        """
        if self.shared_with_everyone:
            return True

        if self.user == user or self.users_allowed.filter(pk=user.pk).exists():
            return True

        for group in self.groups_allowed.all():
            if user.groups.filter(pk=group.pk).exists():
                return True

        return False

    @classmethod
    def check_accessible(cls, pk, user):
        # noinspection PyUnresolvedReferences
        try:
            # noinspection PyUnresolvedReferences
            record = cls.objects.get(pk=pk)
            if record.can_be_accessed(user):
                return record
        except cls.DoesNotExist:
            pass
        raise Http404("PK {} is not accessible".format(pk))

    def extra_users_groups(self, acs):
        """
        Returns a list of what users/groups can access this object that cannot access all of those specified.

        acs: a list of AccessControl instances.
        """
        return who_cannot_access(self.user, self.users_allowed.all(), self.groups_allowed.all(), acs)

    @staticmethod
    def validate_restrict_access_raw(user, users_allowed, groups_allowed, acs):
        """
        Checks that the specified permissions don't exceed those on the specified objects.
        """
        # Trivial case: no objects to restrict.
        if len(acs) == 0:
            return set(), set()

        extra_users, extra_groups = who_cannot_access(user, users_allowed, groups_allowed, acs)
        if len(extra_users) > 0:
            if len(extra_users) == 1 and user in extra_users:
                # If this user has access via the groups allowed on all of the elements of acs,
                # then we're OK.
                if all([x.can_be_accessed(user) for x in acs]):
                    extra_users = []

        return extra_users, extra_groups

    def validate_restrict_access(self, acs):
        """
        Checks whether access is restricted to those that can access all of the specified objects.
        """
        # If this instance is not saved, then bail as we can't access users_allowed or groups_allowed.
        if not self.pk:
            return

        bad_users, bad_groups = AccessControl.validate_restrict_access_raw(
            self.user,
            self.users_allowed.all(),
            self.groups_allowed.all(),
            acs
        )

        users_error = None
        groups_error = None
        if len(bad_users) > 0:
            users_error = ValidationError(
                'User(s) %(users_str)s cannot be granted access',
                code="extra_users",
                params={"users_str": ", ".join([str(x) for x in bad_users])}
            )

        if len(bad_groups) > 0:
            groups_error = ValidationError(
                'Group(s) %(groups_str)s cannot be granted access',
                code="extra_groups",
                params={"groups_str": ", ".join([str(x) for x in bad_groups])}
            )

        if users_error is not None and groups_error is not None:
            raise ValidationError([users_error, groups_error])
        elif users_error is not None:
            raise users_error
        elif groups_error is not None:
            raise groups_error

    def validate_identical_access(self, ac):
        """
        Check that this instance has the same access as the specified one.
        """
        if self.user != ac.user:
            raise ValidationError(
                "Instances have different users: %s, %s" % (self.user, ac.user), code="different_user"
            )

        non_overlapping_users_allowed = set(self.users_allowed.all()).symmetric_difference(ac.users_allowed.all())
        if len(non_overlapping_users_allowed) > 0:
            raise ValidationError(
                "Instances allow different users access", code="different_users_allowed"
            )

        non_overlapping_groups_allowed = set(self.groups_allowed.all()).symmetric_difference(ac.groups_allowed.all())
        if len(non_overlapping_groups_allowed) > 0:
            raise ValidationError(
                "Instances allow different groups access", code="different_groups_allowed"
            )

    @classmethod
    def filter_by_user(cls, user, is_admin=False, queryset=None):
        """ Retrieve a QuerySet of all records of this class that are visible
            to the specified user.

        @param user: user that must be able to see the records
        @param is_admin: override the filter, and just return all records.
        @param queryset: add the filter to an existing queryset instead of
            cls.objects.all()
        @raise StandardError: if is_admin is true, but user is not in the
            administrator group.
        """
        if queryset is None:
            queryset = cls.objects.all()
        if is_admin:
            if not admin_check(user):
                raise Exception('User is not an administrator.')
        else:
            user_plus = KiveUser.kiveify(user)
            allowed_items = queryset.filter(user_plus.access_query())
            queryset = queryset.filter(pk__in=allowed_items)
        return queryset

    def grant_everyone_access(self):
        self.groups_allowed.add(Group.objects.get(pk=groups.EVERYONE_PK))

    def grant_from_json(self, permissions_json):
        """
        Given a JSON string as produced by a PermissionsField, add permissions to this object.
        """
        permissions = json.loads(permissions_json)
        users_to_grant = User.objects.filter(username__in=permissions[0])
        groups_to_grant = Group.objects.filter(name__in=permissions[1])
        self.grant_from_permissions_list([users_to_grant, groups_to_grant])

    def grant_from_permissions_list(self, permissions_list):
        """
        Given a list with two entries (one iterable of users and one of groups), add permissions.
        """
        self.users_allowed.add(*permissions_list[0])
        self.groups_allowed.add(*permissions_list[1])

    def copy_permissions(self, source):
        """ Copy users_allowed and groups_allowed from the source object.

        @param source: another AccessControl object
        """
        self.grant_from_permissions_list((source.users_allowed.all(),
                                          source.groups_allowed.all()))

    def intersect_permissions(self, users_qs=None, groups_qs=None):
        """
        Intersects the parameter QuerySets with this object's permissions.
        """
        eligible_user_pks = itertools.chain([self.user.pk],
                                            self.users_allowed.values_list("pk", flat=True))

        users_qs = users_qs if users_qs is not None else User.objects.all()
        groups_qs = groups_qs if groups_qs is not None else Group.objects.all()

        # If the Everyone group has access to this object, then we don't filter.
        if not self.groups_allowed.filter(pk=groups.EVERYONE_PK).exists():
            users_qs = users_qs.filter(pk__in=eligible_user_pks)
            groups_qs = groups_qs.filter(
                pk__in=self.groups_allowed.values_list("pk", flat=True)
            )

        return users_qs, groups_qs

    def other_users_groups(self):
        """
        Returns users and groups that don't already have access.
        """
        user_pks_already_allowed = self.users_allowed.values_list("pk", flat=True)
        group_pks_already_allowed = self.groups_allowed.values_list("pk", flat=True)

        addable_users = User.objects.exclude(
            pk__in=itertools.chain([self.user.pk], user_pks_already_allowed)
        )
        addable_groups = Group.objects.exclude(pk__in=group_pks_already_allowed)

        return addable_users, addable_groups


@python_2_unicode_compatible
class Datatype(AccessControl):
    """
    Abstract definition of a semantically atomic type of data.
    Related to :model:`metadata.models.CompoundDatatype`
    """
    name = models.CharField("Datatype name",
                            max_length=maxlengths.MAX_NAME_LENGTH,
                            help_text="The name for this Datatype")
    description = models.TextField("Datatype description",
                                   help_text="A description for this Datatype",
                                   max_length=maxlengths.MAX_DESCRIPTION_LENGTH)

    # auto_now_add: set to now on instantiation (editable=False)
    date_created = models.DateTimeField("Date created", auto_now_add=True, help_text="Date Datatype was defined")

    restricts = models.ManyToManyField(
        'self',
        symmetrical=False,
        related_name="restricted_by",
        blank=True,
        help_text="Captures hierarchical is-a classifications among Datatypes"
    )

    prototype = models.OneToOneField("librarian.Dataset", null=True, blank=True,
                                     related_name="datatype_modelled", on_delete=models.SET_NULL)

    class Meta:
        unique_together = ("user", "name")

    @property
    def restricts_str(self):
        return ','.join([dt['name'] for dt in self.restricts.values()])

    def __init__(self, *args, **kwargs):
        super(Datatype, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.effective_constraints = {}

    def __str__(self):
        return self.name

    @property
    def absolute_url(self):
        return reverse("datatype_detail", kwargs={"id": self.pk})

    @staticmethod
    def parse_boolean(string):
        """
        Parse a string as a boolean, returning None if it could not be parsed.
        """
        true_ptn = re.compile(r"(^True$)|(^true$)|(^TRUE$)|^T$|^t$|^1$")
        false_ptn = re.compile(r"(^False$)|(^false$)|(^FALSE$)|^F$|^f$|^0$")
        if true_ptn.match(string):
            return True
        elif false_ptn.match(string):
            return False
        return None

    @staticmethod
    def parse_numeric(string):
        """
        Parse a string as a number, returning an integer if it looks
        like one, otherwise a float.
        """
        try:
            return int(string)
        except ValueError:
            try:
                return float(string)
            except ValueError:
                return None

    def has_restriction(self):
        """
        Does this Datatype restrict any others? Note that a Datatype is
        not complete if it has no restrictions.
        """
        return hasattr(self, "restricts")

    def has_prototype(self):
        """
        Does this Datatype have a prototype defined?
        """
        return self.prototype is not None

    def has_basic_constraints(self):
        """
        Does this Datatype have any basic constraints?
        """
        return hasattr(self, "basic_constraints")

    def is_numeric(self):
        """
        Does this Datatype restrict an INT or a FLOAT?
        """
        return self.get_builtin_type().pk in datatypes.NUMERIC_BUILTIN_PKS

    def is_string(self):
        """
        Does this Datatype restrict a STR?
        """
        return self.get_builtin_type().pk == datatypes.STR_PK

    def is_restricted_by(self, possible_restrictor_datatype):
        """
        Determine if this datatype is ever *properly* restricted,
        directly or indirectly, by a given datatype.

        PRE: there is no circular restriction in the possible restrictor
        datatype (this would cause an infinite recursion).
        """
        for restrictedDataType in possible_restrictor_datatype.restricts.all():
            # Case 1: If restrictions restrict self, return true
            # Case 2: Check if any restricted Datatypes themselves restrict self
            # If any restricted Datatypes themselves restrict self, propagate
            # this information to the parent Datatype as restricting self
            if restrictedDataType.is_restriction(self):
                return True
        # Return False if Cases 1 or 2 are never encountered
        return False

    def is_restriction(self, possible_restricted_datatype):
        """
        True if this Datatype restricts the parameter, directly or indirectly.

        This induces a partial ordering A <= B if A is a restriction of B.
        For example, a DNA sequence is a restriction of a string.
        """
        return (self == possible_restricted_datatype or possible_restricted_datatype.is_restricted_by(self))

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
        if BC_type in self.effective_constraints:
            return self.effective_constraints[BC_type]

        min_or_max = min if BC_type in (BasicConstraint.MIN_LENGTH, BasicConstraint.MIN_VAL) else max
        numeric = BC_type in (BasicConstraint.MIN_VAL, BasicConstraint.MAX_VAL)

        # Default values. If numeric, min -oo and max oo. If not, min 0, max oo.
        effective_val = min_or_max(float("inf"), -float("inf") if numeric else 0)

        # If it's a length constraint on a number, or a value constraint
        # on not a number, the effective constraint is None. Arguably,
        # this should be a ValueError.
        effective_BC = None
        if (numeric and self.is_numeric()) or (not numeric and self.is_string()):
            my_BC = self.basic_constraints.filter(ruletype=BC_type)
            if my_BC.exists():
                effective_BC = my_BC.first()
                effective_val = Datatype.parse_numeric(my_BC.first().rule)

            else:
                # If this instance has no supertypes, we don't touch
                # effective_BC or effective_val.
                if self.has_restriction():
                    for supertype in self.restricts.all():
                        # self.logger.debug("Checking supertype \"{}\" with pk={} for BasicConstraints of form \"{}\"".
                        #                   format(supertype, supertype.pk, BC_type))
                        # Recursive case: go through all of the supertypes and take the maximum.
                        supertype_BC, supertype_val = supertype.get_effective_num_constraint(BC_type)

                        if min_or_max(effective_val, supertype_val) == effective_val:
                            effective_BC = supertype_BC
                            effective_val = supertype_val

        result = (effective_BC, effective_val)
        self.effective_constraints[BC_type] = result
        return result

    def get_all_regexps(self):
        """
        Retrieves all of the REGEXP BasicConstraints acting on this
        instance.

        PRE: all of this instance's supertypes are clean (in the Django
        sense), as are this instance's REGEXP BasicConstraints.
        """
        if hasattr(self, "all_regexps"):
            return self.all_regexps
        all_regexp_BCs = list(self.basic_constraints.filter(ruletype=BasicConstraint.REGEXP))
        for supertype in self.restricts.all():
            all_regexp_BCs.extend(supertype.get_all_regexps())
        self.all_regexps = all_regexp_BCs
        return all_regexp_BCs

    def get_effective_datetimeformat(self):
        """
        Retrieves the date-time format string effective for this
        instance.

        There can only be one such format string acting on this or its
        supertypes.  Moreover, this returns None if this instance
        restricts any other atomic type than STR.

        PRE: this instance has at most one DATETIMEFORMAT
        BasicConstraint (and it is clean if it exists), and all its
        supertypes are clean (in the Django sense).
        """
        if BasicConstraint.DATETIMEFORMAT in self.effective_constraints:
            return self.effective_constraints[BasicConstraint.DATETIMEFORMAT]

        result = None
        if self.is_string() and self.pk != datatypes.STR_PK:
            my_dtf = self.basic_constraints.filter(ruletype=BasicConstraint.DATETIMEFORMAT)
            if my_dtf.exists():
                result = my_dtf.first()
            else:
                for supertype in self.restricts.all():
                    result = result or supertype.get_effective_datetimeformat()
                    if result:
                        break

        # If we reach this point, there is no effective datetimeformat constraint.
        self.effective_constraints[BasicConstraint.DATETIMEFORMAT] = result
        return result

    def get_builtin_type(self):
        """
        Get the Shipyard builtin type restricted by this Datatype.

        This retrieves the most restrictive one under the ordering:
        BOOL < INT < FLOAT < STR

        PRE: this Datatype restricts at least one clean Datatype
        (thus at least restricts STR).
        """
        if hasattr(self, "builtin_type"):
            return self.builtin_type

        builtin_type_ids = (datatypes.BOOL_PK,
                            datatypes.INT_PK,
                            datatypes.FLOAT_PK,
                            datatypes.STR_PK)

        if self.id in builtin_type_ids:
            self.builtin_type = self
            return self

        for builtin_type_id in builtin_type_ids:
            builtin_type = Datatype.objects.get(id=builtin_type_id)
            if self.is_restriction(builtin_type):
                break
        self.builtin_type = builtin_type
        return builtin_type

    def _clean_restrictions(self):
        """
        Check that this Datatype does not create a circular restriction,
        and that all its supertypes have the same atomic Datatype. This
        is a helper function for clean()

        PRE
        This Datatype has at least one supertype.
        """
        if self.is_restricted_by(self):
            raise ValidationError('Datatype "{}" has a circular restriction'.format(self))

        # February 18, 2014: we do not allow Datatypes to simultaneously restrict
        # supertypes whose "built-in types" are not all either STR, BOOL, or INT/FLOAT
        # (they can be a mix of INT and FLOAT though).
        supertype_builtin_types = set([])
        for supertype in self.restricts.all():
            supertype_builtin_types.add(supertype.get_builtin_type().pk)

        # Primary keys of Shipyard numeric types.
        if len(supertype_builtin_types) > 1 and supertype_builtin_types != datatypes.NUMERIC_BUILTIN_PKS:
            raise ValidationError('Datatype "{}" restricts multiple built-in, non-numeric types'.format(self))

    def _clean_prototype(self):
        """
        Check that this Datatype's prototype is coherent. Here, we don't
        actually check the prototype by running the verification method,
        we just make sure it has the correct structure: it must have a
        CDT with 2 columns: column 1 is a string "example" field, column
        2 is a bool "valid" field.  This CDT will be hard-coded and
        loaded into the database on creation. This is a helper function
        for clean().

        PRE
        This datatype has a prototype.
        """
        PROTOTYPE_CDT = CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK)
        if self.prototype.is_raw():
            raise ValidationError((
                'Prototype Dataset for Datatype "{}" should have '
                'CompoundDatatype "{}", but it is raw').format(self,
                                                               PROTOTYPE_CDT))

        my_prototype_cdt = self.prototype.get_cdt()
        if not my_prototype_cdt.is_identical(PROTOTYPE_CDT):
            raise ValidationError((
                'Prototype Dataset for Datatype "{}" should have '
                'CompoundDatatype "{}", but it has "{}"').format(self,
                                                                 PROTOTYPE_CDT,
                                                                 my_prototype_cdt))

        self.prototype.clean()

    def _check_num_constraint_against_supertypes(self, constraint, error_message):
        """
        Check that the minval/maxval/minlen/maxlen constraint on this
        Datatype is at least as restrictive as the equivalent constraint
        on its supertypes. This is a helper function for
        _check_basic_constraints_against_supertypes, which in turn is a
        helper for clean().

        INPUTS
        constraint      the type of constraint to check, one of
                        BasicConstraint.(MIN|MAX)_(VAL|LENGTH)
        error_message   message to raise if the check fails (see code for
                        format)

        PRE
        1) The current Datatype restricts at least one other Datatype
        2) The current Datatype has at most one of the type of constraint
        we are going to check
        """
        my_constraint = self.basic_constraints.filter(ruletype=constraint)
        if not my_constraint.exists():
            return

        my_value = Datatype.parse_numeric(my_constraint.first().rule)
        # TODO: duplicated. Maybe pass in min/max as a parameter?
        min_or_max = min if constraint in (BasicConstraint.MIN_VAL, BasicConstraint.MIN_LENGTH) else max

        for supertype in self.restricts.all():
            supertype_value = supertype.get_effective_num_constraint(constraint)[1]
            if supertype_value == my_value or min_or_max(supertype_value, my_value) == my_value:
                raise ValidationError(error_message.format(self, my_value, supertype, supertype_value))

    def _check_datetime_constraint_against_supertypes(self):
        """
        Check that there is only one DATETIMEFORMAT between this
        Datatype and all of its supertypes. This is a helper function
        for _check_basic_constraints_against_supertypes(), which is in
        turn a helper for clean().

        PRE
        1) The current Datatype restricts at least one other Datatype
        """
        dtf_count = self.basic_constraints.filter(ruletype="datetimeformat").count()
        for supertype in self.restricts.all():
            dtf_count += supertype.basic_constraints.filter(ruletype=BasicConstraint.DATETIMEFORMAT).count()
        if dtf_count > 1:
            raise ValidationError(('Datatype "{}" should have only one DATETIMEFORMAT restriction acting on it, '
                                   'but it has {}'.format(self, dtf_count)))

    def _check_basic_constraints_against_supertypes(self):
        """
        Check constraints for coherence against the supertypes'
        constraints. This is a helper function for clean().

        PRE
        1) This Datatype has at least one supertype
        """
        # Check numerical constraints for coherence against the supertypes' constraints.
        self._check_num_constraint_against_supertypes(
            BasicConstraint.MIN_VAL,
            'Datatype "{}" has MIN_VAL {}, but its supertype "{}" has a larger or equal MIN_VAL of {}')
        self._check_num_constraint_against_supertypes(
            BasicConstraint.MAX_VAL,
            'Datatype "{}" has MAX_VAL {}, but its supertype "{}" has a smaller or equal MAX_VAL of {}')
        self._check_num_constraint_against_supertypes(
            BasicConstraint.MIN_LENGTH,
            'Datatype "{}" has MIN_LENGTH {}, but its supertype "{}" has a longer or equal MIN_LENGTH of {}')
        self._check_num_constraint_against_supertypes(
            BasicConstraint.MAX_LENGTH,
            'Datatype "{}" has MAX_LENGTH {}, but its supertype "{}" has a shorter or equal MAX_LENGTH of {}')
        self._check_datetime_constraint_against_supertypes()

    def _check_constraint_intervals(self):
        """
        Check that the "paired" constraints (ie. min/maxlen, min/maxval)
        for this Datatype make sense. That is, that effective min_val <=
        max_val if applicable, and if this Datatype is an integer, then
        the closed interval [min_val, max_val] actually contains any
        integers. This is a helper function for clean().

        PRE
        1) This Datatype has at least one supertype
        """
        if self.is_numeric():
            min_val = self.get_effective_num_constraint(BasicConstraint.MIN_VAL)[1]
            max_val = self.get_effective_num_constraint(BasicConstraint.MAX_VAL)[1]
            if (min_val > max_val):
                raise ValidationError(('Datatype "{}" has effective MIN_VAL {} exceeding its effective MAX_VAL {}'
                                       .format(self, min_val, max_val)))

            if (self.get_builtin_type().pk == datatypes.INT_PK and math.floor(max_val) < math.ceil(min_val)):
                raise ValidationError((('Datatype "{}" has built-in type INT, but there are no integers between its '
                                        'effective MIN_VAL {} and its effective MAX_VAL {}')
                                       .format(self, min_val, max_val)))

        # Check that effective min_length <= max_length if applicable.
        elif self.is_string():
            min_length = self.get_effective_num_constraint(BasicConstraint.MIN_LENGTH)[1]
            max_length = self.get_effective_num_constraint(BasicConstraint.MAX_LENGTH)[1]
            if (min_length > max_length):
                raise ValidationError(('Datatype "{}" has effective MIN_LENGTH {} exceeding its effective MAX_LENGTH {}'
                                       .format(self, min_length, max_length)))

    def _clean_basic_constraints(self):
        """
        Check the coherence of this Datatype's basic constraints.
        This is a helper for clean(). Note that we don't check
        the values against the supertypes here.
        """
        # Check that there is at most one BasicConstraint of every type except
        # REGEXP directly associated to this instance.
        ruletypes = self.basic_constraints.values("ruletype")
        constraint_counts = ruletypes.annotate(count=models.Count("id"))
        for row in constraint_counts:
            if row["count"] > 1 and row["ruletype"] != "regexp":
                raise ValidationError(('Datatype "{}" has {} constraints of type {}, but should have at most one'
                                       .format(self, row["count"], row["ruletype"])))
        for bc in self.basic_constraints.all():
            bc.clean()

    def _verify_prototype(self):
        """
        Check that Datatype's prototype correctly identifies values as
        being valid or invalid.

        ASSUMPTIONS
        1) This Datatype has a prototype, and it is clean
        """
        self.logger.debug('Checking constraints for Datatype "{}" on its prototype'.format(self))

        with open(self.prototype.dataset_file.path, "rt") as f:
            reader = csv.reader(f)
            next(reader)  # skip header - we already know it's good from cleaning the prototype
            summary = summarize_csv([self, Datatype.objects.get(pk=datatypes.BOOL_PK)], reader)

        try:
            failing_cells = summary["failing_cells"].keys()
        except KeyError:
            failing_cells = []

        with open(self.prototype.dataset_file.path, "r") as f:
            reader = csv.reader(f)
            next(reader)  # skip header again
            for rownum, row in enumerate(reader, start=1):
                # This has to be not None, since the prototype was
                # successfully uploaded.
                valid = Datatype.parse_boolean(row[1])

                if valid and (rownum, 1) in failing_cells:
                    raise ValidationError(('The prototype for Datatype "{}" indicates the value "{}" should be '
                                           'valid, but it failed constraints').format(self, row[0]))
                elif not valid and (rownum, 1) not in failing_cells:
                    raise ValidationError('The prototype for Datatype "{}" indicates the value "{}" should be '
                                          'invalid, but it passed all constraints'.format(self, row[0]))

    def clean(self):
        """
        Checks coherence of this Datatype. Since there is no procedure
        for creating Datatypes except via the constructor, the checks
        can happen in any order.

        Note that a Datatype must be saved into the database before it's
        complete, as a complete Datatype must be a restriction of a
        Shipyard atomic Datatype (STR, INT, FLOAT, or BOOL).  This
        necessitates a complete_clean() routine.
        """
        if self.has_restriction():
            self.logger.debug('Cleaning restrictions on Datatype "{}"'.format(self))
            self._clean_restrictions()

            self.validate_restrict_access(self.restricts.all())

        if self.has_basic_constraints():
            self.logger.debug('Cleaning basic constraints for Datatype "{}"'.format(self))
            self._clean_basic_constraints()
            if self.has_restriction():
                self._check_constraint_intervals()
                self._check_basic_constraints_against_supertypes()

        if self.has_prototype():
            self.logger.debug('Cleaning prototype for Datatype "{}"'.format(self))
            self._clean_prototype()
            self._verify_prototype()

    def is_complete(self):
        """
        Returns whether this Datatype has a complete definition; i.e.
        restricts a Shipyard atomic.
        """
        STR = Datatype.objects.get(pk=datatypes.STR_PK)
        return self.has_restriction() and self.is_restriction(STR)

    def complete_clean(self):
        """
        Checks completeness and coherence of this Datatype.

        First calls clean; then confirms that this Datatype restricts
        a Shipyard atomic Datatype.
        """
        self.clean()
        if not self.is_complete():
            raise ValidationError('Datatype "{}" does not restrict any of the Shipyard atomic Datatypes'.format(self))

    def check_basic_constraints(self, string_to_check):
        """
        Check the specified string against basic constraints.

        This includes both whether or not the string can be interpreted
        as all of the Shipyard atomic types it inherits from, but also
        whether it then checks out against all BasicConstraints.

        If it fails against even the simplest casting test (i.e. the
        string could not be cast to the appropriate Python type), return
        a list containing a describing string.  If not, return a list of
        BasicConstraints that it failed (hopefully it's empty!).

        PRE: this Datatype and by extension all of its BasicConstraints
        are clean.  That means that only the appropriate
        BasicConstraints for this Datatype are applied.
        """
        ####
        # First, determine what Shipyard atomic datatypes this
        # restricts.  Then, check it against any type-specific
        # BasicConstraints (MIN|MAX_LENGTH or DATETIMEFORMAT for
        # strings, MIN|MAX_VAL for numerical types).

        constraints_failed = []
        if self.is_string():
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
        elif self.is_numeric():
            is_int = self.get_builtin_type().pk == datatypes.INT_PK
            parse, type_str = (int, "integer") if is_int else (float, "float")
            try:
                parse(string_to_check)
            except ValueError:
                return ["Was not {}".format(type_str)]

            # Check the numeric-type BasicConstraints.
            eff_min_val_BC, eff_min_val = self.get_effective_num_constraint(BasicConstraint.MIN_VAL)
            if eff_min_val_BC is not None and parse(string_to_check) < eff_min_val:
                constraints_failed.append(eff_min_val_BC)
            else:
                eff_max_val_BC, eff_max_val = self.get_effective_num_constraint(BasicConstraint.MAX_VAL)
                if eff_max_val_BC is not None and parse(string_to_check) > eff_max_val:
                    constraints_failed.append(eff_max_val_BC)

        # Otherwise, it's a boolean.
        else:
            if Datatype.parse_boolean(string_to_check) is None:
                return ["Was not Boolean"]

        ####
        # Check all REGEXP-type BasicConstraints.
        for re_BC in self.get_all_regexps():
            constraint_re = re.compile(re_BC.rule)
            if not constraint_re.search(string_to_check):
                constraints_failed.append(re_BC)

        return constraints_failed

    @transaction.atomic
    def remove(self):
        """
        Remove this Datatype and anything tied to it from the system.
        """
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["Datatypes"]

        builtin_pks = {
            datatypes.STR_PK,
            datatypes.BOOL_PK,
            datatypes.FLOAT_PK,
            datatypes.INT_PK,
            datatypes.NATURALNUMBER_PK
        }
        if self.pk in builtin_pks:
            self.logger.warning("Cannot remove builtin datatypes.")
            return removal_plan

        removal_plan["Datatypes"].add(self)

        if self.prototype is not None:
            # The prototype is a Dataset so we have to check its Dataset.
            if self.prototype not in removal_plan["Datasets"]:
                prototype_removal_plan = self.prototype.build_removal_plan(removal_plan)
                update_removal_plan(removal_plan, prototype_removal_plan)

        for descendant_dt in self.restricted_by.all():
            if descendant_dt not in removal_plan["Datatypes"]:
                removal_plan = update_removal_plan(
                    removal_plan,
                    descendant_dt.build_removal_plan(removal_plan)
                )

        cdts_affected = self.CDTMs.all().values("compounddatatype")
        for cdt in CompoundDatatype.objects.filter(pk__in=cdts_affected):
            if cdt not in removal_plan["CompoundDatatypes"]:
                removal_plan = update_removal_plan(
                    removal_plan,
                    cdt.build_removal_plan(removal_plan)
                )

        return removal_plan


@python_2_unicode_compatible
class BasicConstraint(models.Model):
    """
    Basic (level 1) constraint on a Datatype.

    The admissible constraints are:
     - (min|max)len (string)
     - (min|max)val (numeric)
     - regexp (this will work on anything)
     - datetimeformat (string -- this is a special case)
    """
    datatype = models.ForeignKey(Datatype, related_name="basic_constraints")

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
        validators=[RegexValidator(re.compile("{}|{}|{}|{}|{}|{}".format(MIN_LENGTH,
                                                                         MAX_LENGTH,
                                                                         MIN_VAL,
                                                                         MAX_VAL,
                                                                         REGEXP,
                                                                         DATETIMEFORMAT)))])

    rule = models.CharField("Rule specification", max_length=100)

    def __str__(self):
        """
        Unicode representation of this BasicConstraint.

        The representation takes the form {rule type}={rule}.
        """
        return "{}={}".format(self.ruletype, self.rule)

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
            raise ValidationError('Parent Datatype "{}" of BasicConstraint "{}" is not complete'
                                  .format(self.datatype, self))

        # Check the rule for coherence.
        if self.ruletype in (BasicConstraint.MIN_LENGTH, BasicConstraint.MAX_LENGTH):
            # MIN/MAX_LENGTH should not apply to anything that restricts INT, FLOAT, or BOOL.  Note that INT <= FLOAT.
            if not self.datatype.is_string():
                raise ValidationError('BasicConstraint "{}" specifies a bound on string length, '
                                      'but its parent Datatype "{}" has builtin type {}'
                                      .format(self, self.datatype, self.datatype.get_builtin_type()))
            try:
                length_constraint = int(self.rule)
            except ValueError:
                raise ValidationError('BasicConstraint "{}" specifies a bound of "{}" on string length, '
                                      'which is not an integer'.format(self, self.rule))

            if length_constraint < 1:
                raise ValidationError('BasicConstraint "{}" specifies a bound of "{}" on string length, '
                                      'which is not positive'.format(self, self.rule))

        elif self.ruletype in (BasicConstraint.MAX_VAL, BasicConstraint.MIN_VAL):
            # This should not apply to a non-numeric.
            if not self.datatype.is_numeric():
                raise ValidationError('BasicConstraint "{}" specifies a bound on numeric value, '
                                      'but its parent Datatype "{}" has builtin type {}'
                                      .format(self, self.datatype, self.datatype.get_builtin_type()))

            try:
                float(self.rule)
            except ValueError:
                raise ValidationError('BasicConstraint "{}" specifies a bound of "{}" on numeric value, '
                                      'which is not a number'.format(self, self.rule))

        elif self.ruletype == BasicConstraint.REGEXP:
            try:
                re.compile(self.rule)
            except re.error:
                raise ValidationError('BasicConstraint "{}" specifies an invalid regular expression "{}"'
                                      .format(self, self.rule))

        # This should not apply to a boolean or a numeric.
        elif self.ruletype == BasicConstraint.DATETIMEFORMAT and not self.datatype.is_string():
            raise ValidationError('BasicConstraint "{}" specifies a date/time format, but its parent Datatype "{}" '
                                  'has builtin type "{}"'
                                  .format(self, self.datatype, self.datatype.get_builtin_type()))


@python_2_unicode_compatible
class CompoundDatatypeMember(models.Model):
    """
    A data type member of a particular CompoundDatatype.
    Related to :model:`archive.models.Dataset`
    Related to :model:`metadata.models.CompoundDatatype`
    """
    compounddatatype = models.ForeignKey(
        "CompoundDatatype", related_name="members",
        help_text="Links this DataType member to a particular CompoundDataType")

    datatype = models.ForeignKey(Datatype, help_text="Specifies which DataType this member is",
                                 related_name="CDTMs")

    column_name = models.CharField(
        "Column name",
        blank=False,
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="Gives datatype a 'column name' as an alternative to column index")

    # MinValueValidator(1) constrains column_idx to be >= 1
    column_idx = models.PositiveIntegerField(
        validators=[MinValueValidator(1)],
        help_text="The column number of this DataType")

    # There is no concept of "null" in a CSV....
    blankable = models.BooleanField(
        help_text="Can this entry be left blank?",
        default=False
    )

    # Constant used elsewhere to denote a blank entry.
    BLANK_ENTRY = "blank"

    # Define database indexing rules to ensure tuple uniqueness
    # A compoundDataType cannot have 2 member definitions with the same column name or column number
    class Meta:
        unique_together = (("compounddatatype", "column_name"),
                           ("compounddatatype", "column_idx"))

    def clean(self):
        self.compounddatatype.validate_restrict_access([self.datatype])

    def __str__(self):
        """
        Describe a CompoundDatatypeMember with it's column number,
        datatype name, and column name
        """
        blankable_marker = "?" if self.blankable else ""
        return '{}: {}{}'.format(self.column_name,
                                 str(self.datatype),
                                 blankable_marker)

    def check_basic_constraints(self, value):
        """
        Check a value for conformance to the underlying Datatype's
        BasicConstraints.
        """
        if value == "":
            if self.blankable:
                return []
            else:
                return [CompoundDatatypeMember.BLANK_ENTRY]
        return self.datatype.check_basic_constraints(value)


@python_2_unicode_compatible
class CompoundDatatype(AccessControl):
    """
    A definition of a structured collection of datatypes,
    the resultant data structure serving as inputs or outputs
    for a Transformation.

    TODO: Fix the docs here
    Related to :model:`copperfish.CompoundDatatypeMember`
    Related to :model:`copperfish.Dataset`
    """
    # This could be pretty long, so we use a TextField instead of a CharField.
    # This will require us to be careful about the widgets we use.
    name = models.TextField(
        "Name",
        help_text="The name of this CompoundDatatype",
        blank=True
    )

    RAW_ID = "__raw__"
    RAW_VERBOSE_NAME = "Unstructured"

    # Implicitly defined:
    #   members (CompoundDatatypeMember/ForeignKey)
    #   conforming_datasets (DatasetStructure/ForeignKey)

    class Meta:
        ordering = ["name"]

    def __init__(self, *args, **kwargs):
        super(CompoundDatatype, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def set_name(self, save=True):
        """
        Set the name of the CompoundDatatype to be its string representation.

        This is necessary as we have to save the CDT before we can add members,
        so we will have to be able to set the name after the fact.
        """
        if self.name == "":
            self.name = self._format()
            if save:
                self.save(update_fields=['name'])

    def _format(self, limit=None):
        """ Represent CompoundDatatype with a list of its members.

        @param limit: The maximum number of members to format before truncating
            the list. In a truncated list, the actual number displayed will be
            limit - 1 to leave room for the "plus X others" message.
        """

        # Okay, this is kind of ugly, and try never to do this,
        # but if we use 'self.members.order_by("column_idx")' here
        # it invalidates results that may have been prefetched (if this
        # is used in a queryset that has prefetched results). So we just
        members = self.members.all()

        # then sort those results in python
        members = sorted(members, key=lambda x: x.column_idx)
        if limit is not None and len(members) > limit:
            excess = len(members) - limit + 1
            members = members[:limit-1]
            members.append('plus {} others'.format(excess))

        # typically it's better to let the database sort data
        # but the list of members is also typically very small
        # so we can get away with no performance hit here

        string_rep = "("
        string_rep += ", ".join(str(m) for m in members)
        string_rep += ")"
        if string_rep == "()":
            string_rep = "[empty CompoundDatatype]"
        return string_rep

    def __str__(self):
        return self._format()

    @property
    def short_name(self):
        return self._format(limit=4)

    @property
    def absolute_url(self):
        return reverse("compound_datatype_detail", kwargs={"id": self.pk})

    @classmethod
    def choices(cls, user):
        """ Load choices for a form field.

        @param user: A valid user to filter which compound datatypes are visible.
        @return: [(id, short_name)]
        """
        choices = ((x.id, x.name)
                   for x in CompoundDatatype.filter_by_user(user))

        return sorted(choices, key=lambda x: (x[1], x[0]))  # short_name, then id

    # clean() is executed prior to save() to perform model validation
    def clean(self):
        """
        Check if Datatype members have consecutive indices from 1 to n
        """
        member_dts = []
        for i, member in enumerate(self.members.order_by("column_idx"), start=1):
            member.full_clean()
            if member.column_idx != i:
                raise ValidationError(('Column indices of CompoundDatatype "{}" are not consecutive starting from 1'
                                       .format(self)))
            member_dts.append(member.datatype)

    def is_restriction(self, other_cdt):
        """
        True if this CDT is a column-wise restriction of its parameter.

        This is trivially true if they are the same CDT; otherwise
        the column names must be exactly the same and each column
        of this CDT is a restriction of the corresponding column
        of the parameter CDT.

        Note that this induces a partial order on CDTs.

        PRE: this CDT and other_cdt are clean.
        """
        if self == other_cdt:
            return True

        # Make sure they have the same number of columns.
        if self.members.count() != other_cdt.members.count():
            return False

        # Since they have the same number of columns at this point,
        # and we have enforced that the numbering of members is
        # consecutive starting from one, we can go through all of this
        # CDT's members and look for the matching one.
        for member in self.members.all().order_by("column_idx"):
            try:
                counterpart = other_cdt.members.get(column_idx=member.column_idx, column_name=member.column_name)
                if not member.datatype.is_restriction(counterpart.datatype):
                    return False
            except CompoundDatatypeMember.DoesNotExist:
                return False
        return True

    def is_identical(self, other_cdt):
        """
        True if this CDT is identical with its parameter; False otherwise.

        This is trivially true if they are the same CDT; otherwise
        the column names and column types must be exactly the same.

        PRE: this CDT and other_cdt are clean.
        """
        my_col_names = [m.column_name for m in self.members.order_by("column_idx")]
        other_col_names = [m.column_name for m in other_cdt.members.order_by("column_idx")]
        return my_col_names == other_col_names and self.is_restriction(other_cdt) and other_cdt.is_restriction(self)

    def _check_header(self, header):
        """
        SYNOPSIS
        Verify that a list of field names (which we presumably read from
        a file) matches the anticipated header for this
        CompoundDatatype. This is a helper function for summarize_csv.

        INPUTS
        header  list of fields forming a header, to check against this
                CompoundDatatype's expected header

        OUTPUTS
        A dictionary with keys indicating header errors. Possible key:
        value pairs are the following.

            - bad_num_cols: length of fieldnames, which does not match
              number of members of this CompoundDatatype.
            - bad_col_indices: list of column indices which do not have
              the same name as the corresponding CompoundDatatypeMember.
              Will only be present if the number of columns is correct.

        """
        summary = {}
        if len(header) != self.members.count():
            summary["bad_num_cols"] = len(header)
            self.logger.debug("Number of CSV columns must match number of CDT members")
            return summary

        # The ith cdt member must have the same name as the ith CSV header.
        bad_col_indices = []
        for cdtm in self.members.all():
            if cdtm.column_name != header[cdtm.column_idx-1]:
                bad_col_indices.append(cdtm.column_idx)
                self.logger.debug(('Incorrect header for column {}: expected "{}", got "{}"'
                                   .format(cdtm.column_idx, cdtm.column_name, header[cdtm.column_idx-1])))

        if bad_col_indices:
            summary["bad_col_indices"] = bad_col_indices

        return summary

    def summarize_csv(self, file_to_check):
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
        content_check_log   summarize_csv is called as part of a content check
                            on a Dataset; this is the log of that check

        OUTPUT
        summary             a dict containing metadata about the CSV, whose keys
                            may be any of the following:

        - bad_num_cols: set if header has wrong number of columns;
          if so, returns number of columns in the header.
        - bad_col_indices: set if header has improperly named columns;
          if so, returns list of indices of bad columns
        - num_rows: number of rows in the CSV
        - failing_cells: dict of non-conforming cells in the file.
          Entries keyed by (rownum, colnum) contain list of tests failed.

        ASSUMPTIONS
        1) content_check_log may only be None if this function is being called
        to check the output of a verification method (ie. we are verifying that
        file_to_check matches VERIF_OUT).
        """
        summary = {}

        # A CSV reader which we will use to check individual
        # cells in the file, as well as creating external CSVs
        # for columns whose DT has a CustomConstraint.
        data_csv = csv.reader(file_to_check)
        try:
            header = next(data_csv)
        except StopIteration:
            self.logger.warning("File {} is empty".format(file_to_check))
            summary["bad_num_cols"] = 0
            return summary

        # Check the header.
        self.logger.debug("Checking header")
        summary.update(self._check_header(header))
        summary["header"] = header

        # If the header was malformed, we don't keep checking
        # constraints.
        if "bad_num_cols" in summary or "bad_col_indices" in summary:
            return summary

        # Check the constraints using the module helper.
        summary.update(summarize_csv(self.members.order_by("column_idx"), data_csv))
        return summary

    def check_constraints(self, row):
        """
        SYNOPSIS
        Checks a row of data against the constraints for each column

        INPUTS
        :param row      A row of data to check for conformance

        OUTPUTS
        :returns        A list whose elements are lists of the
                        failed constraints

        """

        def _check_constr(check, value):
            if check is None:
                return []
            err = check.check_basic_constraints(value)
            return ['Failed check \'%s\'' % e if not isinstance(e, (str, six.string_types)) else e for e in err]

        # noinspection PyTypeChecker
        return [_check_constr(chk, val)
                for chk, val in map(None, self.members.order_by("column_idx"), row)]

    @property
    def num_conforming_datasets(self):
        """
        Returns the number of Datasets that conform to this CompoundDatatype.
        Is this even possible?
        """
        return 0

    @transaction.atomic
    def remove(self):
        """
        Handle removal of this CDT from the database, including all records that tied to it.
        """
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["CompoundDatatypes"]

        builtin_pks = {
            CDTs.VERIF_IN_PK,
            CDTs.VERIF_OUT_PK,
            CDTs.PROTOTYPE_PK
        }
        if self.pk in builtin_pks:
            self.logger.warning("Cannot remove builtin CompoundDatatypes.")
            return removal_plan

        removal_plan["CompoundDatatypes"].add(self)

        for dataset_structure in self.conforming_datasets.all().select_related("dataset"):
            if dataset_structure.dataset not in removal_plan["Datasets"]:
                update_removal_plan(removal_plan, dataset_structure.dataset.build_removal_plan(removal_plan))

        # Remove any Transformations that had this CDT.
        transfs_to_remove = set((xs.transf_xput.definite.transformation.definite
                                 for xs in self.xput_structures.all()))
        for definite_transf in transfs_to_remove:
            if definite_transf.__class__.__name__ == "Method":
                already_marked = definite_transf in removal_plan["Methods"]
            else:
                already_marked = definite_transf in removal_plan["Pipelines"]
            if not already_marked:
                removal_plan = update_removal_plan(removal_plan, definite_transf.build_removal_plan(removal_plan))

        return removal_plan
