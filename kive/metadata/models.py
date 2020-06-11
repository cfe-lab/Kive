"""
metadata.models

Shipyard data models relating to metadata: Datatypes and their related
paraphernalia, CompoundDatatypes, etc.
"""
from django.db import models, transaction
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.http import Http404
from django.contrib.auth.models import User, Group
from django.db.models import Q

import json
import itertools

from constants import groups, users

import logging
from portal.views import admin_check

LOGGER = logging.getLogger(__name__)  # Module level logger.


# We delete objects in this order:
deletion_order = [
    "Datasets", "Batches", "ContainerRuns", "ContainerApps", "Containers",
    "ContainerFamilies"
]


@transaction.atomic
def remove_helper(removal_plan):
    for class_name in deletion_order:
        if class_name in removal_plan:
            for obj_to_delete in removal_plan[class_name]:
                try:
                    obj_to_delete.refresh_from_db()
                    obj_to_delete.delete()
                except ObjectDoesNotExist:
                    pass


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
    user = models.ForeignKey(User, on_delete=models.CASCADE)
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
