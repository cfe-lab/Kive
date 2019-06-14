"""
Unit tests for Shipyard metadata models.
"""
from django.test import TestCase, skipIfDBFeature
from django.contrib.auth.models import User, Group

from metadata.models import BasicConstraint, Datatype, everyone_group
from constants import datatypes, groups


samplecode_path = "../samplecode"


@skipIfDBFeature('is_mocked')
class AccessControlTests(TestCase):
    """
    Tests of functionality of the AccessControl abstract class.
    """
    def setUp(self):
        self.dt_owner = User.objects.create_user(
            "Noonian",
            "nsingh@compuserve.com",
            "feeeeeeelings"
        )
        self.dt_owner.save()
        self.dt_owner.groups.add(everyone_group())

        self.lore = User.objects.create_user(
            "Lore",
            "cto@borg.net",
            "Asimov's Three Laws"
        )
        self.lore.save()
        self.lore.groups.add(everyone_group())

        self.developers_group = Group.objects.get(pk=groups.DEVELOPERS_PK)

        self.bool_dt = Datatype.objects.get(pk=datatypes.BOOL_PK)
        self.ac_dt = Datatype(user=self.dt_owner, name="True", description="Python True")
        self.ac_dt.save()
        self.ac_dt.restricts.add(self.bool_dt)
        self.ac_dt.basic_constraints.create(
            ruletype=BasicConstraint.REGEXP,
            rule="True"
        )

        self.users_to_intersect = User.objects.filter(pk__in=[self.dt_owner.pk, self.lore.pk])
        self.groups_to_intersect = Group.objects.filter(pk__in=[self.developers_group.pk,
                                                                everyone_group().pk])

    def test_intersect_permissions_no_querysets_no_perms(self):
        """
        Test of intersect_permissions when no querysets are specified and no permissions are given.
        """
        users_qs, groups_qs = self.ac_dt.intersect_permissions()
        self.assertSetEqual({self.dt_owner}, set(users_qs))
        self.assertFalse(groups_qs.exists())

    def test_intersect_permissions_no_querysets_with_perms(self):
        """
        Test of intersect_permissions when no querysets are specified and some permissions are given.
        """
        self.ac_dt.users_allowed.add(self.lore)
        self.ac_dt.groups_allowed.add(self.developers_group)

        users_qs, groups_qs = self.ac_dt.intersect_permissions()
        self.assertSetEqual({self.dt_owner, self.lore}, set(users_qs))
        self.assertSetEqual({self.developers_group}, set(groups_qs))

    def test_intersect_permissions_no_querysets_everyone_perm(self):
        """
        Test of intersect_permissions when no querysets are specified and the Everyone group has access.
        """
        self.ac_dt.users_allowed.add(self.lore)
        self.ac_dt.groups_allowed.add(everyone_group())

        users_qs, groups_qs = self.ac_dt.intersect_permissions()
        self.assertSetEqual(set(User.objects.all()), set(users_qs))
        self.assertSetEqual(set(Group.objects.all()), set(groups_qs))

    def test_intersect_permissions_querysets_specified_no_perms(self):
        """
        Test of intersect_permissions when querysets are specified and no permissions are given.
        """
        users_qs, groups_qs = self.ac_dt.intersect_permissions(users_qs=self.users_to_intersect,
                                                               groups_qs=self.groups_to_intersect)
        self.assertSetEqual({self.dt_owner}, set(users_qs))
        self.assertFalse(groups_qs.exists())

    def test_intersect_permissions_querysets_with_perms(self):
        """
        Test of intersect_permissions when querysets are specified and some permissions are given.
        """
        self.ac_dt.users_allowed.add(self.dt_owner)
        self.ac_dt.groups_allowed.add(self.developers_group)

        users_qs, groups_qs = self.ac_dt.intersect_permissions(users_qs=self.users_to_intersect,
                                                               groups_qs=self.groups_to_intersect)
        self.assertSetEqual({self.dt_owner}, set(users_qs))
        self.assertSetEqual({self.developers_group}, set(groups_qs))

    def test_intersect_permissions_querysets_everyone_perm(self):
        """
        Test of intersect_permissions when querysets are specified and the Everyone group has access.
        """
        self.ac_dt.groups_allowed.add(everyone_group())

        users_qs, groups_qs = self.ac_dt.intersect_permissions(users_qs=self.users_to_intersect,
                                                               groups_qs=self.groups_to_intersect)
        self.assertSetEqual(set(self.users_to_intersect), set(users_qs))
        self.assertSetEqual(set(self.groups_to_intersect), set(groups_qs))
