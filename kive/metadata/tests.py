"""
Unit tests for Shipyard metadata models.
"""
from django.test import TestCase, skipIfDBFeature
from django.contrib.auth.models import User, Group

from librarian.models import Dataset
from metadata.models import everyone_group
from constants import groups


samplecode_path = "../samplecode"


@skipIfDBFeature('is_mocked')
class AccessControlTests(TestCase):
    """
    Tests of functionality of the AccessControl abstract class.
    """
    def setUp(self):
        self.ds_owner = User.objects.create_user(
            "Noonian",
            "nsingh@compuserve.com",
            "feeeeeeelings"
        )
        self.ds_owner.save()
        self.ds_owner.groups.add(everyone_group())

        self.lore = User.objects.create_user(
            "Lore",
            "cto@borg.net",
            "Asimov's Three Laws"
        )
        self.lore.save()
        self.lore.groups.add(everyone_group())

        self.developers_group = Group.objects.get(pk=groups.DEVELOPERS_PK)

        self.dataset = Dataset.create_empty(user=self.ds_owner)
        self.dataset.name = "Test"
        self.dataset.description = "Test dataset"
        self.dataset.save()

        self.users_to_intersect = User.objects.filter(pk__in=[self.ds_owner.pk, self.lore.pk])
        self.groups_to_intersect = Group.objects.filter(pk__in=[self.developers_group.pk,
                                                                everyone_group().pk])

    def test_intersect_permissions_no_querysets_no_perms(self):
        """
        Test of intersect_permissions when no querysets are specified and no permissions are given.
        """
        users_qs, groups_qs = self.dataset.intersect_permissions()
        self.assertSetEqual({self.ds_owner}, set(users_qs))
        self.assertFalse(groups_qs.exists())

    def test_intersect_permissions_no_querysets_with_perms(self):
        """
        Test of intersect_permissions when no querysets are specified and some permissions are given.
        """
        self.dataset.users_allowed.add(self.lore)
        self.dataset.groups_allowed.add(self.developers_group)

        users_qs, groups_qs = self.dataset.intersect_permissions()
        self.assertSetEqual({self.ds_owner, self.lore}, set(users_qs))
        self.assertSetEqual({self.developers_group}, set(groups_qs))

    def test_intersect_permissions_no_querysets_everyone_perm(self):
        """
        Test of intersect_permissions when no querysets are specified and the Everyone group has access.
        """
        self.dataset.users_allowed.add(self.lore)
        self.dataset.groups_allowed.add(everyone_group())

        users_qs, groups_qs = self.dataset.intersect_permissions()
        self.assertSetEqual(set(User.objects.all()), set(users_qs))
        self.assertSetEqual(set(Group.objects.all()), set(groups_qs))

    def test_intersect_permissions_querysets_specified_no_perms(self):
        """
        Test of intersect_permissions when querysets are specified and no permissions are given.
        """
        users_qs, groups_qs = self.dataset.intersect_permissions(users_qs=self.users_to_intersect,
                                                                 groups_qs=self.groups_to_intersect)
        self.assertSetEqual({self.ds_owner}, set(users_qs))
        self.assertFalse(groups_qs.exists())

    def test_intersect_permissions_querysets_with_perms(self):
        """
        Test of intersect_permissions when querysets are specified and some permissions are given.
        """
        self.dataset.users_allowed.add(self.ds_owner)
        self.dataset.groups_allowed.add(self.developers_group)

        users_qs, groups_qs = self.dataset.intersect_permissions(users_qs=self.users_to_intersect,
                                                                 groups_qs=self.groups_to_intersect)
        self.assertSetEqual({self.ds_owner}, set(users_qs))
        self.assertSetEqual({self.developers_group}, set(groups_qs))

    def test_intersect_permissions_querysets_everyone_perm(self):
        """
        Test of intersect_permissions when querysets are specified and the Everyone group has access.
        """
        self.dataset.groups_allowed.add(everyone_group())

        users_qs, groups_qs = self.dataset.intersect_permissions(users_qs=self.users_to_intersect,
                                                                 groups_qs=self.groups_to_intersect)
        self.assertSetEqual(set(self.users_to_intersect), set(users_qs))
        self.assertSetEqual(set(self.groups_to_intersect), set(groups_qs))
