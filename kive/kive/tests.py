from django.conf import settings
from django.test import TestCase, TransactionTestCase

from django.db import connections, IntegrityError
from django.apps import apps
from django.core import serializers
from django.utils.six import StringIO
from django.core.management import call_command

from rest_framework.test import APIRequestFactory, force_authenticate

import os
import shutil

from metadata.models import kive_user

stash_dir = "StashedWhileTesting"
targets = ["CodeResources",
           "Datasets",
           "Logs",
           "Sandboxes",
           "VerificationLogs",
           "VerificationScripts",
           "StagedFiles"]


class DuckRequest(object):
    """ A fake request used to test serializers. """
    def __init__(self, user=None):
        self.user = user or kive_user()
        self.GET = {}

    def build_absolute_uri(self, url):
        return url


class DuckContext(dict):
    """ A fake context used to test serializers. """
    def __init__(self, user=None):
        self['request'] = DuckRequest(user=user)


class BaseTestCases:
    """ A class to hide our base classes so they won't be executed as tests.
    """

    class ApiTestCase(TestCase):
        """
        Base test case used for all API testing.

        Such test cases should provide tests of:
         - list
         - detail
         - creation (if applicable)
         - redaction
         - removal
         - any other detail or list routes

        In addition, inheriting classes must provide appropriate values for
        self.list_path and self.list_view in their setUp().
        """
        def setUp(self):
            self.factory = APIRequestFactory()
            self.kive_user = kive_user()

        def test_auth(self):
            """
            Test that the API URL is correctly defined and requires a logged-in user.
            """
            # First try to access while not logged in.
            request = self.factory.get(self.list_path)
            response = self.list_view(request)
            self.assertEquals(response.data["detail"], "Authentication credentials were not provided.")

            # Now log in and check that "detail" is not passed in the response.
            force_authenticate(request, user=self.kive_user)
            response = self.list_view(request)
            self.assertNotIn('detail', response.data)


class KiveTransactionTestCase(TransactionTestCase):

    serialized_rollback = True

    def _fixture_setup(self):

        for db_name in self._databases_names(include_mirrors=False):
            # Reset sequences
            if self.reset_sequences:
                self._reset_sequences(db_name)

            # If we need to provide replica initial data from migrated apps,
            # then do so.
            if self.serialized_rollback and hasattr(connections[db_name], "_test_serialized_contents"):
                if self.available_apps is not None:
                    apps.unset_available_apps()

                with connections[db_name].constraint_checks_disabled():
                    data = StringIO(connections[db_name]._test_serialized_contents)
                    try_again = []
                    objects_to_save = serializers.deserialize(
                        "json",
                        data,
                        using=connections[db_name].creation.connection.alias,
                        ignorenonexistent=False)

                    while objects_to_save:
                        for obj in objects_to_save:
                            try:
                                obj.save()
                            except IntegrityError:
                                try_again.append(obj)

                        objects_to_save = try_again
                        try_again = []

                if self.available_apps is not None:
                    apps.set_available_apps(self.available_apps)

            if self.fixtures:
                # We have to use this slightly awkward syntax due to the fact
                # that we're using *args and **kwargs together.
                call_command('loaddata', *self.fixtures,
                             **{'verbosity': 0, 'database': db_name})


def install_fixture_files(fixture_name):
    """
    Helper that installs the FieldFiles for a given fixture.
    """
    fixture_files_path = os.path.join("FixtureFiles", fixture_name)
    assert os.path.isdir(fixture_files_path)

    os.makedirs(stash_dir)  # We want this to fail if it already exists.

    for target in targets:
        target_path = os.path.join(settings.MEDIA_ROOT, target)
        if os.path.isdir(target_path):
            shutil.move(target_path, os.path.join(stash_dir, target))

        dir_to_install = os.path.join(fixture_files_path, target)
        if os.path.isdir(dir_to_install):
            shutil.copytree(dir_to_install, target_path)
        else:
            os.mkdir(target_path)


def restore_production_files():
    """
    Helper that removes all FieldFiles used by a test fixture and puts the stashed files back.
    """
    if not os.path.isdir(stash_dir):
        return

    for target in targets:
        target_path = os.path.join(settings.MEDIA_ROOT, target)
        if os.path.isdir(target_path):
            shutil.rmtree(target_path)

        dir_to_restore = os.path.join(stash_dir, target)
        if os.path.isdir(dir_to_restore):
            shutil.move(dir_to_restore, target_path)

    shutil.rmtree(stash_dir)
