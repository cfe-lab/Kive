from django.conf import settings
from django.test import TestCase

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


def install_fixture_files(fixture_name):
    """
    Helper that installs the FieldFiles for a given fixture.
    """
    fixture_files_path = os.path.join("FixtureFiles", fixture_name)
    assert os.path.isdir(fixture_files_path)

    if os.path.isdir(stash_dir):
        shutil.rmtree(stash_dir)
    os.makedirs(stash_dir)

    for target in targets:
        target_path = os.path.join(settings.MEDIA_ROOT, target)
        if os.path.isdir(target_path):
            shutil.move(target_path, os.path.join(stash_dir, target))

        dir_to_install = os.path.join(fixture_files_path, target)
        if os.path.isdir(dir_to_install):
            shutil.copytree(dir_to_install, target_path)


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
