import errno
import os
import shutil
from StringIO import StringIO

from django.conf import settings
from django.test import TestCase

from rest_framework.test import APIRequestFactory, force_authenticate

from metadata.models import kive_user


class DuckRequest(object):
    """ A fake request used to test serializers. """
    def __init__(self, user=None):
        self.user = user or kive_user()
        self.GET = {}
        self.META = {}
        self.method = 'GET'

    def build_absolute_uri(self, url):
        return url


class DuckContext(dict):
    """ A fake context used to test serializers. """
    def __init__(self, user=None, **kwargs):
        super(DuckContext, self).__init__(**kwargs)
        self['request'] = DuckRequest(user=user)


class BaseTestCases(object):
    """ A class to hide our base classes so they won't be executed as tests.
    """
    def __init__(self):
        pass

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


def dummy_file(content, name='dummy_file'):
    """ Create an in-memory, file-like object.

    :param str content: the contents of the file
    :param str name: a name for the file
    :return: an object that looks like an open file handle.
    """

    data_file = StringIO(content)
    data_file.name = name
    data_file.__enter__ = lambda: None
    data_file.__exit__ = lambda extype, value, traceback: None
    return data_file


def check_media_root_is_test():
    if os.path.basename(settings.MEDIA_ROOT) != 'Testing':
        raise RuntimeError(
            "MEDIA_ROOT doesn't end with 'Testing', use test settings.")


def install_fixture_files(fixture_name):
    """
    Helper that installs the FieldFiles for a given fixture.
    """
    remove_fixture_files()  # Remove any leftovers
    fixture_files_path = os.path.join("FixtureFiles", fixture_name)
    assert os.path.isdir(fixture_files_path)

    for target in os.listdir(fixture_files_path):
        target_path = os.path.join(settings.MEDIA_ROOT, target)
        dir_to_install = os.path.join(fixture_files_path, target)
        shutil.copytree(dir_to_install, target_path)


def remove_fixture_files():
    """
    Helper that removes all FieldFiles used by a test fixture.
    """
    check_media_root_is_test()
    try:
        os.makedirs(settings.MEDIA_ROOT)
        # If that succeeded, then the folder is empty.
        return
    except OSError as ex:
        if ex.errno != errno.EEXIST:
            raise

    for dirname in os.listdir(settings.MEDIA_ROOT):
        target_path = os.path.join(settings.MEDIA_ROOT, dirname)
        shutil.rmtree(target_path)
