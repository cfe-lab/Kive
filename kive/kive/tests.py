from contextlib import contextmanager
import os
import shutil
from StringIO import StringIO

from django.conf import settings
from django.test import TestCase

from mock import Mock

from rest_framework.test import APIRequestFactory, force_authenticate

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
        self.META = {}
        self.method = 'GET'

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


@contextmanager
def mock_relations(*models):
    """ Mock all related field managers to make pure unit tests possible.

    with mock_relations(Dataset):
        dataset = Dataset()
        check = dataset.content_checks.create()  # returns mock object
    """
    try:
        for model in models:
            model_name = model._meta.object_name
            model.old_relations = {}
            model.old_objects = model.objects
            for related_object in model._meta.related_objects:
                name = related_object.name
                model.old_relations[name] = getattr(model, name)
                setattr(model, name, Mock(name='{}.{}'.format(model_name, name)))
            model.objects = Mock(name=model_name + '.objects')

        yield

    finally:
        for model in models:
            old_objects = getattr(model, 'old_objects', None)
            if old_objects is not None:
                model.objects = old_objects
                del model.old_objects
            old_relations = getattr(model, 'old_relations', None)
            if old_relations is not None:
                for name, relation in old_relations.iteritems():
                    setattr(model, name, relation)
                del model.old_relations


def dummy_file(content, name='dummy_file'):
    """ Create an in-memory, file-like object.

    :param str content: the contents of the file
    :param str name: a name for the file
    :return: an object that looks like an open file handle.
    """

    data_file = StringIO(content)
    data_file.name = name
    data_file.__enter__ = lambda: None
    data_file.__exit__ = lambda type, value, traceback: None
    return data_file


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
