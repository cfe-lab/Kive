import errno
import logging
import os
import shutil
from contextlib import contextmanager

from django.utils.six import StringIO
from datetime import timedelta

from django.core import serializers
from django.db import IntegrityError
from django.db import connections
from django.apps import apps
from django.core.management import call_command

from django.conf import settings
from django.contrib.auth import SESSION_KEY, HASH_SESSION_KEY, BACKEND_SESSION_KEY
from django.contrib.auth.models import User
from django.contrib.sessions.backends.db import SessionStore
from django.contrib.sessions.models import Session
from django.test import TestCase, TransactionTestCase, Client
from django.utils.timezone import now
from mock import patch

from rest_framework.test import APIRequestFactory, force_authenticate
from django_mock_queries.mocks import mocked_relations

from constants import users
from metadata.models import kive_user, KiveUser


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


class ViewMockTestCase(TestCase, object):
    def create_client(self):
        patcher = mocked_relations(User, Session)
        patcher.start()
        self.addCleanup(patcher.stop)

        user = User(pk=users.KIVE_USER_PK)
        User.objects.add(user)
        User.objects.model = User
        # noinspection PyUnresolvedReferences
        patcher = patch.object(User._meta, 'default_manager', User.objects)
        patcher.start()
        self.addCleanup(patcher.stop)
        dummy_session_key = 'dummysession'
        dummy_session = Session(
            session_key=dummy_session_key,
            expire_date=now() + timedelta(days=1),
            session_data=SessionStore().encode({
                SESSION_KEY: users.KIVE_USER_PK,
                HASH_SESSION_KEY: user.get_session_auth_hash(),
                BACKEND_SESSION_KEY: 'django.contrib.auth.backends.ModelBackend'}))
        Session.objects.add(dummy_session)
        client = Client()
        client.cookies[settings.SESSION_COOKIE_NAME] = dummy_session_key
        client.force_login(kive_user())
        return client


class BaseTestCases(object):
    """ A class to hide our base classes so they won't be executed as tests.
    """
    def __init__(self):
        pass

    class ApiTestCase(TestCase, object):
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

        def mock_viewset(self, viewset_class):
            model = viewset_class.queryset.model
            patcher = mocked_relations(model, User, KiveUser)
            patcher.start()
            self.addCleanup(patcher.stop)

            user = User(pk=users.KIVE_USER_PK)
            User.objects.add(user)

            self.kive_kive_user = KiveUser(pk=users.KIVE_USER_PK, username="kive")
            KiveUser.objects.add(self.kive_kive_user)

            # noinspection PyUnresolvedReferences
            patcher2 = patch.object(viewset_class,
                                    'queryset',
                                    model.objects)
            patcher2.start()
            self.addCleanup(patcher2.stop)

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

    class SlurmExecutionTestCase(TransactionTestCase):
        """
        Base test case used in lieu of TransactionTestCase for tests involving Slurm execution.

        This is a monkey-patched version of TransactionTestCase.
        """
        serialized_rollback = True

        @staticmethod
        def deserialize_db_from_string(db_creation, data):
            """
            Reloads the database with data from a string generated by
            the serialize_db_to_string method.
            """
            from django.utils.six import StringIO

            data = StringIO(data)
            objects_to_add = list(serializers.deserialize("json", data, using=db_creation.connection.alias))
            while len(objects_to_add) > 0:
                cannot_add_yet = []
                for obj in objects_to_add:
                    try:
                        obj.save()
                    except IntegrityError:
                        cannot_add_yet.append(obj)

                objects_to_add = cannot_add_yet

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

                    # Use our monkey-patched version of deserialize_db_from_string.
                    BaseTestCases.SlurmExecutionTestCase.deserialize_db_from_string(
                        connections[db_name].creation,
                        connections[db_name]._test_serialized_contents
                    )

                    if self.available_apps is not None:
                        apps.set_available_apps(self.available_apps)

                if self.fixtures:
                    # We have to use this slightly awkward syntax due to the fact
                    # that we're using *args and **kwargs together.
                    call_command('loaddata', *self.fixtures,
                                 **{'verbosity': 0, 'database': db_name})

        def check_run_OK(self, run):
            for step in run.runsteps.all():
                for rsic in step.RSICs.all():
                    self.assertTrue(rsic.is_successful())

                if step.has_subrun():
                    self.check_run_OK(step.child_run)

                self.assertTrue(step.is_successful())

            for outcable in run.runoutputcables.all():
                self.assertTrue(outcable.is_successful())

            self.assertTrue(run.is_successful())


def dummy_file(content, name='dummy_file', mode='rb'):
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

    containers_path = os.path.join(settings.MEDIA_ROOT, 'Containers')
    if not os.path.exists(containers_path):
        os.makedirs(containers_path)
    test_container_path = os.path.join(containers_path,
                                       settings.DEFAULT_CONTAINER)
    if not os.path.exists(test_container_path):
        alpine_container_path = os.path.abspath(os.path.join(
            __file__,
            '..',
            '..',
            '..',
            'samplecode',
            'singularity',
            'python2-alpine-trimmed.simg'))
        os.symlink(alpine_container_path, test_container_path)


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


def strip_removal_plan(plan):
    plan_not_blanks = {key: value
                       for key, value in plan.items()
                       if value}
    return plan_not_blanks


@contextmanager
def capture_log_stream(log_level, logger_name):
    mocked_stderr = StringIO()
    stream_handler = logging.StreamHandler(mocked_stderr)
    logger = logging.getLogger(logger_name)
    logger.addHandler(stream_handler)
    old_level = logger.level
    logger.level = log_level
    try:
        yield mocked_stderr
    finally:
        logger.removeHandler(stream_handler)
        logger.level = old_level
