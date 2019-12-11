"""
Shipyard models pertaining to the librarian app.
"""

from datetime import datetime, timedelta
import os
import random
import re
import tempfile
import logging
import json
import shutil
import stat
from io import BytesIO
from zipfile import ZipFile

import django.utils.six as dsix

from django.core.exceptions import ValidationError
from django.contrib.auth.models import User, Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, skipIfDBFeature, Client
from django.core.urlresolvers import reverse, resolve
from django.core.files import File
from django.core.files.base import ContentFile
# from django.utils.timezone import get_default_timezone, get_current_timezone
from django.utils import timezone
from mock import patch

from rest_framework.test import force_authenticate, APIRequestFactory
from rest_framework import status

from constants import groups
from container.models import ContainerFamily, ContainerArgument, Container
from librarian.ajax import ExternalFileDirectoryViewSet, DatasetViewSet
from librarian.models import Dataset, ExternalFileDirectory
from librarian.serializers import DatasetSerializer
from metadata.models import kive_user, everyone_group

import file_access_utils
import kive.testing_utils as tools
from kive.tests import BaseTestCases, DuckContext, capture_log_stream
from librarian.management.commands import find_orphans

FROM_FILE_END = 2

samplecode_path = os.path.abspath(os.path.join(__file__, '../../../samplecode'))


@skipIfDBFeature('is_mocked')
class LibrarianTestCase(TestCase, object):
    """
    Set up a database state for unit testing the librarian app.

    This extends PipelineTestCase, which itself extended
    other stuff (follow the chain).
    """
    def setUp(self):
        """Set up default database state for librarian unit testing."""
        self.myUser = User.objects.create_user('john',
                                               'lennon@thebeatles.com',
                                               'johnpassword')
        self.ringoUser = User.objects.create_user('ringo',
                                                  'starr@thebeatles.com',
                                                  'ringopassword')
        self.singlet_dataset = Dataset.create_dataset(
            os.path.join(samplecode_path, "singlet_cdt_large.csv"),
            self.myUser,
            groups_allowed=[everyone_group()],
            name="singlet",
            description="lol")

    def tearDown(self):
        tools.clean_up_all_files()


class DatasetTests(LibrarianTestCase):

    def setUp(self):
        super(DatasetTests, self).setUp()

        # Turn off logging, so the test output isn't polluted.
        logging.getLogger('Dataset').setLevel(logging.CRITICAL)
        logging.getLogger('CompoundDatatype').setLevel(logging.CRITICAL)

        rows = 10
        seqlen = 10

        self.data = ""
        for i in range(rows):
            seq = "".join([random.choice("ATCG") for _ in range(seqlen)])
            self.data += "patient{},{}\n".format(i, seq)
        self.header = "header,sequence"

        self.data_file = tempfile.NamedTemporaryFile(delete=False)
        data_str = self.header + "\n" + self.data
        self.data_file.write(data_str.encode())
        self.file_path = self.data_file.name
        self.data_file.close()

        self.dsname = "good data"
        self.dsdesc = "some headers and sequences"
        self.dataset = Dataset.create_dataset(
            file_path=self.file_path,
            user=self.myUser,
            keep_file=True,
            name=self.dsname,
            description=self.dsdesc
        )

    def tearDown(self):
        super(DatasetTests, self).tearDown()
        os.remove(self.file_path)

    def test_filehandle(self):
        """
        Test that you can pass a filehandle to create_dataset() to make a dataset.
        """
        import datetime
        dt = datetime.datetime.now()
        # Turn off logging, so the test output isn't polluted.
        logging.getLogger('Dataset').setLevel(logging.CRITICAL)
        logging.getLogger('CompoundDatatype').setLevel(logging.CRITICAL)

        with tempfile.NamedTemporaryFile(delete=True) as tmpfile:
            tmpfile.file.write("Random stuff".encode())
            tmpfile.file.flush()  # flush python buffer to os buffer
            os.fsync(tmpfile.file.fileno())  # flush os buffer to disk
            tmpfile.file.seek(0)  # go to beginning of file before calculating expected md5

            expected_md5 = file_access_utils.compute_md5(tmpfile)
            tmpfile.file.seek(0)  # return to beginning before creating a Dataset

            raw_datatype = None  # raw compound datatype
            name = "Test file handle" + str(dt.microsecond)
            desc = "Test create dataset with file handle"
            dataset = Dataset.create_dataset(
                file_path=None,
                user=self.myUser,
                cdt=raw_datatype,
                keep_file=True,
                name=name,
                description=desc,
                check=True,
                file_handle=tmpfile
            )

        self.assertIsNotNone(Dataset.objects.filter(name=name).get(),
                             msg="Can't find Dataset in DB for name=" + name)

        actual_md5 = Dataset.objects.filter(id=dataset.id).get().MD5_checksum
        self.assertEqual(actual_md5, expected_md5,
                         msg="Checksum for Dataset ({}) file does not match expected ({})".format(
                             actual_md5,
                             expected_md5
                         ))

    def test_dataset_creation(self):
        """
        Test coherence of a freshly created Dataset.
        """
        self.assertEqual(self.dataset.clean(), None)
        self.assertEqual(self.dataset.has_data(), True)
        self.assertTrue(self.dataset.is_raw())

        self.assertEqual(self.dataset.user, self.myUser)
        self.assertEqual(self.dataset.name, self.dsname)
        self.assertEqual(self.dataset.description, self.dsdesc)
        self.assertEqual(self.dataset.date_created.date(), timezone.now().date())
        self.assertEqual(self.dataset.date_created < timezone.now(), True)
        self.assertEqual(self.dataset.file_source, None)
        self.assertEqual(os.path.basename(self.dataset.dataset_file.path), os.path.basename(self.file_path))
        self.data_file.close()

    def test_dataset_increase_permissions_from_json(self):
        """
        Test increase_permissions_from_json reaches any usurping Datasets.
        """
        # First, we revoke Everyone permissions on a Dataset.
        self.singlet_dataset.groups_allowed.remove(everyone_group())

        # We store the original contents of a Dataset...
        self.singlet_dataset.dataset_file.open()
        orig_contents = self.singlet_dataset.dataset_file.read()
        self.singlet_dataset.dataset_file.close()
        orig_md5 = self.singlet_dataset.MD5_checksum

        # ... and then we corrupt it.
        self.singlet_dataset.MD5_checksum = "corruptedmd5"
        self.singlet_dataset.save()

        usurping_ds = Dataset(
            name="Usurping DS",
            description="Usurps self.singlet_dataset",
            user=self.myUser,
            dataset_file=ContentFile(orig_contents),
            MD5_checksum=orig_md5
        )
        usurping_ds.save()

        # Now, let's try to grant some permissions on self.singlet_dataset.
        new_perms_json = json.dumps(
            [
                [self.ringoUser.username],
                [Group.objects.get(pk=groups.DEVELOPERS_PK).name]
            ]
        )
        self.singlet_dataset.increase_permissions_from_json(new_perms_json)

        self.assertTrue(self.singlet_dataset.users_allowed.filter(pk=self.ringoUser.pk).exists())
        self.assertFalse(usurping_ds.users_allowed.filter(pk=self.ringoUser.pk).exists())

        self.assertTrue(self.singlet_dataset.groups_allowed.filter(pk=groups.DEVELOPERS_PK).exists())
        self.assertFalse(usurping_ds.groups_allowed.filter(pk=groups.DEVELOPERS_PK).exists())

    def test_update_name(self):
        dataset = self.singlet_dataset
        self.assertEqual('singlet', dataset.name)

        user = dataset.user
        client = Client()
        client.force_login(user)
        expected_name = 'Changed to Synglet'

        response = client.post(reverse('dataset_view',
                                       kwargs=dict(dataset_id=dataset.id)),
                               dict(name=expected_name))

        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)
        dataset.refresh_from_db()
        self.assertEqual(expected_name, dataset.name)

    def test_increase_permissions(self):
        dataset = self.singlet_dataset
        dataset.groups_allowed.clear()
        self.assertFalse(dataset.shared_with_everyone)

        user = dataset.user
        client = Client()
        client.force_login(user)

        response = client.post(reverse('dataset_view',
                                       kwargs=dict(dataset_id=dataset.id)),
                               dict(name='synglet',
                                    permissions_1='Everyone'))

        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)
        dataset.refresh_from_db()
        self.assertTrue(dataset.shared_with_everyone)

    def test_source_container_run_permissions(self):
        """ Dataset can't have more permissions than source container run. """
        user = self.singlet_dataset.user
        family = ContainerFamily.objects.create(user=user)
        container = family.containers.create(user=user)
        app = container.apps.create()
        argument = app.arguments.create(type='O')
        run = app.runs.create(user=user)

        dataset = self.singlet_dataset
        dataset.groups_allowed.clear()
        run.datasets.create(dataset=dataset,
                            argument=argument)
        self.assertFalse(dataset.shared_with_everyone)
        expected_errors = {'permissions': ['Select a valid choice. Everyone '
                                           'is not one of the available '
                                           'choices.']}

        user = dataset.user
        client = Client()
        client.force_login(user)

        response = client.post(reverse('dataset_view',
                                       kwargs=dict(dataset_id=dataset.id)),
                               dict(name='synglet',
                                    permissions_1='Everyone'))

        self.assertEqual(200, response.status_code)  # Form error, not redirect
        self.assertEqual(expected_errors,
                         response.context['dataset_form'].errors)
        dataset.refresh_from_db()
        self.assertFalse(dataset.shared_with_everyone)

    def test_bulk_upload(self):
        file1 = SimpleUploadedFile("file1.txt", b"Content of file 1.")
        file2 = SimpleUploadedFile("file2.txt", b"Content of file 2.")
        client = Client()
        client.force_login(self.myUser)

        response = client.post(reverse('datasets_add_bulk'),
                               dict(dataset_files=[file1, file2],
                                    compound_datatype='__raw__',
                                    permissions_1='Everyone'))

        self.assertEqual(200, response.status_code)  # Form error, not redirect
        old_form = response.context.get('bulkAddDatasetForm')
        if old_form is not None:
            self.assertEqual([], old_form.errors)
            self.fail('Should not have old form.')

        self.assertEqual(2, response.context['num_files_added'])
        dataset2, dataset1 = Dataset.objects.all()[:2]
        self.assertRegexpMatches(dataset1.name, r'file1\.txt.*')
        self.assertRegexpMatches(dataset2.name, r'file2\.txt.*')
        self.assertTrue(dataset1.is_uploaded)

    def test_archive_upload(self):
        bytes_file = BytesIO()
        with ZipFile(bytes_file, "w") as f:
            f.writestr("foo.txt", b"The first file.")
            f.writestr("bar.txt", b"The second file.")
        uploading_file = SimpleUploadedFile("file1.zip", bytes_file.getvalue())

        client = Client()
        client.force_login(self.myUser)

        response = client.post(reverse('datasets_add_archive'),
                               dict(dataset_file=uploading_file,
                                    compound_datatype='__raw__',
                                    permissions_1='Everyone'))

        self.assertEqual(200, response.status_code)  # Form error, not redirect
        old_form = response.context.get('archiveAddDatasetForm')
        if old_form is not None:
            self.assertEqual({}, old_form.errors)
            self.assertEqual([], old_form.non_field_errors())
            self.fail('Should not have old form.')

        self.assertEqual(2, response.context['num_files_added'])
        dataset2, dataset1 = Dataset.objects.all()[:2]
        self.assertRegexpMatches(dataset1.name, r'foo\.txt.*')
        self.assertRegexpMatches(dataset2.name, r'bar\.txt.*')
        self.assertTrue(dataset1.is_uploaded)


@skipIfDBFeature('is_mocked')
class DatasetWithFileTests(TestCase):

    def setUp(self):
        self.myUser = User.objects.create_user('john',
                                               'lennon@thebeatles.com',
                                               'johnpassword')
        self.singlet_dataset = Dataset.create_dataset(
            os.path.join(samplecode_path, "singlet_cdt_large.csv"),
            self.myUser,
            groups_allowed=[everyone_group()],
            name="singlet",
            description="lol")
        self.raw_dataset = Dataset.create_dataset(
            os.path.join(samplecode_path, "step_0_raw.fasta"),
            user=self.myUser,
            groups_allowed=[everyone_group()],
            name="raw_DS",
            description="lol")

    def tearDown(self):
        tools.clean_up_all_files()

    def test_Dataset_check_MD5(self):
        old_md5 = "7dc85e11b5c02e434af5bd3b3da9938e"
        new_md5 = "d41d8cd98f00b204e9800998ecf8427e"

        self.assertEqual(self.raw_dataset.compute_md5(), old_md5)

        # Initially, no change to the raw dataset has occured, so the md5 check will pass
        self.assertEqual(self.raw_dataset.clean(), None)

        # The contents of the file are changed, disrupting file integrity
        self.raw_dataset.dataset_file.close()
        self.raw_dataset.dataset_file.open(mode='w')
        self.raw_dataset.dataset_file.close()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('File integrity of "{}" lost. Current checksum "{}" does not equal expected '
                                          'checksum "{}"'.format(self.raw_dataset, new_md5, old_md5)),
                                self.raw_dataset.clean)

    def test_Dataset_filename_MD5_clash(self):
        ds1, ds2 = Dataset.objects.all()[:2]
        ds1.name = ds2.name
        ds1.MD5_checksum = ds2.MD5_checksum
        ds1.save()
        msg = "A Dataset with that name and MD5 already exists"
        self.assertRaisesRegexp(ValidationError, msg, ds1.validate_uniqueness_on_upload)


class DatasetApiMockTests(BaseTestCases.ApiTestCase):

    def setUp(self):
        self.mock_viewset(DatasetViewSet)
        super(DatasetApiMockTests, self).setUp()
        # num_cols = 12

        self.list_path = reverse("dataset-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = 43
        self.detail_path = reverse("dataset-detail",
                                   kwargs={'pk': self.detail_pk})
        self.redaction_path = reverse("dataset-redaction-plan",
                                      kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("dataset-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.redaction_view, _, _ = resolve(self.redaction_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        tz = timezone.get_current_timezone()
        apples_date = timezone.make_aware(datetime(2017, 1, 1), tz)
        apples = Dataset(pk=42,
                         name='apples',
                         description='chosen',
                         date_created=apples_date,
                         is_uploaded=True,
                         user=self.kive_kive_user)
        cherries_date = timezone.make_aware(datetime(2017, 1, 2), tz)
        cherries = Dataset(pk=43,
                           name='cherries',
                           date_created=cherries_date,
                           is_uploaded=True,
                           MD5_checksum='1234',
                           user=self.kive_kive_user)
        bananas_date = timezone.make_aware(datetime(2017, 1, 3), tz)
        bananas = Dataset(pk=44,
                          name='bananas',
                          date_created=bananas_date,
                          user=self.kive_kive_user)
        Dataset.objects.add(apples,
                            cherries,
                            bananas)

    def test_list(self):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)
        self.assertEquals(response.data[2]['name'], 'bananas')

    def test_filter_smart(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=smart&filters[0][val]=ch")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)
        self.assertEquals(response.data[0]['name'], 'cherries')
        self.assertEquals(response.data[1]['description'], 'chosen')

    def test_filter_name(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=name&filters[0][val]=ch")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'cherries')

    def test_filter_description(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=description&filters[0][val]=ch")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['description'], 'chosen')

    def test_filter_user(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=user&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)

    def test_filter_uploaded(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=uploaded")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)

    def test_filter_md5(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=md5&filters[0][val]=1234")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'cherries')

    def test_filter_date(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=createdafter&filters[0][val]=02 Jan 2017 0:00" +
            "&filters[1][key]=createdbefore&filters[1][val]=02 Jan 2017 0:00")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'cherries')

    def test_filter_unknown(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=bogus&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals({u'detail': u'Unknown filter key: bogus'},
                          response.data)


@skipIfDBFeature('is_mocked')
class PurgeDataTests(TestCase):

    @staticmethod
    def create_dataset(is_uploaded=False, name='Test name', description='Test description'):
        with tempfile.TemporaryFile() as f:
            f.write('I am a file!'.encode())
            f.seek(0)
            dataset = Dataset.create_dataset(
                file_path=None,
                user=kive_user(),
                users_allowed=None,
                groups_allowed=None,
                cdt=None,
                keep_file=True,
                name=name,
                description=description,
                file_source=None,
                check=True,
                file_handle=f,
                is_uploaded=is_uploaded
            )
        return dataset

    @staticmethod
    def create_container():
        user = kive_user()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        return container

    @staticmethod
    def create_app(container):
        app = container.apps.create(memory=200, threads=1)
        app.write_inputs('test_input')
        app.write_outputs('test_output')
        return app

    @staticmethod
    def create_run(app):
        run = app.runs.create(name='test_run', user=kive_user())
        return run

    @staticmethod
    def add_dataset_to_run(app, run, dataset, atype='input'):
        if atype == 'input':
            aatype = ContainerArgument.INPUT
        elif atype == 'output':
            aatype = ContainerArgument.OUTPUT
        else:
            raise UserWarning('Must provide a string, either "input" or "output"')
        run.datasets.create(
            argument=app.arguments.get(
                type=aatype,
                position=1
            ),
            dataset=dataset
        )
        run.save(schedule=False)

    def test_find_orphans(self):
        datasets = {
            'orphan': self.create_dataset(name='Orphan name', description='Orphan description'),
            'input_dataset': self.create_dataset(is_uploaded=True, name='Input name', description='Input description'),
            'output_dataset': self.create_dataset(name='Output name', description='Output description'),
            'unused_dataset': self.create_dataset(is_uploaded=True, name='Unused name', description='Unused description')
        }
        for i in range(20):
            datasets['orphan_{}'.format(i)] = self.create_dataset(
                name='Orphan {}'.format(i),
                description='Orphan description {}'.format(i)
            )
        container = self.create_container()
        app = self.create_app(container)
        run = self.create_run(app)
        self.add_dataset_to_run(app, run, datasets['input_dataset'])
        self.add_dataset_to_run(app, run, datasets['output_dataset'], atype='output')
        orphans = find_orphans.Command.find_orphans()
        ids_and_paths = []

        # Verify the input and output datasets exist
        self.dataset_exists(datasets['input_dataset'].id, datasets['input_dataset'].dataset_file.path)
        self.dataset_exists(datasets['output_dataset'].id, datasets['input_dataset'].dataset_file.path)

        # Check all the orphan files and records exist
        for orphan in orphans:
            _id = orphan.id
            try:
                path = orphan.dataset_file.path
            except ValueError:
                path = None

            # Verify the orphans exist
            self.dataset_exists(_id, path)
            ids_and_paths.append((_id, path))

        # Remove orphan records and files
        find_orphans.Command.remove_orphans(orphans)
        for _id, path in ids_and_paths:

            # Verify the orphan record and path no longer exist
            self.dataset_does_not_exist(_id, path)

        # Verify the input and output datasets still exist
        self.dataset_exists(datasets['input_dataset'].id, datasets['input_dataset'].dataset_file.path)
        self.dataset_exists(datasets['output_dataset'].id, datasets['input_dataset'].dataset_file.path)

    def dataset_exists(self, dataset_id, dataset_path):
        """[summary]

        Arguments:
            dataset {Dataset} -- A Dataset object
        """
        if dataset_path:
            assert os.path.isfile(dataset_path)
        try:
            Dataset.objects.get(id=dataset_id)
        except Dataset.DoesNotExist:
            raise ValidationError('Dataset should exist')

    def dataset_does_not_exist(self, dataset_id, dataset_path):
        """[summary]

        Arguments:
            dataset {Dataset} -- A Dataset object
        """
        assert not os.path.isfile(dataset_path)
        try:
            Dataset.objects.get(id=dataset_id)
        except Dataset.DoesNotExist:
            pass


@skipIfDBFeature('is_mocked')
class DatasetApiTests(BaseTestCases.ApiTestCase):

    def setUp(self):
        super(DatasetApiTests, self).setUp()
        num_cols = 12

        self.list_path = reverse("dataset-list")
        # This should equal librarian.ajax.DatasetViewSet.as_view({"get": "list"}).
        self.list_view, _, _ = resolve(self.list_path)

        with tempfile.NamedTemporaryFile() as f:
            data = ','.join(map(str, range(num_cols)))
            f.write(data.encode())
            f.seek(0)
            self.test_dataset = Dataset.create_dataset(
                file_path=None,
                user=self.kive_user,
                users_allowed=None,
                groups_allowed=None,
                cdt=None,
                keep_file=True,
                name="Test dataset",
                description="Test data for a test that tests test data",
                file_source=None,
                check=True,
                file_handle=f,
            )
            self.test_dataset_path = "{}{}/".format(self.list_path,
                                                    self.test_dataset.pk)
            self.n_preexisting_datasets = 1

        self.detail_pk = self.test_dataset.pk
        self.detail_path = reverse("dataset-detail",
                                   kwargs={'pk': self.detail_pk})
        self.redaction_path = reverse("dataset-redaction-plan",
                                      kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("dataset-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.redaction_view, _, _ = resolve(self.redaction_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def tearDown(self):
        for d in Dataset.objects.all():
            d.dataset_file.delete()

    def test_dataset_add(self):
        num_cols = 12
        num_files = 2
        expected_summaries = [('My cool file 1', True),  # name, uploaded
                              ('My cool file 0', True),
                              ('Test dataset', False)]

        with tempfile.TemporaryFile() as f:
            data = ','.join(map(str, range(num_cols)))
            f.write(data.encode())
            for i in range(num_files):
                f.seek(0, FROM_FILE_END)
                f.write('data file {}\n'.format(i).encode())
                f.seek(0)
                request = self.factory.post(
                    self.list_path,
                    {
                        'name': "My cool file %d" % i,
                        'description': 'A really cool file',
                        # No CompoundDatatype -- this is raw.
                        'dataset_file': f
                    }
                )

                force_authenticate(request, user=self.kive_user)
                resp = self.list_view(request).render().data

                self.assertIsNone(resp.get('errors'))
                self.assertEquals(resp['name'], "My cool file %d" % i)

        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        resp = self.list_view(request).data

        self.assertEquals(len(resp), num_files + self.n_preexisting_datasets)
        summaries = [(entry['name'], entry['uploaded'])
                     for entry in resp]
        self.assertEquals(expected_summaries, summaries)

    def test_dataset_add_with_blank_externals(self):
        """ Browser API leaves external dir and path blank. """
        f = SimpleUploadedFile("example.txt", b"File contents")
        request = self.factory.post(
            self.list_path,
            dict(name="Some file",
                 external_path='',
                 externalfiledirectory='',
                 dataset_file=f))

        force_authenticate(request, user=self.kive_user)
        resp = self.list_view(request).render().data

        self.assertIsNone(resp.get('errors'))
        self.assertIsNone(resp.get('non_field_errors'))
        self.assertEquals(resp['name'], "Some file")

    def test_dataset_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEquals(response.data['Datasets'], 1)
        self.assertEquals(response.data['Containers'], 0)

    def test_dataset_removal(self):
        start_count = Dataset.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Dataset.objects.all().count()
        self.assertEquals(end_count, start_count - 1)

    def test_dataset_redaction_plan(self):
        request = self.factory.get(self.redaction_path)
        force_authenticate(request, user=self.kive_user)
        response = self.redaction_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['Datasets'], 1)
        self.assertEquals(response.data['OutputLogs'], 0)

    def test_dataset_redaction(self):

        request = self.factory.patch(self.detail_path,
                                     {'is_redacted': "true"})
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        dataset = Dataset.objects.get(pk=self.detail_pk)
        self.assertTrue(dataset.is_redacted())

    def test_dataset_purge(self):
        request = self.factory.patch(self.detail_path,
                                     json.dumps({'dataset_file': None}),
                                     content_type='application/json')
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        dataset = Dataset.objects.get(pk=self.detail_pk)
        self.assertFalse(dataset.has_data())

    def test_dataset_purge_again(self):
        # Purge the dataset file.
        Dataset.objects.get(pk=self.detail_pk).dataset_file.delete(save=True)

        # Now send a request to purge it again. Should do nothing.
        request = self.factory.patch(self.detail_path,
                                     json.dumps({'dataset_file': None}),
                                     content_type='application/json')
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        dataset = Dataset.objects.get(pk=self.detail_pk)
        self.assertFalse(dataset.has_data())

    def test_dataset_view_purged(self):
        dataset = Dataset.objects.get(id=self.detail_pk)
        dataset.dataset_file.delete(save=True)

        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(
            response.data['description'],
            "Test data for a test that tests test data")
        self.assertFalse(response.data['has_data'])
        self.assertFalse(response.data['is_redacted'])


@skipIfDBFeature('is_mocked')
class DatasetSerializerTests(TestCase):
    """
    Tests of DatasetSerializer.
    """
    def setUp(self):
        self.factory = APIRequestFactory()
        self.list_path = reverse("dataset-list")

        self.myUser = User.objects.create_user('john',
                                               'lennon@thebeatles.com',
                                               'johnpassword')
        self.kive_user = kive_user()
        self.duck_context = DuckContext()

        num_cols = 12
        self.raw_file_contents = ','.join(map(str, range(num_cols))).encode()

        self.kive_file_contents = """col1
foo
bar
baz
"""

        self.data_to_serialize = {
            "name": "SerializedData",
            "description": "Dataset for testing deserialization",
            "users_allowed": [],
            "groups_allowed": []
        }

        # An external file directory.
        self.working_dir = tempfile.mkdtemp()
        self.efd = ExternalFileDirectory(
            name="WorkingDirectory",
            path=self.working_dir
        )
        self.efd.save()

        # An external file.
        _, self.ext_fn = tempfile.mkstemp(dir=self.working_dir)
        with open(self.ext_fn, "wb") as f:
            f.write(self.raw_file_contents)

        self.csv_file_temp_open_mode = "w+t"
        if dsix.PY2:
            self.csv_file_temp_open_mode = "w+b"

    def tearDown(self):
        shutil.rmtree(self.working_dir)

    def test_validate(self):
        """
        Test validating a new Dataset.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_with_users_allowed(self):
        """
        Test validating a new Dataset with users allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["users_allowed"].append(self.myUser.username)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_with_groups_allowed(self):
        """
        Test validating a new Dataset with groups allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["groups_allowed"].append(everyone_group().name)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_externally_backed(self):
        """
        Test validating a new Dataset with external backing.
        """
        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        self.data_to_serialize["external_path"] = self.ext_fn
        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        self.assertTrue(ds.is_valid())

    def test_validate_externally_backed_no_efd(self):
        """
        If external_path is present, externalfiledirectory should be also.
        """
        self.data_to_serialize["external_path"] = self.ext_fn
        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        self.assertFalse(ds.is_valid())
        self.assertListEqual(ds.errors["non_field_errors"],
                             ["externalfiledirectory must be specified"])

    def test_validate_externally_backed_no_external_path(self):
        """
        If externalfiledirectory is present, external_path should be also.
        """
        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        self.assertFalse(ds.is_valid())
        self.assertListEqual(ds.errors["non_field_errors"],
                             ["external_path must be specified"])

    def test_validate_dataset_file_specified(self):
        """
        If dataset_file is specified, external_path and externalfiledirectory should not be.
        """
        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        self.data_to_serialize["external_path"] = self.ext_fn

        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertFalse(ds.is_valid())
            self.assertSetEqual(
                set([str(e) for e in ds.errors["non_field_errors"]]),
                {
                    "external_path should not be specified if dataset_file is",
                    "externalfiledirectory should not be specified if dataset_file is"
                }
            )

    def test_create(self):
        """
        Test creating a Dataset.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            # Probe the Dataset to make sure everything looks fine.
            self.assertEquals(dataset.name, self.data_to_serialize["name"])
            self.assertEquals(dataset.description, self.data_to_serialize["description"])
            self.assertEquals(dataset.user, self.kive_user)
            self.assertTrue(bool(dataset.dataset_file))

    def test_create_do_not_retain(self):
        """
        Test creating a Dataset but without retaining a file in the DB.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["save_in_db"] = False

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            # Probe the Dataset to make sure everything looks fine.
            self.assertEquals(dataset.name, self.data_to_serialize["name"])
            self.assertEquals(dataset.description, self.data_to_serialize["description"])
            self.assertEquals(dataset.user, self.kive_user)
            self.assertFalse(bool(dataset.dataset_file))

    def test_create_with_users_allowed(self):
        """
        Test validating a new Dataset with users allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["users_allowed"].append(self.myUser.username)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            self.assertListEqual(list(dataset.users_allowed.all()),
                                 [self.myUser])

    def test_create_with_groups_allowed(self):
        """
        Test validating a new Dataset with groups allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["groups_allowed"].append(everyone_group().name)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            self.assertListEqual(list(dataset.groups_allowed.all()),
                                 [everyone_group()])

    def test_create_externally_backed(self):
        """
        Test creating a Dataset from external data.
        """
        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        self.data_to_serialize["external_path"] = os.path.basename(self.ext_fn)

        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        ds.is_valid()
        dataset = ds.save()

        # Probe the Dataset to make sure everything looks fine.
        self.assertEquals(dataset.name, self.data_to_serialize["name"])
        self.assertEquals(dataset.description, self.data_to_serialize["description"])
        self.assertEquals(dataset.user, self.kive_user)
        self.assertEquals(dataset.external_path, os.path.basename(self.ext_fn))
        self.assertEquals(dataset.externalfiledirectory, self.efd)
        self.assertFalse(bool(dataset.dataset_file))

    def test_create_externally_backed_internal_copy(self):
        """
        Test creating a Dataset from external data and keeping an internal copy.
        """

        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        self.data_to_serialize["external_path"] = os.path.basename(self.ext_fn)
        self.data_to_serialize["save_in_db"] = True

        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        ds.is_valid()
        dataset = ds.save()

        # Probe the Dataset to make sure everything looks fine.
        self.assertEquals(dataset.name, self.data_to_serialize["name"])
        self.assertEquals(dataset.description, self.data_to_serialize["description"])
        self.assertEquals(dataset.user, self.kive_user)
        self.assertEquals(dataset.external_path, os.path.basename(self.ext_fn))
        self.assertEquals(dataset.externalfiledirectory, self.efd)
        self.assertTrue(bool(dataset.dataset_file))
        dataset.dataset_file.open("rb")
        with dataset.dataset_file:
            self.assertEquals(dataset.dataset_file.read(), self.raw_file_contents)


class ExternalFileDirectoryApiMockTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        self.mock_viewset(ExternalFileDirectoryViewSet)
        super(ExternalFileDirectoryApiMockTests, self).setUp()

        self.list_path = reverse("externalfiledirectory-list")
        self.detail_pk = 43
        self.detail_path = reverse("externalfiledirectory-detail",
                                   kwargs={'pk': self.detail_pk})

        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)

        ExternalFileDirectory.objects.add(ExternalFileDirectory(id=42,
                                                                name="apples",
                                                                path="/bank/apples"),
                                          ExternalFileDirectory(id=43,
                                                                name="cherries",
                                                                path="/dock/cherries"),
                                          ExternalFileDirectory(id=44,
                                                                name="bananas",
                                                                path="/dock/bananas"))

    def test_list(self):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)
        self.assertEquals(response.data[2]['name'], 'bananas')

    def test_filter_smart(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=smart&filters[0][val]=ban")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)
        self.assertEquals(response.data[0]['name'], 'bananas')
        self.assertEquals(response.data[1]['path'], '/bank/apples')

    def test_filter_name(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=name&filters[0][val]=ban")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'bananas')

    def test_filter_path(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=path&filters[0][val]=bank")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['path'], '/bank/apples')

    def test_filter_unknown(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=bogus&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals({u'detail': u'Unknown filter key: bogus'},
                          response.data)

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['name'], 'cherries')

    @patch('os.walk')
    def test_list_files(self, mock_walk):
        mock_walk.return_value = [('/dock/cherries', [], ['foo.txt', 'bar.txt'])]
        expected_data = {
            'url': u'http://testserver/api/externalfiledirectories/43/',
            'pk': 43,
            'list_files': [('/dock/cherries/foo.txt', '[cherries]/foo.txt'),
                           ('/dock/cherries/bar.txt', '[cherries]/bar.txt')],
            'name': u'cherries',
            'path': u'/dock/cherries'
        }
        path = reverse("externalfiledirectory-list-files",
                       kwargs={'pk': self.detail_pk})

        view, _, _ = resolve(path)
        request = self.factory.get(path)
        force_authenticate(request, user=self.kive_user)
        response = view(request, pk=self.detail_pk)
        self.assertDictEqual(expected_data, response.data)


@skipIfDBFeature('is_mocked')
class ExternalFileTests(TestCase):

    def setUp(self):
        self.myUser = User.objects.create_user('john',
                                               'lennon@thebeatles.com',
                                               'johnpassword')

        self.working_dir = tempfile.mkdtemp()
        self.efd = ExternalFileDirectory(
            name="WorkingDirectory",
            path=self.working_dir
        )
        self.efd.save()

        self.ext1_path = "ext1.txt"
        self.ext1_contents = "First test file"
        with open(os.path.join(self.working_dir, self.ext1_path), "wb") as f:
            f.write(self.ext1_contents.encode())

        self.ext2_path = "ext2.txt"
        self.ext2_contents = "Second test file"
        with open(os.path.join(self.working_dir, self.ext2_path), "wb") as f:
            f.write(self.ext2_contents.encode())

        os.makedirs(os.path.join(self.working_dir, "ext_subdir"))
        os.makedirs(os.path.join(self.working_dir, "ext_subdir2"))

        self.ext_sub1_path = os.path.join("ext_subdir", "ext_sub1.txt")
        self.ext_sub1_contents = "Test file in subdirectory"
        with open(os.path.join(self.working_dir, self.ext_sub1_path), "wb") as f:
            f.write(self.ext_sub1_contents.encode())

        self.external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            externalfiledirectory=self.efd
        )
        self.external_file_ds_no_internal = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd
        )
        self.external_file_ds_subdir = Dataset.create_dataset(
            os.path.join(self.working_dir, "ext_subdir", "ext_sub1.txt"),
            user=self.myUser,
            externalfiledirectory=self.efd
        )
        self.non_external_dataset = Dataset(
            user=self.myUser,
            name="foo",
            description="Foo",
            dataset_file=ContentFile("Foo")
        )
        self.non_external_dataset.save()

    def tearDown(self):
        shutil.rmtree(self.working_dir)

    def test_save(self):
        """Calling save() normalizes the path."""
        new_working_dir = tempfile.mkdtemp()
        unnamed_efd = ExternalFileDirectory(name="TestSaveDir", path="{}/./".format(new_working_dir))
        unnamed_efd.save()
        self.assertEquals(unnamed_efd.path, os.path.normpath(new_working_dir))
        shutil.rmtree(new_working_dir)

    def test_list_files(self):
        expected_list = [
            (os.path.join(self.working_dir, self.ext1_path), "[WorkingDirectory]/{}".format(self.ext1_path)),
            (os.path.join(self.working_dir, "ext2.txt"), "[WorkingDirectory]/ext2.txt"),
            (os.path.join(self.working_dir, "ext_subdir", "ext_sub1.txt"),
             "[WorkingDirectory]/ext_subdir/ext_sub1.txt")
        ]
        self.assertSetEqual(set(expected_list), set(self.efd.list_files()))

    def test_create_dataset_external_file(self):
        """
        Create a Dataset from an external file, making a copy in the database.
        """
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            externalfiledirectory=self.efd
        )

        self.assertEquals(external_file_ds.external_path, self.ext1_path)

        external_file_ds.dataset_file.open("r")
        with external_file_ds.dataset_file:
            self.assertEquals(external_file_ds.dataset_file.read(), self.ext1_contents)

        with open(os.path.join(self.working_dir, self.ext1_path), "rb") as f:
            self.assertEquals(file_access_utils.compute_md5(f), external_file_ds.MD5_checksum)

    def test_create_dataset_external_file_no_internal_copy(self):
        """
        Create a Dataset from an external file without making a copy in the database.
        """
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd
        )

        self.assertEquals(external_file_ds.external_path, self.ext1_path)
        self.assertFalse(bool(external_file_ds.dataset_file))

        with open(os.path.join(self.working_dir, self.ext1_path), "rb") as f:
            self.assertEquals(file_access_utils.compute_md5(f), external_file_ds.MD5_checksum)

    def test_create_dataset_external_file_subdirectory(self):
        """
        Create a Dataset from an external file in a subdirectory of the external file directory.
        """
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext_sub1_path),
            user=self.myUser,
            externalfiledirectory=self.efd
        )

        self.assertEquals(external_file_ds.externalfiledirectory, self.efd)
        self.assertEquals(external_file_ds.external_path, self.ext_sub1_path)

        external_file_ds.dataset_file.open("r")
        with external_file_ds.dataset_file:
            self.assertEquals(external_file_ds.dataset_file.read(), self.ext_sub1_contents)

        with open(os.path.join(self.working_dir, self.ext_sub1_path), "rb") as f:
            self.assertEquals(file_access_utils.compute_md5(f), external_file_ds.MD5_checksum)

    def test_get_file_handle(self):
        """
        Test retrieving a file handle.
        """
        ext_sub1_path = os.path.join(self.working_dir, "ext_subdir", "ext_sub1.txt")
        external_file_ds = Dataset.create_dataset(
            ext_sub1_path,
            user=self.myUser,
            externalfiledirectory=self.efd
        )

        # Where possible get_file_handle uses the internal copy.
        with external_file_ds.get_open_file_handle("r") as data_handle:
            self.assertEquals(data_handle, external_file_ds.dataset_file)

        # It falls back on the external copy.
        external_file_ds.dataset_file.delete()
        with external_file_ds.get_open_file_handle('r') as external_file_handle:
            self.assertEquals(os.path.abspath(external_file_handle.name), ext_sub1_path)

    def test_get_file_handle_subdirectory(self):
        """
        Test retrieving a file handle on a Dataset with a file in a subdirectory.
        """
        # Where possible get_file_handle uses the internal copy.
        with self.external_file_ds.get_open_file_handle('r') as data_handle:
            self.assertEquals(data_handle, self.external_file_ds.dataset_file)

        # It falls back on the external copy.
        with self.external_file_ds_no_internal.get_open_file_handle('r') as external_file_handle:
            self.assertEquals(
                os.path.abspath(external_file_handle.name),
                os.path.abspath(os.path.join(self.working_dir, self.ext1_path))
            )

    def test_external_absolute_path(self):
        """
        Retrieve the external absolute path of an externally-backed Dataset.
        """
        ext1_path = os.path.join(self.working_dir, self.ext1_path)
        ext_sub1_path = os.path.join(self.working_dir, self.ext_sub1_path)

        self.assertEquals(self.external_file_ds.external_absolute_path(), ext1_path)
        self.assertEquals(self.external_file_ds_no_internal.external_absolute_path(), ext1_path)
        self.assertEquals(self.external_file_ds_subdir.external_absolute_path(), ext_sub1_path)
        self.assertIsNone(self.non_external_dataset.external_absolute_path())

    def test_has_data(self):
        """
        Dataset factors in presence/absence of external files when checking for data.
        """
        self.assertTrue(self.external_file_ds.has_data())
        self.assertTrue(self.external_file_ds_no_internal.has_data())
        self.assertTrue(self.external_file_ds_subdir.has_data())

        # We make an externally-backed Dataset to mess with.
        ext_path = "ext_test_has_data.txt"
        ext_contents = "File has data"
        with open(os.path.join(self.working_dir, ext_path), "wb") as f:
            f.write(ext_contents.encode())

        external_path = os.path.join(self.working_dir, ext_path)
        external_file_ds_no_internal = Dataset.create_dataset(
            external_path,
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd
        )
        # Delete this file.
        os.remove(external_path)
        assert not external_file_ds_no_internal.has_data()
        expected_error = r"No such file or directory: .*ext_test_has_data\.txt"
        with self.assertRaisesRegexp(IOError, expected_error):
            external_file_ds_no_internal.has_data(raise_errors=True)

        # Now test when the file exists but is unreadable.
        with open(os.path.join(self.working_dir, ext_path), "wb") as f:
            f.write(ext_contents.encode())
        self.assertTrue(external_file_ds_no_internal.has_data())
        os.chmod(external_path, stat.S_IWUSR | stat.S_IXUSR)
        assert not external_file_ds_no_internal.has_data()
        expected_error = r"Permission denied: .*ext_test_has_data\.txt"
        with self.assertRaisesRegexp(IOError, expected_error):
            external_file_ds_no_internal.has_data(raise_errors=True)

    def test_has_no_data(self):
        """ Purged dataset should not raise exception from has_data. """
        self.external_file_ds_no_internal.external_path = ''
        self.external_file_ds_no_internal.externalfiledirectory = None
        self.assertFalse(self.external_file_ds_no_internal.has_data())
        self.assertFalse(self.external_file_ds_no_internal.has_data(raise_errors=True))

    def test_clean_efd_external_path_both_set(self):
        """
        Both or neither of externalfiledirectory and external_path are set.
        """
        self.external_file_ds.clean()

        self.external_file_ds.externalfiledirectory = None
        self.assertRaisesRegexp(
            ValidationError,
            "Both externalfiledirectory and external_path should be set or neither should be set",
            self.external_file_ds.clean
        )

        self.external_file_ds.externalfiledirectory = self.efd
        self.external_file_ds.external_path = ""
        self.assertRaisesRegexp(
            ValidationError,
            "Both externalfiledirectory and external_path should be set or neither should be set",
            self.external_file_ds.clean
        )

        # Reduce this to a purely internal Dataset.
        self.external_file_ds.externalfiledirectory = None
        self.external_file_ds.clean()

    def test_external_file_redact_this(self):
        """
        Externally-backed Datasets should have external_path and externalfiledirectory cleared on redaction.
        """
        self.external_file_ds.redact_this()
        self.external_file_ds.refresh_from_db()
        self.assertEquals(self.external_file_ds.external_path, "")
        self.assertIsNone(self.external_file_ds.externalfiledirectory)

    def test_file_check_passes(self):
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        expected_log_messages = ''
        start_time = timezone.now()

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue()

        end_time = timezone.now()

        external_file_ds.refresh_from_db()
        self.assertGreaterEqual(external_file_ds.last_time_checked, start_time)
        self.assertLessEqual(external_file_ds.last_time_checked, end_time)
        self.assertFalse(external_file_ds.is_external_missing)
        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_missing_one(self):
        Dataset.objects.all().delete()  # Remove existing datasets.

        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.last_time_checked = timezone.now() - timedelta(minutes=1)
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())
        expected_log_messages = """\
Missing 1 external dataset. Most recent from {}, last checked a minute ago.
""".format(external_file_ds.external_absolute_path())
        start_time = timezone.now()

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue()

        external_file_ds.refresh_from_db()
        self.assertLess(external_file_ds.last_time_checked, start_time)
        self.assertTrue(external_file_ds.is_external_missing)
        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_missing_two(self):
        Dataset.objects.all().delete()  # Remove existing datasets.

        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.last_time_checked = timezone.now() - timedelta(minutes=5)
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())

        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext2_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.last_time_checked = timezone.now() - timedelta(minutes=4)
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())
        expected_log_messages = """\
Missing 2 external datasets. Most recent from {}, last checked 4 minutes ago.
""".format(external_file_ds.external_absolute_path())

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue().replace(u'\xa0', ' ')

        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_batches(self):
        Dataset.objects.all().delete()  # Remove existing datasets.

        for _ in range(10):
            Dataset.create_dataset(
                os.path.join(self.working_dir, self.ext1_path),
                user=self.myUser,
                keep_file=False,
                externalfiledirectory=self.efd)

        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext2_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.last_time_checked = timezone.now() - timedelta(minutes=4)
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())
        expected_log_messages = """\
Missing 1 external dataset. Most recent from {}, last checked 4 minutes ago.
""".format(external_file_ds.external_absolute_path())

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check(batch_size=10)
            log_messages = mocked_stderr.getvalue().replace(u'\xa0', ' ')

        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_file_restored(self):
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.is_external_missing = True
        external_file_ds.save()
        expected_log_messages = ''
        start_time = timezone.now()

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue()

        end_time = timezone.now()

        external_file_ds.refresh_from_db()
        self.assertGreaterEqual(external_file_ds.last_time_checked, start_time)
        self.assertLessEqual(external_file_ds.last_time_checked, end_time)
        self.assertFalse(external_file_ds.is_external_missing)
        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_still_missing(self):
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext2_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.is_external_missing = True
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())
        expected_log_messages = ''
        start_time = timezone.now()

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue()

        external_file_ds.refresh_from_db()
        self.assertLess(external_file_ds.last_time_checked, start_time)
        self.assertTrue(external_file_ds.is_external_missing)
        self.assertMultiLineEqual(expected_log_messages, log_messages)
