from io import BytesIO
import six
from zipfile import ZipFile

from django.contrib.auth.models import Group, User
from django.core.files.base import ContentFile
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse
from django.test import TestCase
from django_mock_queries.mocks import mocked_relations
import mock
from mock import patch

from constants import users
from container.models import ContainerRun, ContainerArgument, ContainerDataset
from kive.tests import dummy_file, strip_removal_plan
from kive.tests import ViewMockTestCase
from librarian.models import Dataset
from metadata.models import kive_user, KiveUser


@mocked_relations(Dataset,
                  ContainerRun,
                  ContainerArgument,
                  ContainerDataset)
class DatasetMockTests(TestCase):
    def test_rows_with_no_errors(self):
        data_file = dummy_file("""\
name,count
Bob,20
Dave,40
""")
        expected_rows = [[('Bob', []), ('20', [])],
                         [('Dave', []), ('40', [])]]

        dataset = Dataset()
        dataset.get_open_file_handle = lambda md: data_file

        rows = list(dataset.rows(data_check=True))

        self.assertEqual(expected_rows, rows)

    def test_rows_insert_at(self):
        data_file = dummy_file("""\
name,count
Bob,20
Dave,40
""")
        expected_rows = [[('', []), ('', []), ('Bob', []), ('20', [])],
                         [('', []), ('', []), ('Dave', []), ('40', [])]]

        dataset = Dataset()
        dataset.get_open_file_handle = lambda md: data_file

        rows = list(dataset.rows(data_check=True, insert_at=[0, 1]))

        self.assertEqual(expected_rows, rows)

    def test_rows_with_no_data_check(self):
        data_file = dummy_file("""\
name,count
Bob,tw3nty
Dave,40
""")
        expected_rows = [['Bob', 'tw3nty'],
                         ['Dave', '40']]

        dataset = Dataset()
        dataset.get_open_file_handle = lambda md: data_file

        rows = list(dataset.rows(data_check=False))

        self.assertEqual(expected_rows, rows)

    def test_rows_with_no_content_check(self):
        data_file = dummy_file("""\
name,count
Bob,tw3nty
Dave,40
""")
        expected_rows = [[('Bob', []), ('tw3nty', [])],
                         [('Dave', []), ('40', [])]]

        dataset = Dataset()
        dataset.get_open_file_handle = lambda md: data_file

        rows = list(dataset.rows(data_check=True))

        self.assertEqual(expected_rows, rows)

    def test_removal_plan(self):
        dataset = Dataset(id=42)
        expected_plan = {'Datasets': {dataset}}

        plan = dataset.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_remove_input_runs(self):
        dataset = Dataset(id=42)
        run = ContainerRun(id=43, state=ContainerRun.COMPLETE)
        argument = ContainerArgument(type=ContainerArgument.INPUT)
        dataset.containers.create(run=run, argument=argument)
        expected_plan = {'ContainerRuns': {run},
                         'Datasets': {dataset}}

        plan = dataset.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_remove_input_runs_still_active(self):
        dataset = Dataset(id=42)
        run = ContainerRun(id=43, state=ContainerRun.RUNNING)
        argument = ContainerArgument(type=ContainerArgument.INPUT)
        dataset.containers.create(run=run, argument=argument)

        with self.assertRaisesRegex(ValueError,
                                    r'ContainerRun id 43 is still active\.'):
            dataset.build_removal_plan()

    def test_removal_skips_output_runs(self):
        dataset = Dataset(id=42)
        run = ContainerRun(id=43)
        argument = ContainerArgument(type=ContainerArgument.OUTPUT)
        dataset.containers.create(run=run, argument=argument)
        expected_plan = {'Datasets': {dataset}}

        plan = dataset.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_remove_duplicate_inputs(self):
        dataset = Dataset(id=42)
        run = ContainerRun(id=43, state=ContainerRun.COMPLETE)
        argument1 = ContainerArgument(type=ContainerArgument.INPUT)
        argument2 = ContainerArgument(type=ContainerArgument.INPUT)
        dataset.containers.create(run=run, argument=argument1)
        dataset.containers.create(run=run, argument=argument2)
        expected_plan = {'Datasets': {dataset},
                         'ContainerRuns': {run}}

        plan = dataset.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))


class DatasetViewMockTests(ViewMockTestCase):
    def setUp(self):
        super(DatasetViewMockTests, self).setUp()
        patcher = mocked_relations(KiveUser, Dataset, Group)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client = self.create_client()
        self.dataset = Dataset(pk='99', user=kive_user())
        self.file_content = b'example data'
        self.dataset.dataset_file = ContentFile(self.file_content, name='example.txt')

        self.other_dataset = Dataset(pk='150', user=User(pk=5))
        self.other_dataset.dataset_file = ContentFile('other content', name='other.txt')
        Dataset.objects.add(self.dataset, self.other_dataset)
        KiveUser.objects.add(KiveUser(pk=users.KIVE_USER_PK))

    def test_datasets(self):
        response = self.client.get(reverse('datasets'))

        self.assertEqual(200, response.status_code)
        self.assertFalse(response.context['is_user_admin'])

    def test_datasets_admin(self):
        kive_user().is_staff = True

        response = self.client.get(reverse('datasets'))

        self.assertEqual(200, response.status_code)
        self.assertTrue(response.context['is_user_admin'])

    def test_dataset_download_404(self):
        response = self.client.get(reverse('dataset_download',
                                           kwargs=dict(dataset_id='1000')))

        self.assertEqual(404, response.status_code)

    def test_dataset_download(self):
        response = self.client.get(reverse('dataset_download',
                                           kwargs=dict(dataset_id='99')))

        self.assertEqual(200, response.status_code)
        content_chunks = list(response.streaming_content)
        self.assertEqual([self.file_content], content_chunks)
        # under python3 serialize_headers returns bytes, not strings
        header_bytes = response.serialize_headers()
        header_str = header_bytes.decode()
        assert isinstance(header_str, six.string_types), "not a string {}".format(type(header_str))
        self.assertIn('Content-Disposition: attachment; filename="example.txt"',
                      header_str)

    def test_dataset_download_missing_file(self):
        mockdataset = mock.Mock()
        mockdataset.get_open_file_handle = mock.Mock(return_value=None)
        mockgetobj = mock.Mock(return_value=mockdataset)
        with patch("librarian.ajax.DatasetViewSet.get_object", new=mockgetobj):
            request = reverse("dataset-download", kwargs={"pk": 9999})
            response = self.client.get(request)
            self.assertEqual(response.status_code, 500, "Expected a server error")
            self.assertIn("Couldn't find dataset file for", response.json()["detail"])
        mockgetobj.assert_called_once()
        mockdataset.get_open_file_handle.assert_called_once()

    def test_dataset_view_404(self):
        response = self.client.get(reverse('dataset_view',
                                           kwargs=dict(dataset_id='1000')))

        self.assertEqual(404, response.status_code)

    def test_dataset_view(self):
        response = self.client.get(reverse('dataset_view',
                                           kwargs=dict(dataset_id='99')))

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.file_content, response.context['sample_content'])
        self.assertEqual('/datasets', response.context['return'])

    def test_dataset_view_output(self):
        """ Link back to the run that generated the output dataset. """
        response = self.client.get(reverse('dataset_view',
                                           kwargs=dict(dataset_id='99')))

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.file_content, response.context['sample_content'])
        self.assertEqual('/datasets', response.context['return'])

    def test_dataset_view_other(self):
        response = self.client.get(reverse('dataset_view',
                                           kwargs=dict(dataset_id='150')))

        self.assertEqual(404, response.status_code)

    def test_dataset_view_other_admin(self):
        kive_user().is_staff = True

        response = self.client.get(reverse('dataset_view',
                                           kwargs=dict(dataset_id='150')))

        self.assertEqual(200, response.status_code)

    def test_datasets_add_archive_empty_form(self):
        response = self.client.post(reverse('datasets_add_archive'))

        self.assertEqual(200, response.status_code)
        self.assertEqual({'dataset_file': [u'This field is required.']},
                         response.context['archiveAddDatasetForm'].errors)

    # noinspection PyUnresolvedReferences
    @patch.multiple(Dataset, register_file=mock.DEFAULT, compute_md5=mock.DEFAULT)
    def test_datasets_add_archive(self, register_file, compute_md5):
        zip_buffer = BytesIO()
        zip_file = ZipFile(zip_buffer, "w")
        expected_content = b"Hello, World!"
        zip_file.writestr("added.txt", expected_content)
        zip_file.close()
        upload_file = SimpleUploadedFile("added.zip", zip_buffer.getvalue())
        response = self.client.post(
            reverse('datasets_add_archive'),
            data=dict(dataset_file=upload_file))

        self.assertEqual(200, response.status_code)
        self.assertEqual(1, response.context['num_files_added'])
        register_file.assert_called_once()
        _, kwargs = register_file.call_args
        self.assertEqual(expected_content, kwargs['file_handle'].getvalue())
        compute_md5.assert_called_once_with()

    # noinspection PyUnresolvedReferences
    @patch.multiple(Dataset, register_file=mock.DEFAULT, compute_md5=mock.DEFAULT)
    def test_datasets_add_bulk(self, register_file, compute_md5):
        filename1 = "added1.txt"
        upload_file1 = SimpleUploadedFile(filename1, b"Hello, World!")
        filename2 = "added2.txt"
        upload_file2 = SimpleUploadedFile(filename2, b"Goodbye, Town!")
        response = self.client.post(
            reverse('datasets_add_bulk'),
            data=dict(dataset_files=[upload_file1, upload_file2]))

        self.assertEqual(200, response.status_code)
        self.assertEqual(2, response.context['num_files_added'])
        self.assertEqual(2, register_file.call_count)
        self.assertEqual(filename1,
                         register_file.call_args_list[0][1]['file_handle'].name)
        self.assertEqual(filename2,
                         register_file.call_args_list[1][1]['file_handle'].name)
        self.assertEqual(2, compute_md5.call_count)

    def test_dataset_lookup_not_found(self):
        md5_checksum = '123456789012345678901234567890ab'
        response = self.client.get(reverse(
            'dataset_lookup',
            kwargs=dict(filename='foo.txt',
                        filesize='100',
                        md5_checksum=md5_checksum)))

        self.assertEqual(200, response.status_code)
        self.assertEqual(0, response.context['num_datasets'])

    def test_dataset_lookup(self):
        md5_checksum = '123456789012345678901234567890ab'
        self.dataset.MD5_checksum = md5_checksum

        response = self.client.get(reverse(
            'dataset_lookup',
            kwargs=dict(filename='foo.txt',
                        filesize='100',
                        md5_checksum=md5_checksum)))

        self.assertEqual(200, response.status_code)
        self.assertEqual(1, response.context['num_datasets'])

    def test_lookup(self):
        """ Page is static, just check that it renders. """
        response = self.client.get(reverse('lookup'))

        self.assertEqual(200, response.status_code)
