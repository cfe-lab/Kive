import os
from io import BytesIO

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import InMemoryUploadedFile, TemporaryUploadedFile
from django.test import TestCase
from django.urls import reverse, resolve
from django_mock_queries.mocks import mocked_relations
from rest_framework.test import force_authenticate

from container.ajax import ContainerAppViewSet
from container.models import Container, ContainerFamily, ContainerApp, \
    ContainerArgument, ContainerFileFormField
from kive.tests import BaseTestCases
from metadata.models import KiveUser


@mocked_relations(Container, ContainerFamily)
class ContainerMockTests(TestCase):
    def test_str(self):
        family = ContainerFamily(name='Spline Reticulator')
        container = Container(tag='v1.0.7', family=family)

        s = str(container)

        self.assertEqual("Spline Reticulator:v1.0.7", s)


class ContainerFileFormFieldMockTests(TestCase):
    def setUp(self):
        super(ContainerFileFormFieldMockTests, self).setUp()
        self.alpine_path = os.path.abspath(os.path.join(
            __file__,
            '..',
            '..',
            '..',
            'samplecode',
            'singularity',
            'python2-alpine-trimmed.simg'))

    def test_uploaded_invalid(self):
        file_data = BytesIO(b'garbage content')
        uploaded_file = InMemoryUploadedFile(
            file=file_data,
            field_name='some_field',
            name='example.simg',
            content_type='application/octet-stream',
            size=15,
            charset=None,
            content_type_extra={})
        field = ContainerFileFormField()
        with self.assertRaisesRegexp(ValidationError,
                                     'Upload a valid container file.'):
            field.clean(uploaded_file)

    def test_uploaded_valid(self):
        with open(self.alpine_path, 'rb') as alpine_file:
            file_data = BytesIO(alpine_file.read())

        uploaded_file = InMemoryUploadedFile(
            file=file_data,
            field_name='some_field',
            name='example.simg',
            content_type='application/octet-stream',
            size=os.stat(self.alpine_path).st_size,
            charset=None,
            content_type_extra={})
        field = ContainerFileFormField()
        field.clean(uploaded_file)

    def test_temp_file_invalid(self):
        file_data = b'garbage content'

        uploaded_file = TemporaryUploadedFile(
            name='example.simg',
            content_type='application/octet-stream',
            size=15,
            charset=None,
            content_type_extra={})
        # noinspection PyArgumentList,PyCallByClass
        uploaded_file.file.write(file_data)
        field = ContainerFileFormField()
        with self.assertRaisesRegexp(ValidationError,
                                     'Upload a valid container file.'):
            field.clean(uploaded_file)

    def test_temp_file_valid(self):
        with open(self.alpine_path, 'rb') as alpine_file:
            uploaded_file = TemporaryUploadedFile(
                name='example.simg',
                content_type='application/octet-stream',
                size=15,
                charset=None,
                content_type_extra={})
            # noinspection PyArgumentList
            uploaded_file.file.close()
            uploaded_file.file = alpine_file
            field = ContainerFileFormField()
            field.clean(uploaded_file)

    def test_none(self):
        uploaded_file = None
        field = ContainerFileFormField(required=False)
        field.clean(uploaded_file)


@mocked_relations(ContainerApp, ContainerArgument)
class ContainerAppMockTests(TestCase):
    def test_arguments(self):
        app = ContainerApp()
        app.arguments.create(name='greetings_csv',
                             position=1,
                             type=ContainerArgument.INPUT)
        app.arguments.create(name='names_csv',
                             position=2,
                             type=ContainerArgument.INPUT)
        app.arguments.create(name='messages_csv',
                             position=1,
                             type=ContainerArgument.OUTPUT)
        expected_inputs = 'greetings_csv names_csv'
        expected_outputs = 'messages_csv'

        inputs = app.inputs
        outputs = app.outputs

        self.assertEqual(expected_inputs, inputs)
        self.assertEqual(expected_outputs, outputs)

    def test_optional_arguments(self):
        app = ContainerApp()
        app.arguments.create(name='greetings_csv',
                             position=1,
                             type=ContainerArgument.INPUT)
        app.arguments.create(name='names_csv',
                             type=ContainerArgument.INPUT)
        app.arguments.create(name='messages_csv',
                             position=1,
                             type=ContainerArgument.OUTPUT)
        expected_inputs = '--names_csv greetings_csv'
        expected_outputs = 'messages_csv'

        inputs = app.inputs
        outputs = app.outputs

        self.assertEqual(expected_inputs, inputs)
        self.assertEqual(expected_outputs, outputs)

    def test_multiple_arguments(self):
        app = ContainerApp()
        app.arguments.create(name='greetings_csv',
                             position=1,
                             type=ContainerArgument.INPUT)
        app.arguments.create(name='names_csv',
                             position=2,
                             allow_multiple=True,
                             type=ContainerArgument.INPUT)
        app.arguments.create(name='messages_csv',
                             position=1,
                             type=ContainerArgument.OUTPUT,
                             allow_multiple=True)
        expected_inputs = 'greetings_csv names_csv*'
        expected_outputs = 'messages_csv/'

        inputs = app.inputs
        outputs = app.outputs

        self.assertEqual(expected_inputs, inputs)
        self.assertEqual(expected_outputs, outputs)

    def test_optional_multiple_arguments(self):
        app = ContainerApp()
        app.arguments.create(name='greetings_csv',
                             position=1,
                             type=ContainerArgument.INPUT)
        app.arguments.create(name='names_csv',
                             allow_multiple=True,
                             type=ContainerArgument.INPUT)
        app.arguments.create(name='messages_csv',
                             position=1,
                             type=ContainerArgument.OUTPUT)
        app.arguments.create(name='log_csv',
                             allow_multiple=True,
                             type=ContainerArgument.OUTPUT)
        expected_inputs = '--names_csv* -- greetings_csv'
        expected_outputs = '--log_csv/ messages_csv'

        inputs = app.inputs
        outputs = app.outputs

        self.assertEqual(expected_inputs, inputs)
        self.assertEqual(expected_outputs, outputs)

    def test_write_inputs(self):
        app = ContainerApp()
        expected_inputs = 'greetings_csv names_csv'

        app.write_inputs(expected_inputs)
        inputs = app.inputs

        self.assertEqual(expected_inputs, inputs)

    def test_write_outputs(self):
        app = ContainerApp()
        expected_outputs = 'greetings_csv names_csv'

        app.write_outputs(expected_outputs)
        outputs = app.outputs

        self.assertEqual(expected_outputs, outputs)

    def test_write_arguments_bad_name(self):
        app = ContainerApp()
        with self.assertRaisesRegexp(ValueError,
                                     r'Invalid argument name: @greetings_csv'):
            app.write_outputs('@greetings_csv names_csv')

    def test_write_optional(self):
        app = ContainerApp()
        expected_outputs = '--greetings_csv names_csv'

        app.write_outputs(expected_outputs)
        outputs = app.outputs

        self.assertEqual(expected_outputs, outputs)

    def test_write_input_multiple(self):
        app = ContainerApp()
        expected_inputs = 'greetings_csv* names_csv'

        app.write_inputs(expected_inputs)
        inputs = app.inputs

        self.assertEqual(expected_inputs, inputs)

    def test_write_output_multiple(self):
        app = ContainerApp()
        expected_outputs = 'greetings_csv/ names_csv'

        app.write_outputs(expected_outputs)
        outputs = app.outputs

        self.assertEqual(expected_outputs, outputs)

    def test_write_input_bad_multiple(self):
        app = ContainerApp()
        with self.assertRaisesRegexp(ValueError,
                                     r'Invalid argument name: greetings_csv/'):
            app.write_inputs('greetings_csv/ names_csv')

    def test_write_output_bad_multiple(self):
        app = ContainerApp()
        with self.assertRaisesRegexp(ValueError,
                                     r'Invalid argument name: greetings_csv*'):
            app.write_outputs('greetings_csv* names_csv')

    def test_write_inputs_divider(self):
        app = ContainerApp()
        expected_inputs = '--greetings_csv* -- names_csv'

        app.write_inputs(expected_inputs)
        inputs = app.inputs

        self.assertEqual(expected_inputs, inputs)


class ContainerAppApiMockTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        self.mock_viewset(ContainerAppViewSet)
        super(ContainerAppApiMockTests, self).setUp()

        patcher = mocked_relations(Container)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.list_path = reverse("containerapp-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = 43
        self.detail_path = reverse("containerapp-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("containerapp-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        self.my_user = User(pk=1000)
        User.objects.add(self.my_user)
        my_kive_user = KiveUser(pk=self.my_user.pk, username='me')
        KiveUser.objects.add(my_kive_user)

        other_user = User(pk=1001)
        User.objects.add(other_user)
        other_kive_user = KiveUser(pk=other_user.pk)
        KiveUser.objects.add(other_kive_user)

        my_container = Container.objects.create(id=100, user=my_kive_user)
        other_container = Container.objects.create(id=101, user=other_kive_user)
        archive = ContainerApp(pk=42, name='archive', description='impressive')
        compress = ContainerApp(pk=43, name='compress')
        backup = ContainerApp(pk=44, name='backup')
        distribute = ContainerApp(pk=45, name='distribute')
        archive.container = compress.container = backup.container = my_container
        distribute.container = other_container
        ContainerApp.objects.add(archive, compress, backup, distribute)

    def test_list(self):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.my_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)
        self.assertEquals(response.data[2]['name'], 'backup')

    def test_filter_smart(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=smart&filters[0][val]=press")
        force_authenticate(request, user=self.my_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)
        self.assertEquals(response.data[0]['name'], 'compress')
        self.assertEquals(response.data[1]['description'], 'impressive')

    def test_filter_name(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=name&filters[0][val]=press")
        force_authenticate(request, user=self.my_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'compress')

    def test_filter_description(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=description&filters[0][val]=press")
        force_authenticate(request, user=self.my_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['description'], 'impressive')

    def test_filter_unknown(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=bogus&filters[0][val]=kive")
        force_authenticate(request, user=self.my_user)
        response = self.list_view(request, pk=None)

        self.assertEquals({u'detail': u'Unknown filter key: bogus'},
                          response.data)
