import logging
import os
from argparse import Namespace
from datetime import timedelta
from io import BytesIO, StringIO

import errno
from django.conf import settings
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import InMemoryUploadedFile, TemporaryUploadedFile
from django.test import TestCase
from django.urls import reverse, resolve
from django.utils import timezone
from django_mock_queries.mocks import mocked_relations
from rest_framework.test import force_authenticate

from container.ajax import ContainerAppViewSet
from container.management.commands import runcontainer, purge_sandboxes
from container.models import Container, ContainerFamily, ContainerApp, \
    ContainerArgument, ContainerFileFormField, ContainerRun, ContainerDataset
from kive.tests import BaseTestCases, strip_removal_plan
from librarian.models import Dataset
from metadata.models import KiveUser
from method.models import Method

EXPECTED_MANAGE_PATH = os.path.abspath(os.path.join(__file__,
                                                    '../../manage.py'))


@mocked_relations(Container, ContainerFamily)
class ContainerFamilyMockTests(TestCase):
    def test_removal(self):
        family = ContainerFamily(id=42)
        expected_plan = {'ContainerFamilies': {family}}

        plan = family.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_removal_with_app(self):
        family = ContainerFamily(id=42)
        container = family.containers.create(id=43)
        expected_plan = {'ContainerFamilies': {family},
                         'Containers': {container}}

        plan = family.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))


@mocked_relations(Container, ContainerFamily, ContainerApp, Method)
class ContainerMockTests(TestCase):
    def test_str(self):
        family = ContainerFamily(name='Spline Reticulator')
        container = Container(tag='v1.0.7', family=family)

        s = str(container)

        self.assertEqual("Spline Reticulator:v1.0.7", s)

    def test_removal(self):
        container = Container(id=42)
        expected_plan = {'Containers': {container}}

        plan = container.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_removal_with_app(self):
        container = Container(id=42)
        app = container.apps.create(id=43)
        expected_plan = {'Containers': {container},
                         'ContainerApps': {app}}

        plan = container.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_removal_with_method(self):
        container = Container(id=42)
        method = container.methods.create(transformation_ptr_id=43)
        expected_plan = {'Containers': {container},
                         'Methods': {method}}

        plan = container.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))


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


@mocked_relations(Container,
                  ContainerApp,
                  ContainerArgument,
                  ContainerRun,
                  ContainerDataset,
                  Dataset)
class ContainerAppMockTests(TestCase):
    def test_display_name(self):
        app = ContainerApp(name='reticulate')
        app.container = Container(tag='v1.0')
        app.container.family = ContainerFamily(name='Splines')
        expected_display_name = 'Splines:v1.0 / reticulate'

        display_name = app.display_name
        app_str = str(app)

        self.assertEqual(expected_display_name, display_name)
        self.assertEqual(expected_display_name, app_str)

    def test_display_default(self):
        app = ContainerApp(name='')  # default app
        app.container = Container(tag='v1.0')
        app.container.family = ContainerFamily(name='Splines')
        expected_display_name = 'Splines:v1.0'

        display_name = app.display_name

        self.assertEqual(expected_display_name, display_name)

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

    def test_removal(self):
        app = ContainerApp(id=42)
        expected_plan = {'ContainerApps': {app}}

        plan = app.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_removal_with_run(self):
        app = ContainerApp(id=42)
        run = app.runs.create(id=43, state=ContainerRun.COMPLETE)
        expected_plan = {'ContainerApps': {app},
                         'ContainerRuns': {run}}

        plan = app.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_removal_with_linked_runs(self):
        """ One run's output is used as another's input, watch for dups. """
        app = ContainerApp(id=42)
        run1 = app.runs.create(id=43, state=ContainerRun.COMPLETE)
        dataset = Dataset.objects.create(id=44)
        run_dataset1 = run1.datasets.create(
            id=45,
            run=run1,
            dataset=dataset,
            argument=ContainerArgument(type=ContainerArgument.OUTPUT))
        run2 = app.runs.create(id=46, state=ContainerRun.COMPLETE)
        run_dataset2 = run2.datasets.create(
            id=47,
            run=run2,
            dataset=dataset,
            argument=ContainerArgument(type=ContainerArgument.INPUT))
        dataset.containers.add(run_dataset1)
        dataset.containers.add(run_dataset2)
        expected_plan = {'ContainerApps': {app},
                         'ContainerRuns': {run1, run2},
                         'Datasets': {dataset}}

        plan = app.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))


class ContainerAppApiMockTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        self.mock_viewset(ContainerAppViewSet)
        super(ContainerAppApiMockTests, self).setUp()

        patcher = mocked_relations(Container, ContainerFamily)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.list_path = reverse("containerapp-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = 43
        self.detail_path = reverse("containerapp-detail",
                                   kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)

        self.my_user = User(pk=1000)
        User.objects.add(self.my_user)
        my_kive_user = KiveUser(pk=self.my_user.pk, username='me')
        KiveUser.objects.add(my_kive_user)

        other_user = User(pk=1001)
        User.objects.add(other_user)
        other_kive_user = KiveUser(pk=other_user.pk)
        KiveUser.objects.add(other_kive_user)

        my_container = Container.objects.create(id=100, user=my_kive_user)
        my_container.family = ContainerFamily.objects.create()
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


@mocked_relations(ContainerRun,
                  ContainerApp,
                  ContainerArgument,
                  ContainerDataset,
                  Dataset)
class ContainerRunMockTests(TestCase):
    def test_slurm_command_default_app(self):
        run = ContainerRun(pk=99)
        run.user = User(username='bob')
        run.app = ContainerApp()
        run.app.container = Container()
        run.app.container.family = ContainerFamily(name='my container')
        sandbox_root = '/Sandboxes'
        expected_command = [
            'sbatch',
            '-J', 'r99 my container',
            '--output', '/Sandboxes/userbob_run99__job%J_node%N_stdout.txt',
            '--error', '/Sandboxes/userbob_run99__job%J_node%N_stderr.txt',
            '--export', 'all',
            '-c', '1',
            '--mem', '6000',
            EXPECTED_MANAGE_PATH,
            'runcontainer',
            '99']

        command = run.build_slurm_command(sandbox_root)

        self.assertListEqual(expected_command, command)

    def test_slurm_command_named_app(self):
        run = ContainerRun(pk=99)
        run.user = User(username='bob')
        run.app = ContainerApp(name='my_app')
        run.app.container = Container()
        run.app.container.family = ContainerFamily(name='my container')
        sandbox_root = '/Sandboxes'
        expected_command = [
            'sbatch',
            '-J', 'r99 my_app',
            '--output', '/Sandboxes/userbob_run99__job%J_node%N_stdout.txt',
            '--error', '/Sandboxes/userbob_run99__job%J_node%N_stderr.txt',
            '--export', 'all',
            '-c', '1',
            '--mem', '6000',
            EXPECTED_MANAGE_PATH,
            'runcontainer',
            '99']

        command = run.build_slurm_command(sandbox_root)

        self.assertListEqual(expected_command, command)

    def test_slurm_command_custom_memory(self):
        run = ContainerRun(pk=99)
        run.user = User(username='bob')
        run.app = ContainerApp(threads=3, memory=100)
        run.app.container = Container()
        run.app.container.family = ContainerFamily(name='my container')
        sandbox_root = '/Sandboxes'
        expected_command = [
            'sbatch',
            '-J', 'r99 my container',
            '--output', '/Sandboxes/userbob_run99__job%J_node%N_stdout.txt',
            '--error', '/Sandboxes/userbob_run99__job%J_node%N_stderr.txt',
            '--export', 'all',
            '-c', '3',
            '--mem', '100',
            EXPECTED_MANAGE_PATH,
            'runcontainer',
            '99']

        command = run.build_slurm_command(sandbox_root)

        self.assertListEqual(expected_command, command)

    def test_slurm_command_priority(self):
        run = ContainerRun(pk=99)
        run.user = User(username='bob')
        run.app = ContainerApp()
        run.app.container = Container()
        run.app.container.family = ContainerFamily(name='my container')
        slurm_queues = (('low', 'kive-low'),
                        ('medium', 'kive-medium'),
                        ('high', 'kive-high'))
        run.priority = 2
        sandbox_root = '/Sandboxes'
        expected_command = [
            'sbatch',
            '-J', 'r99 my container',
            '--output', '/Sandboxes/userbob_run99__job%J_node%N_stdout.txt',
            '--error', '/Sandboxes/userbob_run99__job%J_node%N_stderr.txt',
            '--export', 'all',
            '-c', '1',
            '--mem', '6000',
            '-p', 'kive-high',
            EXPECTED_MANAGE_PATH,
            'runcontainer',
            '99']

        command = run.build_slurm_command(sandbox_root, slurm_queues)

        self.assertListEqual(expected_command, command)

    def test_removal(self):
        run = ContainerRun(id=42, state=ContainerRun.COMPLETE)
        expected_plan = {'ContainerRuns': {run}}

        plan = run.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_remove_outputs(self):
        run = ContainerRun(id=42, state=ContainerRun.COMPLETE)
        dataset = Dataset(id=43)
        argument = ContainerArgument(type=ContainerArgument.OUTPUT)
        run.datasets.create(dataset=dataset,
                            argument=argument)
        expected_plan = {'ContainerRuns': {run},
                         'Datasets': {dataset}}

        plan = run.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_removal_skips_inputs(self):
        run = ContainerRun(id=42, state=ContainerRun.COMPLETE)
        dataset = Dataset(id=43)
        argument = ContainerArgument(type=ContainerArgument.INPUT)
        run.datasets.create(dataset=dataset,
                            argument=argument)
        expected_plan = {'ContainerRuns': {run}}

        plan = run.build_removal_plan()

        self.assertEqual(expected_plan, strip_removal_plan(plan))

    def test_remove_running(self):
        run = ContainerRun(id=42, state=ContainerRun.RUNNING)

        with self.assertRaisesRegexp(ValueError,
                                     r'ContainerRun id 42 is still active.'):
            run.build_removal_plan()

    def create_sandbox(self, run_id, age=timedelta(0), size=1):
        """ Create a run and its sandbox.

        :param int run_id: id number
        :param timedelta age: how long ago the run ended
        :param int size: number of bytes to write in the sandbox folder
        :return ContainerRun: the new run object
        """
        sandbox_root = os.path.join(settings.MEDIA_ROOT, settings.SANDBOX_PATH)
        try:
            os.makedirs(sandbox_root)
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise
        now = timezone.now()
        user = User(username='joe')
        run_command = runcontainer.Command()
        run = ContainerRun.objects.create(id=run_id, user=user, app_id=100)
        run_command.create_sandbox(run)
        self.assertTrue(os.path.exists(run.sandbox_path))
        run.end_time = now - age
        with open(os.path.join(run.sandbox_path, 'contents.txt'), 'w') as f:
            f.write('.' * size)
        return run

    def test_purge_incomplete(self):
        run = self.create_sandbox(42, age=timedelta(minutes=10), size=100)
        run.end_time = None

        purge_sandboxes.Command().handle(delay=timedelta(minutes=2),
                                         unregistered=False)

        self.assertFalse(run.sandbox_purged)
        self.assertTrue(os.path.exists(run.sandbox_path))
        self.assertIsNone(run.sandbox_size)

    def test_purge_too_new(self):
        run = self.create_sandbox(42, age=timedelta(minutes=10), size=100)

        purge_sandboxes.Command().handle(delay=timedelta(minutes=11),
                                         unregistered=False)

        self.assertFalse(run.sandbox_purged)
        self.assertTrue(os.path.exists(run.sandbox_path))
        self.assertEqual(100, run.sandbox_size)

    def test_purge_no_sandbox(self):
        """ Sometimes a sandbox doesn't get created, and the path is blank. """
        run = self.create_sandbox(42, age=timedelta(minutes=10), size=100)
        run.sandbox_path = ''

        purge_sandboxes.Command().handle(delay=timedelta(minutes=10),
                                         unregistered=False)

        self.assertFalse(run.sandbox_purged)
        self.assertIsNone(run.sandbox_size)

    def test_purge_folder(self):
        run = self.create_sandbox(42, age=timedelta(minutes=10), size=100)

        purge_sandboxes.Command().handle(delay=timedelta(minutes=10),
                                         unregistered=False)

        self.assertTrue(run.sandbox_purged)
        self.assertFalse(os.path.exists(run.sandbox_path))
        self.assertEqual(100, run.sandbox_size)

    def test_purge_info_logging(self):
        self.create_sandbox(42, age=timedelta(minutes=11), size=100)
        self.create_sandbox(43, age=timedelta(minutes=10), size=200)
        self.create_sandbox(44, age=timedelta(minutes=9), size=400)
        expected_messages = u"""\
Removed 2 sandboxes containing 300\xa0bytes.
"""
        mocked_stderr = StringIO()
        stream_handler = logging.StreamHandler(mocked_stderr)
        logger = logging.getLogger('container.management.commands.purge_sandboxes')
        logger.addHandler(stream_handler)
        old_level = logger.level
        logger.level = logging.INFO
        try:
            purge_sandboxes.Command().handle(delay=timedelta(minutes=9.5),
                                             unregistered=False)
        finally:
            logger.removeHandler(stream_handler)
            logger.level = old_level

        log_messages = mocked_stderr.getvalue()
        self.assertEqual(expected_messages, log_messages)

    def test_purge_debug_logging(self):
        self.create_sandbox(42, age=timedelta(minutes=11), size=100)
        self.create_sandbox(43, age=timedelta(minutes=10), size=200)
        self.create_sandbox(44, age=timedelta(minutes=9), size=400)
        expected_messages = u"""\
Run 42 contained 100\xa0bytes.
Run 43 contained 200\xa0bytes.
Removed 2 sandboxes containing 300\xa0bytes.
"""
        mocked_stderr = StringIO()
        stream_handler = logging.StreamHandler(mocked_stderr)
        logger = logging.getLogger('container.management.commands.purge_sandboxes')
        logger.addHandler(stream_handler)
        old_level = logger.level
        logger.level = logging.DEBUG
        try:
            purge_sandboxes.Command().handle(delay=timedelta(minutes=9.5),
                                             unregistered=False)
        finally:
            logger.removeHandler(stream_handler)
            logger.level = old_level

        log_messages = mocked_stderr.getvalue()
        self.assertEqual(expected_messages, log_messages)


@mocked_relations(ContainerRun, ContainerApp, ContainerArgument)
class RunContainerMockTests(TestCase):
    def build_run(self):
        run = ContainerRun()
        run.app = ContainerApp()
        run.app.container = Container()
        run.app.container.file = Namespace(path='/tmp/foo.simg')
        run.sandbox_path = '/tmp/box23'
        run.app.arguments.create(type=ContainerArgument.INPUT, name='in_csv')
        run.app.arguments.create(type=ContainerArgument.OUTPUT, name='out_csv')
        return run

    def test_default_app(self):
        run = self.build_run()
        handler = runcontainer.Command()
        expected_command = [
            'singularity',
            'run',
            '--contain',
            '--cleanenv',
            '-B', '/tmp/box23/input:/mnt/input,/tmp/box23/output:/mnt/output',
            '/tmp/foo.simg',
            '/mnt/input/in_csv',
            '/mnt/output/out_csv']

        command = handler.build_command(run)

        self.assertListEqual(expected_command, command)

    def test_named_app(self):
        run = self.build_run()
        run.app.name = 'other_app'
        handler = runcontainer.Command()
        expected_command = [
            'singularity',
            'run',
            '--contain',
            '--cleanenv',
            '-B', '/tmp/box23/input:/mnt/input,/tmp/box23/output:/mnt/output',
            '--app', 'other_app',
            '/tmp/foo.simg',
            '/mnt/input/in_csv',
            '/mnt/output/out_csv']

        command = handler.build_command(run)

        self.assertListEqual(expected_command, command)
