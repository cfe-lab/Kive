import os
from argparse import Namespace
import tempfile
import io
import zipfile
import tarfile

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.files.base import File
from django.test import TestCase
from django.urls import reverse, resolve
from django_mock_queries.mocks import mocked_relations
from mock import patch
from rest_framework.test import force_authenticate

from container.ajax import ContainerAppViewSet
from container.management.commands import runcontainer
from container.models import Container, ContainerFamily, ContainerApp, \
    ContainerArgument, ContainerRun, ContainerDataset
from container.forms import ContainerForm
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


class ContainerCleanMockTests(TestCase):
    def setUp(self):
        super(ContainerCleanMockTests, self).setUp()
        self.alpine_path = os.path.abspath(os.path.join(
            __file__,
            '..',
            '..',
            '..',
            'samplecode',
            'singularity',
            'python2-alpine-trimmed.simg'))

        fd, self.useless_file = tempfile.mkstemp()
        with io.open(fd, mode="w") as f:
            f.write(u"foobar")

        hello_world_script = """\
#! /bin/bash
echo Hello World
"""
        _, self.zip_archive = tempfile.mkstemp()
        _, self.tar_archive = tempfile.mkstemp()
        _, self.tgz_archive = tempfile.mkstemp()
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write(hello_world_script)
            with zipfile.ZipFile(self.zip_archive, mode="w") as z:
                z.write(f.name, arcname="hello_world.sh")

            with tarfile.open(self.tar_archive, mode="w") as t:
                t.add(f.name, arcname="hello_world.sh")

            with tarfile.open(self.tgz_archive, mode="w:gz") as tgz:
                tgz.add(f.name, arcname="hello_world.sh")

    def tearDown(self):
        os.remove(self.useless_file)
        os.remove(self.zip_archive)
        os.remove(self.tar_archive)
        os.remove(self.tgz_archive)

    def test_validate_singularity_container_pass(self):
        """
        A proper Singularity container should pass validation.
        :return:
        """
        Container.validate_singularity_container(self.alpine_path)

    def test_validate_singularity_container_fail(self):
        """
        A non-Singularity container should raise an error.
        :return:
        """
        with self.assertRaisesMessage(
                ValidationError,
                Container.DEFAULT_ERROR_MESSAGES["invalid_singularity_container"]
        ):
            Container.validate_singularity_container(self.useless_file)

    def test_clean_good_singularity_image(self):
        """
        A proper Singularity container should pass validation.
        :return:
        """
        container = Container(id=42)
        container.file_type = Container.SIMG
        with open(self.alpine_path, 'rb') as alpine_file:
            container.file = File(alpine_file)
            container.clean()

    def test_clean_singularity_image_with_parent(self):
        """
        A Singularity container should not have a parent.
        :return:
        """
        parent = Container(id=41)
        container = Container(id=42, parent=parent, file_type=Container.SIMG)
        with open(self.alpine_path, "rb") as alpine_file:
            container.file = File(alpine_file)

            with self.assertRaisesMessage(
                    ValidationError,
                    Container.DEFAULT_ERROR_MESSAGES["singularity_cannot_have_parent"]
            ):
                container.clean()

    def test_good_zip_archive(self):
        """
        A good zip archive container passes validation.
        :return:
        """
        parent = Container(id=41, file_type=Container.SIMG)
        container = Container(id=42, file_type=Container.ZIP, parent=parent)
        with open(self.zip_archive, "rb") as zip_archive:
            container.file = File(zip_archive)
            container.clean()

    def test_good_tar_archive(self):
        """
        A good tar archive container passes validation.
        :return:
        """
        parent = Container(id=41, file_type=Container.SIMG)
        container = Container(id=42, file_type=Container.TAR, parent=parent)
        with open(self.tar_archive, "rb") as tar_archive:
            container.file = File(tar_archive)
            container.clean()

    def test_archive_with_no_parent(self):
        """
        An archive container must have a parent.
        :return:
        """
        container = Container(id=42, file_type=Container.ZIP)
        with open(self.zip_archive, "rb") as zip_archive:
            container.file = File(zip_archive)

            with self.assertRaisesMessage(
                    ValidationError,
                    Container.DEFAULT_ERROR_MESSAGES["archive_must_have_parent"]
            ):
                container.clean()

    def bad_archive_test_helper(self, archive_type):
        """
        Helper for testing bad archive containers.
        :return:
        """
        parent = Container(id=41, file_type=Container.SIMG)
        container = Container(id=42, file_type=archive_type, parent=parent)
        with open(self.useless_file, "rb") as f:
            container.file = File(f)
            with self.assertRaisesMessage(
                    ValidationError,
                    Container.DEFAULT_ERROR_MESSAGES["invalid_archive"]
            ):
                container.clean()

    def test_bad_zip_archive(self):
        """
        A bad zip archive file fails validation.
        :return:
        """
        self.bad_archive_test_helper(Container.ZIP)

    def test_bad_tar_archive(self):
        """
        A bad tar archive file fails validation.
        :return:
        """
        self.bad_archive_test_helper(Container.TAR)

    def test_bad_tgz_archive(self):
        """
        A bad tgz archive file fails validation.
        :return:
        """
        self.bad_archive_test_helper(Container.TGZ)

    @patch("container.models.Container.validate_singularity_container")
    def test_skip_singularity_validation(self, mock_val):
        """
        Skip Singularity validation if it's marked as having already been done.
        :param mock_val:
        :return:
        """
        container = Container(id=42, file_type=Container.SIMG)
        container.singularity_validated = True
        container.clean()
        mock_val.assert_not_called()


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
@patch('django.conf.settings.MEDIA_ROOT', new='/tmp/kive_media')
class ContainerRunMockTests(TestCase):
    def test_slurm_command_default_app(self):
        run = ContainerRun(pk=99)
        run.user = User(username='bob')
        run.app = ContainerApp()
        run.app.container = Container()
        run.app.container.family = ContainerFamily(name='my container')
        run.sandbox_path = 'run23'
        expected_command = [
            'sbatch',
            '-J', 'r99 my container',
            '--parsable',
            '--output', '/tmp/kive_media/run23/logs/job%J_node%N_stdout.txt',
            '--error', '/tmp/kive_media/run23/logs/job%J_node%N_stderr.txt',
            '-c', '1',
            '--mem', '6000',
            EXPECTED_MANAGE_PATH,
            'runcontainer',
            '99']

        command = run.build_slurm_command()

        self.assertListEqual(expected_command, command)

    def test_slurm_command_named_app(self):
        run = ContainerRun(pk=99)
        run.user = User(username='bob')
        run.app = ContainerApp(name='my_app')
        run.app.container = Container()
        run.app.container.family = ContainerFamily(name='my container')
        run.sandbox_path = 'run23'
        expected_command = [
            'sbatch',
            '-J', 'r99 my_app',
            '--parsable',
            '--output', '/tmp/kive_media/run23/logs/job%J_node%N_stdout.txt',
            '--error', '/tmp/kive_media/run23/logs/job%J_node%N_stderr.txt',
            '-c', '1',
            '--mem', '6000',
            EXPECTED_MANAGE_PATH,
            'runcontainer',
            '99']

        command = run.build_slurm_command()

        self.assertListEqual(expected_command, command)

    def test_slurm_command_custom_memory(self):
        run = ContainerRun(pk=99)
        run.user = User(username='bob')
        run.app = ContainerApp(threads=3, memory=100)
        run.app.container = Container()
        run.app.container.family = ContainerFamily(name='my container')
        run.sandbox_path = 'run23'
        expected_command = [
            'sbatch',
            '-J', 'r99 my container',
            '--parsable',
            '--output', '/tmp/kive_media/run23/logs/job%J_node%N_stdout.txt',
            '--error', '/tmp/kive_media/run23/logs/job%J_node%N_stderr.txt',
            '-c', '3',
            '--mem', '100',
            EXPECTED_MANAGE_PATH,
            'runcontainer',
            '99']

        command = run.build_slurm_command()

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
        run.sandbox_path = 'run23'
        expected_command = [
            'sbatch',
            '-J', 'r99 my container',
            '--parsable',
            '--output', '/tmp/kive_media/run23/logs/job%J_node%N_stdout.txt',
            '--error', '/tmp/kive_media/run23/logs/job%J_node%N_stderr.txt',
            '-c', '1',
            '--mem', '6000',
            '-p', 'kive-high',
            EXPECTED_MANAGE_PATH,
            'runcontainer',
            '99']

        command = run.build_slurm_command(slurm_queues)

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

    def test_build_dataset_name(self):
        run = ContainerRun(id=42)
        handler = runcontainer.Command()
        
        scenarios = [('example_csv', 'example_42.csv'),
                     ('example_tar_gz', 'example_42.tar.gz'),
                     ('csv', '42.csv'),
                     ('_csv', '_42.csv'),
                     ('_', '__42'),
                     ('no_extension', 'no_extension_42')]
        
        for argument_name, expected_dataset_name in scenarios:
            dataset_name = handler.build_dataset_name(run, argument_name)

            self.assertEqual(expected_dataset_name, dataset_name)
