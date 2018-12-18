# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
from datetime import datetime

from django.contrib.auth.models import User, Group
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, skipIfDBFeature
from django.test.client import Client
from django.urls import reverse, resolve
from django.utils.timezone import make_aware, utc
from mock import patch
from rest_framework.reverse import reverse as rest_reverse
from rest_framework import status
from rest_framework.test import force_authenticate

from container.models import ContainerFamily, ContainerApp, Container, \
    ContainerRun, ContainerArgument, Batch, ContainerLog
from kive.tests import BaseTestCases, install_fixture_files
from librarian.models import Dataset, ExternalFileDirectory


@skipIfDBFeature('is_mocked')
class ContainerAppTests(TestCase):
    def test_default_app_empty(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        client = Client()
        client.force_login(user)
        response = client.post(reverse('container_app_add',
                                       kwargs=dict(container_id=container.id)),
                               dict(threads=1, memory=100))
        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)

    def test_add(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        client = Client()
        client.force_login(user)
        expected_inputs = 'names_csv greetings_csv'
        expected_outputs = 'messages_csv'

        response = client.post(reverse('container_app_add',
                                       kwargs=dict(container_id=container.id)),
                               dict(threads=1,
                                    memory=100,
                                    inputs=expected_inputs,
                                    outputs=expected_outputs))

        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)
        app, = container.apps.all()
        self.assertEqual(expected_inputs, app.inputs)
        self.assertEqual(expected_outputs, app.outputs)

    def test_write_inputs_replaces(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        app = container.apps.create()

        old_inputs = 'old_in_csv'
        expected_outputs = 'old_out_csv'
        expected_inputs = '--greetings_csv* -- names_csv'
        app.write_inputs(old_inputs)
        app.write_outputs(expected_outputs)

        app.write_inputs(expected_inputs)
        inputs = app.inputs
        outputs = app.outputs

        self.assertEqual(expected_inputs, inputs)
        self.assertEqual(expected_outputs, outputs)


@skipIfDBFeature('is_mocked')
class ContainerAppApiTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        super(ContainerAppApiTests, self).setUp()
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.test_app = ContainerApp.objects.create(container=container,
                                                    name='test')

        self.list_path = reverse("containerapp-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = self.test_app.pk
        self.detail_path = reverse("containerapp-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("containerapp-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        self.container_path = reverse("container-detail",
                                      kwargs={'pk': container.pk})

    def test_add(self):
        request1 = self.factory.get(self.list_path)
        force_authenticate(request1, user=self.kive_user)
        start_count = len(self.list_view(request1).data)
        expected_inputs = 'alpha bravo'
        expected_outputs = 'charlie delta'

        request2 = self.factory.post(self.list_path,
                                     dict(name="zoo app",
                                          container=self.container_path,
                                          description='A really cool app',
                                          inputs=expected_inputs,
                                          outputs=expected_outputs),
                                     format="json")

        force_authenticate(request2, user=self.kive_user)
        resp = self.list_view(request2).render().data

        self.assertIn('id', resp)
        self.assertEquals(resp['name'], "zoo app")

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data

        self.assertEquals(len(resp), start_count + 1)
        self.assertEquals(resp[-1]['description'], "A really cool app")
        self.assertEquals(resp[-1]['inputs'], expected_inputs)
        self.assertEquals(resp[-1]['outputs'], expected_outputs)

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEquals(response.data['ContainerApps'], 1)

    def test_removal(self):
        start_count = ContainerApp.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = ContainerApp.objects.all().count()
        self.assertEquals(end_count, start_count - 1)


@skipIfDBFeature('is_mocked')
class ContainerRunApiTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        super(ContainerRunApiTests, self).setUp()
        user = User.objects.first()
        self.assertIsNotNone(user)
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        app = ContainerApp.objects.create(container=container, name='test')
        arg = app.arguments.create(type=ContainerArgument.INPUT)
        dataset = Dataset.create_empty(user=user)
        self.test_run = app.runs.create(user=user)
        self.test_run.datasets.create(argument=arg, dataset=dataset)

        self.list_path = reverse("containerrun-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = self.test_run.pk
        self.detail_path = reverse("containerrun-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("containerrun-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def test_removal_plan(self):
        self.test_run.state = ContainerRun.COMPLETE
        self.test_run.save()
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEquals(response.data['ContainerRuns'], 1)

    def test_removal(self):
        self.test_run.state = ContainerRun.COMPLETE
        self.test_run.save()
        start_count = ContainerRun.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = ContainerRun.objects.all().count()
        self.assertEquals(end_count, start_count - 1)


@skipIfDBFeature('is_mocked')
class ContainerRunTests(TestCase):
    fixtures = ['container_run']
    
    def setUp(self):
        super(ContainerRunTests, self).setUp()
        install_fixture_files('container_run')

    def test_no_outputs(self):
        run = ContainerRun.objects.get(id=1)
        expected_entries = [dict(created=make_aware(datetime(2000, 1, 1), utc),
                                 name='names_csv',
                                 size='30\xa0bytes',
                                 type='Input',
                                 url='/dataset_view/1')]
        client = Client()
        client.force_login(run.user)
        response = client.get(reverse('container_run_detail',
                                      kwargs=dict(pk=run.pk)))

        self.assertEqual('New', response.context['state_name'])
        self.assertListEqual(expected_entries, response.context['data_entries'])

    def test_outputs(self):
        run = ContainerRun.objects.get(id=1)
        run.state = ContainerRun.COMPLETE
        run.end_time = make_aware(datetime(2000, 1, 2), utc)
        run.save()
        dataset = Dataset.objects.first()
        argument = run.app.arguments.get(name='greetings_csv')
        run.datasets.create(argument=argument, dataset=dataset)
        log = run.logs.create(short_text='Job completed.', type=ContainerLog.STDERR)
        expected_entries = [dict(created=make_aware(datetime(2000, 1, 1), utc),
                                 name='names_csv',
                                 size='30\xa0bytes',
                                 type='Input',
                                 url='/dataset_view/1'),
                            dict(created=make_aware(datetime(2000, 1, 2), utc),
                                 name='stderr',
                                 size='14\xa0bytes',
                                 type='Log',
                                 url='/container_logs/{}/'.format(log.id)),
                            dict(created=make_aware(datetime(2000, 1, 1), utc),
                                 name='greetings_csv',
                                 size='30\xa0bytes',
                                 type='Output',
                                 url='/dataset_view/1')]
        client = Client()
        client.force_login(run.user)
        response = client.get(reverse('container_run_detail',
                                      kwargs=dict(pk=run.pk)))

        self.assertEqual('Complete', response.context['state_name'])
        self.assertListEqual(expected_entries, response.context['data_entries'])

    @patch('container.models.check_call')
    def test(self, mock_check_call):
        run = ContainerRun.objects.filter(state=ContainerRun.NEW).first()
        self.assertIsNotNone(run)
        user = run.user

        run.request_stop(user)

        run.refresh_from_db()
        self.assertEqual(ContainerRun.CANCELLED, run.state)
        self.assertEqual(user, run.stopped_by)
        self.assertIsNotNone(run.end_time)
        self.assertEqual(0, mock_check_call.call_count)

    @patch('container.models.check_call')
    def test_cancel_running(self, mock_check_call):
        run = ContainerRun.objects.filter(state=ContainerRun.NEW).first()
        self.assertIsNotNone(run)
        run.state = ContainerRun.RUNNING
        run.slurm_job_id = 42
        run.save()
        user = run.user

        run.request_stop(user)

        run.refresh_from_db()
        self.assertEqual(ContainerRun.CANCELLED, run.state)
        self.assertEqual(user, run.stopped_by)
        self.assertIsNotNone(run.end_time)
        mock_check_call.assert_called_with(['scancel', '-f', '42'])


@skipIfDBFeature('is_mocked')
class BatchApiTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        super(BatchApiTests, self).setUp()
        user = User.objects.first()
        self.assertIsNotNone(user)
        self.test_batch = Batch.objects.create(user=user)
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.test_app = ContainerApp.objects.create(container=container, name='test')
        self.test_arg = self.test_app.arguments.create(type=ContainerArgument.INPUT)
        self.dataset = Dataset.create_empty(user=user)
        self.test_run = self.test_app.runs.create(user=user, batch=self.test_batch)
        self.test_run.datasets.create(argument=self.test_arg, dataset=self.dataset)

        self.list_path = reverse("batch-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = self.test_batch.pk
        self.detail_path = reverse("batch-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("batch-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def test_add(self):
        request1 = self.factory.get(self.list_path)
        force_authenticate(request1, user=self.kive_user)
        start_count = len(self.list_view(request1).data)

        app_url = rest_reverse('containerapp-detail',
                               kwargs=dict(pk=self.test_app.pk))
        arg_url = rest_reverse('containerargument-detail',
                               kwargs=dict(pk=self.test_arg.pk))
        dataset_url = rest_reverse('dataset-detail',
                                   kwargs=dict(pk=self.dataset.pk))
        request2 = self.factory.post(
            self.list_path,
            dict(name="my batch",
                 description='A really cool batch',
                 runs=[dict(name='my run',
                            app=app_url,
                            datasets=[dict(argument=arg_url,
                                           dataset=dataset_url)])]),
            format="json")

        force_authenticate(request2, user=self.kive_user)
        resp = self.list_view(request2).render().data

        self.assertIn('id', resp)
        self.assertEquals(resp['name'], "my batch")

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data
        resp_batch = resp[0]

        self.assertEquals(len(resp), start_count + 1)
        self.assertEquals(resp_batch['description'], "A really cool batch")

        resp_run = resp_batch['runs'][0]
        self.assertEquals(resp_run['name'], 'my run')

    def test_removal_plan(self):
        self.test_run.state = ContainerRun.COMPLETE
        self.test_run.save()
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEquals(response.data['Batches'], 1)
        self.assertEquals(response.data['ContainerRuns'], 1)

    def test_removal(self):
        self.test_run.state = ContainerRun.COMPLETE
        self.test_run.save()
        start_count = Batch.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Batch.objects.all().count()
        self.assertEquals(end_count, start_count - 1)


@skipIfDBFeature('is_mocked')
class RunContainerTests(TestCase):
    fixtures = ['container_run']

    def setUp(self):
        super(RunContainerTests, self).setUp()
        install_fixture_files('container_run')
        self.called_command = None
        self.call_stdout = ''
        self.call_stderr = ''
        self.call_return_code = 0

    def dummy_call(self, command, stdout, stderr):
        self.called_command = command
        stdout.write(self.call_stdout)
        stderr.write(self.call_stderr)
        return self.call_return_code

    def test_run(self):
        run = ContainerRun.objects.get(name='fixture run')
        everyone = Group.objects.get(name='Everyone')
        run.groups_allowed.clear()
        run.groups_allowed.add(everyone)

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        self.assertEqual(ContainerRun.COMPLETE, run.state)
        sandbox_path = run.sandbox_path
        self.assertTrue(sandbox_path)
        input_path = os.path.join(sandbox_path, 'input/names_csv')
        self.assertTrue(os.path.exists(input_path), input_path + ' should exist.')
        output_path = os.path.join(sandbox_path, 'output/greetings_csv')
        self.assertTrue(os.path.exists(output_path), output_path + ' should exist.')

        self.assertEqual(2, run.datasets.count())
        self.assertIsNotNone(run.submit_time)
        self.assertIsNotNone(run.start_time)
        self.assertIsNotNone(run.end_time)
        self.assertLessEqual(run.submit_time, run.start_time)
        self.assertLessEqual(run.start_time, run.end_time)
        output_dataset = run.datasets.get(
            argument__type=ContainerArgument.OUTPUT).dataset
        expected_groups = [('Everyone', )]
        dataset_groups = list(output_dataset.groups_allowed.values_list('name'))
        self.assertEqual(expected_groups, dataset_groups)

    def test_already_started(self):
        """ Pretend that another instance of the command already started. """
        run = ContainerRun.objects.get(name='fixture run')
        run.state = ContainerRun.LOADING
        run.save()

        with self.assertRaisesRegexp(
                CommandError,
                r'Expected state N for run id \d+, but was L'):
            call_command('runcontainer', str(run.id))

        run.refresh_from_db()
        self.assertEqual('', run.sandbox_path)

    def test_missing_output(self):
        """ Configure an extra output that the image doesn't know about. """
        run = ContainerRun.objects.get(name='fixture run')
        dataset = Dataset.objects.first()
        extra_arg = run.app.arguments.create(name='extra_csv')
        run.datasets.create(argument=extra_arg, dataset=dataset)

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        self.assertEqual(ContainerRun.FAILED, run.state)

        self.assertEqual(2, run.datasets.count())

    def test_external_dataset(self):
        run = ContainerRun.objects.get(name='fixture run')
        external_path = os.path.abspath(os.path.join(__file__,
                                                     '../../../samplecode'))
        external_directory = ExternalFileDirectory.objects.create(
            name='samplecode',
            path=external_path)
        dataset = Dataset.objects.get(name='names.csv')
        dataset.dataset_file = ''
        dataset.external_path = 'singularity/host_input/example_names.csv'
        dataset.externalfiledirectory = external_directory
        dataset.save()

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        self.assertEqual(ContainerRun.COMPLETE, run.state)
        sandbox_path = run.sandbox_path
        self.assertTrue(sandbox_path)
        input_path = os.path.join(sandbox_path, 'input/names_csv')
        self.assertTrue(os.path.exists(input_path), input_path + ' should exist.')
        output_path = os.path.join(sandbox_path, 'output/greetings_csv')
        self.assertTrue(os.path.exists(output_path), output_path + ' should exist.')

        self.assertEqual(2, run.datasets.count())

    def test_external_dataset_missing(self):
        run = ContainerRun.objects.get(name='fixture run')
        external_path = os.path.abspath(os.path.join(__file__,
                                                     '../../../samplecode'))
        external_directory = ExternalFileDirectory.objects.create(
            name='samplecode',
            path=external_path)
        dataset = Dataset.objects.get(name='names.csv')
        dataset.dataset_file = ''
        dataset.external_path = 'singularity/host_input/missing_file.csv'
        dataset.externalfiledirectory = external_directory
        dataset.save()
        self.assertIsNone(dataset.get_open_file_handle())

        with self.assertRaisesRegexp(
                IOError,
                r"No such file or directory: .*missing_file\.csv"):
            call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        self.assertEqual(ContainerRun.FAILED, run.state)

    @patch('container.management.commands.runcontainer.call')
    def test_short_stdout(self, mocked_call):
        mocked_call.side_effect = self.dummy_call
        self.call_stdout = expected_stdout = 'This should be written to stdout.'
        self.call_stderr = expected_stderr = 'Look for this on stderr.'

        run = ContainerRun.objects.get(name='fixture run')

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        self.assertEqual(ContainerRun.COMPLETE, run.state)
        stdout = run.logs.get(type=ContainerLog.STDOUT)
        stderr = run.logs.get(type=ContainerLog.STDERR)
        self.assertEqual(expected_stdout, stdout.short_text)
        self.assertEqual(expected_stderr, stderr.short_text)

    @patch('container.management.commands.runcontainer.call')
    def test_long_stderr(self, mocked_call):
        mocked_call.side_effect = self.dummy_call
        self.call_stdout = expected_stdout = 'This should be written to stdout.'
        self.call_stderr = expected_stderr = 'Look for this on stderr. ' * 81

        run = ContainerRun.objects.get(name='fixture run')

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        self.assertEqual(ContainerRun.COMPLETE, run.state)
        stdout = run.logs.get(type=ContainerLog.STDOUT)
        stderr = run.logs.get(type=ContainerLog.STDERR)
        self.assertEqual(expected_stdout, stdout.short_text)
        self.assertEqual(expected_stderr, stderr.long_text.read())
        self.assertEqual(expected_stdout, stdout.read())
        self.assertEqual(expected_stderr, stderr.read())
        self.assertEqual(expected_stdout[:10], stdout.read(10))
        self.assertEqual(expected_stderr[:10], stderr.read(10))
