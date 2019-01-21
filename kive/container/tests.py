# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import os
import re
import shutil
from contextlib import contextmanager
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from django.conf import settings
from django.contrib.auth.models import User, Group
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, skipIfDBFeature
from django.test.client import Client
from django.urls import reverse, resolve
from django.utils import timezone
from django.utils.timezone import make_aware, utc
from mock import patch
from rest_framework.reverse import reverse as rest_reverse
from rest_framework import status
from rest_framework.test import force_authenticate

from container.management.commands import purge, runcontainer
from container.models import ContainerFamily, ContainerApp, Container, \
    ContainerRun, ContainerArgument, Batch, ContainerLog
from kive.tests import BaseTestCases, install_fixture_files, capture_log_stream
from librarian.models import Dataset, ExternalFileDirectory, get_upload_path


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

    @patch('container.models.check_output')
    def test_slurm_ended(self, mock_check_output):
        ContainerRun.objects.update(slurm_job_id=None)
        self.test_run.slurm_job_id = 42
        self.test_run.save()
        other_run = self.test_run.app.runs.create(user=self.test_run.user,
                                                  slurm_job_id=43)
        end_time = (datetime.now() -
                    timedelta(seconds=61)).strftime('%y-%m-%dT%H:%M:%S')
        mock_check_output.return_value = """\
42|<end-time>
42.batch|<end-time>
""".replace('<end-time>', end_time)

        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)

        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.test_run.refresh_from_db()
        other_run.refresh_from_db()
        self.assertEqual(ContainerRun.FAILED, self.test_run.state)
        self.assertEqual(ContainerRun.NEW, other_run.state)
        self.assertIsNotNone(self.test_run.end_time)
        self.assertIsNone(other_run.end_time)

    @patch('container.models.check_output')
    def test_slurm_just_ended(self, mock_check_output):
        ContainerRun.objects.update(slurm_job_id=None)
        self.test_run.slurm_job_id = 42
        self.test_run.save()
        other_run = self.test_run.app.runs.create(user=self.test_run.user,
                                                  slurm_job_id=43)
        end_time = (datetime.now() -
                    timedelta(seconds=59)).strftime('%Y-%m-%dT%H:%M:%S')
        mock_check_output.return_value = """\
42|<end-time>
42.batch|<end-time>
""".replace('<end-time>', end_time)

        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)

        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.test_run.refresh_from_db()
        other_run.refresh_from_db()
        self.assertEqual(ContainerRun.NEW, self.test_run.state)
        self.assertEqual(ContainerRun.NEW, other_run.state)
        self.assertIsNone(self.test_run.end_time)
        self.assertIsNone(other_run.end_time)

    @patch('container.models.check_output')
    def test_check_slurm_after_success(self, mock_check_output):
        """ When a run is already completed, don't mark it as failed. """
        end_time = timezone.now() - timedelta(seconds=61)
        end_time_text = end_time.strftime('%y-%m-%dT%H:%M:%S')
        ContainerRun.objects.update(slurm_job_id=None)
        self.test_run.slurm_job_id = 42
        self.test_run.state = ContainerRun.COMPLETE
        self.test_run.end_time = end_time
        self.test_run.save()
        mock_check_output.return_value = """\
42|<end-time>
42.batch|<end-time>
""".replace('<end-time>', end_time_text)

        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)

        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.test_run.refresh_from_db()
        self.assertEqual(ContainerRun.COMPLETE, self.test_run.state)
        self.assertEqual(end_time, self.test_run.end_time)

    @patch('container.models.check_output')
    def test_runs_finished(self, mock_check_output):
        ContainerRun.objects.update(slurm_job_id=None)
        self.test_run.slurm_job_id = 42
        self.test_run.save()
        other_run = self.test_run.app.runs.create(user=self.test_run.user,
                                                  slurm_job_id=43)
        end_time = (datetime.now() -
                    timedelta(seconds=61)).strftime('%Y-%m-%dT%H:%M:%S')
        mock_check_output.return_value = """\
42|<end-time>
42.batch|<end-time>
43|<end-time>
43.batch|<end-time>
""".replace('<end-time>', end_time)

        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)

        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.test_run.refresh_from_db()
        other_run.refresh_from_db()
        self.assertEqual(ContainerRun.FAILED, self.test_run.state)
        self.assertEqual(ContainerRun.FAILED, other_run.state)
        self.assertIsNotNone(self.test_run.end_time)
        self.assertIsNotNone(other_run.end_time)

    @patch('container.models.check_output')
    def test_some_runs_finished(self, mock_check_output):
        ContainerRun.objects.update(slurm_job_id=None)
        self.test_run.slurm_job_id = 42
        self.test_run.save()
        other_run = self.test_run.app.runs.create(user=self.test_run.user,
                                                  slurm_job_id=43)
        end_time = (datetime.now() -
                    timedelta(seconds=61)).strftime('%Y-%m-%dT%H:%M:%S')
        mock_check_output.return_value = """\
42|<end-time>
42.batch|<end-time>
43|Unknown
""".replace('<end-time>', end_time)

        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)

        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.test_run.refresh_from_db()
        other_run.refresh_from_db()
        self.assertEqual(ContainerRun.FAILED, self.test_run.state)
        self.assertEqual(ContainerRun.NEW, other_run.state)
        self.assertIsNotNone(self.test_run.end_time)
        self.assertIsNone(other_run.end_time)

    @patch('container.models.check_output')
    def test_no_active_runs(self, mock_check_output):
        ContainerRun.objects.update(state=ContainerRun.CANCELLED)

        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)

        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual([], mock_check_output.call_args_list)

    @patch('container.models.check_output')
    def test_null_slurm_job_id(self, mock_check_output):
        ContainerRun.objects.update(slurm_job_id=None)

        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request)

        self.assertEqual(status.HTTP_200_OK, response.status_code)
        self.assertEqual([], mock_check_output.call_args_list)


@skipIfDBFeature('is_mocked')
class ContainerRunTests(TestCase):
    fixtures = ['container_run']

    def setUp(self):
        super(ContainerRunTests, self).setUp()
        install_fixture_files('container_run')

    def test_no_outputs(self):
        run = ContainerRun.objects.get(id=1)
        expected_entries = [dict(created=make_aware(datetime(2000, 1, 1), utc),
                                 name='names.csv',
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
        dataset = Dataset.objects.create(
            user=run.user,
            name='greetings_123.csv',
            date_created=make_aware(datetime(2000, 1, 1), utc))
        argument = run.app.arguments.get(name='greetings_csv')
        run.datasets.create(argument=argument, dataset=dataset)
        log = run.logs.create(short_text='Job completed.', type=ContainerLog.STDERR)
        expected_entries = [dict(created=make_aware(datetime(2000, 1, 1), utc),
                                 name='names.csv',
                                 size='30\xa0bytes',
                                 type='Input',
                                 url='/dataset_view/1'),
                            dict(created=make_aware(datetime(2000, 1, 2), utc),
                                 name='stderr',
                                 size='14\xa0bytes',
                                 type='Log',
                                 url='/container_logs/{}/'.format(log.id)),
                            dict(created=make_aware(datetime(2000, 1, 1), utc),
                                 name='greetings_123.csv',
                                 size='missing',
                                 type='Output',
                                 url='/dataset_view/2')]
        client = Client()
        client.force_login(run.user)
        response = client.get(reverse('container_run_detail',
                                      kwargs=dict(pk=run.pk)))

        self.assertEqual('Complete', response.context['state_name'])
        self.assertListEqual(expected_entries, response.context['data_entries'])

    @patch.dict('os.environ', KIVE_LOG='/tmp/forbidden.log')
    @patch('container.models.check_output')
    def test_launch_run(self, mock_check_output):
        mock_check_output.return_value = '42\n'
        expected_slurm_job_id = 42
        run = ContainerRun.objects.filter(state=ContainerRun.NEW).first()
        self.assertIsNotNone(run)
        run.slurm_job_id = None
        run.sandbox_path = ''
        run.save()

        run.schedule()

        run.refresh_from_db()
        self.assertNotEqual('', run.sandbox_path)
        self.assertEqual(expected_slurm_job_id, run.slurm_job_id)
        (check_output_args, check_output_kwargs), = mock_check_output.call_args_list
        self.assertEqual('sbatch', check_output_args[0][0])
        self.assertNotIn('KIVE_LOG', check_output_kwargs['env'])

    @patch.dict('os.environ', KIVE_LOG='/tmp/forbidden.log')
    @patch('container.models.check_output')
    def test_launch_without_kive_log(self, mock_check_output):
        """ What if KIVE_LOG isn't set? """
        del os.environ['KIVE_LOG']
        mock_check_output.return_value = '42\n'
        expected_slurm_job_id = 42
        run = ContainerRun.objects.filter(state=ContainerRun.NEW).first()
        self.assertIsNotNone(run)
        run.slurm_job_id = None
        run.sandbox_path = ''
        run.save()

        run.schedule()

        run.refresh_from_db()
        self.assertNotEqual('', run.sandbox_path)
        self.assertEqual(expected_slurm_job_id, run.slurm_job_id)
        (check_output_args, check_output_kwargs), = mock_check_output.call_args_list
        self.assertEqual('sbatch', check_output_args[0][0])
        self.assertNotIn('KIVE_LOG', check_output_kwargs['env'])

    @patch('container.models.check_call')
    def test_cancel_new_run(self, mock_check_call):
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
        expected_dataset_name = 'greetings_{}.csv'.format(run.id)
        expected_dataset_path = get_upload_path(Dataset, expected_dataset_name)

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        self.assertEqual(ContainerRun.COMPLETE, run.state)
        sandbox_path = run.full_sandbox_path
        self.assertTrue(sandbox_path)
        input_path = os.path.join(sandbox_path, 'input', 'names_csv')
        self.assertTrue(os.path.exists(input_path),
                        input_path + ' should exist.')
        upload_path = os.path.join(sandbox_path, 'upload',
                                   expected_dataset_name)
        self.assertTrue(os.path.exists(upload_path),
                        upload_path + ' should exist.')

        self.assertEqual(2, run.datasets.count())
        self.assertIsNotNone(run.submit_time)
        self.assertIsNotNone(run.start_time)
        self.assertIsNotNone(run.end_time)
        self.assertLessEqual(run.submit_time, run.start_time)
        self.assertLessEqual(run.start_time, run.end_time)
        output_dataset = run.datasets.get(
            argument__type=ContainerArgument.OUTPUT).dataset
        self.assertEqual(expected_dataset_name, output_dataset.name)
        self.assertEqual(expected_dataset_path,
                         output_dataset.dataset_file.name)
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
        self.assertEqual('', run.full_sandbox_path)

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
        sandbox_path = run.full_sandbox_path
        self.assertTrue(sandbox_path)
        input_path = os.path.join(sandbox_path, 'input/names_csv')
        self.assertTrue(os.path.exists(input_path),
                        input_path + ' should exist.')
        upload_path = os.path.join(sandbox_path,
                                   'upload',
                                   'greetings_{}.csv'.format(run.id))
        self.assertTrue(os.path.exists(upload_path),
                        upload_path + ' should exist.')

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
        self.assertIsNotNone(run.end_time)

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
        #
        stderr_bytes = stderr.long_text.read()
        stderr_str = stderr_bytes.decode('ascii')
        self.assertEqual(expected_stderr, stderr_str)
        self.assertEqual(expected_stdout, stdout.read())
        #
        stderr_bytes = stderr.read()
        stderr_str = stderr_bytes.decode('ascii')
        self.assertEqual(expected_stderr, stderr_str)
        self.assertEqual(expected_stdout[:10], stdout.read(10))
        self.assertEqual(expected_stderr[:10], stderr.read(10))
        upload_length = len(ContainerLog.UPLOAD_DIR)
        self.assertEqual(ContainerLog.UPLOAD_DIR, stderr.long_text.name[:upload_length])


@skipIfDBFeature('is_mocked')
class PurgeTests(TestCase):
    fixtures = ['container_run']

    def setUp(self):
        super(PurgeTests, self).setUp()
        install_fixture_files('container_run')
        os.mkdir(ContainerRun.SANDBOX_ROOT)
        self.delete_existing_storage()

    def create_sandbox(self, age=timedelta(0), size=1):
        """ Create a run and its sandbox.

        :param timedelta age: how long ago the run ended
        :param int size: number of bytes to write in the sandbox folder
        :return ContainerRun: the new run object
        """
        now = timezone.now()
        user = User.objects.get(username='kive')
        run_command = runcontainer.Command()
        app = ContainerApp.objects.first()
        run = ContainerRun.objects.create(user=user, app=app)
        run_command.fill_sandbox(run)
        self.assertTrue(os.path.exists(run.full_sandbox_path))
        run.end_time = now - age
        with open(os.path.join(run.full_sandbox_path, 'contents.txt'),
                  'wb') as f:
            f.write(b'.' * size)
        run.save()
        return run

    def create_outputs(self,
                       run,
                       output_size=0,
                       stdout_size=0,
                       stderr_size=0,
                       age=timedelta(seconds=0)):
        output_path = os.path.join(run.full_sandbox_path,
                                   'output',
                                   'greetings_csv')
        with open(output_path, 'wb') as f:
            f.write(b'.' * output_size)
        with open(os.path.join(run.full_sandbox_path, 'logs', 'stdout.txt'),
                  'wb') as f:
            f.write(b'.' * stdout_size)
        with open(os.path.join(run.full_sandbox_path, 'logs', 'stderr.txt'),
                  'wb') as f:
            f.write(b'.' * stderr_size)

        old_end_time = run.end_time
        runcontainer.Command().save_outputs(run)
        if old_end_time is not None and run.end_time != old_end_time:
            run.end_time = old_end_time
            run.save()

        for run_dataset in run.datasets.filter(argument__type='O'):
            dataset = run_dataset.dataset
            dataset.date_created = timezone.now() - age
            dataset.save()
        for log in run.logs.all():
            log.date_created = timezone.now() - age
            log.save()

    def delete_existing_storage(self):
        for container in Container.objects.all():
            container.file.delete()
        try:
            os.remove(os.path.join(settings.MEDIA_ROOT, Container.UPLOAD_DIR, 'README.md'))
        except OSError:
            pass
        for run in ContainerRun.objects.filter(
                sandbox_purged=False).exclude(
                sandbox_path=''):
            run.delete_sandbox()
            run.sandbox_purged = True
            run.save()
        for log in ContainerLog.objects.exclude(long_text='').exclude(
                long_text=None):
            log.long_text.delete()
        for dataset in Dataset.objects.exclude(dataset_file='').exclude(
                dataset_file=None):
            dataset.dataset_file.delete()
        shutil.rmtree(os.path.join(settings.MEDIA_ROOT, Dataset.UPLOAD_DIR),
                      ignore_errors=True)

    def test_purge_incomplete(self):
        run = self.create_sandbox(age=timedelta(minutes=10), size=100)
        run.end_time = None
        run.save()

        purge.Command().handle(start=50, stop=50)

        run.refresh_from_db()
        self.assertFalse(run.sandbox_purged)
        self.assertTrue(os.path.exists(run.full_sandbox_path))
        self.assertIsNone(run.sandbox_size)

    def test_purge_too_small(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=100)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=200)

        purge.Command().handle(start=400, stop=400)

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertFalse(run1.sandbox_purged)
        self.assertFalse(run2.sandbox_purged)
        self.assertTrue(os.path.exists(run1.full_sandbox_path))
        self.assertEqual(100, run1.sandbox_size)

    def test_purge_no_sandbox(self):
        """ Sometimes a sandbox doesn't get created, and the path is blank. """
        run = self.create_sandbox(size=500)
        run.sandbox_path = ''
        run.save()

        purge.Command().handle(start=400, stop=400)

        run.refresh_from_db()
        self.assertFalse(run.sandbox_purged)
        self.assertIsNone(run.sandbox_size)

    def test_purge_folder(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=400)

        purge.Command().handle(start=500, stop=500)

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertTrue(run1.sandbox_purged)
        self.assertFalse(run2.sandbox_purged)
        self.assertFalse(os.path.exists(run1.full_sandbox_path))
        self.assertTrue(os.path.exists(run2.full_sandbox_path))
        self.assertEqual(200, run1.sandbox_size)

    def test_purge_start(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=400)

        purge.Command().handle(start=600, stop=400)

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertFalse(run1.sandbox_purged)
        self.assertFalse(run2.sandbox_purged)

    def test_purge_stop(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=400)

        purge.Command().handle(start=400, stop=199)

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertTrue(run1.sandbox_purged)
        self.assertTrue(run2.sandbox_purged)

    def test_unregistered_folder(self):
        run = self.create_sandbox(size=500)

        left_overs_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'left_overs')
        os.mkdir(left_overs_path)
        with open(os.path.join(left_overs_path, 'contents.txt'), 'wb') as f:
            f.write(b'.' * 100)

        purge.Command().handle(start=400, stop=400, synch=True)

        run.refresh_from_db()
        self.assertFalse(run.sandbox_purged)  # registered sandboxes skipped
        self.assertFalse(os.path.exists(left_overs_path))
        self.assertTrue(os.path.exists(run.full_sandbox_path))

    def test_unregistered_file(self):
        left_overs_path = os.path.join(ContainerRun.SANDBOX_ROOT,
                                       'left_overs.txt')
        with open(left_overs_path, 'wb') as f:
            f.write(b'.' * 100)

        purge.Command().handle(start=400, stop=400, synch=True)

        self.assertFalse(os.path.exists(left_overs_path))

    def test_unregistered_too_new(self):
        folder_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'left_overs')
        os.mkdir(folder_path)
        with open(os.path.join(folder_path, 'contents.txt'), 'wb') as f:
            f.write(b'.' * 100)

        file_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'extras.txt')
        with open(file_path, 'wb') as f:
            f.write(b'.' * 100)

        purge.Command().handle(wait=timedelta(seconds=30), synch=True)

        self.assertTrue(os.path.exists(folder_path))
        self.assertTrue(os.path.exists(file_path))

    @contextmanager
    def capture_log_stream(self, log_level):
        with capture_log_stream(
                log_level,
                'container.management.commands.purge') as mocked_stderr:
            yield mocked_stderr

    def assertLogStreamEqual(self, expected, messages):
        cleaned_messages = re.sub(r'(run|log|dataset) \d+',
                                  r'\1 <id>',
                                  messages).replace('\xa0', ' ')
        # noinspection PyTypeChecker
        self.assertMultiLineEqual(expected, cleaned_messages)

    def test_info_logging(self):
        self.create_sandbox(age=timedelta(minutes=11), size=100)
        self.create_sandbox(age=timedelta(minutes=10), size=200)
        self.create_sandbox(age=timedelta(minutes=9), size=400)
        expected_messages = u"""\
Purged 2 container runs containing 300 bytes from 11 minutes ago to 10 minutes ago.
"""
        with self.capture_log_stream(logging.INFO) as mocked_stderr:
            purge.Command().handle(start=500, stop=500)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_debug_logging(self):
        self.create_sandbox(age=timedelta(minutes=11), size=100)
        self.create_sandbox(age=timedelta(minutes=10), size=200)
        self.create_sandbox(age=timedelta(minutes=9), size=400)
        expected_messages = u"""\
Starting purge.
Purged container run <id> containing 100 bytes.
Purged container run <id> containing 200 bytes.
Purged 2 container runs containing 300 bytes from 11 minutes ago to 10 minutes ago.
"""
        with self.capture_log_stream(logging.DEBUG) as mocked_stderr:
            purge.Command().handle(start=500, stop=500)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_error_logging_synch(self):
        left_overs_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'left_overs')
        os.mkdir(left_overs_path)
        with open(os.path.join(left_overs_path, 'contents.txt'), 'wb') as f:
            f.write(b'.'*100)

        with open(os.path.join(ContainerRun.SANDBOX_ROOT,
                               'extras.txt'), 'wb') as f:
            f.write(b'.'*200)
        expected_messages = u"""\
Purged 2 unregistered container run files containing 300 bytes.
"""
        with self.capture_log_stream(logging.ERROR) as mocked_stderr:
            purge.Command().handle(synch=True)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_warn_logging_synch(self):
        left_overs_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'left_overs')
        os.mkdir(left_overs_path)
        with open(os.path.join(left_overs_path, 'contents.txt'), 'wb') as f:
            f.write(b'.'*100)

        with open(os.path.join(ContainerRun.SANDBOX_ROOT,
                               'extras.txt'), 'wb') as f:
            f.write(b'.'*200)
        expected_messages = u"""\
Purged unregistered file 'ContainerRuns/extras.txt' containing 200 bytes.
Purged unregistered file 'ContainerRuns/left_overs' containing 100 bytes.
Purged 2 unregistered container run files containing 300 bytes.
"""
        with self.capture_log_stream(logging.WARN) as mocked_stderr:
            purge.Command().handle(synch=True)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_synch_batch(self):
        for i in range(11):
            with open(os.path.join(ContainerRun.SANDBOX_ROOT,
                                   'extras{:02d}.txt'.format(i)), 'wb') as f:
                f.write(b'.' * 1024)
        expected_messages = u"""\
Purged 11 unregistered container run files containing 11.0 KB.
"""
        with self.capture_log_stream(logging.ERROR) as mocked_stderr:
            purge.Command().handle(synch=True, batch_size=10)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_sandbox_before_output(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=1))
        self.create_outputs(run, output_size=200, age=timedelta(minutes=9))

        purge.Command().handle(start=400, stop=400, sandbox_aging=10)

        run.refresh_from_db()
        dataset = run.datasets.filter(argument__type='O').get().dataset
        self.assertTrue(run.sandbox_purged)
        self.assertNotEqual('', dataset.dataset_file)  # Not purged.

    def test_sandbox_and_output(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=1))
        self.create_outputs(run, output_size=200, age=timedelta(minutes=9))

        purge.Command().handle(start=150, stop=150, sandbox_aging=10)

        run.refresh_from_db()
        dataset = run.datasets.filter(argument__type='O').get().dataset
        self.assertTrue(run.sandbox_purged)
        self.assertEqual('', dataset.dataset_file)  # Purged.

    def test_debug_logging_datasets(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=1))
        self.create_outputs(run, output_size=200, age=timedelta(minutes=1))

        expected_messages = u"""\
Starting purge.
Purged container run <id> containing 300 bytes.
Purged dataset <id> containing 200 bytes.
Purged 1 container run containing 300 bytes from a minute ago.
Purged 1 dataset containing 200 bytes from a minute ago.
"""
        with self.capture_log_stream(logging.DEBUG) as mocked_stderr:
            purge.Command().handle(start=150, stop=150, sandbox_aging=10)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_ignore_sandboxes_already_purged(self):
        run1 = self.create_sandbox(size=1000, age=timedelta(minutes=1))
        self.create_sandbox(size=100, age=timedelta(minutes=1))
        ContainerRun.set_sandbox_sizes()
        run1.refresh_from_db()
        run1.delete_sandbox()
        run1.sandbox_purged = True
        run1.save()

        expected_messages = u"""\
Starting purge.
No purge needed for 100 bytes: 100 bytes of container runs.
"""
        with self.capture_log_stream(logging.DEBUG) as mocked_stderr:
            purge.Command().handle(start=500, stop=500)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_unable_to_purge_enough(self):
        run = self.create_sandbox(size=200, age=timedelta(minutes=1))
        self.create_outputs(run, output_size=100, age=timedelta(minutes=1))

        # Upload a new dataset, so it can't be purged.
        output_path = os.path.join(ContainerRun.SANDBOX_ROOT,
                                   'extra.txt')
        with open(output_path, 'wb') as f:
            f.write(b'.' * 1000)
        user = User.objects.get(username='kive')
        Dataset.create_dataset(output_path, name='extra.txt', user=user)

        expected_messages = u"""\
Starting purge.
Purged container run <id> containing 300 bytes.
Purged dataset <id> containing 100 bytes.
Purged 1 container run containing 300 bytes from a minute ago.
Purged 1 dataset containing 100 bytes from a minute ago.
Cannot reduce storage to 500 bytes: 1000 bytes of datasets.
"""
        with self.capture_log_stream(logging.DEBUG) as mocked_stderr:
            purge.Command().handle(start=500, stop=500)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_skip_purged_datasets(self):
        run = self.create_sandbox(size=200, age=timedelta(minutes=1))
        self.create_outputs(run, output_size=1000, age=timedelta(minutes=1))

        run.delete_sandbox()
        run.sandbox_purged = True
        run.save()
        Dataset.set_dataset_sizes()
        dataset = Dataset.objects.get(dataset_size=1000)
        dataset.dataset_file.delete()

        expected_messages = u"""\
Starting purge.
No purge needed for 0 bytes: empty storage.
"""
        with self.capture_log_stream(logging.DEBUG) as mocked_stderr:
            purge.Command().handle(start=500, stop=500)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_synch_datasets(self):
        left_overs_path = os.path.join(settings.MEDIA_ROOT,
                                       Dataset.UPLOAD_DIR,
                                       '2018_06',
                                       'left_over.txt')
        os.makedirs(os.path.dirname(left_overs_path))
        with open(left_overs_path, 'wb') as f:
            f.write(b'.'*100)

        expected_messages = u"""\
Purged 1 unregistered dataset file containing 100 bytes.
"""
        with self.capture_log_stream(logging.ERROR) as mocked_stderr:
            purge.Command().handle(synch=True)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_synch_empty_folder(self):
        left_overs_path = os.path.join(settings.MEDIA_ROOT,
                                       Dataset.UPLOAD_DIR,
                                       '2018_06',
                                       'empty_child')
        os.makedirs(left_overs_path)

        purge.Command().handle(synch=True)

        self.assertFalse(os.path.exists(left_overs_path))
        # Parent will get purged next time.
        self.assertTrue(os.path.exists(os.path.dirname(left_overs_path)))

    def test_purge_batch(self):
        for i in range(12):
            self.create_sandbox(age=timedelta(minutes=i+1), size=100)
        expected_messages = u"""\
Purged 11 container runs containing 1.1 KB from 12 minutes ago to 2 minutes ago.
"""
        with self.capture_log_stream(logging.INFO) as mocked_stderr:
            purge.Command().handle(start=100, stop=100, batch_size=10)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_container_log(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=10))
        self.create_outputs(run,
                            output_size=200,
                            stdout_size=2100,
                            age=timedelta(minutes=10))
        log = run.logs.first()
        log_path = log.long_text.name

        purge.Command().handle(start=0, stop=0)

        run.refresh_from_db()
        log.refresh_from_db()
        self.assertTrue(run.sandbox_purged)
        self.assertEqual('', log.long_text)  # Purged.
        self.assertFalse(os.path.exists(log_path))

    def test_container_log_debugging(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=9))
        self.create_outputs(run,
                            output_size=200,
                            stdout_size=2100,
                            age=timedelta(minutes=10))
        run.delete_sandbox()
        run.sandbox_purged = True
        run.save()
        expected_messages = u"""\
Starting purge.
Purged dataset <id> containing 200 bytes.
Purged container log <id> containing 2.1 KB.
Purged 1 container log containing 2.1 KB from 9 minutes ago.
Purged 1 dataset containing 200 bytes from 10 minutes ago.
"""
        with self.capture_log_stream(logging.DEBUG) as mocked_stderr:
            purge.Command().handle(start=0, stop=0)
            log_messages = mocked_stderr.getvalue()

        self.maxDiff = 1200
        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_synch_container_log(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=9))
        self.create_outputs(run,
                            output_size=200,
                            stdout_size=2100,
                            age=timedelta(minutes=10))
        log = run.logs.get(type='O')

        logs_root = os.path.join(settings.MEDIA_ROOT, ContainerLog.UPLOAD_DIR)
        extra_log_path = os.path.join(logs_root, 'extra.log')
        with open(extra_log_path, 'wb') as f:
            f.write(b'.'*400)

        purge.Command().handle(synch=True)

        self.assertTrue(os.path.exists(os.path.join(settings.MEDIA_ROOT,
                                                    log.long_text.name)))
        self.assertFalse(os.path.exists(extra_log_path))

    def test_synch_containers(self):
        logs_root = os.path.join(settings.MEDIA_ROOT, Container.UPLOAD_DIR)
        extra_log_path = os.path.join(logs_root, 'extra.simg')
        with open(extra_log_path, 'wb') as f:
            f.write(b'.'*400)

        expected_log_messages = """\
Purged unregistered file 'Containers/extra.simg' containing 400 bytes.
Purged 1 unregistered container file containing 400 bytes.
"""

        with self.capture_log_stream(logging.WARN) as mocked_stderr:
            purge.Command().handle(synch=True)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_log_messages, log_messages)

    def test_synch_missing_folders(self):
        self.assertEqual("Testing", os.path.basename(settings.MEDIA_ROOT))
        shutil.rmtree(ContainerRun.SANDBOX_ROOT,
                      ignore_errors=True)
        shutil.rmtree(os.path.join(settings.MEDIA_ROOT,
                                   ContainerLog.UPLOAD_DIR),
                      ignore_errors=True)
        shutil.rmtree(os.path.join(settings.MEDIA_ROOT,
                                   Dataset.UPLOAD_DIR),
                      ignore_errors=True)

        purge.Command().handle(synch=True)

    def test_no_purging_containers(self):
        family = ContainerFamily.objects.first()
        container = family.containers.create(user=family.user)
        with NamedTemporaryFile() as f:
            f.write(b'.'*100)
            container.file.save('new_container.simg', f)
        prefix_length = len(Container.UPLOAD_DIR)
        path_prefix = container.file.name[:prefix_length]

        expected_log_messages = (
            "Cannot reduce storage to 99 bytes: 100 bytes of containers.\n")

        with self.capture_log_stream(logging.ERROR) as mocked_stderr:
            purge.Command().handle(start=99, stop=99)
            log_messages = mocked_stderr.getvalue()

        self.assertEqual(Container.UPLOAD_DIR, path_prefix)
        self.assertLogStreamEqual(expected_log_messages, log_messages)
