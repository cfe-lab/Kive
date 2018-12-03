# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os

from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, skipIfDBFeature
from django.test.client import Client
from django.urls import reverse, resolve
from rest_framework.reverse import reverse as rest_reverse
from rest_framework import status
from rest_framework.test import force_authenticate

from container.models import ContainerFamily, ContainerApp, Container, ContainerRun, ContainerArgument, Batch
from kive.tests import BaseTestCases, install_fixture_files
from librarian.models import Dataset


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
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEquals(response.data['ContainerRuns'], 1)

    def test_removal(self):
        start_count = ContainerRun.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = ContainerRun.objects.all().count()
        self.assertEquals(end_count, start_count - 1)


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

        self.assertEquals(len(resp_run['datasets']), 1)

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEquals(response.data['Batches'], 1)
        self.assertEquals(response.data['ContainerRuns'], 1)

    def test_removal(self):
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

    def test_run(self):
        run = ContainerRun.objects.get(name='fixture run')

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
