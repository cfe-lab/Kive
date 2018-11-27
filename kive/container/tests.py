# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib.auth.models import User
from django.test import TestCase, skipIfDBFeature
from django.test.client import Client
from django.urls import reverse, resolve
from rest_framework import status
from rest_framework.test import force_authenticate

from container.models import ContainerFamily, ContainerApp, Container, ContainerRun
from kive.tests import BaseTestCases


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
                               {})
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
                               dict(inputs=expected_inputs,
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
                                          outputs=expected_outputs))

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
class ContainerRunTests(TestCase):
    def test(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        app = ContainerApp.objects.create(container=container, name='test')
        client = Client()
        client.force_login(user)
        start_count = ContainerRun.objects.count()
        expected_count = start_count + 1

        response = client.post(reverse('container_run_add'),
                               data=dict(app=app.id))

        end_count = ContainerRun.objects.count()

        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)
        self.assertEqual(expected_count, end_count)
