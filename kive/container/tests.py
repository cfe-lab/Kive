# -*- coding: utf-8 -*-
import json
import logging
import os
import re
import shutil
import warnings
import subprocess as sp
from contextlib import contextmanager
from datetime import datetime, timedelta
from io import BytesIO
import pathlib
from tarfile import TarFile, TarInfo
from tempfile import NamedTemporaryFile, mkstemp
from time import time
import unittest.mock
from zipfile import ZipFile
from filecmp import cmp

from django.conf import settings
from django.contrib.auth.models import User, Group
from django.core.files.base import ContentFile, File
from django.core.management import call_command
from django.core.management.base import CommandError
from django.core.exceptions import ValidationError
from django.test import TestCase, skipIfDBFeature
from django.test.client import Client
from django.urls import reverse, resolve
from django.utils import timezone
from django.utils.timezone import make_aware, utc
from django.core.files.uploadedfile import InMemoryUploadedFile, TemporaryUploadedFile
from django.core.exceptions import NON_FIELD_ERRORS
from mock import patch
from rest_framework.reverse import reverse as rest_reverse
from rest_framework import status
from rest_framework.test import force_authenticate

from container.management.commands import purge, runcontainer
# import container.models as cm
from container.models import (
    ContainerFamily, ContainerApp, Container, ContainerRun, ContainerDataset,
    ContainerArgument, ContainerArgumentType, Batch, ContainerLog,
    PipelineCompletionStatus, ExistingRunsError, multi_check_output
)
from container.forms import ContainerForm
from kive.tests import BaseTestCases, install_fixture_files, capture_log_stream
from librarian.models import Dataset, ExternalFileDirectory, get_upload_path
from file_access_utils import use_field_file


def create_tar_content(container=None, content=None):
    """ Create a tar file for an archive container.

    :param container: the container to attach this file to (not saved)
    :param content: the JSON content to write into the container
    :returns: a BytesIO object with the tar file contents
    """
    bytes_file = BytesIO()
    with TarFile(fileobj=bytes_file, mode="w") as f:
        foo = BytesIO(b"The first file.")
        foo_info = TarInfo('foo.txt')
        foo_info.size = len(foo.getvalue())
        f.addfile(foo_info, foo)

        bar = BytesIO(b"The second file.")
        bar_info = TarInfo('bar.txt')
        bar_info.size = len(bar.getvalue())
        f.addfile(bar_info, bar)

        driver = BytesIO(b"#! /usr/bin/env python\nprint('Hello World')\n")
        driver_info = TarInfo('driver.py')
        driver_info.size = len(driver.getvalue())
        f.addfile(driver_info, driver)

        if content is not None:
            pipeline = BytesIO(json.dumps(content['pipeline']).encode('utf8'))
            pipeline_info = TarInfo('kive/pipeline1.json')
            pipeline_info.size = len(pipeline.getvalue())
            f.addfile(pipeline_info, pipeline)
    if container is not None:
        container.file = ContentFile(bytes_file.getvalue(), "container.tar")
        container.file_type = Container.TAR
    return bytes_file


def create_valid_tar_content(container=None):
    return create_tar_content(container, content=dict(
        files=[("bar.txt", False), ("foo.txt", False), ("driver.py", True)],
        pipeline=dict(default_config=dict(memory=200,
                                          threads=2),
                      inputs=[dict(dataset_name='in1')],
                      steps=[dict(driver='driver.py',
                                  inputs=[dict(dataset_name="in1",
                                               source_step=0,
                                               source_dataset_name="in1")],
                                  outputs=["out1"])],
                      outputs=[dict(dataset_name="out1",
                                    source_step=1,
                                    source_dataset_name="out1")])))


@patch('container.models.SLEEP_SECS', 0)
class TestMultiCheckOutput(TestCase):
    """Perform tests on multi_check_output ."""

    @patch('container.models.check_output')
    def test_oserror01(self, mock_check_output):
        """An OSError exception that check_output raises should be propagated up."""
        # mock_check_output.side_effect = FileNotFoundError
        mock_check_output.side_effect = OSError(99, 'Forgetaboutit')
        with self.assertRaises(OSError):
            multi_check_output(['bla'])

    @patch('container.models.check_output')
    def test_callprocerror01(self, mock_check_output):
        """A CalledProcessError exception that check_output raises
        should be propagated up, but only after a number of retries,
        """
        mock_check_output.side_effect = sp.CalledProcessError(returncode=2,
                                                              cmd=["baddy"])
        num_retries = 3
        with self.assertRaises(sp.CalledProcessError):
            multi_check_output(['bla'], num_retry=num_retries)
        self.assertEqual(mock_check_output.call_count, num_retries)

    @patch('container.models.check_output')
    def test_callprocerror02(self, mock_check_output):
        """If a CalledProcessError exception is raised by check_output less
        than num_retries times, then the call should succeed.
        """
        proc_err = sp.CalledProcessError(returncode=2, cmd=["baddy"])
        num_retries = 3
        res_str = 'hello baby'
        mock_check_output.side_effect = [proc_err, proc_err, res_str]
        res_got = multi_check_output(['bla'], num_retry=num_retries)
        self.assertEqual(mock_check_output.call_count, num_retries)
        self.assertEqual(res_got, res_str)


@skipIfDBFeature('is_mocked')
class ContainerTests(TestCase):
    def create_zip_content(self, container):
        bytes_file = BytesIO()
        with ZipFile(bytes_file, "w") as f:
            f.writestr("foo.txt", b"The first file.")
            f.writestr("bar.txt", b"The second file.")
            f.writestr("driver.py", b"#! /usr/bin/env python\nprint('Hello World')\n")
        container.file = ContentFile(bytes_file.getvalue(), "container.zip")
        container.file_type = Container.ZIP

    def add_zip_content(self, container, filename, content):
        with ZipFile(container.file_path, 'a') as f:
            f.writestr(filename, content)

    def create_tar_content(self, container):
        create_tar_content(container)

    def add_tar_content(self, container, filename, content):
        with TarFile(container.file_path, 'a') as f:
            tarinfo = TarInfo(filename)
            tarinfo.size = len(content)
            f.addfile(tarinfo, BytesIO(content.encode('utf8')))

    def create_sing_content(self, container):
        image_path = os.path.abspath(os.path.join(__file__,
                                                  '..',
                                                  '..',
                                                  '..',
                                                  'samplecode',
                                                  'singularity',
                                                  'python2-alpine-trimmed.simg'))
        with open(image_path, 'rb') as fi:
            container.file = File(fi)
        container.file_type = Container.SIMG

    def test_default_content(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_zip_content(container)
        container.save()
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=5000,
                                                                  threads=1),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))

        content = container.get_content()
        content.pop('id')
        self.assertEqual(expected_content, content)

    def test_loaded_zip_content(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_zip_content(container)
        container.save()
        self.add_zip_content(container, "kive/pipeline1.json", """
{
    "default_config": {"memory": 200, "threads": 2},
    "inputs": [],
    "steps": [],
    "outputs": []}
""")
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=200,
                                                                  threads=2),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))

        content = container.get_content()
        content.pop('id')
        self.assertEqual(expected_content, content)

    def test__sing_content_is_serialisable(self):
        """The content returned for a singularity container must be serialisable."""
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_sing_content(container)
        content = container.get_content()
        json.dumps(content)
        # assert False, "force fail"

    def test_loaded_tar_content(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_tar_content(container)
        container.save()
        self.add_tar_content(container, "kive/pipeline1.json", """
{
    "default_config": {"memory": 200, "threads": 2},
    "inputs": [],
    "steps": [],
    "outputs": []}
""")
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=200,
                                                                  threads=2),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))

        content = container.get_content()
        content.pop('id')
        self.assertEqual(expected_content, content)

    def test_revised_content(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_zip_content(container)
        container.save()
        self.add_zip_content(container, "kive/pipeline1.json", '"old content"')
        self.add_zip_content(container, "kive/pipeline2.json", """
{
    "default_config": {"memory": 200, "threads": 2},
    "inputs": [],
    "steps": [],
    "outputs": []}
""")
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=200,
                                                                  threads=2),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))

        content = container.get_content()
        content.pop('id')
        self.assertEqual(expected_content, content)

    def test_content_order(self):
        """ Zip index order matters, not file name. """
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_zip_content(container)
        container.save()
        self.add_zip_content(container, "kive/pipeline2.json", '"old content"')
        self.add_zip_content(container, "kive/pipeline1.json", """
{
    "default_config": {"memory": 200, "threads": 2},
    "inputs": [],
    "steps": [],
    "outputs": []}
""")
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=200,
                                                                  threads=2),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))

        content = container.get_content()
        content.pop('id')
        self.assertEqual(expected_content, content)

    def test_write_zip_content(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_zip_content(container)
        container.save()
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=200,
                                                                  threads=2),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))

        container.write_archive_content(expected_content)
        content = container.get_content()
        content.pop('id')

        self.assertEqual(expected_content, content)

    def test_write_tar_content(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_tar_content(container)
        container.save()
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=200,
                                                                  threads=2),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))
        expected_apps_count = 0  # Pipeline is incomplete, so no app created.

        container.write_archive_content(expected_content)
        content = container.get_content()
        content.pop('id')

        self.assertEqual(expected_content, content)
        self.assertEqual(expected_apps_count, container.apps.count())

    def test_rewrite_content(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_zip_content(container)
        container.save()
        self.add_zip_content(container, "kive/pipeline1.json", '"old content"')
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=200,
                                                                  threads=2),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))

        with warnings.catch_warnings():
            # Register for warnings about duplicate file names.
            warnings.showwarning = lambda message, *args: self.fail(message)
            container.write_archive_content(expected_content)
        content = container.get_content()
        content.pop('id')

        self.assertEqual(expected_content, content)

    def test_write_complete_content(self):
        """Writing a proper pipeline both updates the content and creates a new app."""
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_tar_content(container)
        container.save()
        expected_content = dict(
            files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
            pipeline=dict(default_config=dict(memory=200,
                                              threads=2),
                          inputs=[dict(dataset_name='in1')],
                          steps=[dict(driver='foo.txt',
                                      inputs=[dict(dataset_name="in1",
                                                   source_step=0,
                                                   source_dataset_name="in1")],
                                      outputs=["out1"])],
                          outputs=[dict(dataset_name="out1",
                                        source_step=1,
                                        source_dataset_name="out1")]))
        expected_apps_count = 1
        expected_memory = 200
        expected_threads = 2
        expected_inputs = "in1"
        expected_outputs = "out1"

        container.write_archive_content(expected_content)

        self.assertEqual(expected_apps_count, container.apps.count())
        app = container.apps.first()
        self.assertEqual(expected_memory, app.memory)
        self.assertEqual(expected_threads, app.threads)
        self.assertEqual(expected_inputs, app.inputs)
        self.assertEqual(expected_outputs, app.outputs)

    def test_write_complete_content_to_container_with_existing_runs(self):
        """Writing a proper pipeline to a container that has existing runs creates a new container."""
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        self.create_tar_content(container)
        container.save()
        updated_content = dict(
            pipeline=dict(default_config=dict(memory=200,
                                              threads=2),
                          inputs=[dict(dataset_name='in1')],
                          steps=[dict(driver='foo.txt',
                                      inputs=[dict(dataset_name="in1",
                                                   source_step=0,
                                                   source_dataset_name="in1")],
                                      outputs=["out1"])],
                          outputs=[dict(dataset_name="out1",
                                        source_step=1,
                                        source_dataset_name="out1")]))
        container.write_archive_content(updated_content)
        container.save()
        old_md5 = container.md5
        app = container.apps.first()
        app.runs.create(
            name="foo",
            state=ContainerRun.NEW,
            user=user
        )

        # Now, update the content.  This should complain about the run.
        updated_content = dict(
            pipeline=dict(default_config=dict(memory=200,
                                              threads=2),
                          inputs=[dict(dataset_name='input1')],
                          steps=[dict(driver='foo.txt',
                                      inputs=[dict(dataset_name="in1",
                                                   source_step=0,
                                                   source_dataset_name="input1")],
                                      outputs=["out1"])],
                          outputs=[dict(dataset_name="output1",
                                        source_step=1,
                                        source_dataset_name="out1")]))
        with self.assertRaises(ExistingRunsError):
            container.write_archive_content(updated_content)

        container.refresh_from_db()

        self.assertEqual(old_md5, container.md5)

    def test_create_content_and_app(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        parent = Container.objects.create(family=family, user=user)
        tar_bytes = create_valid_tar_content()

        client = Client()
        client.force_login(user)
        expected_inputs = 'in1'
        expected_outputs = 'out1'

        response = client.post(reverse('container_add',
                                       kwargs=dict(family_id=family.id)),
                               dict(tag='test',
                                    parent=parent.id,
                                    file=ContentFile(tar_bytes.getvalue(),
                                                     "container.tar")))
        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)
        new_container = Container.objects.first()
        app, = new_container.apps.all()
        self.assertEqual(expected_inputs, app.inputs)
        self.assertEqual(expected_outputs, app.outputs)
        self.assertNotEqual('', new_container.md5)
        self.assertIsNotNone(new_container.md5)

    def test_create_singularity_with_app(self):
        """Adding a singularity container with a main program and an app should succeed.
        The singularity image is in Kive/samplecode"""
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        image_path = os.path.abspath(os.path.join(__file__,
                                                  '..',
                                                  '..',
                                                  '..',
                                                  'samplecode',
                                                  'singularity',
                                                  'python2-alpine-trimmed.simg'))
        expected_app_count = 2

        client = Client()
        client.force_login(user)

        with open(image_path, 'rb') as f:
            response = client.post(reverse('container_add',
                                           kwargs=dict(family_id=family.id)),
                                   dict(tag='test',
                                        file=File(f)))
        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)
        new_container = Container.objects.first()
        self.assertEqual(expected_app_count, new_container.apps.count())

    def test_extract_zip(self):
        run = ContainerRun()
        run.create_sandbox(prefix='test_extract_zip')
        sandbox_path = run.full_sandbox_path
        try:
            user = User.objects.first()
            family = ContainerFamily.objects.create(user=user)
            container = Container.objects.create(family=family, user=user)
            self.create_zip_content(container)
            container.save()

            container.extract_archive(sandbox_path)

            self.assertTrue(os.path.exists(os.path.join(sandbox_path, 'foo.txt')))
            self.assertTrue(os.path.exists(os.path.join(sandbox_path, 'bar.txt')))
        finally:
            shutil.rmtree(sandbox_path)

    def test_extract_tar(self):
        run = ContainerRun()
        run.create_sandbox(prefix='test_extract_tar')
        sandbox_path = run.full_sandbox_path
        try:
            user = User.objects.first()
            family = ContainerFamily.objects.create(user=user)
            container = Container.objects.create(family=family, user=user)
            self.create_tar_content(container)
            container.save()

            container.extract_archive(sandbox_path)

            self.assertTrue(os.path.exists(os.path.join(sandbox_path, 'foo.txt')))
            self.assertTrue(os.path.exists(os.path.join(sandbox_path, 'bar.txt')))
        finally:
            shutil.rmtree(sandbox_path)

    def test_pipeline_state_valid(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = family.containers.create(user=user)
        create_valid_tar_content(container)
        container.save()
        expected_pipeline_state = Container.VALID

        pipeline_state = container.get_pipeline_state()

        self.assertEqual(expected_pipeline_state, pipeline_state)

    def test_pipeline_state_nothing_defined(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = family.containers.create(user=user)
        create_tar_content(
            container,
            dict(
                pipeline=dict(
                    inputs=[],
                    steps=[],
                    outputs=[]
                )
            )
        )
        container.save()
        expected_pipeline_state = Container.INCOMPLETE
        pipeline_state = container.get_pipeline_state()
        self.assertEqual(expected_pipeline_state, pipeline_state)

    def test_pipeline_state_invalid_pipeline(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = family.containers.create(user=user)
        create_tar_content(container, dict(pipeline="This is not a pipeline!"))
        container.save()
        expected_pipeline_state = Container.INCOMPLETE
        pipeline_state = container.get_pipeline_state()
        self.assertEqual(expected_pipeline_state, pipeline_state)

    def test_pipeline_state_incomplete(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = family.containers.create(user=user)
        create_tar_content(
            container,
            dict(
                pipeline=dict(
                    inputs=[{"dataset_name": 'in1'}],
                    steps=[],
                    outputs=[]
                )
            )
        )
        container.save()
        expected_pipeline_state = Container.INCOMPLETE
        pipeline_state = container.get_pipeline_state()
        self.assertEqual(expected_pipeline_state, pipeline_state)

    def test_pipeline_state_no_json(self):
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = family.containers.create(user=user)
        create_tar_content(container)
        container.save()
        expected_pipeline_state = Container.EMPTY
        pipeline_state = container.get_pipeline_state()
        self.assertEqual(expected_pipeline_state, pipeline_state)

    def test_faulty_sing_content01(self):
        """get_singularity_content() should raise the appropriate ValidationError
        when presented with a non singularity file.
        """
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        # tarfile is not a singularity container
        self.create_tar_content(container)
        container.save()
        with self.assertRaises(ValidationError):
            container.get_singularity_content()

    @patch('container.models.check_output')
    def test_sing_no_deffile(self, mock_check_output):
        # singularity container without a deffile
        # this output taken from 'singularity inspect -d -j ubuntu.simg'
        mock_check_output.return_value = b"""
{
    "data": {
        "attributes": {
            "deffile": null
        },
        "type": "container"
    }
}
"""
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        got_cont = container.get_singularity_content()
        exp_cont = {'applist': []}
        self.assertEqual(got_cont, exp_cont)

    @patch('container.models.Container.get_singularity_content')
    def test_sing_app_from_content(self, mock_get_singularity_content):
        mock_get_singularity_content.return_value = None
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        # should just return...
        container.create_app_from_content()
        # this too
        mock_get_singularity_content.return_value = {}
        container.create_app_from_content()

    @patch('container.models.check_output')
    def test_sing_faulty_deffile(self, mock_check_output):
        # singularity container with a faulty deffile
        mock_check_output.return_value = b"""
{
    "data": {
        "attributes": {
            "deffile": "%apphelp bla"
       },
        "type": "container"
    }
}
"""
        expected_content = dict(
            applist=[dict(appname='',
                          error_messages=['labels string not set',
                                          'run string not set'],
                          helpstring='',
                          io_args=(None, None),
                          labeldict=None,
                          memory=None,
                          numthreads=None,
                          runstring=None),
                     dict(appname='bla',
                          error_messages=['labels string not set',
                                          'run string not set'],
                          helpstring='',
                          io_args=(None, None),
                          labeldict=None,
                          memory=None,
                          numthreads=None,
                          runstring=None)])
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)

        content = container.get_singularity_content()

        self.assertEqual(expected_content, content)


@skipIfDBFeature('is_mocked')
class ContainerApiTests(BaseTestCases.ApiTestCase):
    def create_zip_content(self):
        bytes_file = BytesIO()
        with ZipFile(bytes_file, "w") as f:
            f.writestr("foo.txt", b"The first file.")
            f.writestr("bar.txt", b"The second file.")
            f.writestr("driver.py", b"#! /usr/bin/env python\nprint('Hello World')\n")
        return bytes_file

    def setUp(self):
        super(ContainerApiTests, self).setUp()
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)
        self.test_container = Container.objects.create(family=family, user=user)

        self.list_path = reverse("container-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = self.test_container.pk
        self.detail_path = reverse("container-detail",
                                   kwargs={'pk': self.detail_pk})
        self.content_path = reverse("container-content",
                                    kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("container-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.content_view, _, _ = resolve(self.content_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        self.family_path = reverse("containerfamily-detail",
                                   kwargs={'pk': family.pk})

        self.image_path = os.path.abspath(os.path.join(
            __file__,
            '..',
            '..',
            '..',
            'samplecode',
            'singularity',
            'python2-alpine-trimmed.simg'))
        self.assertTrue(os.path.exists(self.image_path), self.image_path)

    def test_removal(self):
        request = self.factory.delete(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

    def test_create_singularity(self):
        request1 = self.factory.get(self.list_path)
        force_authenticate(request1, user=self.kive_user)
        start_count = len(self.list_view(request1).data)
        expected_tag = "v1.0"
        expected_description = 'A really cool container'

        with open(self.image_path, 'rb') as f:
            request2 = self.factory.post(self.list_path,
                                         dict(tag=expected_tag,
                                              family=self.family_path,
                                              description=expected_description,
                                              file_type=Container.SIMG,
                                              file=f))

            force_authenticate(request2, user=self.kive_user)
            resp = self.list_view(request2).data
            request2.close()  # Closes uploaded temp files.

        self.assertIn('id', resp)
        self.assertEqual(resp['tag'], expected_tag)

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data

        self.assertEqual(len(resp), start_count + 1)
        self.assertEqual(resp[0]['description'], expected_description)
        self.assertNotEqual(resp[0]['md5'], '')
        self.assertIsNotNone(resp[0]['md5'])

    def test_create_zip(self):
        expected_tag = "v1.0"
        z = self.create_zip_content()
        z.seek(0)
        request2 = self.factory.post(self.list_path,
                                     dict(tag=expected_tag,
                                          family=self.family_path,
                                          parent=self.detail_path,
                                          description='A really cool container',
                                          file_type=Container.ZIP,
                                          file=z))

        force_authenticate(request2, user=self.kive_user)
        resp = self.list_view(request2).data

        self.assertIn('id', resp)
        self.assertEqual(resp['tag'], expected_tag)

    def test_get_content(self):
        self.test_container.file_type = Container.ZIP
        self.test_container.file.save(
            'test.zip',
            ContentFile(self.create_zip_content().getvalue()))
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=5000,
                                                                  threads=1),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))

        request1 = self.factory.get(self.content_path)
        force_authenticate(request1, user=self.kive_user)
        content = self.content_view(request1, pk=self.detail_pk).data
        content.pop('id')

        self.assertEqual(expected_content, content)

    def test_put_content(self):
        self.test_container.file_type = Container.ZIP
        self.test_container.file.save(
            'test.zip',
            ContentFile(self.create_zip_content().getvalue()))
        old_md5 = self.test_container.md5
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=400,
                                                                  threads=3),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]),
                                id=self.test_container.id)

        request1 = self.factory.put(self.content_path,
                                    expected_content,
                                    format='json')
        force_authenticate(request1, user=self.kive_user)
        content = self.content_view(request1, pk=self.detail_pk).data

        self.assertEqual(expected_content, content)
        self.test_container.refresh_from_db()
        new_md5 = self.test_container.md5
        self.assertNotEqual('', new_md5)
        self.assertNotEqual(old_md5, new_md5)

    def test_put_bad_content(self):
        self.test_container.file_type = Container.ZIP
        self.test_container.file.save(
            'test.zip',
            ContentFile(self.create_zip_content().getvalue()))
        bad_content = {}
        expected_content = dict(pipeline=["This field is required."])

        request1 = self.factory.put(self.content_path,
                                    bad_content,
                                    format='json')
        force_authenticate(request1, user=self.kive_user)
        response = self.content_view(request1, pk=self.detail_pk)
        content = response.data

        self.assertEqual(expected_content, content)
        self.assertEqual(400, response.status_code)

    def test_write_archive_content_copy(self):
        self.test_container.file_type = Container.ZIP
        self.test_container.tag = 'v1'
        self.test_container.description = 'v1 description'
        self.test_container.file.save(
            'test.zip',
            ContentFile(self.create_zip_content().getvalue()))
        put_content = dict(pipeline=dict(
                                         default_config=dict(memory=400,
                                                             threads=3),
                                         inputs=[],
                                         steps=[],
                                         outputs=[]),
                           new_tag='v2')
        expected_content = dict(files=[("bar.txt", False), ("driver.py", True), ("foo.txt", False)],
                                pipeline=dict(default_config=dict(memory=400,
                                                                  threads=3),
                                              inputs=[],
                                              steps=[],
                                              outputs=[]))  # id not shown here

        request1 = self.factory.put(self.content_path,
                                    put_content,
                                    format='json')
        force_authenticate(request1, user=self.kive_user)
        content = self.content_view(request1, pk=self.detail_pk).data

        new_container_id = content.pop('id')
        self.assertNotEqual(self.test_container.id, new_container_id)  # New record
        self.assertEqual(expected_content, content)
        new_container = Container.objects.get(id=new_container_id)
        self.assertEqual('v2', new_container.tag)
        self.assertEqual('v1 description', new_container.description)
        self.assertNotEqual(self.test_container.file.path, new_container.file.path)
        if not new_container.file.name.startswith('Containers/test'):
            self.fail('Unexpected container path: ' + new_container.file.name)

    def test_write_content_copy_with_description(self):
        self.test_container.file_type = Container.ZIP
        self.test_container.tag = 'v1'
        self.test_container.description = 'v1 description'
        self.test_container.file.save(
            'test.zip',
            ContentFile(self.create_zip_content().getvalue()))
        put_content = dict(
                           pipeline=dict(default_config=dict(memory=400,
                                                             threads=3),
                                         inputs=[],
                                         steps=[],
                                         outputs=[]),
                           new_tag='v2',
                           new_description='v2 description')

        request1 = self.factory.put(self.content_path,
                                    put_content,
                                    format='json')
        force_authenticate(request1, user=self.kive_user)
        content = self.content_view(request1, pk=self.detail_pk).data

        new_container = Container.objects.get(id=content['id'])
        self.assertEqual('v2', new_container.tag)
        self.assertEqual('v2 description', new_container.description)

    def test_write_content_duplicate_tag(self):
        self.test_container.file_type = Container.ZIP
        self.test_container.tag = 'v1'
        self.test_container.file.save(
            'test.zip',
            ContentFile(self.create_zip_content().getvalue()))
        bad_content = dict(pipeline=dict(default_config=dict(memory=400,
                                                             threads=3),
                                         inputs=[],
                                         steps=[],
                                         outputs=[]),
                           new_tag='v1')  # Duplicate tag!
        expected_content = dict(new_tag=["Tag already exists."])

        request1 = self.factory.put(self.content_path,
                                    bad_content,
                                    format='json')
        force_authenticate(request1, user=self.kive_user)
        response = self.content_view(request1, pk=self.detail_pk)
        content = response.data

        self.assertEqual(expected_content, content)
        self.assertEqual(400, response.status_code)

    def test_create_tar_with_app(self):
        tar_bytes = create_valid_tar_content()
        request2 = self.factory.post(self.list_path,
                                     dict(tag='v1.0',
                                          family=self.family_path,
                                          parent=self.detail_path,
                                          description='A really cool container',
                                          file_type=Container.TAR,
                                          file=ContentFile(tar_bytes.getvalue(),
                                                           "container.tar")))

        force_authenticate(request2, user=self.kive_user)
        resp = self.list_view(request2).data

        self.assertIn('id', resp)
        container_id = resp['id']
        container = Container.objects.get(id=container_id)
        self.assertEqual(1, container.apps.count())

    def test_write_content_existing_run(self):
        tar_bytes = create_valid_tar_content()
        request1 = self.factory.post(self.list_path,
                                     dict(tag='v1.0',
                                          family=self.family_path,
                                          parent=self.detail_path,
                                          description='A really cool container',
                                          file_type=Container.TAR,
                                          file=ContentFile(tar_bytes.getvalue(),
                                                           "container.tar")))

        force_authenticate(request1, user=self.kive_user)
        container_id = self.list_view(request1).data['id']
        container = Container.objects.get(id=container_id)
        app = container.apps.first()
        app.runs.create(
            name="foo",
            state=ContainerRun.NEW,
            user=self.kive_user)

        put_content = dict(pipeline=dict(default_config=dict(memory=400,
                                                             threads=3),
                                         inputs=[],
                                         steps=[],
                                         outputs=[]))
        expected_content = dict(
            pipeline=["Container has runs. Save changes as a new container."])

        content_path = reverse("container-content",
                               kwargs={'pk': container_id})
        content_view, _, _ = resolve(content_path)
        request2 = self.factory.put(content_path,
                                    put_content,
                                    format='json')
        force_authenticate(request2, user=self.kive_user)
        response2 = content_view(request2, pk=container_id)
        content = response2.data

        self.assertEqual(expected_content, content)
        self.assertEqual(400, response2.status_code)


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
        self.assertEqual(resp['name'], "zoo app")

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data

        self.assertEqual(len(resp), start_count + 1)
        self.assertEqual(resp[-1]['description'], "A really cool app")
        self.assertEqual(resp[-1]['inputs'], expected_inputs)
        self.assertEqual(resp[-1]['outputs'], expected_outputs)

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEqual(response.data['ContainerApps'], 1)

    def test_removal(self):
        start_count = ContainerApp.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = ContainerApp.objects.all().count()
        self.assertEqual(end_count, start_count - 1)


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
        app.arguments.create(type=ContainerArgument.OUTPUT)
        dataset = Dataset.objects.create(user=user)
        content_file = ContentFile('a,b\n0,9')
        dataset.dataset_file.save('in1.csv', content_file)
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

        self.assertEqual(response.data['ContainerRuns'], 1)

    def test_removal(self):
        self.test_run.state = ContainerRun.COMPLETE
        self.test_run.save()
        start_count = ContainerRun.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = ContainerRun.objects.all().count()
        self.assertEqual(end_count, start_count - 1)

    def test_add(self):
        request1 = self.factory.get(self.list_path)
        force_authenticate(request1, user=self.kive_user)
        start_count = len(self.list_view(request1).data)

        input_argument = self.test_run.app.arguments.get(
            type=ContainerArgument.INPUT)
        input_dataset = self.test_run.datasets.get(
            argument=input_argument).dataset
        app_url = rest_reverse(str('containerapp-detail'),
                               kwargs=dict(pk=self.test_run.app_id))
        arg_url = rest_reverse(str('containerargument-detail'),
                               kwargs=dict(pk=input_argument.id))
        dataset_url = rest_reverse(str('dataset-detail'),
                                   kwargs=dict(pk=input_dataset.pk))
        request2 = self.factory.post(
            self.list_path,
            dict(name='my run',
                 description='A really cool run',
                 app=app_url,
                 datasets=[dict(argument=arg_url,
                                dataset=dataset_url)]),
            format="json")

        force_authenticate(request2, user=self.kive_user)
        resp = self.list_view(request2).render().data

        self.assertIn('id', resp)
        self.assertEqual(resp['name'], "my run")

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data
        resp_run = resp[0]

        self.assertEqual(len(resp), start_count + 1)
        self.assertEqual(resp_run['description'], "A really cool run")

    def test_add_rerun(self):
        self.test_run.name = 'original name'
        self.test_run.save()
        request1 = self.factory.get(self.list_path)
        force_authenticate(request1, user=self.kive_user)
        start_count = len(self.list_view(request1).data)

        input_argument = self.test_run.app.arguments.get(
            type=ContainerArgument.INPUT)
        input_dataset = self.test_run.datasets.get(
            argument=input_argument).dataset
        app_url = rest_reverse(str('containerapp-detail'),
                               kwargs=dict(pk=self.test_run.app_id))
        request2 = self.factory.post(
            self.list_path,
            dict(name='ignored name',
                 original_run=self.detail_path),
            format="json")

        force_authenticate(request2, user=self.kive_user)
        resp = self.list_view(request2).render().data

        self.assertIn('id', resp)
        self.assertEqual("original name (rerun)", resp['name'])

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data
        resp_run = resp[0]

        self.assertEqual(len(resp), start_count + 1)
        expected_app_url = request3.build_absolute_uri(app_url)
        self.assertEqual(expected_app_url, resp_run['app'])
        run = ContainerRun.objects.get(id=resp_run['id'])
        run_dataset = run.datasets.get()
        self.assertEqual(input_argument.id, run_dataset.argument_id)
        self.assertEqual(input_dataset.id, run_dataset.dataset_id)

    @patch('container.models.transaction.on_commit')
    def test_add_rerun_find_input(self, mock_on_commit):
        app = self.test_run.app
        input_argument = app.arguments.get(type=ContainerArgument.INPUT)
        output_argument = app.arguments.get(type=ContainerArgument.OUTPUT)

        content_file = ContentFile('x,y\n1,2')
        output1 = Dataset.objects.create(user=self.test_run.user, name='output1')
        output1.dataset_file.save('example.csv', content_file)
        self.test_run.datasets.create(argument=output_argument, dataset=output1)

        # run2 consumes an output from self.test_run
        run2 = ContainerRun.objects.create(user=self.test_run.user,
                                           app=self.test_run.app,
                                           state=ContainerRun.FAILED)
        run2.datasets.create(argument=input_argument, dataset=output1)

        # Purge the input to run 2.
        output1.dataset_file.delete()

        # run3 is a rerun of self.test_run to reproduce the input for run 2.
        run3 = ContainerRun.objects.create(user=self.test_run.user,
                                           name='source rerun',
                                           app=self.test_run.app,
                                           original_run=self.test_run,
                                           state=ContainerRun.COMPLETE)
        output1b = Dataset.objects.create(user=self.test_run.user, name='output1b')
        output1b.dataset_file.save('example_b.csv', content_file)
        run3.datasets.create(argument=output_argument, dataset=output1b)

        # Now we request a rerun of run 2.
        run2_path = reverse("containerrun-detail", kwargs={'pk': run2.id})
        request1 = self.factory.get(self.list_path)
        force_authenticate(request1, user=self.kive_user)
        start_count = len(self.list_view(request1).data)

        request2 = self.factory.post(
            self.list_path,
            dict(name='my rerun',
                 original_run=run2_path),
            format="json")

        force_authenticate(request2, user=self.kive_user)
        resp = self.list_view(request2).render().data

        self.assertIn('id', resp)
        self.assertEqual("(rerun)", resp['name'])

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data
        resp_run = resp[0]

        self.assertEqual(len(resp), start_count + 1)
        run = ContainerRun.objects.get(id=resp_run['id'])
        run_dataset = run.datasets.get()
        self.assertEqual(output1b.id, run_dataset.dataset_id)

        self.assertEqual(1, len(mock_on_commit.call_args_list))

    @patch('container.models.transaction.on_commit')
    def test_add_rerun_with_dependencies(self, mock_on_commit):
        app = self.test_run.app
        input_argument = app.arguments.get(type=ContainerArgument.INPUT)
        output_argument = app.arguments.get(type=ContainerArgument.OUTPUT)

        content_file = ContentFile('x,y\n1,2')
        output1 = Dataset.objects.create(user=self.test_run.user, name='output1')
        output1.dataset_file.save('example.csv', content_file)
        self.test_run.datasets.create(argument=output_argument, dataset=output1)

        # run2 consumes an output from self.test_run
        run2 = ContainerRun.objects.create(user=self.test_run.user,
                                           name='example run',
                                           app=self.test_run.app,
                                           state=ContainerRun.FAILED)
        run2.datasets.create(argument=input_argument, dataset=output1)

        # Purge the input to run 2.
        output1.dataset_file.delete()

        # Now we request a rerun of run 2.
        run2_path = reverse("containerrun-detail", kwargs={'pk': run2.id})
        request1 = self.factory.get(self.list_path)
        force_authenticate(request1, user=self.kive_user)
        start_count = len(self.list_view(request1).data)

        request2 = self.factory.post(
            self.list_path,
            dict(name='ignored name',
                 original_run=run2_path),
            format="json")

        force_authenticate(request2, user=self.kive_user)
        resp = self.list_view(request2).render().data

        self.assertIn('id', resp)
        self.assertEqual("example run (rerun)", resp['name'])

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data

        self.assertEqual(len(resp), start_count + 2)
        resp_run = resp[1]
        run = ContainerRun.objects.get(id=resp_run['id'])
        self.assertEqual(0, run.datasets.count())
        resp_run_nested = resp[0]
        run_nested = ContainerRun.objects.get(id=resp_run_nested['id'])
        self.assertEqual(self.test_run, run_nested.original_run)

        self.assertEqual(1, len(mock_on_commit.call_args_list))

    @patch('container.models.check_output')
    def test_slurm_ended_a_while_ago(self, mock_check_output):
        ContainerRun.objects.update(slurm_job_id=None)
        self.test_run.slurm_job_id = 42
        self.test_run.save()
        other_run = self.test_run.app.runs.create(user=self.test_run.user,
                                                  slurm_job_id=43)
        end_time = (datetime.now() -
                    timedelta(minutes=15, seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')
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
    def test_slurm_ended_recently(self, mock_check_output):
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
        self.assertTrue(self.test_run.is_warned)

    @patch('container.models.check_output')
    def test_slurm_just_ended(self, mock_check_output):
        ContainerRun.objects.update(slurm_job_id=None)
        self.test_run.slurm_job_id = 42
        self.test_run.save()
        other_run = self.test_run.app.runs.create(user=self.test_run.user,
                                                  slurm_job_id=43)
        end_time = (datetime.now() -
                    timedelta(seconds=58)).strftime('%Y-%m-%dT%H:%M:%S')
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
                    timedelta(minutes=16)).strftime('%Y-%m-%dT%H:%M:%S')
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
                    timedelta(minutes=16)).strftime('%Y-%m-%dT%H:%M:%S')
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
                                 url='/dataset_view/{}'.format(dataset.id))]
        client = Client()
        client.force_login(run.user)
        response = client.get(reverse('container_run_detail',
                                      kwargs=dict(pk=run.pk)))

        self.assertEqual('Complete', response.context['state_name'])
        self.assertListEqual(expected_entries, response.context['data_entries'])

    def test_rerun_failed_run(self):
        run = ContainerRun.objects.get(id=1)
        run.state = ContainerRun.FAILED
        run.end_time = make_aware(datetime(2000, 1, 1), utc)
        run.set_md5()
        run.save()

        rerun = ContainerRun.objects.create(
            user=run.user,
            app=run.app,
            batch=run.batch,
            name=run.get_rerun_name(),
            description=run.description,
            priority=run.priority,
            end_time=make_aware(datetime(2000, 1, 2), utc),
            original_run=run)

        rerun.create_inputs_from_original_run()

        dataset = Dataset.objects.create(
            user=rerun.user,
            name='greetings_123.csv',
            date_created=make_aware(datetime(2000, 1, 1), utc))
        argument = rerun.app.arguments.get(name='greetings_csv')
        rerun.datasets.create(argument=argument, dataset=dataset)
        log = rerun.logs.create(short_text='Job completed.', type=ContainerLog.STDERR)
        rerun.set_md5()
        rerun.state = ContainerRun.COMPLETE
        rerun.save()
        expected_entries = [dict(created=make_aware(datetime(2000, 1, 1), utc),
                                 is_changed='no',
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
                                 is_changed='NEW',
                                 name='greetings_123.csv',
                                 size='missing',
                                 type='Output',
                                 url='/dataset_view/{}'.format(dataset.id))]
        client = Client()
        client.force_login(rerun.user)

        response = client.get(reverse('container_run_detail',
                                      kwargs=dict(pk=rerun.pk)))

        self.assertEqual('Complete', response.context['state_name'])
        self.assertListEqual(expected_entries, response.context['data_entries'])

    # noinspection PyUnresolvedReferences
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

    # noinspection PyUnresolvedReferences
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

    @patch('container.models.check_output')
    def test_launch_with_dependencies(self, mock_check_output):
        mock_check_output.side_effect = ['42\n', '43\n']
        expected_source_slurm_job_id = 42
        expected_main_slurm_job_id = 43

        main_run = ContainerRun.objects.filter(state=ContainerRun.NEW).first()
        self.assertIsNotNone(main_run)
        main_run.slurm_job_id = None
        main_run.sandbox_path = ''
        main_run.save()

        source_run = ContainerRun.objects.create(user=main_run.user,
                                                 app=main_run.app)

        main_run.schedule(dependencies={source_run.id: {}})

        main_run.refresh_from_db()
        source_run.refresh_from_db()
        self.assertEqual(expected_main_slurm_job_id, main_run.slurm_job_id)
        self.assertEqual(expected_source_slurm_job_id, source_run.slurm_job_id)
        self.assertEqual(2, len(mock_check_output.call_args_list))

        main_run_sbatch_args = mock_check_output.call_args_list[1][0][0]
        self.assertIn('--dependency=afterok:42', main_run_sbatch_args)

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


class ContainerDatasetTest(TestCase):

    def test_multi_input_validation(self):
        # Mono-valued input should have no multi_position
        arg = ContainerArgument(type=ContainerArgument.INPUT, position=0, allow_multiple=False)
        dataset = ContainerDataset(argument=arg)
        dataset.clean()
        dataset.multi_position = 0
        with self.assertRaises(ValidationError):
            dataset.clean()

        # Multi-valued input should have a multi_position
        arg = ContainerArgument(type=ContainerArgument.INPUT, position=None, allow_multiple=True)
        dataset = ContainerDataset(argument=arg, multi_position=0)
        dataset.clean()
        dataset.multi_position = None
        with self.assertRaises(ValidationError):
            dataset.clean()


@skipIfDBFeature('is_mocked')
class ContainerLogTests(TestCase):
    fixtures = ['container_run']

    def setUp(self):
        super(ContainerLogTests, self).setUp()
        install_fixture_files('container_run')

    def test_detail_view(self):
        run = ContainerRun.objects.get(id=1)
        log = run.logs.create(type=ContainerLog.STDOUT, short_text='.'*1001)

        client = Client()
        client.force_login(run.user)
        response = client.get(reverse('container_log_detail',
                                      kwargs=dict(pk=log.pk)))

        self.assertEqual(200, response.status_code)

    def test_short(self):
        run = ContainerRun.objects.get(id=1)
        log = run.logs.create(type=ContainerLog.STDOUT, short_text='.'*101)
        expected_display = '.' * 101
        expected_size_display = '101\xa0bytes'

        self.assertEqual(expected_display, log.preview)
        self.assertEqual(expected_size_display, log.size_display)

    def test_trimmed(self):
        run = ContainerRun.objects.get(id=1)
        log = run.logs.create(type=ContainerLog.STDOUT, short_text='.'*1001)
        expected_display = '.' * 1000 + '[...download to see the remaining 1\xa0byte.]'
        expected_size_display = '1001\xa0bytes'

        self.assertEqual(expected_display, log.preview)
        self.assertEqual(expected_size_display, log.size_display)

    def test_long(self):
        run = ContainerRun.objects.get(id=1)
        log = run.logs.create(type=ContainerLog.STDOUT)
        os.makedirs(ContainerRun.SANDBOX_ROOT)
        log_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'example.log')
        with open(log_path, 'wb+') as f:
            f.write(b'.'*2001)
            long_text = File(f)
            log.long_text.save('example.log', long_text)

        expected_display = '.' * 1000 + '[...download to see the remaining 1001\xa0bytes.]'
        expected_size_display = '2.0\xa0KB'

        self.assertEqual(expected_display, log.preview)
        self.assertEqual(expected_size_display, log.size_display)

    def test_purged(self):
        run = ContainerRun.objects.get(id=1)
        log = run.logs.create(type=ContainerLog.STDOUT, log_size=2001)

        expected_display = '[purged]'
        expected_size_display = 'missing'

        self.assertEqual(expected_display, log.preview)
        self.assertEqual(expected_size_display, log.size_display)


@skipIfDBFeature('is_mocked')
class ContainerLogApiTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        super().setUp()
        user = User.objects.first()
        self.assertIsNotNone(user)
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        app = ContainerApp.objects.create(container=container, name='test')
        run = app.runs.create(user=user)
        self.test_log = run.logs.create(type=ContainerLog.STDOUT,
                                        short_text='log content')

        self.list_path = reverse("containerlog-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = self.test_log.pk
        self.detail_path = reverse("containerlog-detail",
                                   kwargs={'pk': self.detail_pk})
        self.detail_view, _, _ = resolve(self.detail_path)

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)

        self.assertEqual(response.data['type'], ContainerLog.STDOUT)

    def test_download(self):
        download_path = reverse("containerlog-download",
                                kwargs={'pk': self.detail_pk})
        download_view, _, _ = resolve(download_path)
        request = self.factory.get(download_path)
        force_authenticate(request, user=self.kive_user)
        response = download_view(request, pk=self.detail_pk)

        self.assertEqual(response.content, b'log content')

    def test_download_permission(self):
        user = User.objects.create()
        self.test_log.run.grant_everyone_access()
        download_path = reverse("containerlog-download",
                                kwargs={'pk': self.detail_pk})
        download_view, _, _ = resolve(download_path)
        request = self.factory.get(download_path)
        force_authenticate(request, user=user)
        response = download_view(request, pk=self.detail_pk)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b'log content')


@skipIfDBFeature('is_mocked')
class ContainerArgumentApiTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        super().setUp()
        user = User.objects.first()
        self.assertIsNotNone(user)
        family = ContainerFamily.objects.create(user=user)
        container = Container.objects.create(family=family, user=user)
        app = ContainerApp.objects.create(container=container, name='test')
        self.test_argument = app.arguments.create(name='test_arg')

        self.list_path = reverse("containerargument-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = self.test_argument.pk
        self.detail_path = reverse("containerargument-detail",
                                   kwargs={'pk': self.detail_pk})
        self.detail_view, _, _ = resolve(self.detail_path)

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)

        self.assertEqual(response.data['name'], 'test_arg')

    def test_detail_permission(self):
        user = User.objects.create()
        self.test_argument.app.container.grant_everyone_access()
        detail_path = reverse("containerargument-detail",
                              kwargs={'pk': self.detail_pk})
        detail_view, _, _ = resolve(detail_path)
        request = self.factory.get(detail_path)
        force_authenticate(request, user=user)
        response = detail_view(request, pk=self.detail_pk)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['name'], 'test_arg')


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

        app_url = rest_reverse(str('containerapp-detail'),
                               kwargs=dict(pk=self.test_app.pk))
        arg_url = rest_reverse(str('containerargument-detail'),
                               kwargs=dict(pk=self.test_arg.pk))
        dataset_url = rest_reverse(str('dataset-detail'),
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
        self.assertEqual(resp['name'], "my batch")

        request3 = self.factory.get(self.list_path)
        force_authenticate(request3, user=self.kive_user)
        resp = self.list_view(request3).data
        resp_batch = resp[0]

        self.assertEqual(len(resp), start_count + 1)
        self.assertEqual(resp_batch['description'], "A really cool batch")

        resp_run = resp_batch['runs'][0]
        self.assertEqual(resp_run['name'], 'my run')

    def test_removal_plan(self):
        self.test_run.state = ContainerRun.COMPLETE
        self.test_run.save()
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEqual(response.data['Batches'], 1)
        self.assertEqual(response.data['ContainerRuns'], 1)

    def test_removal(self):
        self.test_run.state = ContainerRun.COMPLETE
        self.test_run.save()
        start_count = Batch.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Batch.objects.all().count()
        self.assertEqual(end_count, start_count - 1)


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
        self.source_path = os.path.abspath(
            os.path.join(
                __file__,
                '..',
                '..',
                '..',
                'samplecode',
                'singularity'
            )
        )

    def dummy_call(self, command, stdout, stderr):
        self.called_command = command
        stdout.write(self.call_stdout)
        stderr.write(self.call_stderr)
        return self.call_return_code

    def assert_run_fails(self, run, *expected_stderr):
        """ Check that a run fails, and look for messages in stderr. """
        with self.assertRaises(
                SystemExit):
            call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        self.assertEqual(ContainerRun.FAILED, run.state)
        self.assertIsNotNone(run.end_time)
        stderr = run.logs.get(type=ContainerLog.STDERR)
        stderr_text = stderr.read()
        for expected_message in expected_stderr:
            self.assertIn(expected_message, stderr_text)

    def assert_files_match(self, file_path1, file_path2, shallow=True):
        self.assertTrue(cmp(file_path1, file_path2, shallow))

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
        command_log_path = os.path.join(sandbox_path, 'logs', 'command.txt')
        self.assertTrue(os.path.exists(command_log_path),
                        command_log_path + ' should exist.')

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

    def test_run_input_bad_md5(self):
        run = ContainerRun.objects.get(name='fixture run')
        everyone = Group.objects.get(name='Everyone')
        run.groups_allowed.clear()
        run.groups_allowed.add(everyone)

        # Tamper with the file.
        input_containerdataset = run.datasets.get(
            argument__type=ContainerArgument.INPUT,
            argument__position=1
        )
        input_dataset = input_containerdataset.dataset
        input_dataset.dataset_file.save("tampered", ContentFile(b"foo"), save=True)
        expected_error = """\
========
Internal Kive Error
========
ValueError: Dataset with pk=1 has an inconsistent checksum \
(original 06f7204f2679744fd76e6b111dc506ba; \
current acbd18db4cc2f85cedef654fccc4a4d8)
"""

        with capture_log_stream(logging.ERROR, 'container') as mocked_stderr:
            self.assert_run_fails(run, expected_error)
            error_log = mocked_stderr.getvalue()

        self.assertIn('Running container failed.', error_log)
        self.assertIn('Traceback', error_log)
        self.assertIn('inconsistent checksum', error_log)

    def test_run_bad_md5(self):
        run = ContainerRun.objects.get(name='fixture run')
        everyone = Group.objects.get(name='Everyone')
        run.groups_allowed.clear()
        run.groups_allowed.add(everyone)

        # Tamper with the file.
        run.app.container.file.save("tampered", ContentFile(b"foo"), save=True)

        self.assert_run_fails(
            run,
            'ValueError: Container fixture family:vFixture file MD5 has changed')

    @staticmethod
    def _test_run_archive_preamble():
        greetings_path = os.path.abspath(os.path.join(__file__,
                                                      '..',
                                                      '..',
                                                      '..',
                                                      'samplecode',
                                                      'singularity',
                                                      'greetings.py'))
        with open(greetings_path, 'rb') as f:
            script_text = f.read()
            script_text = script_text.replace(b'Hello,', b'Howdy,')
            script_text = b'#!/usr/bin/env python\n' + script_text
        content = dict(pipeline=dict(
            inputs=[dict(dataset_name="names_csv")],
            steps=[dict(driver="greetings.py",
                        inputs=[dict(dataset_name="names_csv",
                                     source_step=0,
                                     source_dataset_name="names_csv")],
                        outputs=["greetings_csv"])],
            outputs=[dict(dataset_name="greetings_csv",
                          source_step=1,
                          source_dataset_name="greetings_csv")]))
        tar_data = BytesIO()
        with TarFile(fileobj=tar_data, mode='w') as t:
            tar_info = TarInfo('greetings.py')
            tar_info.size = len(script_text)
            t.addfile(tar_info, BytesIO(script_text))
        tar_data.seek(0)
        run = ContainerRun.objects.get(name='fixture run')
        old_container = run.app.container
        container = Container.objects.create(
            parent=old_container,
            family=old_container.family,
            user=old_container.user,
            tag='tar_test',
            file_type=Container.TAR)
        container.file.save('test_howdy.tar', ContentFile(tar_data.getvalue()))
        container.write_archive_content(content)
        container.save()
        run.app = container.apps.create(memory=200, threads=1)
        run.app.write_inputs('names_csv')
        run.app.write_outputs('greetings_csv')
        run.save()

        return run, container, old_container

    def test_run_archive(self):
        run, container, old_container = self._test_run_archive_preamble()
        expected_greetings = b'''\
greeting
"Howdy, Alice"
"Howdy, Bob"
"Howdy, Carolyn"
"Howdy, Darius"
'''
        expected_stderr = ""

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        stderr_log = run.logs.get(type=ContainerLog.STDERR)
        self.assertEqual(expected_stderr, stderr_log.read(1000))
        self.assertEqual(ContainerRun.COMPLETE, run.state)
        output_dataset = run.datasets.get(
            argument__type=ContainerArgument.OUTPUT).dataset
        greetings = output_dataset.dataset_file.read()
        self.assertEqual(expected_greetings, greetings)

    def test_run_archive_step_directories(self):
        run, container, old_container = self._test_run_archive_preamble()

        call_command('runcontainer', str(run.id))

        # Check that the appropriate directories were created.
        run.refresh_from_db()

        input_dir = os.path.join(run.full_sandbox_path, "input")
        self.assertTrue(os.path.isdir(input_dir))
        input_names_csv = os.path.join(input_dir, "names_csv")
        self.assertTrue(os.path.isfile(input_names_csv))

        step1_bin_dir = os.path.join(run.full_sandbox_path, "step1", "bin")
        self.assertTrue(os.path.isdir(step1_bin_dir))
        step1_script = os.path.join(step1_bin_dir, "greetings.py")
        self.assertTrue(os.path.isfile(step1_script))

        step1_input_dir = os.path.join(run.full_sandbox_path, "step1", "input")
        self.assertTrue(os.path.isdir(step1_input_dir))
        step1_input_names_csv = os.path.join(step1_input_dir, "names_csv")
        self.assertTrue(os.path.isfile(step1_input_names_csv))
        self.assert_files_match(input_names_csv, step1_input_names_csv)

        step1_output_dir = os.path.join(run.full_sandbox_path, "step1", "output")
        self.assertTrue(os.path.isdir(step1_output_dir))
        step1_output_greetings_csv = os.path.join(step1_output_dir, "step1_greetings_{}.csv".format(run.pk))
        self.assertTrue(os.path.isfile(step1_output_greetings_csv))

        output_dir = os.path.join(run.full_sandbox_path, "output")
        self.assertTrue(os.path.isdir(output_dir))  # this should be empty
        self.assertEqual(os.listdir(output_dir), [])

        upload_dir = os.path.join(run.full_sandbox_path, "upload")
        final_greetings_csv = os.path.join(upload_dir, "greetings_{}.csv".format(run.pk))
        self.assertTrue(os.path.isfile(final_greetings_csv))
        self.assert_files_match(step1_output_greetings_csv, final_greetings_csv)

    def test_run_archive_bad_md5(self):
        """Running an archive container with a bad MD5 should raise ValueError."""
        run, container, old_container = self._test_run_archive_preamble()
        container.file.save("tampered", ContentFile(b"foo"), save=True)
        self.assert_run_fails(
            run,
            'ValueError: Container fixture family:tar_test file MD5 has changed')

    def test_run_archive_parent_bad_md5(self):
        """Running an archive container whose parent has a bad MD5 should raise ValueError."""
        run, container, old_container = self._test_run_archive_preamble()
        old_container.file.save("tampered", ContentFile(b"foo"), save=True)
        self.assert_run_fails(
            run,
            'ValueError: Container fixture family:vFixture file MD5 has changed')

    def test_step_stdout(self):
        greetings_path = os.path.abspath(os.path.join(__file__,
                                                      '..',
                                                      '..',
                                                      '..',
                                                      'samplecode',
                                                      'singularity',
                                                      'greetings.py'))
        with open(greetings_path, 'rb') as f:
            script_text = b'#!/usr/bin/env python\nprint("Starting up.")\n'
            script_text += f.read()
        content = dict(pipeline=dict(
            inputs=[dict(dataset_name="names_csv")],
            steps=[dict(driver="greetings.py",
                        inputs=[dict(dataset_name="names_csv",
                                     source_step=0,
                                     source_dataset_name="names_csv")],
                        outputs=["greetings_csv"])],
            outputs=[dict(dataset_name="greetings_csv",
                          source_step=1,
                          source_dataset_name="greetings_csv")]))
        tar_data = BytesIO()
        with TarFile(fileobj=tar_data, mode='w') as t:
            tar_info = TarInfo('greetings.py')
            tar_info.size = len(script_text)
            t.addfile(tar_info, BytesIO(script_text))
        tar_data.seek(0)
        run = ContainerRun.objects.get(name='fixture run')
        old_container = run.app.container
        container = Container.objects.create(
            parent=old_container,
            family=old_container.family,
            user=old_container.user,
            tag='tar_test',
            file_type=Container.TAR)
        container.file.save('test_howdy.tar', ContentFile(tar_data.getvalue()))
        container.write_archive_content(content)
        container.save()
        run.app = container.apps.create(memory=200, threads=1)
        run.app.write_inputs('names_csv')
        run.app.write_outputs('greetings_csv')
        run.save()
        expected_stdout = """\
========
Processing step 1: greetings.py
========
Starting up.
"""
        expected_stderr = ""

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        stderr_log = run.logs.get(type=ContainerLog.STDERR)
        self.assertEqual(expected_stderr, stderr_log.read(1000))
        stdout_log = run.logs.get(type=ContainerLog.STDOUT)
        self.assertEqual(expected_stdout, stdout_log.read(1000))
        self.assertEqual(ContainerRun.COMPLETE, run.state)

    def _test_run_multistep_archive_helper(self):
        pairs_text = """\
x,y
0,1
1,1
1,2
2,3
"""
        content = dict(pipeline=dict(
            inputs=[dict(dataset_name="pairs_csv")],
            steps=[dict(driver="sums_and_products.py",
                        inputs=[dict(dataset_name="pairs_csv",
                                     source_step=0,
                                     source_dataset_name="pairs_csv")],
                        outputs=["sums_csv"],
                        dependencies=["scanner.py"]),
                   dict(driver="sum_summary.py",
                        inputs=[dict(dataset_name="sums_csv",
                                     source_step=1,
                                     source_dataset_name="sums_csv")],
                        outputs=["summary_csv"])],
            outputs=[dict(dataset_name="summary_csv",
                          source_step=2,
                          source_dataset_name="summary_csv")]))
        tar_data = BytesIO()
        with TarFile(fileobj=tar_data, mode='w') as t:
            for script_name in ('sums_and_products.py', 'sum_summary.py', 'scanner.py'):
                with open(os.path.join(self.source_path, script_name), 'rb') as f:
                    script_text = f.read()
                    script_text = b'#!/usr/bin/env python\n' + script_text
                tar_info = TarInfo(script_name)
                tar_info.size = len(script_text)
                t.addfile(tar_info, BytesIO(script_text))
        tar_data.seek(0)
        run = ContainerRun.objects.get(name='fixture run')
        old_container = run.app.container
        container = Container.objects.create(
            parent=old_container,
            family=old_container.family,
            user=old_container.user,
            tag='multi_test',
            file_type=Container.TAR)
        container.file.save('test_multi.tar', ContentFile(tar_data.getvalue()))
        container.write_archive_content(content)
        container.save()
        run.app = container.apps.create(memory=200, threads=1)
        run.app.write_inputs('pairs_csv')
        run.app.write_outputs('summary_csv')
        pairs_dataset = Dataset.create_dataset(
            file_path=None,
            user=run.user,
            file_handle=ContentFile(pairs_text.encode("utf-8"), name="pairs.csv")
        )
        run_input = run.datasets.get()
        run_input.dataset = pairs_dataset
        run_input.argument = run.app.arguments.get(type=ContainerArgument.INPUT)
        run_input.save()
        run.save()
        return run

    def test_run_multistep_archive(self):
        run = self._test_run_multistep_archive_helper()
        expected_summary = b"""\
sum,product,bigger
1,0,sum
2,1,sum
3,2,sum
5,6,product
"""
        expected_stderr = ""

        call_command('runcontainer', str(run.id))
        run.refresh_from_db()

        stderr_log = run.logs.get(type=ContainerLog.STDERR)
        self.assertEqual(expected_stderr, stderr_log.read(1000))
        self.assertEqual(ContainerRun.COMPLETE, run.state)
        output_dataset = run.datasets.get(
            argument__type=ContainerArgument.OUTPUT).dataset
        summary = output_dataset.dataset_file.read()
        self.assertEqual(expected_summary, summary)
        sandbox_path = run.full_sandbox_path
        command1_log_path = os.path.join(sandbox_path,
                                         'logs',
                                         'step_1_command.txt')
        self.assertTrue(os.path.exists(command1_log_path),
                        command1_log_path + ' should exist.')
        command2_log_path = os.path.join(sandbox_path,
                                         'logs',
                                         'step_2_command.txt')
        self.assertTrue(os.path.exists(command2_log_path),
                        command2_log_path + ' should exist.')

    def test_run_multistep_archive_bin_directories(self):
        run = self._test_run_multistep_archive_helper()

        call_command('runcontainer', str(run.id))

        run.refresh_from_db()

        # Step 1 defines dependencies, so unrelated script should not be there.
        step1_bin_dir = os.path.join(run.full_sandbox_path, "step1", "bin")
        self.assertTrue(os.path.isdir(step1_bin_dir))
        step1_script = os.path.join(step1_bin_dir, "sums_and_products.py")
        self.assertTrue(os.path.isfile(step1_script))
        step1_helper = os.path.join(step1_bin_dir, "scanner.py")
        self.assertTrue(os.path.isfile(step1_helper))
        step1_unrelated = os.path.join(step1_bin_dir, "sum_summary.py")
        self.assertFalse(os.path.isfile(step1_unrelated))

        # Step 2 does not define dependencies, so all scripts should be there.
        step2_bin_dir = os.path.join(run.full_sandbox_path, "step2", "bin")
        self.assertTrue(os.path.isdir(step2_bin_dir))
        step2_script = os.path.join(step2_bin_dir, "sum_summary.py")
        self.assertTrue(os.path.isfile(step2_script))
        step2_unrelated = os.path.join(step2_bin_dir, "sums_and_products.py")
        self.assertTrue(os.path.isfile(step2_unrelated))

    def test_run_multistep_archive_input_output_directories(self):
        run = self._test_run_multistep_archive_helper()
        call_command('runcontainer', str(run.id))
        run.refresh_from_db()

        input_dir = os.path.join(run.full_sandbox_path, "input")
        self.assertTrue(os.path.isdir(input_dir))
        input_pairs_csv = os.path.join(input_dir, "pairs_csv")
        self.assertTrue(os.path.isfile(input_pairs_csv))

        step1_input_dir = os.path.join(run.full_sandbox_path, "step1", "input")
        self.assertTrue(os.path.isdir(step1_input_dir))
        step1_input_pairs_csv = os.path.join(step1_input_dir, "pairs_csv")
        self.assertTrue(os.path.isfile(step1_input_pairs_csv))
        self.assert_files_match(input_pairs_csv, step1_input_pairs_csv)

        step1_output_dir = os.path.join(run.full_sandbox_path, "step1", "output")
        self.assertTrue(os.path.isdir(step1_output_dir))
        step1_output_sums_csv = os.path.join(step1_output_dir, "step1_sums_{}.csv".format(run.pk))
        self.assertTrue(os.path.isfile(step1_output_sums_csv))

        step2_input_dir = os.path.join(run.full_sandbox_path, "step2", "input")
        self.assertTrue(os.path.isdir(step2_input_dir))
        step2_input_sums_csv = os.path.join(step2_input_dir, "sums_csv")
        self.assertTrue(os.path.isfile(step2_input_sums_csv))
        self.assert_files_match(step1_output_sums_csv, step2_input_sums_csv)

        step2_output_dir = os.path.join(run.full_sandbox_path, "step2", "output")
        self.assertTrue(os.path.isdir(step2_output_dir))
        step2_output_summary_csv = os.path.join(step2_output_dir, "step2_summary_{}.csv".format(run.pk))
        self.assertTrue(os.path.isfile(step2_output_summary_csv))

        output_dir = os.path.join(run.full_sandbox_path, "output")
        self.assertTrue(os.path.isdir(output_dir))  # this should be empty
        self.assertEqual(os.listdir(output_dir), [])

        upload_dir = os.path.join(run.full_sandbox_path, "upload")
        final_summary_csv = os.path.join(upload_dir, "summary_{}.csv".format(run.pk))
        self.assertTrue(os.path.isfile(final_summary_csv))
        self.assert_files_match(step2_output_summary_csv, final_summary_csv)

    def test_multiple_inputs_and_outputs_directories(self):
        names_text = """\
name
Alice
Bob
Carol
David
"""
        raw_salutations_text = """\
raw_salutation
hi
hola
bye
what up
"""
        content = dict(pipeline=dict(
            inputs=[dict(dataset_name="names_csv"),
                    dict(dataset_name="raw_salutations_csv")],
            steps=[dict(driver="hello_goodbye_converter.py",
                        inputs=[dict(dataset_name="raw_salutations_csv",
                                     source_step=0,
                                     source_dataset_name="raw_salutations_csv")],
                        outputs=["regularized_salutations_csv", "opposite_csv"]),
                   dict(driver="salutations.py",
                        inputs=[dict(dataset_name="names_csv",
                                     source_step=0,
                                     source_dataset_name="names_csv"),
                                dict(dataset_name="regularized_salutations_csv",
                                     source_step=1,
                                     source_dataset_name="regularized_salutations_csv"),
                                ],
                        outputs=["formatted_salutations_csv"]),
                   dict(driver="salutations.py",
                        inputs=[dict(dataset_name="names_csv",
                                     source_step=0,
                                     source_dataset_name="names_csv"),
                                dict(dataset_name="opposite_csv",
                                     source_step=1,
                                     source_dataset_name="opposite_csv"),
                                ],
                        outputs=["opposite_formatted_salutations_csv"]),
                   ],
            outputs=[dict(dataset_name="salutation_and_name_csv",
                          source_step=2,
                          source_dataset_name="formatted_salutations_csv"),
                     dict(dataset_name="opposite_salutation_and_name_csv",
                          source_step=3,
                          source_dataset_name="opposite_formatted_salutations_csv")]))
        tar_data = BytesIO()
        with TarFile(fileobj=tar_data, mode='w') as t:
            for script_name in ('hello_goodbye_converter.py', 'salutations.py'):
                with open(os.path.join(self.source_path, script_name), 'rb') as f:
                    script_text = f.read()
                tar_info = TarInfo(script_name)
                tar_info.size = len(script_text)
                t.addfile(tar_info, BytesIO(script_text))
        tar_data.seek(0)

        cf = ContainerFamily.objects.get(name="fixture family")
        parent = cf.containers.get(tag="vFixture")

        # Make a new archive container.
        container = Container.objects.create(
            parent=parent,
            family=cf,
            user=parent.user,
            tag='multistep_multiinput_multioutput',
            file_type=Container.TAR)
        container.file.save('test_multi.tar', ContentFile(tar_data.getvalue()))
        container.write_archive_content(content)
        container.save()

        archive_app = container.apps.create(memory=200, threads=1)
        archive_app.write_inputs('names_csv raw_salutations_csv')
        archive_app.write_outputs('salutation_and_name_csv opposite_salutation_and_name_csv')
        names_dataset = Dataset.create_dataset(
            file_path=None,
            user=container.user,
            file_handle=ContentFile(names_text.encode("utf-8"), name="names.csv")
        )
        raw_salutations_dataset = Dataset.create_dataset(
            file_path=None,
            user=container.user,
            file_handle=ContentFile(raw_salutations_text.encode("utf-8"), name="raw_salutations.csv")
        )

        run = archive_app.runs.create(
            name="PipelineRun",
            user=container.user
        )
        run.datasets.create(
            argument=archive_app.arguments.get(type=ContainerArgument.INPUT, position=1),
            dataset=names_dataset
        )
        run.datasets.create(
            argument=archive_app.arguments.get(type=ContainerArgument.INPUT, position=2),
            dataset=raw_salutations_dataset
        )
        run.save(schedule=False)

        # Run it!
        call_command('runcontainer', str(run.id))
        run.refresh_from_db()

        # Check that the input directory is in order.
        input_dir = os.path.join(run.full_sandbox_path, "input")
        self.assertTrue(os.path.isdir(input_dir))
        input_names_csv = os.path.join(input_dir, "names_csv")
        self.assertTrue(os.path.isfile(input_names_csv))
        input_raw_salutations_csv = os.path.join(input_dir, "raw_salutations_csv")
        self.assertTrue(os.path.isfile(input_raw_salutations_csv))

        # Check that the step 1 directory is in order.
        step1_input_dir = os.path.join(run.full_sandbox_path, "step1", "input")
        self.assertTrue(os.path.isdir(step1_input_dir))
        step1_input_raw_salutations_csv = os.path.join(step1_input_dir, "raw_salutations_csv")
        self.assertTrue(os.path.isfile(step1_input_raw_salutations_csv))

        step1_output_dir = os.path.join(run.full_sandbox_path, "step1", "output")
        step1_output_regularized_salutations_csv = os.path.join(
            step1_output_dir,
            "step1_regularized_salutations_{}.csv".format(run.pk)
        )
        step1_output_opposite_csv = os.path.join(
            step1_output_dir,
            "step1_opposite_{}.csv".format(run.pk)
        )
        self.assertTrue(os.path.isdir(step1_output_dir))
        self.assertTrue(os.path.isfile(step1_output_regularized_salutations_csv))
        self.assertTrue(os.path.isfile(step1_output_opposite_csv))

        # Check step 2's directory.
        step2_input_dir = os.path.join(run.full_sandbox_path, "step2", "input")
        self.assertTrue(os.path.isdir(step2_input_dir))
        step2_input_names_csv = os.path.join(step2_input_dir, "names_csv")
        step2_input_regularized_salutations_csv = os.path.join(step2_input_dir, "regularized_salutations_csv")
        self.assertTrue(os.path.isfile(step2_input_names_csv))
        self.assertTrue(os.path.isfile(step2_input_regularized_salutations_csv))
        self.assert_files_match(input_names_csv, step2_input_names_csv)
        self.assert_files_match(step2_input_regularized_salutations_csv,
                                step1_output_regularized_salutations_csv)

        step2_output_dir = os.path.join(run.full_sandbox_path, "step2", "output")
        self.assertTrue(os.path.isdir(step2_output_dir))
        step2_output_formatted_salutations_csv = os.path.join(
            step2_output_dir,
            "step2_formatted_salutations_{}.csv".format(run.pk)
        )
        self.assertTrue(os.path.isfile(step2_output_formatted_salutations_csv))

        # Check step 3's directory.
        step3_input_dir = os.path.join(run.full_sandbox_path, "step3", "input")
        self.assertTrue(os.path.isdir(step3_input_dir))
        step3_input_names_csv = os.path.join(step3_input_dir, "names_csv")
        step3_input_opposite_csv = os.path.join(step3_input_dir, "opposite_csv")
        self.assertTrue(os.path.isfile(step3_input_names_csv))
        self.assertTrue(os.path.isfile(step3_input_opposite_csv))
        self.assert_files_match(input_names_csv, step3_input_names_csv)
        self.assert_files_match(step3_input_opposite_csv, step1_output_opposite_csv)

        step3_output_dir = os.path.join(run.full_sandbox_path, "step3", "output")
        self.assertTrue(os.path.isdir(step3_output_dir))
        step3_output_opposite_formatted_salutations_csv = os.path.join(
            step3_output_dir,
            "step3_opposite_formatted_salutations_{}.csv".format(run.pk)
        )
        self.assertTrue(os.path.isfile(step3_output_opposite_formatted_salutations_csv))

        # Lastly check that the output directory is fine.
        output_dir = os.path.join(run.full_sandbox_path, "output")
        self.assertTrue(os.path.isdir(output_dir))  # this should be empty
        self.assertEqual(os.listdir(output_dir), [])

        upload_dir = os.path.join(run.full_sandbox_path, "upload")
        final_formatted_salutations_csv = os.path.join(upload_dir, "salutation_and_name_{}.csv".format(run.pk))
        self.assertTrue(os.path.isfile(final_formatted_salutations_csv))
        self.assert_files_match(step2_output_formatted_salutations_csv,
                                final_formatted_salutations_csv)

        final_opposite_formatted_salutations = os.path.join(
            upload_dir,
            "opposite_salutation_and_name_{}.csv".format(run.pk)
        )
        self.assertTrue(os.path.isfile(final_opposite_formatted_salutations))
        self.assert_files_match(step3_output_opposite_formatted_salutations_csv,
                                final_opposite_formatted_salutations)

    def test_mount_directories(self):
        """Test that the correct directories are mounted in the right places."""
        pipeline = {
            "inputs": [
                {"dataset_name": "input_text"}
            ],
            "steps": [
                {
                    "driver": "scanner.py",
                    "inputs": [
                        {
                            "dataset_name": "step1_input_text",
                            "source_step": 0,
                            "source_dataset_name": "input_text"
                        }
                    ],
                    "outputs": ["summary_json"]
                },
                {
                    "driver": "scanner.py",
                    "inputs": [
                        {
                            "dataset_name": "step2_input_text",
                            "source_step": 1,
                            "source_dataset_name": "summary_json"
                        }
                    ],
                    "outputs": ["summary_json"]
                }
            ],
            "outputs": [
                {
                    "dataset_name": "step1_summary_json",
                    "source_step": 1,
                    "source_dataset_name": "summary_json"
                },
                {
                    "dataset_name": "step2_summary_json",
                    "source_step": 2,
                    "source_dataset_name": "summary_json"
                }
            ]
        }
        content = {"pipeline": pipeline}

        tar_data = BytesIO()
        with TarFile(fileobj=tar_data, mode='w') as t:
            with open(os.path.join(self.source_path, "scanner.py"), 'rb') as f:
                script_text = f.read()
            tar_info = TarInfo("scanner.py")
            tar_info.size = len(script_text)
            t.addfile(tar_info, BytesIO(script_text))
        tar_data.seek(0)

        cf = ContainerFamily.objects.get(name="fixture family")
        parent = cf.containers.get(tag="vFixture")

        # Make a new archive container for this archive container.
        container = Container.objects.create(
            parent=parent,
            family=cf,
            user=parent.user,
            tag='multistep_multiinput_multioutput',
            file_type=Container.TAR)
        container.file.save('test_multi.tar', ContentFile(tar_data.getvalue()))
        container.write_archive_content(content)
        container.save()

        archive_app = container.apps.create(memory=200, threads=1)
        archive_app.write_inputs('input_text')
        archive_app.write_outputs('step1_summary_json step2_summary_json')
        input_text = """\
Line 1
Line 2
Line 3
"""
        input_text_dataset = Dataset.create_dataset(
            file_path=None,
            user=container.user,
            file_handle=ContentFile(input_text.encode("utf-8"), name="input_text")
        )

        run = archive_app.runs.create(
            name="CheckMountPoints",
            user=container.user
        )
        run.datasets.create(
            argument=archive_app.arguments.get(type=ContainerArgument.INPUT, position=1),
            dataset=input_text_dataset
        )
        run.save(schedule=False)

        # Run it!
        call_command('runcontainer', str(run.id))
        run.refresh_from_db()

        # Now inspect the results.  The first step's input should have three lines; the second should have one.
        step1_summary_cds = run.datasets.get(argument__type=ContainerArgument.OUTPUT, argument__position=1)
        with use_field_file(step1_summary_cds.dataset.dataset_file) as f:
            step1_summary = json.loads(f.read().decode("utf-8"))
        self.assertEqual(step1_summary["lines"], 3)
        self.assertEqual(step1_summary["mnt_input_contents"], ["step1_input_text"])
        self.assertEqual(step1_summary["mnt_output_contents"], ["step1_summary_json_{}".format(run.pk)])

        step2_summary_cds = run.datasets.get(argument__type=ContainerArgument.OUTPUT, argument__position=2)
        with use_field_file(step2_summary_cds.dataset.dataset_file) as f:
            step2_summary = json.loads(f.read().decode("utf-8"))
        self.assertEqual(step2_summary["lines"], 1)
        self.assertEqual(step2_summary["mnt_input_contents"], ["step2_input_text"])
        self.assertEqual(step2_summary["mnt_output_contents"], ["step2_summary_json_{}".format(run.pk)])

    def test_already_started(self):
        """ Pretend that another instance of the command already started. """
        run = ContainerRun.objects.get(name='fixture run')
        run.state = ContainerRun.LOADING
        run.save()

        with self.assertRaisesRegex(
                CommandError,
                r'Expected state N for run id \d+, but was L'):
            call_command('runcontainer', str(run.id))

        run.refresh_from_db()
        self.assertEqual('', run.full_sandbox_path)

    def test_missing_output(self):
        """ Configure an extra output that the image doesn't know about. """
        run = ContainerRun.objects.get(name='fixture run')
        dataset = Dataset.objects.first()
        extra_arg = run.app.arguments.create(name='extra_csv',
                                             position=None,
                                             type=ContainerArgument.INPUT)
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

        self.assert_run_fails(run,
                              'No such file',
                              'singularity/host_input/missing_file.csv')

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

    def test_find_rerun_input(self):
        run1 = ContainerRun.objects.get(name='fixture run')
        app = run1.app
        input_argument = app.arguments.get(type=ContainerArgument.INPUT)
        output_argument = app.arguments.get(type=ContainerArgument.OUTPUT)

        content_file = ContentFile('x,y\n1,2')
        output1 = Dataset.objects.create(user=run1.user, name='output1')
        output1.dataset_file.save('example.csv', content_file)
        output1.set_md5()
        output1.save()
        run1.datasets.create(argument=output_argument, dataset=output1)

        # run2 consumes an output from run1
        run2 = ContainerRun.objects.create(user=run1.user,
                                           app=run1.app,
                                           state=ContainerRun.FAILED)
        run2.datasets.create(argument=input_argument, dataset=output1)

        # run2 produces an output
        output2 = Dataset.objects.create(user=run2.user, name='output2')
        content_file2 = ContentFile('greeting\n')
        output2.dataset_file.save('out.csv', content_file2)
        output2.set_md5()
        output2.save()
        run2.datasets.create(argument=output_argument, dataset=output2)
        run2.set_md5()

        # Purge the input to run 2.
        output1.dataset_file.delete()

        # run3 is a rerun of run1 to reproduce the input for run 2.
        run3 = ContainerRun.objects.create(user=run1.user,
                                           name='source rerun',
                                           app=run1.app,
                                           original_run=run1,
                                           state=ContainerRun.COMPLETE)
        output1b = Dataset.objects.create(user=run1.user, name='output1b')
        output1b.dataset_file.save('example_b.csv', content_file)
        output1b.set_md5()
        output1b.save()
        run3.datasets.create(argument=output_argument, dataset=output1b)

        # run4 is a rerun of run 2, and the one we are going to execute.
        run4 = ContainerRun.objects.create(user=run2.user,
                                           app=run2.app,
                                           original_run=run2)

        call_command('runcontainer', str(run4.id))

        run4.refresh_from_db()
        run_dataset = run4.datasets.get(argument__type=ContainerArgument.INPUT)
        self.assertEqual(output1b.id, run_dataset.dataset_id)
        self.assertEqual(run2.md5, run4.md5)

    def test_find_rerun_input_changes(self):
        run1 = ContainerRun.objects.get(name='fixture run')
        app = run1.app
        input_argument = app.arguments.get(type=ContainerArgument.INPUT)
        output_argument = app.arguments.get(type=ContainerArgument.OUTPUT)

        content_file1 = ContentFile('x,y\n1,2')
        content_file1b = ContentFile('x,y\n10,20')
        output1 = Dataset.objects.create(user=run1.user, name='output1')
        output1.dataset_file.save('example.csv', content_file1)
        output1.set_md5()
        output1.save()
        run1.datasets.create(argument=output_argument, dataset=output1)

        # run2 consumes an output from run1
        run2 = ContainerRun.objects.create(user=run1.user,
                                           app=run1.app,
                                           state=ContainerRun.FAILED)
        run2.datasets.create(argument=input_argument, dataset=output1)

        # run2 produces an output
        output2 = Dataset.objects.create(user=run2.user, name='output2')
        content_file2 = ContentFile('greeting\n')
        output2.dataset_file.save('out.csv', content_file2)
        output2.set_md5()
        output2.save()
        run2.datasets.create(argument=output_argument, dataset=output2)
        run2.set_md5()

        # Purge the input to run 2.
        output1.dataset_file.delete()

        # run3 is a rerun of run1 to reproduce the input for run 2.
        run3 = ContainerRun.objects.create(user=run1.user,
                                           name='source rerun',
                                           app=run1.app,
                                           original_run=run1,
                                           state=ContainerRun.COMPLETE)
        output1b = Dataset.objects.create(user=run1.user, name='output1b')
        output1b.dataset_file.save('example_b.csv', content_file1b)
        output1b.set_md5()
        output1b.save()
        run3.datasets.create(argument=output_argument, dataset=output1b)

        # run4 is a rerun of run 2, and the one we are going to execute.
        run4 = ContainerRun.objects.create(user=run2.user,
                                           app=run2.app,
                                           original_run=run2)

        call_command('runcontainer', str(run4.id))

        run4.refresh_from_db()
        run_dataset = run4.datasets.get(argument__type=ContainerArgument.INPUT)
        self.assertEqual(output1b.id, run_dataset.dataset_id)
        self.assertNotEqual(run2.md5, run4.md5)

    def test_rerun_input_exists(self):
        run1 = ContainerRun.objects.get(name='fixture run')
        app = run1.app
        input_argument = app.arguments.get(type=ContainerArgument.INPUT)
        output_argument = app.arguments.get(type=ContainerArgument.OUTPUT)

        content_file = ContentFile('x,y\n1,2')
        output1 = Dataset.objects.create(user=run1.user, name='output1')
        output1.dataset_file.save('example.csv', content_file)
        output1.set_md5(output1.dataset_file.path)
        output1.save()
        run1.datasets.create(argument=output_argument, dataset=output1)

        # run2 consumes an output from run1
        run2 = ContainerRun.objects.create(user=run1.user,
                                           app=run1.app,
                                           state=ContainerRun.FAILED)
        run2.datasets.create(argument=input_argument, dataset=output1)

        # run4 is a rerun of run 2, and the one we are going to execute.
        run4 = ContainerRun.objects.create(user=run2.user,
                                           app=run2.app,
                                           original_run=run2)
        # Inputs have not been purged, so no extra reruns needed.
        reruns_needed = run4.create_inputs_from_original_run()
        self.assertEqual(set(), reruns_needed)

        call_command('runcontainer', str(run4.id))

        run4.refresh_from_db()
        run_dataset = run4.datasets.get(argument__type=ContainerArgument.INPUT)
        self.assertEqual(output1.id, run_dataset.dataset_id)

    def test_rerun_input_missing(self):
        run1 = ContainerRun.objects.get(name='fixture run')
        app = run1.app
        input_argument = app.arguments.get(type=ContainerArgument.INPUT)
        output_argument = app.arguments.get(type=ContainerArgument.OUTPUT)

        content_file = ContentFile('x,y\n1,2')
        output1 = Dataset.objects.create(user=run1.user, name='output1')
        output1.dataset_file.save('example.csv', content_file)
        run1.datasets.create(argument=output_argument, dataset=output1)

        # run2 consumes an output from run1
        run2 = ContainerRun.objects.create(user=run1.user,
                                           app=run1.app,
                                           state=ContainerRun.FAILED)
        run2.datasets.create(argument=input_argument, dataset=output1)

        # Purge the input to run 2.
        output1.dataset_file.delete()

        # run4 is a rerun of run 2, and the one we are going to execute.
        run4 = ContainerRun.objects.create(user=run2.user,
                                           app=run2.app,
                                           original_run=run2)

        self.assert_run_fails(run4, 'RuntimeError: Inputs missing from reruns')

    def test_fixed_argument_formatting(self):
        app = ContainerApp.objects.first()
        run = app.runs.first()
        args = list(app.arguments.all())
        command = runcontainer.Command.build_command(run)

        expected = []
        for arg in args:
            if arg.type == ContainerArgument.INPUT:
                expected.append("/mnt/input/{}".format(arg.name))
            else:
                expected.append("/mnt/output/{}".format(arg.name))
        self.assertEqual(len(args), 2, "Expected run to have two arguments")
        self.assertEqual(expected, command[-len(expected):])

    def test_full_argument_formatting(self):
        # Set up test data
        # NOTE(nknight): The order of these specs matters; it matches the order that the
        # associated ContainerDataset objects would be returned from the database.
        argspecs = [
            {
                "name": "positional_input",
                "position": 0,
                "type": ContainerArgument.INPUT,
            },
            {
                "name": "positional_output",
                "position": 2,
                "type": ContainerArgument.OUTPUT
            },
            {
                "name": "optional_input",
                "position": None,
                "type": ContainerArgument.INPUT,
            },
            {
                "name": "multiple_optional_input",
                "position": None,
                "type": ContainerArgument.INPUT,
                "allow_multiple": True,
            }
            # TODO(nknight): Add spec for a positional output directory
        ]

        app = ContainerApp(
            container=Container.objects.get(pk=1),
        )
        app.save()

        args = [ContainerArgument(app=app, **spec) for spec in argspecs]
        for arg in args:
            arg.save()

        datasets = []

        def make_dataset(arg, multi_position=None):
            dataset = unittest.mock.Mock()
            dataset.argument = arg
            dataset.dataset.name = arg.name
            if multi_position is not None:
                dataset.dataset.name += str(multi_position)
            dataset.multi_position = multi_position
            datasets.append(dataset)

        for arg in (a for a in args if a.type == ContainerArgument.INPUT):
            if arg.allow_multiple:
                make_dataset(arg, multi_position=0)
                make_dataset(arg, multi_position=1)
            else:
                make_dataset(arg)

        mock_run = unittest.mock.Mock()
        mock_run.app = app
        mock_run.container.file.path = "mock_container_path"
        mock_run.full_sandbox_path = "mock_sandbox_path"
        mock_run.datasets.all = unittest.mock.Mock(return_value=iter(datasets))

        # Exercise method under test
        full_command = runcontainer.Command.build_command(mock_run)

        # Ignore unrelated changes in the command format
        self.assertNotIn("--app", full_command, "Unexpected '--app' name in command")
        command_args = full_command[7:]

        self.assertEqual(
            command_args,
            [
                "--optional_input", "/mnt/input/optional_input",
                "--multiple_optional_input", "/mnt/input/multiple_optional_input0",
                "/mnt/input/multiple_optional_input1", "--",
                "/mnt/input/positional_input", "/mnt/output/positional_output"
            ],
        )

    def test_output_argument_dataset_naming(self):
        run_output_path = pathlib.Path("/asdf/output")  # Simulates the run's output dir
        outputpath = run_output_path / "semi/"  # Simulates run's directory output argument
        runid = 2356

        self.assertEqual(
            runcontainer.Command._build_directory_dataset_name(
                runid,
                run_output_path,
                outputpath / "test.csv",
            ),
            "semi/test_2356.csv",
        )
        self.assertEqual(
            runcontainer.Command._build_directory_dataset_name(
                runid,
                run_output_path,
                outputpath / "test.tar.gz",
            ),
            "semi/test_2356.tar.gz",
        )
        self.assertEqual(
            runcontainer.Command._build_directory_dataset_name(
                runid,
                run_output_path,
                outputpath / "test",
            ),
            "semi/test_2356",
        )
        self.assertEqual(
            runcontainer.Command._build_directory_dataset_name(
                runid, run_output_path, outputpath / "colon" / "test.png"
            ),
            "semi/colon/test_2356.png",
        )


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
        for run in ContainerRun.objects.exclude(sandbox_path=''):
            run.delete_sandbox()
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
        self.assertNotEqual('', run.sandbox_path)
        self.assertTrue(os.path.exists(run.full_sandbox_path))
        self.assertIsNone(run.sandbox_size)

    def test_purge_too_small(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=100)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=200)

        purge.Command().handle(start=400, stop=400)

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertNotEqual('', run1.sandbox_path)
        self.assertNotEqual('', run2.sandbox_path)
        self.assertTrue(os.path.exists(run1.full_sandbox_path))
        self.assertEqual(100, run1.sandbox_size)

    def test_purge_no_sandbox(self):
        """ Sometimes a sandbox doesn't get created, and the path is blank. """
        run = self.create_sandbox(size=500)
        run.sandbox_path = ''
        run.save()

        purge.Command().handle(start=400, stop=400)

        run.refresh_from_db()
        self.assertIsNone(run.sandbox_size)

    def test_purge_folder(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=400)
        run1_path = run1.full_sandbox_path
        run2_path = run2.full_sandbox_path

        purge.Command().handle(start=500, stop=500)

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertEqual('', run1.sandbox_path)
        self.assertNotEqual('', run2.sandbox_path)
        self.assertFalse(os.path.exists(run1_path))
        self.assertTrue(os.path.exists(run2_path))
        self.assertEqual(200, run1.sandbox_size)

    def test_purge_missing_folder(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=400)
        run1_path = run1.full_sandbox_path
        shutil.rmtree(run1_path)
        expected_log_message = 'Missing 1 containerrun file from 20 minutes ago.\n'

        with self.capture_log_stream(logging.ERROR) as mocked_stderr:
            purge.Command().handle(start=500, stop=500)
            log_messages = mocked_stderr.getvalue()

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertEqual(expected_log_message, log_messages)
        self.assertEqual('', run1.sandbox_path)

    def test_purge_broken_link(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=400)
        run1_path = run1.full_sandbox_path
        link_path = os.path.join(run1_path, 'broken_link.txt')
        source_path = os.path.join(run1_path, 'does_not_exist.txt')
        os.symlink(source_path, link_path)
        expected_log_message = ''

        with self.capture_log_stream(logging.ERROR) as mocked_stderr:
            purge.Command().handle(start=500, stop=500)
            log_messages = mocked_stderr.getvalue()

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertEqual(expected_log_message, log_messages)
        self.assertEqual('', run1.sandbox_path)

    def test_synch_broken_link(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run1_path = run1.full_sandbox_path
        link_path = os.path.join(run1_path, 'broken_link.txt')
        source_path = '../does_not_exist.txt'  # Symbolic link contains 21 bytes.
        os.symlink(source_path, link_path)
        run1.sandbox_path = ''  # Abandon the sandbox to get cleaned up by synch.
        run1.save()
        expected_log_message = 'Purged 1 unregistered container run file ' \
                               'containing 221 bytes.\n'

        with self.capture_log_stream(logging.ERROR) as mocked_stderr:
            purge.Command().handle(synch=True)
            log_messages = mocked_stderr.getvalue()

        self.assertEqual(expected_log_message, log_messages)

    def test_purge_start(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=400)

        purge.Command().handle(start=600, stop=400)

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertNotEqual('', run1.sandbox_path)
        self.assertNotEqual('', run2.sandbox_path)

    def test_purge_stop(self):
        run1 = self.create_sandbox(age=timedelta(minutes=20), size=200)
        run2 = self.create_sandbox(age=timedelta(minutes=10), size=400)

        purge.Command().handle(start=400, stop=199)

        run1.refresh_from_db()
        run2.refresh_from_db()
        self.assertEqual('', run1.sandbox_path)
        self.assertEqual('', run2.sandbox_path)

    def test_unregistered_folder(self):
        run = self.create_sandbox(size=500)

        left_overs_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'left_overs')
        os.mkdir(left_overs_path)
        with open(os.path.join(left_overs_path, 'contents.txt'), 'wb') as f:
            f.write(b'.' * 100)

        purge.Command().handle(start=400, stop=400, synch=True)

        run.refresh_from_db()
        self.assertNotEqual('', run.sandbox_path)  # registered sandboxes skipped
        self.assertFalse(os.path.exists(left_overs_path))
        self.assertTrue(os.path.exists(run.full_sandbox_path))

    def test_unregistered_file(self):
        left_overs_path = os.path.join(ContainerRun.SANDBOX_ROOT,
                                       'left_overs.txt')
        with open(left_overs_path, 'wb') as f:
            f.write(b'.' * 100)

        purge.Command().handle(start=400, stop=400, synch=True)

        self.assertFalse(os.path.exists(left_overs_path))

    def test_unregistered_by_age(self):
        too_new_time = time() - 20
        old_enough_time = time() - 40

        new_folder_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'new_left_overs')
        os.mkdir(new_folder_path)
        child1_path = os.path.join(new_folder_path, 'contents1.txt')
        with open(child1_path, 'wb') as f:
            f.write(b'.' * 100)
        os.utime(child1_path, (old_enough_time, old_enough_time))
        child2_path = os.path.join(new_folder_path, 'contents2.txt')
        with open(child2_path, 'wb') as f:
            f.write(b'.' * 100)
        os.utime(child2_path, (too_new_time, too_new_time))

        old_folder_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'old_left_overs')
        os.mkdir(old_folder_path)
        child3_path = os.path.join(old_folder_path, 'contents3.txt')
        with open(child3_path, 'wb') as f:
            f.write(b'.' * 100)
        os.utime(child3_path, (old_enough_time, old_enough_time))

        new_file_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'new_extras.txt')
        with open(new_file_path, 'wb') as f:
            f.write(b'.' * 100)
        os.utime(new_file_path, (too_new_time, too_new_time))

        old_file_path = os.path.join(ContainerRun.SANDBOX_ROOT, 'old_extras.txt')
        with open(old_file_path, 'wb') as f:
            f.write(b'.' * 100)
        os.utime(old_file_path, (old_enough_time, old_enough_time))

        purge.Command().handle(wait=timedelta(seconds=30), synch=True)

        self.assertTrue(os.path.exists(new_folder_path))
        self.assertTrue(os.path.exists(new_file_path))
        self.assertFalse(os.path.exists(old_folder_path))
        self.assertFalse(os.path.exists(old_file_path))

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
        self.assertEqual('', run.sandbox_path)
        self.assertNotEqual('', dataset.dataset_file)  # Not purged.

    def test_sandbox_and_output(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=1))
        self.create_outputs(run, output_size=200, age=timedelta(minutes=9))

        purge.Command().handle(start=150, stop=150, sandbox_aging=10)

        run.refresh_from_db()
        dataset = run.datasets.filter(argument__type='O').get().dataset
        self.assertEqual('', run.sandbox_path)
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

    def test_missing_dataset_file(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=1))
        run.delete_sandbox()
        run.save()
        dataset = Dataset.objects.create(
            user=run.user,
            dataset_file='Datasets/2019_02/does_not_exist.txt',
            date_created=timezone.now() - timedelta(minutes=1))
        argument = run.app.arguments.get(type='O')
        run.datasets.create(argument=argument,
                            dataset=dataset)

        expected_messages = u"""\
Missing dataset file 'Datasets/2019_02/does_not_exist.txt' from a minute ago.
Missing 1 dataset file from a minute ago.
"""
        with self.capture_log_stream(logging.WARN) as mocked_stderr:
            purge.Command().handle()
            log_messages = mocked_stderr.getvalue()

        dataset.refresh_from_db()
        self.assertLogStreamEqual(expected_messages, log_messages)
        self.assertEqual('', dataset.dataset_file)

    def test_multiple_dataset_files_missing(self):
        run1 = self.create_sandbox(size=100, age=timedelta(minutes=1))
        run1.delete_sandbox()
        run1.save()
        dataset = Dataset.objects.create(
            user=run1.user,
            dataset_file='Datasets/2019_02/does_not_exist.txt',
            date_created=timezone.now() - timedelta(minutes=1))
        argument = run1.app.arguments.get(type='O')
        run1.datasets.create(argument=argument,
                             dataset=dataset)
        run2 = self.create_sandbox(size=100, age=timedelta(minutes=1))
        run2.delete_sandbox()
        run2.save()
        dataset = Dataset.objects.create(
            user=run2.user,
            dataset_file='Datasets/2019_02/also_gone.txt',
            date_created=timezone.now() - timedelta(minutes=5))
        argument = run2.app.arguments.get(type='O')
        run2.datasets.create(argument=argument,
                             dataset=dataset)

        expected_messages = u"""\
Missing 2 dataset files from 5 minutes ago to a minute ago.
"""
        with self.capture_log_stream(logging.ERROR) as mocked_stderr:
            purge.Command().handle()
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_purge_missing_log(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=1))
        self.create_outputs(run, output_size=200, age=timedelta(minutes=1))
        stdout_log = run.logs.get(type=ContainerLog.STDOUT)
        stdout_log.long_text = 'ContainerLogs/does_not_exist.txt'
        stdout_log.save()

        expected_messages = u"""\
Missing containerlog file 'ContainerLogs/does_not_exist.txt' from a minute ago.
Missing 1 containerlog file from a minute ago.
"""
        with self.capture_log_stream(logging.WARN) as mocked_stderr:
            purge.Command().handle(start=500, stop=500)
            log_messages = mocked_stderr.getvalue()

        self.assertLogStreamEqual(expected_messages, log_messages)

    def test_ignore_sandboxes_already_purged(self):
        run1 = self.create_sandbox(size=1000, age=timedelta(minutes=1))
        self.create_sandbox(size=100, age=timedelta(minutes=1))
        run1.sandbox_size = 1000
        run1.delete_sandbox()
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
        Dataset.create_dataset(output_path,
                               name='extra.txt',
                               user=user,
                               is_uploaded=True)

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
        run.save()
        dataset = run.datasets.get(argument__type='O').dataset
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
        expected_messages = """\
Purged unregistered file 'Datasets/2018_06/empty_child' containing 0 bytes.
Purged 1 unregistered dataset file containing 0 bytes.
"""

        with self.capture_log_stream(logging.WARN) as mocked_stderr:
            purge.Command().handle(synch=True)
            log_messages = mocked_stderr.getvalue()

        self.assertFalse(os.path.exists(left_overs_path))
        # Parent will get purged next time.
        self.assertTrue(os.path.exists(os.path.dirname(left_overs_path)))
        self.assertLogStreamEqual(expected_messages, log_messages)

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
        self.assertEqual('', run.sandbox_path)
        self.assertEqual('', log.long_text)  # Purged.
        self.assertFalse(os.path.exists(log_path))

    def test_container_log_debugging(self):
        run = self.create_sandbox(size=100, age=timedelta(minutes=9))
        self.create_outputs(run,
                            output_size=200,
                            stdout_size=2100,
                            age=timedelta(minutes=10))
        run.delete_sandbox()
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

    @patch("os.stat")
    @patch("os.path.islink")
    def test_skip_deleting_missing_file(self, islink, stat):
        islink.return_value = False
        stat.side_effect = OSError(2, "No such file or directory")
        fakepath = "fake_file_path"

        size = purge.Command.get_file_size(fakepath)

        self.assertIsNone(size)
        islink.assert_called_with(fakepath)
        stat.assert_called_with(fakepath)


@skipIfDBFeature('is_mocked')
class ContainerFormMockTests(TestCase):
    def setUp(self):
        super(ContainerFormMockTests, self).setUp()
        self.alpine_path = os.path.abspath(os.path.join(
            __file__,
            '..',
            '..',
            '..',
            'samplecode',
            'singularity',
            'python2-alpine-trimmed.simg'))
        self.everyone_permissions = [[], ["everyone"]]
        self.form_data = {
            "parent": None,
            "tag": "v0.1",
            "description": "Testing ContainerForm",
            "permissions": self.everyone_permissions
        }

        hello_world_script = u"""\
#! /bin/bash
echo Hello World
"""
        pipeline_dict = {
            "steps": [
                {
                    "driver": "hello_world.sh"
                }
            ]
        }
        _, self.zip_archive = mkstemp()
        with ZipFile(self.zip_archive, mode="w") as z:
            z.writestr("hello_world.sh", hello_world_script)
            z.writestr("kive/pipeline1.json", json.dumps(pipeline_dict))
        self.zip_size = os.path.getsize(self.zip_archive)

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
        form = ContainerForm(self.form_data, files={"file": uploaded_file})
        self.assertFalse(form.is_valid())
        self.assertTrue(form.has_error(NON_FIELD_ERRORS, code="invalid_singularity_container"))

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

        form = ContainerForm(self.form_data, files={"file": uploaded_file})
        self.assertTrue(form.is_valid())
        self.assertTrue(form.instance.singularity_validated)

    def test_uploaded_empty(self):
        empty_file = BytesIO(b"")
        form = ContainerForm(self.form_data, files={"file": empty_file})
        self.assertFalse(form.is_valid())
        self.assertTrue(form.has_error(NON_FIELD_ERRORS, code="invalid_archive"))

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

        form = ContainerForm(self.form_data, files={"file": uploaded_file})
        self.assertFalse(form.is_valid())
        self.assertTrue(form.has_error(NON_FIELD_ERRORS, code="invalid_singularity_container"))

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

            form = ContainerForm(self.form_data, files={"file": uploaded_file})
            self.assertTrue(form.is_valid())
            self.assertTrue(form.instance.singularity_validated)

    def test_bad_file_extension(self):
        with open(self.alpine_path, 'rb') as alpine_file:
            uploaded_file = TemporaryUploadedFile(
                name='example.simglolwut',
                content_type='application/octet-stream',
                size=15,
                charset=None,
                content_type_extra={})
            # noinspection PyArgumentList
            uploaded_file.file.close()
            uploaded_file.file = alpine_file

            form = ContainerForm(self.form_data, files={"file": uploaded_file})
            self.assertFalse(form.is_valid())
            self.assertTrue(form.has_error(NON_FIELD_ERRORS, code="bad_extension"))

    def test_upload_archive_container(self):
        """
        Archive containers should not call any validation nor flag that the container has been validated.
        :return:
        """
        my_user = User(pk=1000)
        my_user.save()

        family = ContainerFamily(name="Dummy family", description="placeholder", user=my_user)
        family.save()
        with open(self.alpine_path, "rb") as f:
            parent = Container(
                id=41,
                family=family,
                file_type=Container.SIMG,
                tag="v0.1",
                description="parent",
                user=my_user
            )
            parent.file.save("alpine.simg", File(f), save=True)

        with open(self.zip_archive, 'rb') as z:
            uploaded_file = TemporaryUploadedFile(
                name='example.zip',
                content_type='application/octet-stream',
                size=self.zip_size,
                charset=None,
                content_type_extra={})
            # noinspection PyArgumentList
            uploaded_file.file.close()
            uploaded_file.file = z

            self.form_data["parent"] = parent.pk
            form = ContainerForm(self.form_data, files={"file": uploaded_file})
            is_valid = form.is_valid()
            self.assertTrue(is_valid)
            self.assertFalse(hasattr(form.instance, "singularity_validated"))


class PipelineCompletionStatusTests(TestCase):
    """
    Tests that a pipeline is correctly marked as complete or not complete.
    """
    def test_empty_pipeline(self):
        empty_pipeline = {
            "inputs": [],
            "steps": [],
            "outputs": []
        }
        pcs = PipelineCompletionStatus(empty_pipeline)
        self.assertFalse(pcs.has_inputs)
        self.assertFalse(pcs.has_steps)
        self.assertFalse(pcs.has_outputs)
        self.assertEqual(0, len(pcs.inputs_not_connected))
        self.assertEqual(0, len(pcs.dangling_outputs))

    def test_only_inputs(self):
        pipeline_only_inputs = {
            "inputs": [
                {
                    "dataset_name": "input1",
                    "x": 0.2,
                    "y": 0.2
                }
            ],
            "steps": [],
            "outputs": []
        }
        pcs = PipelineCompletionStatus(pipeline_only_inputs)
        self.assertTrue(pcs.has_inputs)
        self.assertFalse(pcs.has_steps)
        self.assertFalse(pcs.has_outputs)
        self.assertEqual(0, len(pcs.inputs_not_connected))
        self.assertEqual(0, len(pcs.dangling_outputs))

    def test_only_steps(self):
        pipeline_only_steps = {
            "inputs": [],
            "steps": [
                {
                    "driver": "filter_quality.sh",
                    "inputs": [
                        {
                            "dataset_name": "quality_csv",
                            "source_step": None,
                            "source_dataset_name": None
                        }
                    ],
                    "outputs": ["bad_cycles_csv"]
                }
            ],
            "outputs": []
        }
        pcs = PipelineCompletionStatus(pipeline_only_steps)
        self.assertFalse(pcs.has_inputs)
        self.assertTrue(pcs.has_steps)
        self.assertFalse(pcs.has_outputs)
        self.assertListEqual(
            pcs.inputs_not_connected,
            [(1, "quality_csv")]
        )
        self.assertEqual(0, len(pcs.dangling_outputs))

    def test_only_outputs(self):
        pipeline_only_outputs = {
            "inputs": [],
            "steps": [],
            "outputs": [
                {
                    "dataset_name": "bad_cycles_csv",
                    "source_step": None,
                    "source_dataset_name": None,
                    "x": 0.8,
                    "y": 0.8
                }
            ]
        }
        pcs = PipelineCompletionStatus(pipeline_only_outputs)
        self.assertFalse(pcs.has_inputs)
        self.assertFalse(pcs.has_steps)
        self.assertTrue(pcs.has_outputs)
        self.assertEqual(len(pcs.inputs_not_connected), 0)
        self.assertListEqual(pcs.dangling_outputs, ["bad_cycles_csv"])

    def test_no_connections(self):
        pipeline_unconnected = {
            "inputs": [
                {
                    "dataset_name": "quality_csv",
                    "x": 0.426540479529696,
                    "y": 0.345062429057889
                }
            ],
            "steps": [
                {
                    "driver": "filter_quality.sh",
                    "inputs": [
                        {
                            "dataset_name": "quality_csv",
                            "source_step": None,
                            "source_dataset_name": None
                        }
                    ],
                    "outputs": ["bad_cycles_csv"],
                    "x": 0.501879443635952,
                    "y": 0.497715260532689,
                    "fill_colour": ""
                }
            ],
            "outputs": [
                {
                    "dataset_name": "bad_cycles_csv",
                    "source_step": None,
                    "source_dataset_name": None,
                    "x": 0.588014776534994,
                    "y": 0.640181611804767
                }
            ]
        }
        pcs = PipelineCompletionStatus(pipeline_unconnected)
        self.assertTrue(pcs.has_inputs)
        self.assertTrue(pcs.has_steps)
        self.assertTrue(pcs.has_outputs)
        self.assertListEqual(
            pcs.inputs_not_connected,
            [(1, "quality_csv")]
        )
        self.assertListEqual(pcs.dangling_outputs, ["bad_cycles_csv"])

    def test_dangling_output(self):
        pipeline_unconnected = {
            "inputs": [
                {
                    "dataset_name": "quality_csv",
                    "x": 0.426540479529696,
                    "y": 0.345062429057889
                }
            ],
            "steps": [
                {
                    "driver": "filter_quality.sh",
                    "inputs": [
                        {
                            "dataset_name": "quality_csv",
                            "source_step": 0,
                            "source_dataset_name": "quality_csv"
                        }
                    ],
                    "outputs": ["bad_cycles_csv"],
                    "x": 0.501879443635952,
                    "y": 0.497715260532689,
                    "fill_colour": ""
                }
            ],
            "outputs": [
                {
                    "dataset_name": "bad_cycles_csv",
                    "source_step": None,
                    "source_dataset_name": None,
                    "x": 0.588014776534994,
                    "y": 0.640181611804767
                }
            ]
        }
        pcs = PipelineCompletionStatus(pipeline_unconnected)
        self.assertTrue(pcs.has_inputs)
        self.assertTrue(pcs.has_steps)
        self.assertTrue(pcs.has_outputs)
        self.assertListEqual(pcs.inputs_not_connected, [])
        self.assertListEqual(pcs.dangling_outputs, ["bad_cycles_csv"])

    def test_unfed_input(self):
        pipeline_unconnected = {
            "inputs": [
                {
                    "dataset_name": "quality_csv",
                    "x": 0.426540479529696,
                    "y": 0.345062429057889
                }
            ],
            "steps": [
                {
                    "driver": "filter_quality.sh",
                    "inputs": [
                        {
                            "dataset_name": "quality_csv",
                            "source_step": None,
                            "source_dataset_name": None
                        }
                    ],
                    "outputs": ["bad_cycles_csv"],
                    "x": 0.501879443635952,
                    "y": 0.497715260532689,
                    "fill_colour": ""
                }
            ],
            "outputs": [
                {
                    "dataset_name": "bad_cycles_csv",
                    "source_step": 1,
                    "source_dataset_name": "bad_cycles_csv",
                    "x": 0.588014776534994,
                    "y": 0.640181611804767
                }
            ]
        }
        pcs = PipelineCompletionStatus(pipeline_unconnected)
        self.assertTrue(pcs.has_inputs)
        self.assertTrue(pcs.has_steps)
        self.assertTrue(pcs.has_outputs)
        self.assertListEqual(pcs.inputs_not_connected, [(1, "quality_csv")])
        self.assertListEqual(pcs.dangling_outputs, [])

    def test_good_pipeline(self):
        good_pipeline = {
            "inputs": [
                {
                    "dataset_name": "quality_csv",
                    "x": 0.426540479529696,
                    "y": 0.345062429057889
                }
            ],
            "steps": [
                {
                    "driver": "filter_quality.sh",
                    "inputs": [
                        {
                            "dataset_name": "quality_csv",
                            "source_step": 0,
                            "source_dataset_name": "quality_csv"
                        }
                    ],
                    "outputs": ["bad_cycles_csv"],
                    "x": 0.501879443635952,
                    "y": 0.497715260532689,
                    "fill_colour": ""
                }
            ],
            "outputs": [
                {
                    "dataset_name": "bad_cycles_csv",
                    "source_step": 1,
                    "source_dataset_name": "bad_cycles_csv",
                    "x": 0.588014776534994,
                    "y": 0.640181611804767
                }
            ]
        }
        pcs = PipelineCompletionStatus(good_pipeline)
        self.assertTrue(pcs.has_inputs)
        self.assertTrue(pcs.has_steps)
        self.assertTrue(pcs.has_outputs)
        self.assertListEqual(pcs.inputs_not_connected, [])
        self.assertListEqual(pcs.dangling_outputs, [])


@skipIfDBFeature('is_mocked')
class ContainerFamilyApiTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        super(ContainerFamilyApiTests, self).setUp()
        user = User.objects.first()
        family = ContainerFamily.objects.create(user=user)

        self.detail_pk = family.pk

        self.list_path = reverse("container-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_path = reverse("containerfamily-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("containerfamily-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        self.family_path = reverse("containerfamily-detail",
                                   kwargs={'pk': family.pk})

        self.image_path = os.path.abspath(os.path.join(
            __file__,
            '..',
            '..',
            '..',
            'samplecode',
            'singularity',
            'python2-alpine-trimmed.simg'))
        self.assertTrue(os.path.exists(self.image_path), self.image_path)

    def test_removal(self):
        request = self.factory.delete(self.family_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)


class ContainerArgumentTests(TestCase):

    def test_required_poly_input_is_forbidden(self):
        with self.assertRaises(ValidationError):
            arg = ContainerArgument(
                position=0, type=ContainerArgument.INPUT, allow_multiple=True,
            )
            arg.clean()

    def test_argument_classification(self):
        specs = [
            (
                ContainerArgumentType.FIXED_INPUT,
                {
                    "position": 0,
                    "type": ContainerArgument.INPUT,
                    "allow_multiple": False,
                },
            ),
            (
                ContainerArgumentType.FIXED_OUTPUT,
                {
                    "position": 0,
                    "type": ContainerArgument.OUTPUT,
                    "allow_multiple": False,
                },
            ),
            (
                ContainerArgumentType.OPTIONAL_INPUT,
                {
                    "position": None,
                    "type": ContainerArgument.INPUT,
                    "allow_multiple": False,
                },
            ),

            (
                ContainerArgumentType.OPTIONAL_MULTIPLE_INPUT,
                {
                    "position": None,
                    "type": ContainerArgument.INPUT,
                    "allow_multiple": True,
                },
            ),

            (
                ContainerArgumentType.FIXED_DIRECTORY_OUTPUT,
                {
                    "position": 0,
                    "type": ContainerArgument.OUTPUT,
                    "allow_multiple": True,
                },
            ),
        ]

        for expected_type, kwargs in specs:
            arg = ContainerArgument(name="test_arg", **kwargs)
            self.assertEqual(expected_type, arg.argtype)
