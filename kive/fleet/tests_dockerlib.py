#!/usr/bin/env python

# some simple  tests for the dockerlib module
from unittest import skipIf, skip

import os.path as osp
import subprocess as sp

import fleet.dockerlib as dockerlib
from django.conf import settings

from django.test import TestCase

TEST_DIR = osp.join(settings.KIVE_HOME, "fleet/dockerlib_test_files")


class DummyDockerLibTests(TestCase):
    def get_docker_handler_class(self):
        return dockerlib.DummyDockerHandler

    def setUp(self):
        self.addTypeEqualityFunc(str, self.assertMultiLineEqual)
        self.docker_handler_class = self.get_docker_handler_class()
        is_docker_alive = self.docker_handler_class.docker_is_alive()
        self.assertTrue(is_docker_alive)

    def test_ident(self):
        """Test the docker_ident call"""
        idstr = self.docker_handler_class.docker_ident()
        # print("Docker idents as:\n{}".format(idstr))
        assert isinstance(idstr, str)


@skipIf(not settings.RUN_DOCKER_TESTS, "Docker tests are disabled")
class DockerLibTests(DummyDockerLibTests):
    def get_docker_handler_class(self):
        return dockerlib.DockerHandler

    @skip("This requires sudo permission. Enable test when we allow new images.")
    def test_load_image01(self):
        """Load a docker image from file"""
        image_name = osp.join(TEST_DIR, "small-alpine.bz2")
        exp_dct = {'REPOSITORY:TAG': 'alpine:latest',
                   'CREATED AT': '2017-06-27 11:42:16 -0700 PDT',
                   'IMAGE ID': '7328f6f8b418',
                   'SIZE': '3.97MB',
                   'DIGEST': '<none>'}
        img_info_dct = dockerlib.DockerHandler._load_image_from_file(image_name)
        # NOTE: we do not check the correctness of the CREATED_SINCE entry (the value
        # will obviously depend on when the test is run). Instead, we simply delete it
        # from the returned dict.
        del img_info_dct[self.docker_handler_class.DOCKER_IMG_CREATED_SINCE]
        assert exp_dct == img_info_dct, "unexpected info dict {}".format(img_info_dct)

    def test_load_image02(self):
        """Loading a nonexistent docker image should raise an exception"""
        with self.assertRaises(sp.CalledProcessError):
            dockerlib.DockerHandler._load_image_from_file("NONEXISTENT-IMAGE")

    def test_docker_images01(self):
        """Sanity check the docker_images output."""
        out_lst = self.docker_handler_class.docker_images()
        assert len(out_lst) > 0, "got zero docker images, cannot perform test"
        expected_set = self.docker_handler_class.DOCKER_IMG_SET
        for dct in out_lst:
            assert expected_set == set(dct.keys()), "mangled image info dct keys {}".format(dct.keys())

    def test_docker_images02(self):
        """Search for a nonexistent docker image"""
        out_lst = self.docker_handler_class.docker_images("BLAA")
        assert len(out_lst) == 0, "got nonzero docker images!"

    def test_two_pipe_command(self):
        expected_output = 'Beta\n'

        output = dockerlib.DockerHandler._run_twopipe_command(
            ['echo', 'Alpha\nBeta\nDelta'],
            ['grep', 'B', '-'])

        self.assertEqual(expected_output, output)

    def test_two_pipe_command_first_not_found(self):
        expected_error = """\
[Errno 2] No such file or directory: echoxxx Alpha
Beta
Delta | grep B -"""

        with self.assertRaises(OSError) as context:
            dockerlib.DockerHandler._run_twopipe_command(
                ['echoxxx', 'Alpha\nBeta\nDelta'],
                ['grep', 'B', '-'])

        self.assertEqual(expected_error, str(context.exception))

    def test_two_pipe_command_second_not_found(self):
        expected_error = """\
[Errno 2] No such file or directory: echo Alpha
Beta
Delta | grepxxx B -"""

        with self.assertRaises(OSError) as context:
            dockerlib.DockerHandler._run_twopipe_command(
                ['echo', 'Alpha\nBeta\nDelta'],
                ['grepxxx', 'B', '-'])

        self.assertEqual(expected_error, str(context.exception))

    def test_two_pipe_command_second_fails(self):
        expected_error = (
            "Command '['grep', '-X', 'B', '-']' returned non-zero exit status 2")
        expected_output = ""

        with self.assertRaises(sp.CalledProcessError) as context:
            dockerlib.DockerHandler._run_twopipe_command(
                ['echo', 'Alpha\nBeta\nDelta'],
                ['grep', '-X', 'B', '-'])

        self.assertEqual(expected_error, str(context.exception))
        self.assertEqual(expected_output, context.exception.output)

    def test(self):
        """ Failure in first process causes failure in second. """
        expected_error = (
            "Command '['grep', 'B', '-']' returned non-zero exit status 1")
        expected_output = ""

        with self.assertRaises(sp.CalledProcessError) as context:
            dockerlib.DockerHandler._run_twopipe_command(
                ['bzip2', 'unknown_file.bz2'],
                ['grep', 'B', '-'])

        self.assertEqual(expected_error, str(context.exception))
        self.assertEqual(expected_output, context.exception.output)
