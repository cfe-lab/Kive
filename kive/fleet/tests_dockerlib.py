#!/usr/bin/env python

# some simple  tests for the dockerlib module
from unittest import skipIf, skip

import os.path as osp
import subprocess as sp

import fleet.dockerlib as dockerlib
from django.conf import settings

import tempfile

from django.test import TestCase
import six

TEST_DIR = osp.join(settings.KIVE_HOME, "fleet/dockerlib_test_files")


class DummyDockerLibTests(TestCase):

    def get_docker_handler_class(self):
        return dockerlib.DummyDockerHandler

    def setUp(self):
        self.addTypeEqualityFunc(str, self.assertMultiLineEqual)
        self.docker_handler_class = self.get_docker_handler_class()
        is_docker_alive = self.docker_handler_class.docker_is_alive()
        self.assertTrue(is_docker_alive)
        print('SEEETUP')

    def test_ident(self):
        """Test the docker_ident call"""
        idstr = self.docker_handler_class.docker_ident()
        # print("Docker idents as:\n{}".format(idstr))
        assert isinstance(idstr, six.string_types)


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
        img_info_dct = self.docker_handler_class._load_image_from_file(image_name)
        # NOTE: we do not check the correctness of the CREATED_SINCE entry (the value
        # will obviously depend on when the test is run). Instead, we simply delete it
        # from the returned dict.
        del img_info_dct[self.docker_handler_class.DOCKER_IMG_CREATED_SINCE]
        assert exp_dct == img_info_dct, "unexpected info dict {}".format(img_info_dct)

    def test_load_image02(self):
        """Loading a nonexistent docker image should raise an exception"""
        with self.assertRaises(sp.CalledProcessError):
            self.docker_handler_class._load_image_from_file("NONEXISTENT-IMAGE")

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

        output = self.docker_handler_class._run_twopipe_command(
            ['echo', 'Alpha\nBeta\nDelta'],
            ['grep', 'B', '-'])
        assert isinstance(output, six.string_types)
        self.assertEqual(expected_output, output)

    def test_two_pipe_command_first_not_found(self):
        expected_error2 = """\
[Errno 2] No such file or directory: echoxxx Alpha
Beta
Delta | grep B -"""
        expected_error3 = """\
[Errno 2] No such file or directory: 'echoxxx': echoxxx Alpha
Beta
Delta | grep B -"""

        with self.assertRaises(OSError) as context:
            self.docker_handler_class._run_twopipe_command(
                ['echoxxx', 'Alpha\nBeta\nDelta'],
                ['grep', 'B', '-'])
        got_err = str(context.exception)
        if got_err != expected_error2 and got_err != expected_error3:
            raise RuntimeError("unexpected error message '{}' '{} '{}'".format(got_err,
                                                                               expected_error2,
                                                                               expected_error3))
        # self.assertEqual(expected_error, ))

    def test_two_pipe_command_second_not_found(self):
        expected_error2 = """\
[Errno 2] No such file or directory: echo Alpha
Beta
Delta | grepxxx B -"""
        expected_error3 = """\
[Errno 2] No such file or directory: 'grepxxx': echo Alpha
Beta
Delta | grepxxx B -"""

        with self.assertRaises(OSError) as context:
            self.docker_handler_class._run_twopipe_command(
                ['echo', 'Alpha\nBeta\nDelta'],
                ['grepxxx', 'B', '-'])
        got_err = str(context.exception)
        if got_err != expected_error2 and got_err != expected_error3:
            raise RuntimeError("unexpected error message '{}' '{} '{}'".format(got_err,
                                                                               expected_error2,
                                                                               expected_error3))
        # self.assertEqual(expected_error, )

    def test_two_pipe_command_second_fails(self):
        expected_error = (
            "Command '['grep', '-X', 'B', '-']' returned non-zero exit status 2")
        expected_output = ""

        with self.assertRaises(sp.CalledProcessError) as context:
            self.docker_handler_class._run_twopipe_command(
                ['echo', 'Alpha\nBeta\nDelta'],
                ['grep', '-X', 'B', '-'])

        self.assertEqual(expected_error, str(context.exception))
        self.assertEqual(expected_output, context.exception.output)

    def test_two_pipe_command_first_fails(self):
        """ Failure in first process causes failure in second. """
        expected_error = (
            "Command '['grep', 'B', '-']' returned non-zero exit status 1")
        expected_output = ""

        with self.assertRaises(sp.CalledProcessError) as context:
            self.docker_handler_class._run_twopipe_command(
                ['bzip2', 'unknown_file.bz2'],
                ['grep', 'B', '-'])

        self.assertEqual(expected_error, str(context.exception))
        self.assertEqual(expected_output, context.exception.output)

    def test_gen_launch_cmd01(self):
        """ Check sanity of generated a launch command """
        hoststepdir = tempfile.gettempdir()
        input_file_paths = ["input1.dat", "input2.dat"]
        output_file_paths = ["output1.dat", "output2.dat", "output3.dat"]
        dep_paths = ["dep01.py", "dep02.py"]
        image_id = "kive-default"
        for driver_name in [None, "my_driver_prog.py"]:
            try:
                retlst = self.docker_handler_class.generate_launch_args(hoststepdir,
                                                                        input_file_paths,
                                                                        output_file_paths,
                                                                        driver_name,
                                                                        dep_paths,
                                                                        image_id)
                assert isinstance(retlst, list), 'expected a list'
                for s in retlst:
                    assert isinstance(s, six.string_types), 'expected a string'
                lverb = True
                if lverb:
                    print("got launch {}".format(retlst))
            except NotImplementedError:
                pass


@skipIf(not settings.RUN_SINGULARITY_TESTS, "Singularity tests are disabled")
class SingularityDockerLibTests(DockerLibTests):

    def get_docker_handler_class(self):
        return dockerlib.SingularityDockerHandler

    def placeholder_test_one(self):
        # assert False, "force fail"
        pass
