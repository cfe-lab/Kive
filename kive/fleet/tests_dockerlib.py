#!/usr/bin/env python

# some simple  tests for the dockerlib module
from unittest import skipIf

import os.path as osp
import subprocess as sp

import fleet.dockerlib as dockerlib
from django.conf import settings

from django.test import TestCase

# NOTE: Here, select which DockerHandler to test.
# we select the DummyDockerHandler by default, so that the automatic tests
# can run without docker

DockerHandler = dockerlib.DockerHandler
# DockerHandler = dockerlib.DummyDockerHandler

TEST_DIR = osp.join(settings.KIVE_HOME, "fleet/dockerlib_test_files")


class DockerLibTests(TestCase):
    def setUp(self):
        # out_str = DockerHandler._run_shell_command(['/usr/bin/docker', '-v'])
        # print("HELLO HOWDY DO {}".format(out_str))
        is_alive = DockerHandler.docker_is_alive()
        if not is_alive:
            raise RuntimeError("docker is not alive")

    def tearDown(self):
        pass

    def test_ident(self):
        """Test the docker_ident call"""
        idstr = DockerHandler.docker_ident()
        # print("Docker idents as:\n{}".format(idstr))
        assert isinstance(idstr, str)

    @skipIf(not settings.RUN_DOCKER_TESTS, "Docker tests are disabled")
    def test_docker_images01(self):
        """Sanity check the docker_images output."""
        out_lst = DockerHandler.docker_images()
        assert len(out_lst) > 0, "got zero docker images, cannot perform test"
        expected_set = DockerHandler.DOCKER_IMG_SET
        for dct in out_lst:
            assert expected_set == set(dct.keys()), "mangled image info dct keys {}".format(dct.keys())

    @skipIf(not settings.RUN_DOCKER_TESTS, "Docker tests are disabled")
    def test_docker_images02(self):
        """Search for a nonexistent docker image"""
        out_lst = DockerHandler.docker_images("BLAA")
        assert len(out_lst) == 0, "got nonzero docker images!"

    @skipIf(not settings.RUN_DOCKER_TESTS, "Docker tests are disabled")
    def test_load_image01(self):
        """Load a docker image from file"""
        image_name = osp.join(TEST_DIR, "small-alpine.bz2")
        exp_dct = {'REPOSITORY:TAG': 'alpine:latest',
                   'CREATED AT': '2017-06-27 11:42:16 -0700 PDT',
                   'IMAGE ID': '7328f6f8b418',
                   'SIZE': '3.97MB',
                   'DIGEST': '<none>'}
        img_info_dct = DockerHandler._load_image_from_file(image_name)
        # NOTE: we do not check the correctness of the CREATED_SINCE entry (the value
        # will obviously depend on when the test is run). Instead, we simply delete it
        # from the returned dict.
        del img_info_dct[DockerHandler.DOCKER_IMG_CREATED_SINCE]
        assert exp_dct == img_info_dct, "unexpected info dict {}".format(img_info_dct)

    @skipIf(not settings.RUN_DOCKER_TESTS, "Docker tests are disabled")
    def test_load_image02(self):
        """Loading a nonexistent docker image should raise an exception"""
        with self.assertRaises(RuntimeError):
            DockerHandler._load_image_from_file("NONEXISTENT-IMAGE")

    def test_load_ps01(self):
        """An non-integer nlast argument should raise an exception"""
        with self.assertRaises(RuntimeError):
            DockerHandler.docker_ps(nlast="bla")

    @skipIf(not settings.RUN_DOCKER_TESTS, "Docker tests are disabled")
    def test_load_ps02(self):
        """Sanity check the returned dicts from docker_ps"""
        res_lst = DockerHandler.docker_ps(nlast=10)
        assert len(res_lst) > 0, "cannot test with a nonzero length list"
        expected_set = DockerHandler.DOCKER_PS_SET
        for ps_info_dct in res_lst:
            got_set = set(ps_info_dct.keys())
            assert expected_set == got_set,\
                "mangled set keys {}\n missing: {}\nunrecognised : {}".format(got_set,
                                                                              expected_set - got_set,
                                                                              got_set - expected_set)

    @skipIf(not settings.RUN_DOCKER_TESTS, "Docker tests are disabled")
    def test_load_ps03(self):
        """Test docker_ps with a valid filter string"""
        DockerHandler.docker_ps(filter_str="status=running")

    @skipIf(not settings.RUN_DOCKER_TESTS, "Docker tests are disabled")
    def test_load_ps04(self):
        """Test docker_ps with an invalid filter string"""
        with self.assertRaises(sp.CalledProcessError):
            DockerHandler.docker_ps(filter_str="bla")
