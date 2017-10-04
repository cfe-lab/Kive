#!/usr/bin/env python

# some simple  tests for the dockerlib module
from unittest import skipIf

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
        with self.assertRaises(RuntimeError):
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
