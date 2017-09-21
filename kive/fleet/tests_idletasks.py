#!/usr/bin/env python

import errno
import inspect
import time
import os.path
import unittest
import shutil

from django.conf import settings
from django.test import TestCase, skipIfDBFeature

import fleet.slurmlib as slurmlib
from datetime import date, timedelta

from fleet.workers import Manager
from librarian.models import Dataset
from archive.models import MethodOutput


@skipIfDBFeature('is_mocked')
class IdleTaskTests(TestCase):
    def setUp(self):
        self.man = Manager(quit_idle=False, history=0,
                           slurm_sched_class=slurmlib.DummySlurmScheduler)

    def tearDown(self):
        self.man.slurm_sched_class.shutdown()

    def test_manager_ok(self):
        """ Make sure we have  a manager class """
        self.assertIsNotNone(self.man)

    def test_add_idletask01(self):
        """Adding a non-generator should raise and exception"""
        def test_func(myargs):
            return myargs+1000.0

        with self.assertRaises(RuntimeError):
            self.man._add_idletask(test_func)

    def test_add_idletask02(self):
        """Adding a generator should work."""
        def test_generator(myargs):
            while True:
                bla = (yield myargs)
                bla += 1

        gen = test_generator(100)
        # just make sure our test is valid
        self.assertTrue(inspect.isgenerator(gen), "The test is broken: test_gen is not a generator")
        self.man._add_idletask(gen)

    def do_idle_tasks_test(self, lst, time_limit):
        """Add three generators and call do_idle_tasks.
        The generators modify lst if they are called."""
        def gen1(target):
            while True:
                (yield None)
                target.append(1)

        def gen2(target):
            while True:
                (yield None)
                target.append(2)

        def gen3(target):
            while True:
                (yield None)
                target.append(3)
        self.man._add_idletask(gen1(lst))
        self.man._add_idletask(gen2(lst))
        self.man._add_idletask(gen3(lst))
        self.man._do_idle_tasks(time_limit)

    def test_add_do_idle_tasks01(self):
        """Add three generators. Calling do_idle_tasks with a big time_limit
        should result in them being called all exactly once."""
        lst, time_limit = [], time.time() + 1000.0
        self.do_idle_tasks_test(lst, time_limit)
        self.assertTrue(len(lst) == 3, "unexpected lst length")
        self.assertTrue(set(lst) == {1, 2, 3}, "unexpected set")

    def test_add_do_idle_tasks02(self):
        """Add three generators. Calling do_idle_tasks with a negative time_limit
        should result in them being called all exactly never."""
        lst, time_limit = [], time.time() - 1000.0
        self.do_idle_tasks_test(lst, time_limit)
        self.assertTrue(len(lst) == 0, "unexpected lst length")
        self.assertTrue(set(lst) == set(), "unexpected set")

    def test_add_do_idle_tasks03(self):
        """ Add four time-delayed generators. Waiting a specific time should
        result in some of them being called and others not.
        """
        def sleep_generator(target, task_id, secs_to_sleep):
            while True:
                (yield None)
                target.append(task_id)
                time.sleep(secs_to_sleep)

        wait_secs = 1.0
        lst = []
        self.man._add_idletask(sleep_generator(lst, 1, wait_secs))
        self.man._add_idletask(sleep_generator(lst, 2, wait_secs))
        self.man._add_idletask(sleep_generator(lst, 3, wait_secs))
        self.man._add_idletask(sleep_generator(lst, 4, wait_secs))
        time_limit = time.time() + 1.5*wait_secs
        self.man._do_idle_tasks(time_limit)
        self.assertTrue(len(lst) == 2, "unexpected lst length")
        # NOTE: the order of the idle_tasks is not defined by the interface
        # However, in fact the queue is rotated to the right...
        self.assertTrue(set(lst) == {1, 4}, "unexpected set")

    def test_create_next_month_upload_dir01(self):
        """ Test the creation of a monthly directory when the
        Dataset dir is not present.
        """
        dataset_dir = os.path.join(settings.MEDIA_ROOT, Dataset.UPLOAD_DIR)
        date_str = (date.today() + timedelta(days=30)).strftime('%Y_%m')
        next_dirname = os.path.join(dataset_dir, date_str)
        # delete the dir iff it exists.
        try:
            shutil.rmtree(dataset_dir)
        except os.error as e:
            if e.errno != errno.ENOENT:
                raise
        gg = Dataset.idle_create_next_month_upload_dir()
        self.man._add_idletask(gg)
        time_limit = time.time() + 1000.0
        self.man._do_idle_tasks(time_limit)
        self.assertTrue(os.path.exists(next_dirname), "directory was not made")

    def test_create_next_month_upload_dir02(self):
        """ Test the creation of a monthly directory where Dataset may be present."""
        dataset_dir = os.path.join(settings.MEDIA_ROOT, Dataset.UPLOAD_DIR)
        date_str = (date.today() + timedelta(days=30)).strftime('%Y_%m')
        next_dirname = os.path.join(dataset_dir, date_str)
        # delete the dir iff it exists.
        try:
            shutil.rmtree(next_dirname)
        except os.error as e:
            if e.errno != errno.ENOENT:
                raise
        gg = Dataset.idle_create_next_month_upload_dir()
        self.man._add_idletask(gg)
        time_limit = time.time() + 1000.0
        self.man._do_idle_tasks(time_limit)
        self.assertTrue(os.path.exists(next_dirname), "directory was not made")

    def test_create_next_month_upload_dir03(self):
        """ Test the creation of a monthly dir, where the dir is already present."""
        dataset_dir = os.path.join(settings.MEDIA_ROOT, Dataset.UPLOAD_DIR)
        date_str = (date.today() + timedelta(days=30)).strftime('%Y_%m')
        next_dirname = os.path.join(dataset_dir, date_str)
        # make the directory iff it doesn't exist
        if not os.path.exists(next_dirname):
            os.makedirs(next_dirname)
        gg = Dataset.idle_create_next_month_upload_dir()
        self.man._add_idletask(gg)
        time_limit = time.time() + 1000.0
        self.man._do_idle_tasks(time_limit)
        self.assertTrue(os.path.exists(next_dirname), "directory was not made")

    def test_dataset_purge01(self):
        max_storage = 1000
        target_size = 5000
        # dataset_dir = os.path.join(settings.MEDIA_ROOT, Dataset.UPLOAD_DIR)

        gg = Dataset.idle_dataset_purge(max_storage=max_storage, target_size=target_size)
        self.man._add_idletask(gg)
        for i in xrange(10):
            # print "TEST", i
            time_limit = time.time() + 10.0
            self.man._do_idle_tasks(time_limit)

    def test_external_file_check01(self):
        # dataset_dir = os.path.join(settings.MEDIA_ROOT, Dataset.UPLOAD_DIR)

        gg = Dataset.idle_external_file_check()
        self.man._add_idletask(gg)
        for i in xrange(10):
            # print "TEST", i
            time_limit = time.time() + 10.0
            self.man._do_idle_tasks(time_limit)

    def test_dataset_purge02(self):
        # dataset_dir = os.path.join(settings.MEDIA_ROOT, Dataset.UPLOAD_DIR)

        gg = Dataset.idle_dataset_purge()
        self.man._add_idletask(gg)
        for i in xrange(10):
            # print "TEST", i
            time_limit = time.time() + 10.0
            self.man._do_idle_tasks(time_limit)

    def test_logfile_purge01(self):
        # dataset_dir = os.path.join(settings.MEDIA_ROOT, Dataset.UPLOAD_DIR)
        gg = MethodOutput.idle_logfile_purge()
        self.man._add_idletask(gg)
        for i in xrange(10):
            # print "TEST", i
            time_limit = time.time() + 10.0
            self.man._do_idle_tasks(time_limit)


if __name__ == "__main__":
    unittest.main()
