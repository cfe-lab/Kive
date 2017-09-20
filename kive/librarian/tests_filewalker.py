#!/usr/bin/env python


# import pytest cannot use this -- sniff
import unittest

import os
import os.path as osp
import random
import tempfile
import shutil

import datetime as dt
import time
import logging

from filewalker import FilePurger, iter_walk

from django.conf import settings
from django.test import TestCase
# import kive.testing_utils as tools


# NOTE: this directory will be created before and also deleted after all tests
# see setUpClass and tearDownClass below
TEST_DIR = osp.join(settings.KIVE_HOME, "librarian/filewalker-tests")

BIG_testdir = osp.join(TEST_DIR, "bigtestdir")


LOGGER = logging.getLogger()

# utility routines for creating test files


def delete_test_dirs():
    shutil.rmtree(TEST_DIR)


def setup_test_dirs():
    # first delete all test dirs if they exist.
    try:
        delete_test_dirs()
    except os.error as e:
        # we don't mind if the dir doesn't exist
        if e.errno == 2:
            pass
        else:
            raise
    # then make the test subdirs.....
    try:
        os.makedirs(BIG_testdir)
    except os.error as e:
        print "making bigtest failed", e
        raise


def create_file(name, size, atime):
    """Create a file of given name, of size in bytes and access time atime."""
    with open(name, "wb") as fo, open("/dev/zero", "r") as fi:
        ntodo = size
        while ntodo > 0:
            nwrite = min(ntodo, 2000)
            fo.write(fi.read(nwrite))
            ntodo -= nwrite
    os.utime(name, (atime, atime))


def create_randomfile(dirname, size, atime):
    """Create a file of a random name in the directory dirname,
    of size in bytes and access time atime.
    Return name, size and atime of the file created.
    """
    with tempfile.NamedTemporaryFile(mode="wb", dir=dirname, suffix=".dat",
                                     delete=False) as fo, open("/dev/zero", "r") as fi:
        name = fo.name
        ntodo = size
        while ntodo > 0:
            nwrite = min(ntodo, 2000)
            fo.write(fi.read(nwrite))
            ntodo -= nwrite
    os.utime(name, (atime, atime))
    return name, size, atime


def create_Gaussfiles(dirname, N, sizemu, sizesigma, timemu, timesigma, lverb=False):
    """ Create N files with random names in directory dirname.
    The size and time of the files follow a Normal distribution with the parameters
    provided.
    A list of tuples (filename, size, atime) is returned.
    """
    if lverb:
        print "Creating %d random files " % N
        print "In directory '%s'" % dirname
        print "Average size: %d" % sizemu
        print "Average time: ", dt.datetime.fromtimestamp(timemu)
        print
    return [create_randomfile(dirname,
                              max(int(random.gauss(sizemu, sizesigma)), 0),
                              random.gauss(timemu, timesigma)) for n in xrange(N)]


def gen_BIGTEST():
    t = time.time()
    HR_FAC = 3600.0
    DAY_FAC = 24*HR_FAC
    WEEK_FAC = 7.0 * DAY_FAC
    num_files = 5000
    return create_Gaussfiles(BIG_testdir, num_files,
                             4000, 200, t-4.0*WEEK_FAC, WEEK_FAC)


def get_datetime(fn):
    return dt.datetime.fromtimestamp(os.path.getatime(fn))


def get_size(fn):
    return os.path.getsize(fn)


def touch(fn):
    """Simulate a 'touch filename'"""
    with open(fn, "r") as fi:
        fi.read(1)


def printfilelst(lst):
    for fn, fsz in lst:
        # print "file: %20s: %6d: '%s'" % (get_datetime(fn), get_size(fn), fn)
        print "file: %20s: %6d: '%s'" % (get_datetime(fn), fsz, fn)


class FileWalkerTests(TestCase):

    @classmethod
    def setUpClass(cls):
        setup_test_dirs()
        cls.bigtest_lst = gen_BIGTEST()
        cls.bigtest_nameset = frozenset([fn for fn, sz, t in cls.bigtest_lst])
        # cls.smalltest_lst = gen_SMALLTEST()

    @classmethod
    def tearDownClass(cls):
        delete_test_dirs()

    # def setUp(self):
    #    self.fp = FilePurger(testdir, 1.0, 1.0, LOGGER)
    # def tearDown(self):
    #    del self.fp

    def _get_big_excset(self, perc):
        """ Choose perc percentage of the big filename set at random.
        """
        nsel = int(perc*len(self.bigtest_lst)/100.0)
        if nsel == 0:
            raise RuntimeError("nsel is zero!")
        return set([fn for fn, sz, t in random.sample(self.bigtest_lst, nsel)])

    def test_initfp(self):
        fp = FilePurger(BIG_testdir, 1.0, 1.0, LOGGER)
        self.assertIsNotNone(fp, "FilePurger creation failed")

    def test_iterwalk01(self, lverb=False):
        """ Test the low-level routine that walks a directory tree."""
        if lverb:
            print "---test_iterwalk01"
        # grace_time_limit = time.time()
        grace_time_limit = None
        iwalk = iter_walk(BIG_testdir, set(), grace_time_limit, LOGGER)
        plst = []
        try:
            while True:
                p = iwalk.next()
                # print "got ", p
                plst.append(p)
        except StopIteration:
            # print "got the stop"
            pass
        # print "ncount , NN", len(plst), len(self.bigtest_lst)
        self.assertEqual(len(plst), len(self.bigtest_lst), "unexpected walk iterations")

        setneed = self.bigtest_nameset
        setgot = set([dir_entry.path for dir_entry in plst])
        self.assertEqual(setneed, setgot, "failed to return expected files")
        if lverb:
            print "---test_iterwalk01 SUCCESS"

    def test_nodir(self):
        """Make sure we handle the purging of a nonexistent dir gracefully."""
        funnydir = osp.join(TEST_DIR, "does_not_exist")
        fp = FilePurger(funnydir, 1.0, 1.0, LOGGER)
        exclude_set = set()
        lst_one = list(fp.next_to_purge(6, exclude_set,
                                        upper_size_limit=0, dodelete=False))
        self.assertEqual(len(lst_one), 0, "unexpected list length")

    def test_too_big_01(self, lverb=False):
        """Call next_to_purge with a number that is too big."""
        if lverb:
            print "---test_purge04"
        fp = FilePurger(BIG_testdir, 1.0, 1.0, LOGGER)
        numtoget = fp.MAX_CACHE + 1
        exclude_set = set()
        with self.assertRaises(RuntimeError):
            fp.next_to_purge(numtoget, exclude_set,
                             upper_size_limit=0,
                             dodelete=True)
        if lverb:
            print "---test_purge04 passed"

    def test_exclude_set(self):
        grace_period_hrs = 0.0
        fp = FilePurger(BIG_testdir, grace_period_hrs, 1.0, LOGGER)
        exclude_set = self._get_big_excset(20.0)
        set_got = set((n for n, sz in fp.next_to_purge(fp.MAX_CACHE, exclude_set,
                                                       upper_size_limit=0,
                                                       dodelete=False)))
        self.assertEqual(exclude_set & set_got, set(), "returned an excluded file!")
        self.assertEqual(len(set_got), fp.MAX_CACHE, "unexpected number of files")

    def test_purge01(self, lverb=False):
        """Test next_to_purge with dodelete=False """
        if lverb:
            print "---test_purge01"
        fp = FilePurger(BIG_testdir, 1.0, 1.0, LOGGER)
        exclude_set = self._get_big_excset(10.0)
        DT_SECS = 1.0
        ff = fp.next_to_purge(fp.MAX_CACHE, exclude_set,
                              upper_size_limit=0,
                              dodelete=False)
        ncount1, ntoo_late, lst1 = 0, 0, []
        try:
            while True:
                ncount1 += 1
                t_stop = time.time() + DT_SECS
                t = ff.send(t_stop)
                t_act = time.time()
                if t is not None:
                    lst1.append(t)
                if t_act-t_stop > 0.0:
                    ntoo_late += 1
        except StopIteration:
            if lverb:
                print "got the stop lst1"
        if lverb:
            print "ncount 01 : %d, ntoo_late: %d " % (ncount1, ntoo_late)
        self.assertEqual(len(lst1), fp.MAX_CACHE, "wrong length of list1")
        # we will let it pass if its late just once...
        self.assertTrue(ntoo_late <= 1, "filepurger returned too late too often")
        ncount2, ntoo_late, lst2 = 0, 0, []
        ff = fp.next_to_purge(fp.MAX_CACHE, exclude_set,
                              upper_size_limit=0,
                              dodelete=False)
        try:
            while True:
                # print "2",
                ncount2 += 1
                t_stop = time.time() + DT_SECS
                t = ff.send(t_stop)
                t_act = time.time()
                if t is not None:
                    lst2.append(t)
                if t_act-t_stop > 0.0:
                    ntoo_late += 1
        except StopIteration:
            if lverb:
                print "got the stop lst2"
        if lverb:
            print "ncount 02 : %d, ntoo_late: %d " % (ncount2, ntoo_late)
        self.assertEqual(len(lst2), fp.MAX_CACHE, "wrong length of list2")
        self.assertTrue(ntoo_late <= 1, "filepurger returned too late too often")
        self.assertEqual(lst1, lst2, "lists are not equal")
        if lverb:
            print "---test_purge01 pass"

    def test_purge02(self, lverb=False):
        """Test next_to_purge with dodelete=True and False.
        """
        if lverb:
            print "---test_purge02"
        fp = FilePurger(BIG_testdir, 1.0, 1.0, LOGGER)
        exclude_set = self._get_big_excset(10.0)
        fp._do_walk(exclude_set)
        assert fp._num_scanned_files > 0, "num scanned files is zero!"
        if lverb:
            print "walk completed, scanned files:", fp._num_scanned_files
        lst_one = [x for x in fp.next_to_purge(6, exclude_set,
                                               upper_size_limit=0, dodelete=False)]
        lsta = [x for x in fp.next_to_purge(3, exclude_set,
                                            upper_size_limit=0, dodelete=True)]
        lstb = [x for x in fp.next_to_purge(3, exclude_set,
                                            upper_size_limit=0, dodelete=True)]
        lst_two = lsta + lstb
        is_equal = (lst_one == lst_two)
        if not is_equal:
            print "test-purge02: lists are not equal"
            print "LEN", len(lst_one), len(lst_two)
            print "LST1", lst_one
            print "LST2", lst_two
            for i, j in zip(lst_one, lst_two):
                print i, j
        self.assertTrue(is_equal, "lists are not equal")
        if lverb:
            print "---test_purge02 pass"

    def test_purge03(self, lverb=False):
        """Test next_to_purge : force a recalculation of the cache.
        """
        if lverb:
            print "---test_purge03 (force a cache recalc)"
        # grace_period_hrs = 1
        # grace_time_limit = datetime.datetime.now() - datetime.timedelta(hours=grace_period_hrs)
        fp = FilePurger(BIG_testdir, 1.0, 1.0, LOGGER)
        exclude_set = self._get_big_excset(10.0)
        fp._do_walk(exclude_set)
        self.assertTrue(fp._num_scanned_files > 0, "num scanned files is zero!")
        if lverb:
            print "walk completed, scanned files:", fp._num_scanned_files
            print "MAX_CACHE", fp.MAX_CACHE
            print "MIN_CACHE", fp.MIN_CACHE
        numtoget = fp.MAX_CACHE - fp.MIN_CACHE + 1
        lst_one = [x for x in fp.next_to_purge(numtoget, exclude_set,
                                               upper_size_limit=0, dodelete=True)]
        if lverb:
            print "got lst1", len(lst_one)
        # NOTE: even though we have set dodelete to true above, we should get
        # the same list again, assuming that the files haven't changed in the interim,
        # because the next call to next_to_purge will rewalk
        # the directory tree. Under 'normal', non-testing conditions, we would
        # have purged the files in lst_one between calls.
        lst_two = [x for x in fp.next_to_purge(numtoget, exclude_set,
                                               upper_size_limit=0, dodelete=True)]
        if lverb:
            print "got lst2", len(lst_two)
        self.assertEqual(lst_one, lst_two, "lists are not equal")
        if lverb:
            print "--test_purge03 pass"

if __name__ == "__main__":
    unittest.main()
