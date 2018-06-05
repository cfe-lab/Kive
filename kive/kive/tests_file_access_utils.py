
import os
import os.path
from django.test import TestCase

import tempfile
import shutil
import file_access_utils as utils


def writebinfile(fname, cont_bytes):
    with open(fname, 'wb') as fo:
        fo.write(cont_bytes)


def readbinfile(fname):
    with open(fname, 'rb') as fi:
        cont_bytes = fi.read()
    return cont_bytes


# COPY_FILE = shutil.copyfile
# COPY_FILE = utils.copyfile
COPY_FILE = utils.copy_and_confirm


class FileAccessTests(TestCase):

    def setUp(self):
        self.testdirname = tempfile.mkdtemp()
        self.small_bytes = b"""This is a silly
little
test
"""
        self.test_fname1 = os.path.join(self.testdirname, "testfile1.dat")
        self.test_fname2 = os.path.join(self.testdirname, "testfile2.dat")

    def tearDown(self):
        shutil.rmtree(self.testdirname)

    def test_copyfile01(self):
        "Copying an existing file should succeed"
        cont_bytes = self.small_bytes
        test_fname1 = self.test_fname1
        test_fname2 = self.test_fname2
        writebinfile(test_fname1, cont_bytes)

        cp_name = COPY_FILE(test_fname1, test_fname2)

        self.assertEqual(cp_name, test_fname2)
        got_bytes = readbinfile(test_fname2)
        self.assertEqual(cont_bytes, got_bytes, "file copy failed")

    def test_same_file(self):
        "Copying the same file to itself should raise an exception"
        cont_bytes = self.small_bytes
        test_fname1 = self.test_fname1
        writebinfile(test_fname1, cont_bytes)
        with self.assertRaises(utils.SameFileError):
            COPY_FILE(test_fname1, test_fname1)

    def test_nonexistent_file(self):
        "Attempting to copy a non-existent file should raise an exception"
        test_fname1 = self.test_fname1
        test_fname2 = self.test_fname2
        with self.assertRaises(IOError):
            COPY_FILE(test_fname1, test_fname2)

    def test_copyfifo(self):
        "Attempting to copy from a FIFO should raise an exception"
        test_fname1 = self.test_fname1
        test_fname2 = self.test_fname2
        os.mkfifo(test_fname1, 0x644)
        with self.assertRaises(shutil.SpecialFileError):
            COPY_FILE(test_fname1, test_fname2)
