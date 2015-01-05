"""
Tests on the Stopwatch functionality used in Run, RunAtomic, etc.
"""

from django.test import TestCase
import archive.tests
import metadata.tests


# Create your tests here.
class StopwatchCleanTests(TestCase):

    # Note that ArchiveTestCase creates self.pE_run, which is a
    # Stopwatch, in its setUp.  We'll use this as our Stopwatch.
    def setUp(self):
        archive.tests.create_archive_test_environment(self)

    def tearDown(self):
        metadata.tests.clean_up_all_files()

    def test_neither_set(self):
        """
        Neither start nor end time is set.  Stopwatch should be clean.
        """
        self.assertIsNone(self.pE_run.clean())

    def test_start_set_end_not_set(self):
        """
        start_time set, end_time not set.  This is fine.
        """
        self.pE_run.start()
        self.assertIsNone(self.pE_run.clean())