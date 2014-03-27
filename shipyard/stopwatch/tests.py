"""
Tests on the Stopwatch functionality used in Run, RunAtomic, etc.
"""

from django.test import TestCase
import archive.models
import datachecking.models
import archive.tests

# Create your tests here.
class StopwatchCleanTests(archive.tests.ArchiveTestSetup):

    # Note that ArchiveTestSetup creates self.pE_run, which is a
    # Stopwatch, in its setUp.  We'll use this as our Stopwatch.

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