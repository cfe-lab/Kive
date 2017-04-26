from django.contrib.auth.models import User
from django.conf import settings
from django.test import skipIfDBFeature

from unittest import skipIf
import tempfile
import os
import shutil

from fleet.workers import Manager
from kive.tests import BaseTestCases
import kive.testing_utils as tools
from metadata.models import everyone_group
from fleet.slurmlib import SlurmScheduler, DummySlurmScheduler
from sandbox.tests_rm import BadRunTestsBase


def execute_simple_run(environment, slurm_sched_class=DummySlurmScheduler):
    """
    A helper function that creates a simple pipeline and executes a run.

    This also populates the object -- e.g. a TestCase or a FixtureBuilder --
    with some variables.

    Returns the Manager object that executed the run.
    """
    tools.create_eric_martin_test_environment(environment)
    tools.create_sandbox_testing_tools_environment(environment)

    user = User.objects.get(username='john')
    # Everything in this pipeline will be a no-op, so all can be linked together
    # without remorse.
    p_basic = tools.make_first_pipeline("P_basic", "Innermost pipeline", user)
    tools.create_linear_pipeline(p_basic, [environment.method_noop, environment.method_noop], "basic_in", "basic_out")
    p_basic.family.grant_everyone_access()
    p_basic.grant_everyone_access()
    p_basic.create_outputs()
    p_basic.save()

    # Set up a dataset with words in it called environment.dataset_words.
    tools.make_words_dataset(environment)

    return Manager.execute_pipeline(
        environment.user_bob,
        p_basic,
        [environment.dataset_words],
        groups_allowed=[everyone_group()],
        slurm_sched_class=slurm_sched_class
    )


def execute_nested_run(environment, slurm_sched_class=DummySlurmScheduler):
    """
    A helper function that creates a nested pipeline and executes a run.

    This also populates the object -- e.g. a TestCase or a FixtureBuilder --
    with some variables.

    Returns the Manager object that executed the run.
    """
    tools.create_eric_martin_test_environment(environment)
    tools.create_sandbox_testing_tools_environment(environment)
    user = User.objects.get(username='john')

    # Everything in this pipeline will be a no-op, so all can be linked together
    # without remorse.
    p_basic = tools.make_first_pipeline("p_basic", "innermost pipeline", user)
    tools.create_linear_pipeline(p_basic, [environment.method_noop, environment.method_noop], "basic_in", "basic_out")
    p_basic.family.grant_everyone_access()
    p_basic.grant_everyone_access()
    p_basic.create_outputs()
    p_basic.save()

    p_sub = tools.make_first_pipeline("p_sub", "second-level pipeline", user)
    tools.create_linear_pipeline(p_sub, [p_basic, p_basic], "sub_in", "sub_out")
    p_sub.family.grant_everyone_access()
    p_sub.grant_everyone_access()
    p_sub.create_outputs()
    p_sub.save()

    p_top = tools.make_first_pipeline("p_top", "top-level pipeline", user)
    tools.create_linear_pipeline(p_top, [p_sub, p_sub, p_sub], "top_in", "top_out")
    p_top.family.grant_everyone_access()
    p_top.grant_everyone_access()
    p_top.create_outputs()
    p_top.save()

    # Set up a dataset with words in it called environment.dataset_words.
    tools.make_words_dataset(environment)

    return Manager.execute_pipeline(
        environment.user_bob,
        p_top,
        [environment.dataset_words],
        groups_allowed=[everyone_group()],
        slurm_sched_class=slurm_sched_class
    )


@skipIfDBFeature('is_mocked')
class SlurmExecutionTests(BaseTestCases.SlurmExecutionTestCase):

    @skipIf(not settings.RUN_SLURM_TESTS, "Slurm tests are disabled")
    def test_simple_run(self):
        """
        Execute a simple run.
        """
        mgr = execute_simple_run(self, slurm_sched_class=SlurmScheduler)
        run = mgr.get_last_run()

        self.check_run_OK(run)

        self.assertTrue(run.is_complete())
        self.assertTrue(run.is_successful())

        self.assertIsNone(run.clean())
        self.assertIsNone(run.complete_clean())

    @skipIf(not settings.RUN_SLURM_TESTS, "Slurm tests are disabled")
    def test_nested_run(self):
        """
        Execute a nested run.
        """
        mgr = execute_nested_run(self, slurm_sched_class=SlurmScheduler)
        run = mgr.get_last_run()

        self.check_run_OK(run)

        self.assertTrue(run.is_complete())
        self.assertTrue(run.is_successful())

        self.assertIsNone(run.clean())
        self.assertIsNone(run.complete_clean())


# @skipIfDBFeature('is_mocked')
# class SlurmExecutionPathWithSpacesTests(SlurmExecutionTests):
#     """
#     Repeat the same tests as SlurmExecutionTests, but with spaces in the Sandbox path.
#     """
#     def setUp(self):
#         self.media_root_original = settings.MEDIA_ROOT
#         # Create this directory/probe that it exists.
#         try:
#             os.mkdir(settings.MEDIA_ROOT)
#         except OSError:
#             # It already exists.
#             pass
#
#         # Make a temporary directory whose name has spaces in it.
#         self.base_with_spaces = tempfile.mkdtemp(
#             suffix="Extra Folder With Spaces",
#             dir=self.media_root_original
#         )
#         # Just to be safe, we end MEDIA_ROOT with a directory named "Testing" as
#         # this is consistent with the way we handle other tests that install fixture files.
#         self.media_root_with_spaces = os.path.join(self.base_with_spaces, "Testing")
#         settings.MEDIA_ROOT = self.media_root_with_spaces
#         SlurmExecutionTests.setUp(self)
#
#     def tearDown(self):
#         SlurmExecutionTests.tearDown(self)
#         settings.MEDIA_ROOT = self.media_root_original
#         shutil.rmtree(self.base_with_spaces)


@skipIfDBFeature('is_mocked')
@skipIf(not settings.RUN_SLURM_TESTS, "Slurm tests are disabled")
class SlurmBadRunTests(BaseTestCases.SlurmExecutionTestCase, BadRunTestsBase):
    """
    Tests a bad run using SlurmScheduler instead of DummySlurmScheduler.

    This inherits from the original test, so that we can test it both ways
    (once in the original way, and once here).
    """
    def setUp(self):
        BaseTestCases.SlurmExecutionTestCase.setUp(self)
        BadRunTestsBase.setUp(self)

    def tearDown(self):
        BaseTestCases.SlurmExecutionTestCase.tearDown(self)
        BadRunTestsBase.tearDown(self)

    @skipIf(not settings.RUN_SLURM_TESTS, "Slurm tests are disabled")
    def test_method_fails(self):
        BadRunTestsBase.test_method_fails(self, slurm_sched_class=SlurmScheduler)