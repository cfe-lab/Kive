
import datetime
from unittest import skipIf

from django.contrib.auth.models import User
from django.conf import settings
from django.test import skipIfDBFeature
from django.utils import timezone

# import tempfile
# import os
# import shutil

from fleet.workers import Manager
from kive.tests import BaseTestCases
import kive.testing_utils as tools
from metadata.models import everyone_group
from fleet.slurmlib import SlurmScheduler, DummySlurmScheduler
from fleet.dockerlib import DummyDockerHandler
from sandbox.tests_rm import BadRunTestsBase


def execute_simple_run(environment, slurm_sched_class):
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
        slurm_sched_class=slurm_sched_class,
        docker_handler_class=DummyDockerHandler
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


@skipIf(not settings.RUN_SLURM_TESTS, "Slurm tests are disabled")
@skipIfDBFeature('is_mocked')
class SlurmExecutionTests(BaseTestCases.SlurmExecutionTestCase):
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


@skipIf(not settings.RUN_SLURM_TESTS, "Slurm tests are disabled")
@skipIfDBFeature('is_mocked')
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

    def test_method_fails(self, slurm_sched_class=SlurmScheduler):
        super(SlurmBadRunTests, self).test_method_fails(slurm_sched_class)


class MockSlurmScheduler(DummySlurmScheduler):
    """ A mocked -up slurm scheduler which will create NODE_FAIL events
    with a job end time set to now() + my_time_delta.
    The NODE_FAIL events are injected every second time get_accounting_info() is called,
    in other times, the status information is passed through unchanged.
    """
    count = 0
    my_time_delta = datetime.timedelta(seconds=-2*settings.NODE_FAIL_TIME_OUT_SECS)
    name_tag = "PAST--"

    @classmethod
    def slurm_ident(cls):
        return "{}--{}--{}".format(cls.name_tag,
                                   cls.my_time_delta,
                                   super(MockSlurmScheduler, cls).slurm_ident())

    @classmethod
    def _mod_to_node_fail(cls, stat_dct):
        """ Modify the status dict to a node_fail state
        with an end time of now() + cls.my_time_delta
        """
        now_time = datetime.datetime.now(timezone.get_current_timezone())
        end_time = now_time + cls.my_time_delta
        for dct in stat_dct.values():
            dct[SlurmScheduler.ACC_STATE] = SlurmScheduler.NODE_FAIL
            dct[SlurmScheduler.ACC_END_TIME] = end_time

    @classmethod
    def get_accounting_info(cls, job_handle_iter=None):
        # print("mock accounting {}".format(cls.count))
        stat_dct = super(MockSlurmScheduler, cls).get_accounting_info(job_handle_iter=job_handle_iter)
        if cls.count % 2 == 0:
            # print("overriding to NODE_FAIL: {}".format(cls.my_time_delta))
            cls._mod_to_node_fail(stat_dct)
        else:
            pass
            # print("returning unchanged accounting info")
        # print("RETURNING {}".format(stat_dct))
        cls.count += 1
        return stat_dct


class Recent_NF_Scheduler(MockSlurmScheduler):
    count = 0
    my_time_delta = datetime.timedelta(seconds=0)
    name_tag = "RECENT--"


@skipIfDBFeature('is_mocked')
class NodeFailExecutionTests(BaseTestCases.SlurmExecutionTestCase):

    def _sched_run_simple(self, slurm_sched_class):
        # print("Running simple run with slurm : '%s'" % slurm_sched_class.slurm_ident())
        mgr = execute_simple_run(self, slurm_sched_class=slurm_sched_class)
        return mgr

    def test_NF_future_run(self):
        """
        Execute a simple run.
        NODE_FAIL is set with a recent job end_time  --> the job should complete
        as normal.
        """
        mgr = self._sched_run_simple(Recent_NF_Scheduler)
        run = mgr.get_last_run()

        self.check_run_OK(run)

        self.assertTrue(run.is_complete())
        self.assertTrue(run.is_successful())

        self.assertIsNone(run.clean())
        self.assertIsNone(run.complete_clean())

    def test_NF_fail_run(self):
        """
        Execute a simple run.
        NODE_FAIL is set with a job end_time in the distant past -->
        the job should fail.
        """
        mgr = self._sched_run_simple(MockSlurmScheduler)
        run = mgr.get_last_run()
        self.assertTrue(run.is_failed())
