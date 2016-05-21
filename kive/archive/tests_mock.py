from unittest.case import TestCase

from mock import PropertyMock, Mock, patch

from kive.mock_setup import mock_relations  # Import before any Django models
from django_mock_queries.query import MockSet

from django.utils import timezone

from archive.models import Run, RunState, RunStep
from constants import runstates


class RunMockTests(TestCase):

    @patch('django.db.transaction.Atomic')
    def test_stop_running(self, mock_transaction):
        """
        Test that a Run properly transitions from Running to Successful when stopped.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)
            run.stop()
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_stop_cancelling(self, mock_transaction):
        """
        Test that a Run properly transitions from Cancelling to Cancelled when stopped.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.CANCELLING_PK), RunState(id=runstates.CANCELLED_PK))
            run = Run(_runstate_id=runstates.CANCELLING_PK)
            run.stop()
            self.assertEqual(runstates.CANCELLED_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_stop_failing(self, mock_transaction):
        """
        Test that a Run properly transitions from Cancelling to Cancelled when stopped.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.FAILING_PK), RunState(id=runstates.FAILED_PK))
            run = Run(_runstate_id=runstates.FAILING_PK)
            run.stop()
            self.assertEqual(runstates.FAILED_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_cancel_pending(self, mock_transaction):
        """
        Test that a Run properly transitions from Pending to Cancelling on cancel().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.PENDING_PK), RunState(id=runstates.CANCELLING_PK))
            run = Run(_runstate_id=runstates.PENDING_PK)
            run.cancel()
            self.assertEqual(runstates.CANCELLING_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_cancel_running(self, mock_transaction):
        """
        Test that a Run properly transitions from Running to Cancelling on cancel().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.CANCELLING_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)
            run.cancel()
            self.assertEqual(runstates.CANCELLING_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_mark_failure(self, mock_transaction):
        """
        Test that a Run properly transitions from Running to Failing on mark_failure().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.FAILING_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)
            run.mark_failure()
            self.assertEqual(runstates.FAILING_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_mark_failure_recurse_upward_no_parent_runstep(self, mock_transaction):
        """
        Test that a Run does not try to recurse upward when there's no parent_runstep.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.FAILING_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)
            # If this works, it didn't try to get at parent_runstep, which is None.
            run.mark_failure(recurse_upward=True)
            self.assertEqual(runstates.FAILING_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_mark_failure_recurse_upward(self, mock_transaction):
        """
        Test that mark_failure() can properly recurse upward.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.FAILING_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)

            run_up_one_level = Run(_runstate_id=runstates.RUNNING_PK)
            run_up_one_level.mark_failure = Mock()

            run.parent_runstep = RunStep(run=run_up_one_level)

            run.mark_failure(recurse_upward=True)
            self.assertEqual(runstates.FAILING_PK, run._runstate_id)
            run.parent_runstep.run.mark_failure.assert_called_once_with(save=True, recurse_upward=True)

    @patch('django.db.transaction.Atomic')
    def test_mark_failure_no_recurse_upward(self, mock_transaction):
        """
        Test that mark_failure() does not recurse upward.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.FAILING_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)

            run_up_one_level = Run(_runstate_id=runstates.RUNNING_PK)
            run_up_one_level.mark_failure = Mock()

            run.parent_runstep = RunStep(run=run_up_one_level)

            run.mark_failure(recurse_upward=False)
            self.assertEqual(runstates.FAILING_PK, run._runstate_id)
            run.parent_runstep.run.mark_failure.assert_not_called()

    @patch('django.db.transaction.Atomic')
    def test_begin_recovery(self, mock_transaction):
        """
        Test that a Run properly transitions from Successful to Running on begin_recovery().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK, end_time=timezone.now())
            run.begin_recovery()
            self.assertEqual(runstates.RUNNING_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_begin_recovery_recurse_upward_no_parent_runstep(self, mock_transaction):
        """
        Test that a Run properly transitions from Successful to Running on begin_recovery().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK, end_time=timezone.now())
            # If this works, it didn't try to get at parent_runstep, which is None.
            run.begin_recovery(recurse_upward=True)
            self.assertEqual(runstates.RUNNING_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_begin_recovery_no_recurse_upward(self, mock_transaction):
        """
        Test that begin_recovery does not recurse upward.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK, end_time=timezone.now())

            run_up_one_level = Run(_runstate_id=runstates.RUNNING_PK)
            run_up_one_level.begin_recovery = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.begin_recovery(recurse_upward=False)
            self.assertEqual(runstates.RUNNING_PK, run._runstate_id)
            run.parent_runstep.run.begin_recovery.assert_not_called()

    @patch('django.db.transaction.Atomic')
    def test_begin_recovery_recurse_upward(self, mock_transaction):
        """
        Test that begin_recovery properly recurses upward.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK, end_time=timezone.now())

            run_up_one_level = Run(_runstate_id=runstates.RUNNING_PK)
            run_up_one_level.begin_recovery = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.begin_recovery(recurse_upward=True)
            self.assertEqual(runstates.RUNNING_PK, run._runstate_id)
            run.parent_runstep.run.begin_recovery.assert_called_once_with(save=True, recurse_upward=True)

    @patch('django.db.transaction.Atomic')
    def test_finish_recovery_successful(self, mock_transaction):
        """
        Test that a Run properly transitions from Running to Successful when recovery finishes.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK, end_time=timezone.now())
            run.is_running = Mock(return_value=True)

            run.finish_recovery()
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_finish_recovery_failed(self, mock_transaction):
        """
        Test that a Run properly transitions from Failing to Failed when recovery finishes.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.FAILING_PK), RunState(id=runstates.FAILED_PK))
            run = Run(_runstate_id=runstates.FAILING_PK, end_time=timezone.now())
            run.is_running = Mock(return_value=False)
            run.is_failing = Mock(return_value=True)

            run.finish_recovery()
            self.assertEqual(runstates.FAILED_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_finish_recovery_cancelled(self, mock_transaction):
        """
        Test that a Run properly transitions from Cancelling to Cancelled when recovery finishes.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.CANCELLING_PK), RunState(id=runstates.CANCELLED_PK))
            run = Run(_runstate_id=runstates.CANCELLING_PK, end_time=timezone.now())
            run.is_running = Mock(return_value=False)
            run.is_failing = Mock(return_value=False)

            run.finish_recovery()
            self.assertEqual(runstates.CANCELLED_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_finish_recovery_recurse_upward_no_parent_runstep(self, mock_transaction):
        """
        Test that a Run does not recurse upward when there's no parent_runstep.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK, end_time=timezone.now())
            run.is_running = Mock(return_value=True)

            # If this works, it didn't try to get at parent_runstep, which is None.
            run.finish_recovery(recurse_upward=True)
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    @patch('django.db.transaction.Atomic')
    def test_finish_recovery_recurse_upward(self, mock_transaction):
        """
        Test that a Run properly recurses upward.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK, end_time=timezone.now())
            run.is_running = Mock(return_value=True)

            run_up_one_level = Run(_runstate_id=runstates.RUNNING_PK)
            run_up_one_level.finish_recovery = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.finish_recovery(recurse_upward=True)
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)
            run.parent_runstep.run.finish_recovery.assert_called_once_with(save=True, recurse_upward=True)

    @patch('django.db.transaction.Atomic')
    def test_finish_recovery_no_recurse_upward(self, mock_transaction):
        """
        Test that a Run doesn't recurse upward.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK, end_time=timezone.now())
            run.is_running = Mock(return_value=True)

            run_up_one_level = Run(_runstate_id=runstates.RUNNING_PK)
            run_up_one_level.finish_recovery = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.finish_recovery(recurse_upward=False)
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)
            run.parent_runstep.run.finish_recovery.assert_not_called()