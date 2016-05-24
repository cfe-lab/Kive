from unittest.case import TestCase

from mock import Mock

from kive.mock_setup import mock_relations  # Import before any Django models
from django_mock_queries.query import MockSet

from django.utils import timezone

from archive.models import Run, RunState, RunStep, RunOutputCable
from constants import runstates, runcomponentstates
from kive.mock_setup import mocked_relations


class RunStateMockTests(TestCase):
    @mocked_relations(Run, RunState)
    def test_stop_running(self):
        """
        Test that a Run properly transitions from Running to Successful when stopped.
        """
        RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
        run = Run(_runstate_id=runstates.RUNNING_PK)
        run.stop()
        self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    def test_stop_running_quarantined_step(self):
        """
        Test that a Run properly transitions from Running to Quarantined if a step is quarantined.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)

            rs = RunStep(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK)
            run.runsteps.add(rs)

            run.stop()
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)

    def test_stop_running_quarantined_outcable(self):
        """
        Test that a Run properly transitions from Running to Quarantined if an outcable is quarantined.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)

            roc = RunOutputCable(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK)
            run.runoutputcables.add(roc)

            run.stop()
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)

    def test_stop_cancelling(self):
        """
        Test that a Run properly transitions from Cancelling to Cancelled when stopped.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.CANCELLING_PK), RunState(id=runstates.CANCELLED_PK))
            run = Run(_runstate_id=runstates.CANCELLING_PK)
            run.stop()
            self.assertEqual(runstates.CANCELLED_PK, run._runstate_id)

    def test_stop_failing(self):
        """
        Test that a Run properly transitions from Cancelling to Cancelled when stopped.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.FAILING_PK), RunState(id=runstates.FAILED_PK))
            run = Run(_runstate_id=runstates.FAILING_PK)
            run.stop()
            self.assertEqual(runstates.FAILED_PK, run._runstate_id)

    def test_cancel_pending(self):
        """
        Test that a Run properly transitions from Pending to Cancelling on cancel().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.PENDING_PK), RunState(id=runstates.CANCELLING_PK))
            run = Run(_runstate_id=runstates.PENDING_PK)
            run.cancel()
            self.assertEqual(runstates.CANCELLING_PK, run._runstate_id)

    def test_cancel_running(self):
        """
        Test that a Run properly transitions from Running to Cancelling on cancel().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.CANCELLING_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)
            run.cancel()
            self.assertEqual(runstates.CANCELLING_PK, run._runstate_id)

    def test_mark_failure(self):
        """
        Test that a Run properly transitions from Running to Failing on mark_failure().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.FAILING_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)
            run.mark_failure()
            self.assertEqual(runstates.FAILING_PK, run._runstate_id)

    def test_mark_failure_recurse_upward_no_parent_runstep(self):
        """
        Test that a Run does not try to recurse upward when there's no parent_runstep.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.FAILING_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)
            # If this works, it didn't try to get at parent_runstep, which is None.
            run.mark_failure(recurse_upward=True)
            self.assertEqual(runstates.FAILING_PK, run._runstate_id)

    def test_mark_failure_recurse_upward(self):
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

    def test_mark_failure_no_recurse_upward(self):
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

    def test_begin_recovery(self):
        """
        Test that a Run properly transitions from Successful to Running on begin_recovery().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK, end_time=timezone.now())
            run.begin_recovery()
            self.assertEqual(runstates.RUNNING_PK, run._runstate_id)

    def test_begin_recovery_recurse_upward_no_parent_runstep(self):
        """
        Test that a Run properly transitions from Successful to Running on begin_recovery().
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK, end_time=timezone.now())
            # If this works, it didn't try to get at parent_runstep, which is None.
            run.begin_recovery(recurse_upward=True)
            self.assertEqual(runstates.RUNNING_PK, run._runstate_id)

    def test_begin_recovery_no_recurse_upward(self):
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

    def test_begin_recovery_recurse_upward(self):
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

    def test_finish_recovery_successful(self):
        """
        Test that a Run properly transitions from Running to Successful when recovery finishes.
        """
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK, end_time=timezone.now())
            run.is_running = Mock(return_value=True)

            run.finish_recovery()
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    def test_finish_recovery_failed(self):
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

    def test_finish_recovery_cancelled(self):
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

    def test_finish_recovery_recurse_upward_no_parent_runstep(self):
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

    def test_finish_recovery_recurse_upward(self):
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

    def test_finish_recovery_no_recurse_upward(self):
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

    def test_quarantine(self):
        """
        Test quarantining of a Run.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK)
            run.is_successful = Mock(return_value=True)

            run.quarantine(recurse_upward=False)
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)

    def test_quarantine_recurse_upward_no_parent_runstep(self):
        """
        Test quarantining of a Run does not recurse upward when no parent_runstep exists.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK)
            run.is_successful = Mock(return_value=True)

            # This would fail if it tried to recurse upward.
            run.quarantine(recurse_upward=True)
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)

    def test_quarantine_no_recurse_upward(self):
        """
        Test quarantining of a Run does not recurse upward when told not to.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK)
            run.is_successful = Mock(return_value=True)

            run_up_one_level = Run(_runstate_id=runstates.SUCCESSFUL_PK)
            run_up_one_level.is_successful = Mock(return_value=True)
            run_up_one_level.quarantine = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.quarantine(recurse_upward=False)
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)
            run.parent_runstep.run.is_successful.assert_not_called()
            run.parent_runstep.run.quarantine.assert_not_called()

    def test_quarantine_recurse_upward_parent_run_not_successful(self):
        """
        Test quarantining of a Run does not recurse upward when the parent run isn't successful.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK)
            run.is_successful = Mock(return_value=True)

            run_up_one_level = Run(_runstate_id=runstates.SUCCESSFUL_PK)
            run_up_one_level.is_successful = Mock(return_value=False)
            run_up_one_level.quarantine = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.quarantine(recurse_upward=True)
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)
            run.parent_runstep.run.is_successful.assert_called_once_with()
            run.parent_runstep.run.quarantine.assert_not_called()

    def test_quarantine_recurse_upward(self):
        """
        Test quarantining of a Run does not recurse upward when the parent run isn't successful.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.SUCCESSFUL_PK)
            run.is_successful = Mock(return_value=True)

            run_up_one_level = Run(_runstate_id=runstates.SUCCESSFUL_PK)
            run_up_one_level.is_successful = Mock(return_value=True)
            run_up_one_level.quarantine = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.quarantine(recurse_upward=True)
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)
            run.parent_runstep.run.is_successful.assert_called_once_with()
            run.parent_runstep.run.quarantine.assert_called_once_with(save=True, recurse_upward=True)

    def test_attempt_decontamination(self):
        """
        Test decontamination of a Run.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.QUARANTINED_PK)
            run.is_quarantined = Mock(return_value=True)

            run.attempt_decontamination()
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    def test_attempt_decontamination_does_nothing_when_runsteps_quarantined(self):
        """
        Decontamination does nothing if a RunStep is still quarantined.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.QUARANTINED_PK)
            run.is_quarantined = Mock(return_value=True)

            rs = RunStep(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK)
            run.runsteps.add(rs)

            run.attempt_decontamination()
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)

    def test_attempt_decontamination_does_nothing_when_runoutputcables_quarantined(self):
        """
        Decontamination does nothing if a RunOutputCable is still quarantined.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.QUARANTINED_PK)
            run.is_quarantined = Mock(return_value=True)

            roc = RunOutputCable(run=run, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK)
            run.runoutputcables.add(roc)

            run.attempt_decontamination()
            self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)

    def test_attempt_decontamination_recurse_upward_no_parent_run(self):
        """
        Decontamination does not recurse upward if there's no parent run.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.QUARANTINED_PK)
            run.is_quarantined = Mock(return_value=True)

            # This would fail if it actually recursed upward.
            run.attempt_decontamination(recurse_upward=True)
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    def test_attempt_decontamination_recurse_upward_parent_run_not_quarantined(self):
        """
        Decontamination does not recurse upward if the parent run isn't quarantined.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.QUARANTINED_PK)
            run.is_quarantined = Mock(return_value=True)

            run_up_one_level = Run()
            run_up_one_level.is_quarantined = Mock(return_value=False)
            run_up_one_level.attempt_decontamination = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.attempt_decontamination(recurse_upward=True)
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)
            run.parent_runstep.run.attempt_decontamination.assert_not_called()

    def test_attempt_decontamination_recurse_upward_parent_run_not_quarantined2(self):
        """
        Test that decontamination recurses upward properly.
        """
        with mock_relations(Run, RunState, RunStep):
            RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
            run = Run(_runstate_id=runstates.QUARANTINED_PK)
            run.is_quarantined = Mock(return_value=True)

            run_up_one_level = Run()
            run_up_one_level.is_quarantined = Mock(return_value=True)
            run_up_one_level.attempt_decontamination = Mock()
            run.parent_runstep = RunStep(run=run_up_one_level)

            run.attempt_decontamination(recurse_upward=True)
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)
            run.parent_runstep.run.attempt_decontamination.assert_called_once_with(save=True, recurse_upward=True)
