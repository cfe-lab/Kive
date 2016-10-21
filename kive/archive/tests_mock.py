from unittest.case import TestCase

from mock import Mock, patch

from django.utils import timezone

from kive.mock_setup import mocked_relations
from django_mock_queries.query import MockSet

from archive.models import Run, RunState, RunStep, RunOutputCable,\
    RunComponentState, ExecLog, RunInput, MethodOutput
from archive.serializers import RunOutputsSerializer
from constants import runstates, runcomponentstates
from librarian.models import ExecRecord, Dataset, ExecRecordOut
from pipeline.models import Pipeline, PipelineOutputCable, PipelineStep
from transformation.models import TransformationInput, TransformationOutput
from django.core.files.base import ContentFile


@mocked_relations(Run, RunState)
class RunStateMockTests(TestCase):

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
        RunState.objects = MockSet(RunState(id=runstates.CANCELLING_PK), RunState(id=runstates.CANCELLED_PK))
        run = Run(_runstate_id=runstates.CANCELLING_PK)
        run.stop()
        self.assertEqual(runstates.CANCELLED_PK, run._runstate_id)

    def test_stop_failing(self):
        """
        Test that a Run properly transitions from Cancelling to Cancelled when stopped.
        """
        RunState.objects = MockSet(RunState(id=runstates.FAILING_PK), RunState(id=runstates.FAILED_PK))
        run = Run(_runstate_id=runstates.FAILING_PK)
        run.stop()
        self.assertEqual(runstates.FAILED_PK, run._runstate_id)

    def test_cancel_pending(self):
        """
        Test that a Run properly transitions from Pending to Cancelling on cancel().
        """
        RunState.objects = MockSet(RunState(id=runstates.PENDING_PK), RunState(id=runstates.CANCELLING_PK))
        run = Run(_runstate_id=runstates.PENDING_PK)
        run.cancel()
        self.assertEqual(runstates.CANCELLING_PK, run._runstate_id)

    def test_cancel_running(self):
        """
        Test that a Run properly transitions from Running to Cancelling on cancel().
        """
        RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.CANCELLING_PK))
        run = Run(_runstate_id=runstates.RUNNING_PK)
        run.cancel()
        self.assertEqual(runstates.CANCELLING_PK, run._runstate_id)

    def test_mark_failure(self):
        """
        Test that a Run properly transitions from Running to Failing on mark_failure().
        """
        RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.FAILING_PK))
        run = Run(_runstate_id=runstates.RUNNING_PK)
        run.mark_failure()
        self.assertEqual(runstates.FAILING_PK, run._runstate_id)

    def test_mark_failure_recurse_upward_no_parent_runstep(self):
        """
        Test that a Run does not try to recurse upward when there's no parent_runstep.
        """
        RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.FAILING_PK))
        run = Run(_runstate_id=runstates.RUNNING_PK)
        # If this works, it didn't try to get at parent_runstep, which is None.
        run.mark_failure(recurse_upward=True)
        self.assertEqual(runstates.FAILING_PK, run._runstate_id)

    def test_mark_failure_recurse_upward(self):
        """
        Test that mark_failure() can properly recurse upward.
        """
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
        RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
        run = Run(_runstate_id=runstates.SUCCESSFUL_PK, end_time=timezone.now())
        run.begin_recovery()
        self.assertEqual(runstates.RUNNING_PK, run._runstate_id)

    def test_begin_recovery_recurse_upward_no_parent_runstep(self):
        """
        Test that a Run properly transitions from Successful to Running on begin_recovery().
        """
        RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
        run = Run(_runstate_id=runstates.SUCCESSFUL_PK, end_time=timezone.now())
        # If this works, it didn't try to get at parent_runstep, which is None.
        run.begin_recovery(recurse_upward=True)
        self.assertEqual(runstates.RUNNING_PK, run._runstate_id)

    def test_begin_recovery_no_recurse_upward(self):
        """
        Test that begin_recovery does not recurse upward.
        """
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
        RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
        run = Run(_runstate_id=runstates.RUNNING_PK, end_time=timezone.now())
        run.is_running = Mock(return_value=True)

        run.finish_recovery()
        self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    def test_finish_recovery_failed(self):
        """
        Test that a Run properly transitions from Failing to Failed when recovery finishes.
        """
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
        RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
        run = Run(_runstate_id=runstates.SUCCESSFUL_PK)
        run.is_successful = Mock(return_value=True)

        run.quarantine(recurse_upward=False)
        self.assertEqual(runstates.QUARANTINED_PK, run._runstate_id)

    def test_quarantine_recurse_upward_no_parent_runstep(self):
        """
        Test quarantining of a Run does not recurse upward when no parent_runstep exists.
        """
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
        RunState.objects = MockSet(RunState(id=runstates.SUCCESSFUL_PK), RunState(id=runstates.QUARANTINED_PK))
        run = Run(_runstate_id=runstates.QUARANTINED_PK)
        run.is_quarantined = Mock(return_value=True)

        run.attempt_decontamination()
        self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)

    def test_attempt_decontamination_does_nothing_when_runsteps_quarantined(self):
        """
        Decontamination does nothing if a RunStep is still quarantined.
        """
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

    def test_attempt_decontamination_recurse_upward(self):
        """
        Test that decontamination recurses upward properly.
        """
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


@mocked_relations(RunStep, RunComponentState)
class RunComponentStateMockTests(TestCase):
    """
    Tests of RunComponent state transitions.
    """

    def test_start(self):
        """
        Test start() of a RunComponent.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.PENDING_PK),
            RunComponentState(id=runcomponentstates.RUNNING_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.PENDING_PK)
        rs.start()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.RUNNING_PK)

    def test_cancel_pending(self):
        """
        Test cancel_pending() of a RunComponent.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.PENDING_PK),
            RunComponentState(id=runcomponentstates.CANCELLED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.PENDING_PK)
        rs.cancel_pending()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.CANCELLED_PK)

    def test_cancel_running(self):
        """
        Test cancel_running() of a RunComponent.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.RUNNING_PK),
            RunComponentState(id=runcomponentstates.CANCELLED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK)
        rs.cancel_running()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.CANCELLED_PK)

    def test_cancel_when_pending(self):
        """
        Test cancel() of a pending RunComponent.
        """
        rs = RunStep(_runcomponentstate_id=runcomponentstates.PENDING_PK)
        rs.cancel_pending = Mock()
        rs.cancel_running = Mock()
        rs.is_pending = Mock(return_value=True)
        rs.cancel()
        rs.cancel_pending.assert_called_once_with(save=True)
        rs.cancel_running.assert_not_called()

    def test_cancel_when_running(self):
        """
        Test cancel() of a running RunComponent.
        """
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK)
        rs.cancel_pending = Mock()
        rs.cancel_running = Mock()
        rs.is_pending = Mock(return_value=False)
        rs.cancel()
        rs.cancel_pending.assert_not_called()
        rs.cancel_running.assert_called_once_with(save=True)

    def test_begin_recovery(self):
        """
        Test begin_recovery().
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.RUNNING_PK),
            RunComponentState(id=runcomponentstates.SUCCESSFUL_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                     run=Run())
        rs.has_ended = Mock(return_value=True)
        rs.run.begin_recovery = Mock()
        rs.begin_recovery()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.RUNNING_PK)
        rs.run.begin_recovery.assert_not_called()

    def test_begin_recovery_recurse_upward(self):
        """
        Test begin_recovery() recurses upward correctly.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.RUNNING_PK),
            RunComponentState(id=runcomponentstates.SUCCESSFUL_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                     run=Run())
        rs.has_ended = Mock(return_value=True)
        rs.run.begin_recovery = Mock()
        rs.begin_recovery(recurse_upward=True)
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.RUNNING_PK)
        rs.run.begin_recovery.assert_called_once_with(save=True, recurse_upward=True)

    def test_finish_successfully(self):
        """
        Test finish_successfully() on a RunComponent.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.SUCCESSFUL_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK,
                     run=Run())
        rs.stop = Mock()
        rs.has_ended = Mock(return_value=False)

        rs.finish_successfully()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.SUCCESSFUL_PK)
        rs.stop.assert_called_once_with(save=False)

    def test_finish_successfully_recovery(self):
        """
        Test finish_successfully() on a RunComponent that's recovering (i.e. has_ended() returns True).
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.SUCCESSFUL_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK,
                     run=Run())
        rs.stop = Mock()
        rs.has_ended = Mock(return_value=True)

        rs.finish_successfully()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.SUCCESSFUL_PK)
        rs.stop.assert_not_called()

    def test_finish_successfully_decontaminate(self):
        """
        Test finish_successfully() on a RunComponent whose ExecRecord comes from a quarantined component.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.SUCCESSFUL_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK,
                     run=Run())
        rs.stop = Mock()
        rs.has_ended = Mock(return_value=False)

        generating_rs = RunStep()
        generating_log = ExecLog(record=generating_rs)
        rs.execrecord = ExecRecord(generator=generating_log)
        generating_rs.is_quarantined = Mock(return_value=True)
        rs.execrecord.decontaminate_runcomponents = Mock()

        rs.finish_successfully()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.SUCCESSFUL_PK)
        rs.stop.assert_called_once_with(save=False)

        generating_rs.is_quarantined.assert_called_once_with()
        rs.execrecord.decontaminate_runcomponents.assert_called_once_with()

    def test_finish_failure(self):
        """
        Test finish_failure() on a RunComponent.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.FAILED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK,
                     run=Run())
        rs.stop = Mock()
        rs.has_ended = Mock(return_value=False)

        rs.finish_failure()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.FAILED_PK)
        rs.stop.assert_called_once_with(save=False)

    def test_finish_failure_recovery(self):
        """
        Test finish_failure() on a RunComponent that's recovering (i.e. has already ended).
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.FAILED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK,
                     run=Run())
        rs.stop = Mock()
        rs.has_ended = Mock(return_value=True)

        rs.finish_failure()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.FAILED_PK)
        rs.stop.assert_not_called()

    def test_finish_failure_recurse_upward_run_not_running(self):
        """
        Test finish_failure() doesn't recurse upward if the parent run is not running.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.FAILED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK,
                     run=Run())
        rs.stop = Mock()
        rs.has_ended = Mock(return_value=True)
        rs.run.is_running = Mock(return_value=False)
        rs.run.mark_failure = Mock()

        rs.finish_failure(recurse_upward=True)
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.FAILED_PK)
        rs.stop.assert_not_called()
        rs.run.mark_failure.assert_not_called()

    def test_finish_failure_recurse_upward(self):
        """
        Test finish_failure() properly recurses upward.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.FAILED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.RUNNING_PK,
                     run=Run())
        rs.stop = Mock()
        rs.has_ended = Mock(return_value=False)
        rs.run.is_running = Mock(return_value=True)
        rs.run.mark_failure = Mock()

        rs.finish_failure(recurse_upward=True)
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.FAILED_PK)
        rs.stop.assert_called_once_with(save=False)
        rs.run.mark_failure.assert_called_once_with(save=True, recurse_upward=True)

    def test_quarantine(self):
        """
        Test quarantining a RunComponent.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.QUARANTINED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                     run=Run())
        rs.stop = Mock()
        rs.run.quarantine = Mock()

        rs.quarantine()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.QUARANTINED_PK)
        rs.run.quarantine.assert_not_called()

    def test_quarantine_recurse_upward_run_not_successful(self):
        """
        Test that no upward recursion occurs if the parent run isn't successful.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.QUARANTINED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                     run=Run())
        rs.stop = Mock()
        rs.run.quarantine = Mock()
        rs.run.is_successful = Mock(return_value=False)

        rs.quarantine(recurse_upward=True)
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.QUARANTINED_PK)
        rs.run.is_successful.assert_called_once_with()
        rs.run.quarantine.assert_not_called()

    def test_quarantine_recurse_upward(self):
        """
        Test of upward recursion in quarantine.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.QUARANTINED_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                     run=Run())
        rs.stop = Mock()
        rs.run.quarantine = Mock()
        rs.run.is_successful = Mock(return_value=True)

        rs.quarantine(recurse_upward=True)
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.QUARANTINED_PK)
        rs.run.is_successful.assert_called_once_with()
        rs.run.quarantine.assert_called_once_with(save=True, recurse_upward=True)

    def test_decontaminate(self):
        """
        Test of decontamination of a RunComponent.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.SUCCESSFUL_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                     run=Run())
        rs.stop = Mock()
        rs.run.attempt_decontamination = Mock()
        rs.run.is_quarantined = Mock()
        rs.run.refresh_from_db = Mock()

        rs.decontaminate()
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.SUCCESSFUL_PK)
        rs.run.is_quarantined.assert_not_called()
        rs.run.attempt_decontamination.assert_not_called()
        rs.run.refresh_from_db.assert_called_once_with()

    def test_decontaminate_recurse_upward_run_not_quarantined(self):
        """
        Test decontamination does not recurse upward if parent run isn't quarantined.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.SUCCESSFUL_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                     run=Run())
        rs.stop = Mock()
        rs.run.attempt_decontamination = Mock()
        rs.run.is_quarantined = Mock(return_value=False)
        rs.run.refresh_from_db = Mock()

        rs.decontaminate(recurse_upward=True)
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.SUCCESSFUL_PK)
        rs.run.is_quarantined.assert_called_once_with()
        rs.run.attempt_decontamination.assert_not_called()
        rs.run.refresh_from_db.assert_called_once_with()

    def test_decontaminate_recurse_upward(self):
        """
        Test decontamination properly recurses upward.
        """
        RunComponentState.objects = MockSet(
            RunComponentState(id=runcomponentstates.SUCCESSFUL_PK)
        )
        rs = RunStep(_runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                     run=Run())
        rs.stop = Mock()
        rs.run.attempt_decontamination = Mock()
        rs.run.is_quarantined = Mock(return_value=True)
        rs.run.refresh_from_db = Mock()

        rs.decontaminate(recurse_upward=True)
        self.assertEqual(rs._runcomponentstate_id, runcomponentstates.SUCCESSFUL_PK)
        rs.run.is_quarantined.assert_called_once_with()
        rs.run.attempt_decontamination.assert_called_once_with(save=True, recurse_upward=True)
        rs.run.refresh_from_db.assert_called_once_with()


@mocked_relations(Run, Pipeline, ExecRecord)
class RunOutputsSerializerMockTests(TestCase):
    def test_no_steps(self):
        run = Run(id=1234, pipeline=Pipeline())
        expected_data = {'output_summary': [], 'input_summary': [], 'id': 1234}

        data = RunOutputsSerializer(run).data

        self.assertEqual(expected_data, data)

    def test_inputs(self):
        run = Run(id=1234, pipeline=Pipeline())
        run.inputs = MockSet(RunInput(index=1, dataset=Dataset()),
                             RunInput(index=2, dataset=Dataset()))
        run.pipeline.inputs = MockSet(TransformationInput(dataset_idx=1,
                                                          dataset_name='foo'),
                                      TransformationInput(dataset_idx=2))
        expected_data = {'output_summary': [],
                         'input_summary': [{'is_ok': True,
                                            'errors': [],
                                            'name': 'foo',
                                            'url': None,
                                            'is_invalid': False,
                                            'display': '1: foo',
                                            'redaction_plan': None,
                                            'step_name': 'Run inputs',
                                            'date': 'removed',
                                            'filename': None,
                                            'type': 'dataset',
                                            'id': None,
                                            'size': 'removed'},
                                           {'is_ok': True,
                                            'errors': [],
                                            'name': '',
                                            'url': None,
                                            'is_invalid': False,
                                            'display': '2: ',
                                            'redaction_plan': None,
                                            'step_name': '',
                                            'date': 'removed',
                                            'filename': None,
                                            'type': 'dataset',
                                            'id': None,
                                            'size': 'removed'}],
                         'id': 1234}

        data = RunOutputsSerializer(run).data

        self.assertEqual(expected_data['input_summary'], data['input_summary'])
        self.assertEqual(expected_data, data)

    def test_run_outputs(self):
        pipeline = Pipeline()
        pipeline.outputs = MockSet(TransformationOutput(dataset_idx=1,
                                                        dataset_name='foo'),
                                   TransformationOutput(dataset_idx=2,
                                                        dataset_name='bar'))
        for o in pipeline.outputs:
            o.transformationoutput = o
        run = Run(id=1234, pipeline=pipeline)
        execrecord1 = ExecRecord()
        execrecord1.execrecordouts = MockSet(ExecRecordOut(dataset=Dataset()))
        execrecord2 = ExecRecord()
        execrecord2.execrecordouts = MockSet(ExecRecordOut(dataset=Dataset()))
        run.runoutputcables = MockSet(
            RunOutputCable(execrecord=execrecord1,
                           pipelineoutputcable=PipelineOutputCable(pipeline=pipeline,
                                                                   output_name='foo')),
            RunOutputCable(execrecord=execrecord2,
                           pipelineoutputcable=PipelineOutputCable(pipeline=pipeline,
                                                                   output_name='bar')))
        expected_data = {'output_summary': [{'date': 'removed',
                                             'display': '1: foo',
                                             'errors': [],
                                             'filename': None,
                                             'id': None,
                                             'is_invalid': False,
                                             'is_ok': True,
                                             'name': 'foo',
                                             'redaction_plan': None,
                                             'size': 'removed',
                                             'step_name': 'Run outputs',
                                             'type': 'dataset',
                                             'url': None},
                                            {'date': 'removed',
                                             'display': '2: bar',
                                             'errors': [],
                                             'filename': None,
                                             'id': None,
                                             'is_invalid': False,
                                             'is_ok': True,
                                             'name': 'bar',
                                             'redaction_plan': None,
                                             'size': 'removed',
                                             'step_name': '',
                                             'type': 'dataset',
                                             'url': None}],
                         'input_summary': [],
                         'id': 1234}

        data = RunOutputsSerializer(run).data

        self.assertEqual(expected_data['output_summary'], data['output_summary'])
        self.assertEqual(expected_data, data)

    @patch('archive.serializers.reverse', return_value='/some/url')
    def test_step_outputs(self, mock_reverse):
        pipeline = Pipeline()
        run = Run(id=1234, pipeline=pipeline)
        step = RunStep(pipelinestep=PipelineStep(step_num=1,
                                                 name='foo.py'))
        step.log = ExecLog(end_time='12 May 2015')
        step.log.methodoutput = MethodOutput(id=99,
                                             return_code=0,
                                             output_log=ContentFile('Done.',
                                                                    name='x'),
                                             error_log=ContentFile('',
                                                                   name='y'))
        run.runsteps = MockSet(step)
        expected_data = {'output_summary': [{'date': '12 May 2015',
                                             'display': 'Standard out',
                                             'errors': [],
                                             'filename': None,
                                             'id': 99,
                                             'is_invalid': False,
                                             'is_ok': True,
                                             'name': 'step_1_stdout',
                                             'redaction_plan': '/some/url',
                                             'size': u'5\xa0bytes',
                                             'step_name': '1: foo.py',
                                             'type': 'stdout',
                                             'url': '/some/url'},
                                            {'date': '12 May 2015',
                                             'display': 'Standard error',
                                             'errors': [],
                                             'filename': None,
                                             'id': 99,
                                             'is_invalid': False,
                                             'is_ok': True,
                                             'name': 'step_1_stderr',
                                             'redaction_plan': '/some/url',
                                             'size': u'0\xa0bytes',
                                             'step_name': '',
                                             'type': 'stderr',
                                             'url': '/some/url'}],
                         'input_summary': [],
                         'id': 1234}

        data = RunOutputsSerializer(run).data

        self.maxDiff = None
        self.assertEqual(expected_data['output_summary'], data['output_summary'])
        self.assertEqual(expected_data, data)

    @patch('archive.serializers.reverse', return_value='/some/url')
    def test_step_outputs_error(self, mock_reverse):
        pipeline = Pipeline()
        run = Run(id=1234, pipeline=pipeline)
        step = RunStep(pipelinestep=PipelineStep(step_num=1,
                                                 name='foo.py'))
        step.log = ExecLog(end_time='12 May 2015')
        step.log.methodoutput = MethodOutput(id=99,
                                             return_code=1,
                                             output_log=ContentFile('Done.',
                                                                    name='x'),
                                             error_log=ContentFile('Bad stuff.',
                                                                   name='y'))
        run.runsteps = MockSet(step)
        expected_data = {'output_summary': [{'date': '12 May 2015',
                                             'display': 'Standard out',
                                             'errors': [],
                                             'filename': None,
                                             'id': 99,
                                             'is_invalid': False,
                                             'is_ok': True,
                                             'name': 'step_1_stdout',
                                             'redaction_plan': '/some/url',
                                             'size': u'5\xa0bytes',
                                             'step_name': '1: foo.py',
                                             'type': 'stdout',
                                             'url': '/some/url'},
                                            {'date': '12 May 2015',
                                             'display': 'Standard error',
                                             'errors': ['return code 1'],
                                             'filename': None,
                                             'id': 99,
                                             'is_invalid': True,
                                             'is_ok': False,
                                             'name': 'step_1_stderr',
                                             'redaction_plan': '/some/url',
                                             'size': u'10\xa0bytes',
                                             'step_name': '',
                                             'type': 'stderr',
                                             'url': '/some/url'}],
                         'input_summary': [],
                         'id': 1234}

        data = RunOutputsSerializer(run).data

        self.maxDiff = None
        self.assertEqual(expected_data['output_summary'], data['output_summary'])
        self.assertEqual(expected_data, data)

    @patch('archive.serializers.reverse', return_value='/some/url')
    def test_step_outputs_missing(self, mock_reverse):
        pipeline = Pipeline()
        run = Run(id=1234, pipeline=pipeline)
        step = RunStep(pipelinestep=PipelineStep(step_num=1,
                                                 name='foo.py'))
        step.log = ExecLog(end_time='12 May 2015')
        step.log.methodoutput = MethodOutput(id=99)
        run.runsteps = MockSet(step)
        expected_data = {'output_summary': [{'date': '12 May 2015',
                                             'display': 'Standard out',
                                             'errors': ['Did not run.'],
                                             'filename': None,
                                             'id': None,
                                             'is_invalid': False,
                                             'is_ok': False,
                                             'name': 'step_1_stdout',
                                             'redaction_plan': None,
                                             'size': 'missing',
                                             'step_name': '1: foo.py',
                                             'type': 'stdout',
                                             'url': None}],
                         'input_summary': [],
                         'id': 1234}

        data = RunOutputsSerializer(run).data

        self.maxDiff = None
        self.assertEqual(expected_data['output_summary'], data['output_summary'])
        self.assertEqual(expected_data, data)

    @patch('archive.serializers.reverse', return_value='/some/url')
    def test_step_outputs_running(self, mock_reverse):
        pipeline = Pipeline()
        run = Run(id=1234, pipeline=pipeline)
        step = RunStep(pipelinestep=PipelineStep(step_num=1,
                                                 name='foo.py'))
        step.log = ExecLog()
        step.log.methodoutput = MethodOutput(id=99)
        run.runsteps = MockSet(step)
        expected_data = {'output_summary': [{'date': None,
                                             'display': 'Running',
                                             'errors': [],
                                             'filename': None,
                                             'id': None,
                                             'is_invalid': False,
                                             'is_ok': False,
                                             'name': 'step_1_stdout',
                                             'redaction_plan': None,
                                             'size': None,
                                             'step_name': '1: foo.py',
                                             'type': 'stdout',
                                             'url': None}],
                         'input_summary': [],
                         'id': 1234}

        data = RunOutputsSerializer(run).data

        self.maxDiff = None
        self.assertEqual(expected_data['output_summary'], data['output_summary'])
        self.assertEqual(expected_data, data)
