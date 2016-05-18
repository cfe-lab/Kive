from unittest.case import TestCase

from mock import PropertyMock, Mock, patch

from kive.mock_setup import mock_relations  # Import before any Django models
from django_mock_queries.query import MockSet

from archive.models import Run, RunState
from constants import runstates


class RunMockTests(TestCase):
    @patch('django.db.transaction.Atomic')
    def test_stop_success(self, mock_transaction):
        with mock_relations(Run, RunState):
            RunState.objects = MockSet(RunState(id=runstates.RUNNING_PK), RunState(id=runstates.SUCCESSFUL_PK))
            run = Run(_runstate_id=runstates.RUNNING_PK)
            run.stop()
            self.assertEqual(runstates.SUCCESSFUL_PK, run._runstate_id)