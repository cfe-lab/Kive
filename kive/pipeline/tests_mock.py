from unittest import TestCase

from kive.mock_setup import mock_relations  # Import before any Django models
from pipeline.models import Pipeline


class PipelineUpdateMockTests(TestCase):
    def test_no_steps(self):
        with mock_relations(Pipeline):
            pipeline = Pipeline()
            Pipeline.steps.all.return_value = []

            updates = pipeline.find_step_updates()

            self.assertEqual([], updates)
