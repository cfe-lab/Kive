from contextlib import contextmanager
import re
from unittest import TestCase

from django.core.exceptions import ValidationError
from mock import PropertyMock, call

from kive.mock_setup import mock_relations  # Import before any Django models
from django_mock_queries.query import MockSet
from constants import datatypes
from metadata.models import CompoundDatatype, CompoundDatatypeMember, Datatype
from method.models import Method
from pipeline.models import Pipeline, PipelineFamily, PipelineStep,\
    PipelineStepInputCable, PipelineOutputCable, PipelineCable
from transformation.models import TransformationInput, XputStructure,\
    TransformationOutput, Transformation


class PipelineMockTests(TestCase):
    """Tests for basic Pipeline functionality."""
    def test_pipeline_no_inputs_no_steps(self):
        """A Pipeline with no inputs and no steps is clean but not complete."""
        with mock_relations(Pipeline, Transformation):
            p = Pipeline(family=PipelineFamily())

            p.clean()

            self.assertRaisesRegexp(
                ValidationError,
                re.escape("Pipeline {} has no steps".format(p)),
                p.complete_clean
            )

    """Tests for basic Pipeline functionality."""
    def test_pipeline_one_valid_input_no_steps(self):
        """A Pipeline with one valid input, but no steps, is clean but not complete."""
        with mock_relations(Pipeline, Transformation):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p, TransformationInput(dataset_idx=1))

            p.clean()

            self.assertRaisesRegexp(
                ValidationError,
                re.escape("Pipeline {} has no steps".format(p)),
                p.complete_clean
            )

    def test_pipeline_one_invalid_input_clean(self):
        """A Pipeline with one input not numbered "1" is not clean."""
        with mock_relations(Pipeline, Transformation):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p, TransformationInput(dataset_idx=4))

            error = "Inputs are not consecutively numbered starting from 1"
            self.assertRaisesRegexp(ValidationError, error, p.clean)
            self.assertRaisesRegexp(ValidationError, error, p.complete_clean)

    def test_pipeline_many_valid_inputs_clean(self):
        """A Pipeline with multiple, properly indexed inputs is clean."""
        with mock_relations(Pipeline, Transformation):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=2),
                            TransformationInput(dataset_idx=1),
                            TransformationInput(dataset_idx=3))

            p.clean()

    def test_pipeline_many_invalid_inputs_clean(self):
        """A Pipeline with multiple, badly indexed inputs is not clean."""
        with mock_relations(Pipeline, Transformation):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=2),
                            TransformationInput(dataset_idx=3),
                            TransformationInput(dataset_idx=4))

            self.assertRaisesRegexp(
                ValidationError,
                "Inputs are not consecutively numbered starting from 1",
                p.clean)

    def test_pipeline_one_valid_step_clean(self):
        """A Pipeline with one validly indexed step and input is clean.

        The PipelineStep and Pipeline are not complete unless there is a
        cable in place.
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            step1.cables_in.clear()
            step1.inputs[0].dataset_name = "input"

            error = 'Input "input" to transformation at step 1 is not cabled'
            step1.clean()
            self.assertRaisesRegexp(ValidationError, error, step1.complete_clean)
            p.clean()
            self.assertRaisesRegexp(ValidationError, error, p.complete_clean)

    def test_pipeline_one_bad_step_clean(self):
        """Test step index check, one badly-indexed step case."""
        with self.create_valid_pipeline() as p:
            p.steps.all()[0].step_num = 10

            self.assertRaisesRegexp(
                    ValidationError,
                    "Steps are not consecutively numbered starting from 1",
                    p.clean)

    def test_pipeline_many_valid_steps_clean(self):
        """Test step index check, well-indexed multi-step case."""
        with mock_relations(Pipeline, PipelineStep, Method, Transformation):
            p = Pipeline(family=PipelineFamily())
            p.inputs = MockSet()
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1))
            m = Method()
            m.inputs = MockSet()
            self.add_inputs(m,
                            TransformationInput(dataset_idx=1))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=2))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=1))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=3))

            p.clean()

    def test_pipeline_many_invalid_steps_clean(self):
        """Test step index check, badly-indexed multi-step case."""
        with mock_relations(Pipeline, PipelineStep, Method, Transformation):
            p = Pipeline(family=PipelineFamily())
            p.inputs = MockSet()
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1))
            m = Method()
            m.inputs = MockSet()
            self.add_inputs(m,
                            TransformationInput(dataset_idx=1))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=1))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=4))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=5))

            self.assertRaisesRegexp(
                    ValidationError,
                    "Steps are not consecutively numbered starting from 1",
                    p.clean)

    def test_pipeline_one_step_valid_cabling_clean(self):
        """Test good step cabling, one-step pipeline."""
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            cable = step1.cables_in.all()[0]
            outcable = p.outcables.all()[0]

            cable.clean()
            step1.clean()
            step1.complete_clean()
            outcable.clean()
            p.clean()
            p.complete_clean()

    def test_pipeline_oneStep_invalid_cabling_invalid_pipeline_input_clean(self):
        """Bad cabling: step looks for input that does not belong to the pipeline."""
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            cable = step1.cables_in.all()[0]

            unrelated_input = self.create_input(datatypes.STR_PK, dataset_idx=3)
            cable.source = unrelated_input

            self.assertRaisesRegexp(ValidationError,
                                    'Pipeline does not have input ".*"',
                                    cable.clean)
            # The following are just the same as the above, propagated upwards through clean()s.
            self.assertRaisesRegexp(ValidationError,
                                    'Pipeline does not have input ".*"',
                                    step1.clean)
            self.assertRaisesRegexp(ValidationError,
                                    'Pipeline does not have input ".*"',
                                    step1.complete_clean)
            self.assertRaisesRegexp(ValidationError,
                                    'Pipeline does not have input ".*"',
                                    p.clean)

    def test_pipeline_oneStep_invalid_cabling_incorrect_cdt_clean(self):
        """Bad cabling: input is of wrong CompoundDatatype."""
        with mock_relations(Pipeline,
                            PipelineStep,
                            Method,
                            CompoundDatatype,
                            Datatype,
                            PipelineStepInputCable,
                            PipelineCable,
                            Transformation):
            del PipelineCable.pipelinestepinputcable
            p = Pipeline(family=PipelineFamily())
            p.inputs = MockSet()
            self.add_inputs(p, self.create_input(datatypes.INT_PK, dataset_idx=1))
            m = Method()
            m.inputs = MockSet()
            self.add_inputs(m, self.create_input(datatypes.STR_PK, dataset_idx=1))

            step1 = PipelineStep(pipeline=p, transformation=m, step_num=1)
            p.steps.add(step1)

            cable = PipelineStepInputCable(pipelinestep=step1,
                                           source_step=0,
                                           source=p.inputs.all()[0],
                                           dest=m.inputs.all()[0])
            cable.pipelinestepinputcable = cable
            step1.cables_in.add(cable)

            cable.clean()
            self.assertRaisesRegexp(
                ValidationError,
                'Custom wiring required for cable "{}"'.format(cable),
                cable.clean_and_completely_wired)

    def test_pipeline_oneStep_cabling_minrow_constraint_may_be_breached_clean(self):
        """ Unverifiable cabling

        Step requests input with possibly too few rows (input min_row
        unspecified).
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            cable = step1.cables_in.all()[0]
            method_input = step1.transformation.inputs[0]
            method_input.structure.min_row = 10
            method_input.dataset_name = "input"

            # It's possible this step may have too few rows
            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 1 may have too few rows",
                    cable.clean)
            # This is just to check that the above propagated up.
            self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 1 may have too few rows",
                p.clean)

    def test_pipeline_oneStep_cabling_minrow_constraints_may_breach_each_other_clean(self):
        """ Unverifiable cabling

        Step requests input with possibly too few rows (input min_row specified).
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            cable = step1.cables_in.all()[0]
            p.inputs[0].structure.min_row = 5
            method_input = step1.transformation.inputs[0]
            method_input.structure.min_row = 10
            method_input.dataset_name = "input"

            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 1 may have too few rows",
                    cable.clean)
            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 1 may have too few rows",
                    p.clean)

    def test_pipeline_oneStep_cabling_maxRow_constraints_may_be_breached_clean(self):
        """ Unverifiable cabling

        Step requests input with possibly too many rows (input max_row
        unspecified)
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            cable = step1.cables_in.all()[0]
            method_input = step1.transformation.inputs[0]
            method_input.structure.max_row = 10
            method_input.dataset_name = "input"

            # The pipeline input is unrestricted, but step 1 has max_row = 10
            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 1 may have too many rows",
                    cable.clean)
            # Check propagation of error.
            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 1 may have too many rows",
                    p.clean)

    def test_pipeline_oneStep_cabling_maxRow_constraints_may_breach_each_other_clean(self):
        """ Unverifiable cabling

        Step requests input with possibly too many rows (max_row set for
        pipeline input).
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            cable = step1.cables_in.all()[0]
            p.inputs[0].structure.max_row = 20
            method_input = step1.transformation.inputs[0]
            method_input.structure.max_row = 10
            method_input.dataset_name = "input"

            # The pipeline max_row is not good enough to guarantee correctness
            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 1 may have too many rows",
                    cable.clean)
            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 1 may have too many rows",
                    p.clean)

    def test_pipeline_oneStep_outcable_references_nonexistent_step_clean(self):
        """ Bad output cabling, request from nonexistent step. """
        with self.create_valid_pipeline() as p:
            outcable = p.outcables[0]
            outcable.source_step = 5

            self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                outcable.clean)
            # Check propagation of error.
            self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                p.clean)

    def test_pipeline_oneStep_outcable_references_invalid_output_clean(self):
        """Bad output cabling, request output not belonging to requested step"""
        with self.create_valid_pipeline() as p:
            unrelated_output = self.create_output(datatypes.STR_PK, dataset_idx=3)
            m2 = Method()
            m2.method = m2
            unrelated_output.transformation = m2
            outcable = p.outcables[0]
            outcable.source = unrelated_output

            self.assertRaisesRegexp(
                ValidationError,
                'Transformation at step 1 does not produce output ".*"',
                outcable.clean)
            self.assertRaisesRegexp(
                ValidationError,
                'Transformation at step 1 does not produce output ".*"',
                p.clean)

    def test_pipeline_oneStep_outcable_references_deleted_output_clean(self):
        """Output cabling, one-step pipeline: request deleted step output (OK)"""
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            step1.outputs_to_delete.add(step1.outputs[0])
            outcable = p.outcables[0]

            outcable.clean()
            p.clean()

    def test_pipeline_oneStep_bad_pipeline_output_indexing_clean(self):
        """Bad output cabling, one-step pipeline: output not indexed 1"""
        with self.create_valid_pipeline() as p:
            outcable = p.outcables[0]
            # Outcable references a valid step and output, but is itself badly indexed
            outcable.output_idx = 9

            outcable.clean()
            self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                p.clean)

    def test_pipeline_manySteps_valid_internal_cabling_clean(self):
        """Test good step cabling, chained-step pipeline."""
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            cable1 = step1.cables_in.all()[0]

            step2 = self.add_step(p)
            cable2 = step2.cables_in.first()

            cable1.clean()
            cable2.clean()
            step1.clean()
            step1.complete_clean()
            step2.clean()
            step2.complete_clean()
            p.clean()

    def test_pipeline_manySteps_cabling_references_invalid_output_clean(self):
        """Bad cabling: later step requests invalid input from previous."""
        with self.create_valid_pipeline() as p:
            step2 = self.add_step(p)
            cable = step2.cables_in.all()[0]

            unrelated_input = self.create_input(datatypes.STR_PK, dataset_idx=3)
            cable.source = unrelated_input

            self.assertRaisesRegexp(
                ValidationError,
                'Transformation at step 1 does not produce output ".*"',
                cable.clean)

            # Check propagation of error.
            self.assertRaisesRegexp(
                ValidationError,
                'Transformation at step 1 does not produce output ".*"',
                step2.clean)
            self.assertRaisesRegexp(
                ValidationError,
                'Transformation at step 1 does not produce output ".*"',
                p.clean)

    def test_pipeline_manySteps_cabling_references_deleted_input_clean(self):
        """Cabling: later step requests input deleted by producing step (OK)."""
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            step1.outputs_to_delete.add(step1.outputs[0])

            step2 = self.add_step(p)
            cable2 = step2.cables_in.all()[0]

            cable2.clean()
            step2.clean()
            p.clean()

    def test_pipeline_manySteps_cabling_references_incorrect_cdt_clean(self):
        """Bad cabling: later step requests input of wrong CompoundDatatype."""
        with self.create_valid_pipeline() as p:
            step2 = self.add_step(p)
            cable2 = step2.cables_in.all()[0]
            input_column = cable2.source.get_cdt().members.all()[0]
            input_column.datatype.id = datatypes.INT_PK  # should be STR_PK

            cable2.clean()
            error_msg = 'Custom wiring required for cable "{}"'.format(str(cable2))
            self.assertRaisesRegexp(ValidationError,
                                    error_msg,
                                    cable2.clean_and_completely_wired)
            self.assertRaisesRegexp(ValidationError,
                                    error_msg,
                                    step2.clean)
            self.assertRaisesRegexp(ValidationError,
                                    error_msg,
                                    p.clean)

    def test_pipeline_manySteps_minRow_constraint_may_be_breached_clean(self):
        """ Unverifiable cabling

        Later step requests input with possibly too few rows (min_row unset for
        providing step).
        """
        with self.create_valid_pipeline() as p:
            step2 = self.add_step(p)
            cable = step2.cables_in.all()[0]
            method_input = step2.transformation.inputs[0]
            method_input.structure.min_row = 10
            method_input.dataset_name = "input"

            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 2 may have too few rows",
                    cable.clean)
            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 2 may have too few rows",
                    p.clean)

    def test_pipeline_manySteps_minrow_constraints_may_breach_each_other_clean(self):
        """ Bad cabling: later step requests input with possibly too few rows.

        (providing step min_row is set)
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            step2 = self.add_step(p)
            cable = step2.cables_in.all()[0]
            prev_output = step1.transformation.outputs[0]
            prev_output.structure.min_row = 5
            method_input = step2.transformation.inputs[0]
            method_input.structure.min_row = 10
            method_input.dataset_name = "input"

            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 2 may have too few rows",
                    cable.clean)
            self.assertRaisesRegexp(
                    ValidationError,
                    "Data fed to input \"input\" of step 2 may have too few rows",
                    p.clean)

    def test_pipeline_manySteps_maxRow_constraint_may_be_breached_clean(self):
        """ Bad cabling: later step requests input with possibly too many rows.

        (max_row unset for providing step)
        """
        with self.create_valid_pipeline() as p:
            step2 = self.add_step(p)
            cable = step2.cables_in.all()[0]
            method_input = step2.transformation.inputs[0]
            method_input.structure.max_row = 100
            method_input.dataset_name = "input"

            self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 2 may have too many rows",
                cable.clean)
            self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 2 may have too many rows",
                p.clean)

    def test_pipeline_manySteps_cabling_maxRow_constraints_may_breach_each_other_clean(self):
        """ Bad cabling: later step requests input with possibly too many rows.

        (max_row for providing step is set)
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            step2 = self.add_step(p)
            cable = step2.cables_in.all()[0]
            prev_output = step1.transformation.outputs[0]
            prev_output.structure.max_row = 100
            method_input = step2.transformation.inputs[0]
            method_input.structure.max_row = 50
            method_input.dataset_name = "input"
            self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 2 may have too many rows",
                cable.clean)
            self.assertRaisesRegexp(
                ValidationError,
                "Data fed to input \"input\" of step 2 may have too many rows",
                p.clean)

    def test_pipeline_manySteps_valid_outcable_clean(self):
        """Good output cabling, chained-step pipeline."""
        with self.create_valid_pipeline() as p:
            self.add_step(p)

            outcable1, outcable2 = p.outcables.all()

            outcable1.clean()
            outcable2.clean()
            p.clean()

    def test_pipeline_manySteps_outcable_references_nonexistent_step_clean(self):
        """Bad output cabling, chained-step pipeline: request from nonexistent step"""
        with self.create_valid_pipeline() as p:
            self.add_step(p)

            outcable1, outcable2 = p.outcables.all()
            outcable1.source_step = 5

            self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                outcable1.clean)
            outcable2.clean()
            self.assertRaisesRegexp(
                ValidationError,
                "Output requested from a non-existent step",
                p.clean)

    def test_pipeline_manySteps_outcable_references_invalid_output_clean(self):
        """Bad output cabling, chained-step pipeline: request output not belonging to requested step"""
        with self.create_valid_pipeline() as p:
            self.add_step(p)

            outcable1, outcable2 = p.outcables.all()
            unrelated_output = self.create_output(datatypes.STR_PK, dataset_idx=3)
            m3 = Method()
            m3.method = m3
            unrelated_output.transformation = m3
            outcable2.source = unrelated_output

            self.assertEquals(outcable1.clean(), None)
            self.assertRaisesRegexp(
                ValidationError,
                'Transformation at step 2 does not produce output ".*"',
                outcable2.clean)
            self.assertRaisesRegexp(
                ValidationError,
                'Transformation at step 2 does not produce output ".*"',
                p.clean)

    def test_pipeline_manySteps_outcable_references_deleted_output_clean(self):
        """Output cabling, chained-step pipeline: request deleted step output (OK)"""
        with self.create_valid_pipeline() as p:
            step2 = self.add_step(p)
            step2.outputs_to_delete.add(step2.outputs[0])

            outcable1, outcable2 = p.outcables.all()

            outcable1.clean()
            outcable2.clean()
            p.clean()

    def test_pipeline_manySteps_outcable_references_invalid_output_index_clean(self):
        """Bad output cabling, chain-step pipeline: outputs not consecutively numbered starting from 1"""
        with self.create_valid_pipeline() as p:
            self.add_step(p)

            outcable1, outcable2 = p.outcables.all()
            outcable2.output_idx = 5

            outcable1.clean()
            outcable2.clean()
            self.assertRaisesRegexp(
                ValidationError,
                "Outputs are not consecutively numbered starting from 1",
                p.clean)

    def test_pipeline_with_1_step_and_2_inputs_both_cabled_good(self):
        """ Pipeline with 1 step with 2 inputs / 1 output

        Both inputs are cabled (good)
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            m = step1.transformation
            cable1 = step1.cables_in.all()[0]
            source = self.create_input(datatypes.STR_PK, dataset_idx=2)
            self.add_inputs(p, source)
            dest = self.create_input(datatypes.STR_PK, dataset_idx=2)
            self.add_inputs(m, dest)
            cable2 = PipelineStepInputCable(pipelinestep=step1,
                                            source_step=0,
                                            source=source,
                                            dest=dest)
            cable2.pipelinestepinputcable = cable2
            step1.cables_in.add(cable2)

            cable1.clean()
            cable2.clean()
            step1.clean()
            step1.complete_clean()
            p.clean()

    def test_pipeline_with_1_step_and_2_inputs_cabled_more_than_once_bad(self):
        """ Pipeline with 1 step with 2 inputs / 1 output

        input 2 is cabled twice (bad)
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            m = step1.transformation
            cable1 = step1.cables_in.all()[0]
            source = self.create_input(datatypes.STR_PK, dataset_idx=2)
            self.add_inputs(p, source)
            dest = self.create_input(datatypes.STR_PK,
                                     dataset_idx=2,
                                     dataset_name="r")
            self.add_inputs(m, dest)
            cable2 = PipelineStepInputCable(pipelinestep=step1,
                                            source_step=0,
                                            source=source,
                                            dest=dest)
            cable2.pipelinestepinputcable = cable2
            step1.cables_in.add(cable2)
            cable3 = PipelineStepInputCable(pipelinestep=step1,
                                            source_step=0,
                                            source=source,
                                            dest=dest)
            cable3.pipelinestepinputcable = cable3
            step1.cables_in.add(cable3)

            cable1.clean()
            cable2.clean()
            cable3.clean()

            self.assertRaisesRegexp(
                ValidationError,
                "Input \"r\" to transformation at step 1 is cabled more than once",
                step1.clean)
            self.assertRaisesRegexp(
                ValidationError,
                "Input \"r\" to transformation at step 1 is cabled more than once",
                step1.complete_clean)
            self.assertRaisesRegexp(
                ValidationError,
                "Input \"r\" to transformation at step 1 is cabled more than once",
                p.clean)

    def test_pipeline_with_1_step_and_2_inputs_but_only_first_input_is_cabled_in_step_1_bad(self):
        """ Pipeline with 1 step with 2 inputs / 1 output

        Only the first input is cabled (bad)
        """
        with self.create_valid_pipeline() as p:
            step1 = p.steps.all()[0]
            m = step1.transformation
            source = self.create_input(datatypes.STR_PK, dataset_idx=2)
            self.add_inputs(p, source)
            dest = self.create_input(datatypes.STR_PK,
                                     dataset_idx=2,
                                     dataset_name="r")
            self.add_inputs(m, dest)

            # Step is clean (cables are OK) but not complete (inputs not quenched).
            step1.clean()
            self.assertRaisesRegexp(
                ValidationError,
                "Input \"r\" to transformation at step 1 is not cabled",
                step1.complete_clean)

    def test_create_outputs(self):
        """
        Create outputs from output cablings; also change the output cablings
        and recreate the outputs to see if they're correct.
        """
        with mock_relations(XputStructure), self.create_valid_pipeline() as p:
            p.outcables.all()[0].output_name = 'step1_out'
            Pipeline.outputs = PropertyMock('Pipeline.outputs')
            Pipeline.outputs.create.return_value = TransformationOutput()

            p.create_outputs()

            self.assertEqual(
                [call(y=0, x=0, dataset_idx=1, dataset_name='step1_out')],
                p.outputs.create.call_args_list)
            self.assertEqual(1, XputStructure.save.call_count)  # @UndefinedVariable

    def test_create_outputs_multi_step(self):
        """Testing create_outputs with a multi-step pipeline."""
        with mock_relations(XputStructure), self.create_valid_pipeline() as p:
            self.add_step(p)
            p.outcables.all()[0].output_name = 'step1_out'
            p.outcables.all()[1].output_name = 'step2_out'
            Pipeline.outputs = PropertyMock('Pipeline.outputs')
            Pipeline.outputs.create.return_value = TransformationOutput()

            p.create_outputs()

            self.assertEqual(
                [call(y=0, x=0, dataset_idx=1, dataset_name='step1_out'),
                 call(y=0, x=0, dataset_idx=2, dataset_name='step2_out')],
                p.outputs.create.call_args_list)
            self.assertEqual(2, XputStructure.save.call_count)  # @UndefinedVariable

    @contextmanager
    def create_valid_pipeline(self):
        with mock_relations(Pipeline,
                            PipelineStep,
                            Method,
                            Transformation,
                            CompoundDatatype,
                            Datatype,
                            PipelineCable,
                            PipelineStepInputCable,
                            PipelineOutputCable):
            del Transformation.method
            del PipelineCable.pipelinestepinputcable
            p = Pipeline(family=PipelineFamily())
            p.inputs = MockSet()
            self.add_inputs(p, self.create_input(datatypes.STR_PK, dataset_idx=1))
            m = Method()
            m.inputs = MockSet()
            m.method = m
            self.add_inputs(m, self.create_input(datatypes.STR_PK, dataset_idx=1))
            self.add_outputs(m, self.create_output(datatypes.STR_PK, dataset_idx=1))

            step1 = PipelineStep(pipeline=p, transformation=m, step_num=1)
            p.steps.add(step1)

            cable = PipelineStepInputCable(pipelinestep=step1,
                                           source_step=0,
                                           source=p.inputs.all()[0],
                                           dest=m.inputs.all()[0])
            cable.pipelinestepinputcable = cable
            step1.cables_in.add(cable)

            outcable = PipelineOutputCable(
                pipeline=p,
                output_idx=1,
                source_step=1,
                source=m.outputs.all()[0],
                output_cdt=m.outputs.all()[0].get_cdt())
            p.outcables.add(outcable)

            yield p

    def add_step(self, pipeline):
        prev_step = pipeline.steps[-1]
        m = Method()
        m.method = m
        m.inputs = MockSet()
        m.outputs = MockSet()
        self.add_inputs(m, self.create_input(datatypes.STR_PK, dataset_idx=1))
        self.add_outputs(m, self.create_output(datatypes.STR_PK, dataset_idx=1))
        step = PipelineStep(pipeline=pipeline,
                            transformation=m,
                            step_num=prev_step.step_num + 1)
        pipeline.steps.add(step)

        cable = PipelineStepInputCable(
            pipelinestep=step,
            source_step=prev_step.step_num,
            source=prev_step.transformation.outputs[0],
            dest=m.inputs[0])
        cable.pipelinestepinputcable = cable
        step.cables_in = MockSet()
        step.cables_in.add(cable)
        outcable = PipelineOutputCable(
            pipeline=pipeline,
            output_idx=step.step_num,
            source_step=step.step_num,
            source=m.outputs[0],
            output_cdt=m.outputs[0].get_cdt())
        pipeline.outcables.add(outcable)

        return step

    def create_input(self, *column_datatype_ids, **kwargs):
        new_input = TransformationInput(**kwargs)
        new_input.transformationinput = new_input
        self.set_structure(new_input, column_datatype_ids)
        return new_input

    def create_output(self, *column_datatype_ids, **kwargs):
        new_output = TransformationOutput(**kwargs)
        new_output.transformationoutput = new_output
        self.set_structure(new_output, column_datatype_ids)
        return new_output

    def set_structure(self, xput, column_datatype_ids):
        if column_datatype_ids:
            cdt = CompoundDatatype()
            cdt.members = MockSet()
            for datatype_id in column_datatype_ids:
                cdt.members.add(CompoundDatatypeMember(
                    datatype=Datatype(id=datatype_id)))
            xput.structure = XputStructure(compounddatatype=cdt)

    def add_inputs(self, transformation, *inputs):
        """ Wire up the inputs to a mocked transformation.
        """
        for t_input in inputs:
            t_input.transformationinput = t_input
            t_input.transformation = transformation
            transformation.inputs.add(t_input)

    def add_outputs(self, transformation, *outputs):
        """ Wire up the outputs to a mocked transformation.
        """
        for t_output in outputs:
            t_output.transformationoutput = t_output
            t_output.transformation = transformation
            transformation.outputs.add(t_output)


class PipelineUpdateMockTests(TestCase):
    def test_no_steps(self):
        with mock_relations(Pipeline):
            pipeline = Pipeline()

            updates = pipeline.find_step_updates()

            self.assertEqual([], updates)
