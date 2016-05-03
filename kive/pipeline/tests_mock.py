import re
from unittest import TestCase

from django.core.exceptions import ValidationError
from django_mock_queries.query import MockSet

from kive.mock_setup import mock_relations  # Import before any Django models
from constants import datatypes
from metadata.models import CompoundDatatype, CompoundDatatypeMember, Datatype
from method.models import Method
from pipeline.models import Pipeline, PipelineFamily, PipelineStep,\
    PipelineStepInputCable
from transformation.models import TransformationInput, XputStructure


class PipelineMockTests(TestCase):
    """Tests for basic Pipeline functionality."""
    def test_pipeline_no_inputs_no_steps(self):
        """A Pipeline with no inputs and no steps is clean but not complete."""
        with mock_relations(Pipeline):
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
        with mock_relations(Pipeline):
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
        with mock_relations(Pipeline):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p, TransformationInput(dataset_idx=4))

            error = "Inputs are not consecutively numbered starting from 1"
            self.assertRaisesRegexp(ValidationError, error, p.clean)
            self.assertRaisesRegexp(ValidationError, error, p.complete_clean)

    def test_pipeline_many_valid_inputs_clean(self):
        """A Pipeline with multiple, properly indexed inputs is clean."""
        with mock_relations(Pipeline):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1),
                            TransformationInput(dataset_idx=2),
                            TransformationInput(dataset_idx=3))

            p.clean()

            p.inputs.order_by.assert_called_once_with('dataset_idx')

    def test_pipeline_many_invalid_inputs_clean(self):
        """A Pipeline with multiple, badly indexed inputs is not clean."""
        with mock_relations(Pipeline):
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
        with mock_relations(Pipeline, PipelineStep, Method):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1))
            m = Method()
            self.add_inputs(m,
                            TransformationInput(dataset_idx=1,
                                                dataset_name="input"))
            step1 = PipelineStep(pipeline=p, transformation=m, step_num=1)
            p.steps.add(step1)
            p.steps.order_by = p.steps.all

            error = 'Input "input" to transformation at step 1 is not cabled'
            step1.clean()
            self.assertRaisesRegexp(ValidationError, error, step1.complete_clean)
            p.clean()
            self.assertRaisesRegexp(ValidationError, error, p.complete_clean)

    def test_pipeline_one_bad_step_clean(self):
        """Test step index check, one badly-indexed step case."""
        with mock_relations(Pipeline, PipelineStep, Method):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1))
            m = Method()
            self.add_inputs(m,
                            TransformationInput(dataset_idx=1))
            step1 = PipelineStep(pipeline=p, transformation=m, step_num=10)
            p.steps.add(step1)
            p.steps.order_by = p.steps.all

            self.assertRaisesRegexp(
                    ValidationError,
                    "Steps are not consecutively numbered starting from 1",
                    p.clean)

    def test_pipeline_many_valid_steps_clean(self):
        """Test step index check, well-indexed multi-step case."""
        with mock_relations(Pipeline, PipelineStep, Method):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1))
            m = Method()
            self.add_inputs(m,
                            TransformationInput(dataset_idx=1))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=1))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=2))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=3))
            p.steps.order_by.return_value = p.steps.all()

            p.clean()
            p.steps.order_by.assert_called_once_with('step_num')

    def test_pipeline_many_invalid_steps_clean(self):
        """Test step index check, badly-indexed multi-step case."""
        with mock_relations(Pipeline, PipelineStep, Method):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1))
            m = Method()
            self.add_inputs(m,
                            TransformationInput(dataset_idx=1))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=1))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=4))
            p.steps.add(PipelineStep(pipeline=p, transformation=m, step_num=5))
            p.steps.order_by = p.steps.all

            self.assertRaisesRegexp(
                    ValidationError,
                    "Steps are not consecutively numbered starting from 1",
                    p.clean)

    def test_pipeline_one_step_valid_cabling_clean(self):
        """Test good step cabling, one-step pipeline."""
        with mock_relations(Pipeline, PipelineStep, Method):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1))
            m = Method()
            self.add_inputs(m,
                            TransformationInput(dataset_idx=1))
            step1 = PipelineStep(pipeline=p, transformation=m, step_num=1)
            p.steps.add(step1)
            p.steps.order_by = p.steps.all

            cable = PipelineStepInputCable(pipelinestep=step1,
                                           source_step=0,
                                           source=p.inputs.all()[0],
                                           dest=m.inputs.all()[0])
            cable.pipelinestepinputcable = cable
            step1.cables_in.add(cable)

            cable.clean()
            step1.clean()
            step1.complete_clean()
            p.clean()
            p.complete_clean()

    def test_pipeline_oneStep_invalid_cabling_invalid_pipeline_input_clean(self):
        """Bad cabling: step looks for input that does not belong to the pipeline."""
        with mock_relations(Pipeline, PipelineStep, Method):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p,
                            TransformationInput(dataset_idx=1))
            m = Method()
            self.add_inputs(m,
                            TransformationInput(dataset_idx=1))
            step1 = PipelineStep(pipeline=p, transformation=m, step_num=1)
            p.steps.add(step1)
            p.steps.order_by = p.steps.all
            unrelated_input = TransformationInput(dataset_idx=3)
            unrelated_input.transformationinput = unrelated_input

            cable = PipelineStepInputCable(pipelinestep=step1,
                                           source_step=0,
                                           source=unrelated_input,
                                           dest=m.inputs.all()[0])
            cable.pipelinestepinputcable = cable
            step1.cables_in.add(cable)

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
                            Datatype):
            p = Pipeline(family=PipelineFamily())
            self.add_inputs(p, self.create_input(datatypes.INT_PK, dataset_idx=1))
            m = Method()
            self.add_inputs(m, self.create_input(datatypes.STR_PK, dataset_idx=1))

            step1 = PipelineStep(pipeline=p, transformation=m, step_num=1)
            p.steps.add(step1)
            p.steps.order_by = p.steps.all

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

    def create_input(self, *column_datatype_ids, **kwargs):
        new_input = TransformationInput(**kwargs)
        if column_datatype_ids:
            cdt = CompoundDatatype()
            cdt.members = MockSet()
            for datatype_id in column_datatype_ids:
                cdt.members.add(CompoundDatatypeMember(
                    datatype=Datatype(id=datatype_id)))
            cdt.members.order_by = cdt.members.all
            new_input.structure = XputStructure(compounddatatype=cdt)
        return new_input

    def add_inputs(self, transformation, *inputs):
        """ Wire up the inputs to a mocked transformation.

        Also make order_by() return them in the given order.
        """
        for t_input in inputs:
            t_input.transformationinput = t_input
            transformation.inputs.add(t_input)
        transformation.inputs.order_by.return_value = inputs


class PipelineUpdateMockTests(TestCase):
    def test_no_steps(self):
        with mock_relations(Pipeline):
            pipeline = Pipeline()

            updates = pipeline.find_step_updates()

            self.assertEqual([], updates)
