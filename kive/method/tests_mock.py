from unittest.case import TestCase

from django.core.exceptions import ValidationError

from kive.mock_setup import mocked_relations  # Import before any Django models
from django_mock_queries.query import MockSet
from metadata.models import CompoundDatatype
from method.models import Method, MethodFamily, CodeResourceRevision,\
    CodeResource
from transformation.models import TransformationInput, TransformationOutput,\
    XputStructure, Transformation
from django.contrib.auth.models import User
from pipeline.models import Pipeline


@mocked_relations(Method, Transformation, TransformationInput, TransformationOutput)
class MethodMockTests(TestCase):
    def test_with_family_unicode(self):
        """ expect "Method revision name and family name" """

        family = MethodFamily(name="Example")
        method = Method(revision_name="rounded", revision_number=3, family=family)
        self.assertEqual(unicode(method),
                         "Example:3 (rounded)")

    def test_without_family_unicode(self):
        """
        unicode() for Test unicode representation when family is unset.
        """
        nofamily = Method(revision_name="foo")

        self.assertEqual(unicode(nofamily),
                         "[family unset]:None (foo)")

    def test_display_name(self):
        method = Method(revision_number=1, revision_name='Example')

        self.assertEqual(method.display_name, '1: Example')

    def test_display_name_without_revision_name(self):
        method = Method(revision_number=1)

        self.assertEqual(method.display_name, '1: ')

    def test_no_inputs_checkInputIndices_good(self):
        """
        Method with no inputs defined should have
        check_input_indices() return with no exception.
        """

        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())

        # check_input_indices() should not raise a ValidationError
        foo.check_input_indices()
        foo.clean()

    def test_single_valid_input_checkInputIndices_good(self):
        """
        Method with a single, 1-indexed input should have
        check_input_indices() return with no exception.
        """

        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        inp = foo.inputs.create(dataset_idx=1)
        inp.transformationinput = inp

        # check_input_indices() should not raise a ValidationError
        foo.check_input_indices()
        foo.clean()

    def test_many_ordered_valid_inputs_checkInputIndices_good(self):
        """
        Test check_input_indices on a method with several inputs,
        correctly indexed and in order.
        """

        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        for i in range(3):
            inp = foo.inputs.create(dataset_idx=i + 1)
            inp.transformationinput = inp

        # check_input_indices() should not raise a ValidationError
        foo.check_input_indices()
        foo.clean()

    def test_many_valid_inputs_scrambled_checkInputIndices_good(self):
        """
        Test check_input_indices on a method with several inputs,
        correctly indexed and in scrambled order.
        """

        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        for i in (3, 1, 2):
            inp = foo.inputs.create(dataset_idx=i)
            inp.transformationinput = inp

        # check_input_indices() should not raise a ValidationError
        foo.check_input_indices()
        foo.clean()

    def test_one_invalid_input_checkInputIndices_bad(self):
        """
        Test input index check, one badly-indexed input case.
        """

        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        inp = foo.inputs.create(dataset_idx=4)
        inp.transformationinput = inp

        # check_input_indices() should raise a ValidationError
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            foo.check_input_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_many_nonconsective_inputs_scrambled_checkInputIndices_bad(self):
        """Test input index check, badly-indexed multi-input case."""
        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        for i in (2, 6, 1):
            inp = foo.inputs.create(dataset_idx=i)
            inp.transformationinput = inp

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            foo.check_input_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_no_outputs_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())

        foo.check_output_indices()
        foo.clean()

    def test_one_valid_output_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        out = foo.outputs.create(dataset_idx=1)
        out.transformationoutput = out

        foo.check_output_indices()
        foo.clean()

    def test_many_valid_outputs_scrambled_checkOutputIndices_good(self):
        """Test output index check, well-indexed multi-output (scrambled order) case."""
        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        for i in (3, 1, 2):
            out = foo.outputs.create(dataset_idx=i)
            out.transformationoutput = out

        foo.check_output_indices()
        foo.clean()

    def test_one_invalid_output_checkOutputIndices_bad(self):
        """Test output index check, one badly-indexed output case."""
        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        out = foo.outputs.create(dataset_idx=4)
        out.transformationoutput = out

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.check_output_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_many_invalid_outputs_scrambled_checkOutputIndices_bad(self):
        """Test output index check, badly-indexed multi-output case."""
        driver = CodeResourceRevision(coderesource=CodeResource())

        foo = Method(driver=driver, family=MethodFamily())
        for i in (2, 6, 1):
            out = foo.outputs.create(dataset_idx=i)
            out.transformationoutput = out

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.check_output_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.clean)

    def create_parent(self):
        parent = Method()
        parent.inputs = MockSet(name='parent.inputs', cls=TransformationInput)
        parent.outputs = MockSet(name='parent.outputs', cls=TransformationOutput)
        for i in range(2):
            inp = parent.inputs.create(dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = parent.outputs.create(dataset_idx=i + 1)
            out.transformationoutput = out
        return parent

    def test_copy_io_from_parent(self):
        parent = self.create_parent()
        expected_inputs = {inp.dataset_idx for inp in parent.inputs}
        expected_outputs = {out.dataset_idx for out in parent.outputs}

        foo = Method(revision_parent=parent)
        foo.copy_io_from_parent()

        self.assertEqual(expected_inputs,
                         {inp.dataset_idx for inp in foo.inputs})
        self.assertEqual(expected_outputs,
                         {out.dataset_idx for out in foo.outputs})

    def test_copy_io_from_no_parent(self):
        foo = Method()
        foo.copy_io_from_parent()

        self.assertEqual(set(),
                         {inp.dataset_idx for inp in foo.inputs})
        self.assertEqual(set(),
                         {out.dataset_idx for out in foo.outputs})

    def test_copy_io_from_parent_does_not_replace_inputs(self):
        parent = self.create_parent()

        foo = Method(revision_parent=parent)
        foo.inputs.create(dataset_idx=1)

        expected_inputs = {inp.dataset_idx for inp in foo.inputs}
        expected_outputs = {out.dataset_idx for out in foo.outputs}

        foo.copy_io_from_parent()

        self.assertEqual(expected_inputs,
                         {inp.dataset_idx for inp in foo.inputs})
        self.assertEqual(expected_outputs,
                         {out.dataset_idx for out in foo.outputs})

    def test_copy_io_from_parent_does_not_replace_outputs(self):
        parent = self.create_parent()

        foo = Method(revision_parent=parent)
        foo.outputs.create(dataset_idx=1)

        expected_inputs = {inp.dataset_idx for inp in foo.inputs}
        expected_outputs = {out.dataset_idx for out in foo.outputs}

        foo.copy_io_from_parent()

        self.assertEqual(expected_inputs,
                         {inp.dataset_idx for inp in foo.inputs})
        self.assertEqual(expected_outputs,
                         {out.dataset_idx for out in foo.outputs})

    @mocked_relations(XputStructure, CompoundDatatype)
    def test_copy_io_from_parent_with_structure(self):
        cdt = CompoundDatatype()
        min_row = 1
        max_row = 100
        structure = XputStructure(compounddatatype=cdt,
                                  min_row=min_row,
                                  max_row=max_row)
        parent = self.create_parent()

        def get_input_structure(self):
            if self.dataset_idx == 2:
                return structure
            raise XputStructure.DoesNotExist

        def get_output_structure(self):
            if self.dataset_idx == 1:
                return structure
            raise XputStructure.DoesNotExist

        TransformationInput.structure = property(get_input_structure)
        TransformationOutput.structure = property(get_output_structure)
        expected_inputs = {inp.dataset_idx for inp in parent.inputs}
        expected_outputs = {out.dataset_idx for out in parent.outputs}

        foo = Method(revision_parent=parent)
        foo.copy_io_from_parent()

        self.assertEqual(expected_inputs,
                         {inp.dataset_idx for inp in foo.inputs})
        self.assertEqual(expected_outputs,
                         {out.dataset_idx for out in foo.outputs})
        create_args = XputStructure.objects.create.call_args_list  # @UndefinedVariable
        self.assertEqual(2, len(create_args))
        _args, kwargs = create_args[0]
        self.assertEqual(100, kwargs['max_row'])

    def test_identical_self(self):
        """A Method should be identical to itself."""
        m = Method(driver=CodeResourceRevision(), user=User())
        self.assertTrue(m.is_identical(m))

    def test_identical(self):
        driver = CodeResourceRevision()
        user = User()
        m1 = Method(revision_name='A', driver=driver, user=user)
        m1.inputs = MockSet(cls=TransformationInput)
        m1.outputs = MockSet(cls=TransformationOutput)
        for i in range(2):
            inp = m1.inputs.create(dataset_name='a_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m1.outputs.create(dataset_name='a_out_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        m2 = Method(revision_name='B', driver=driver, user=user)
        m2.inputs = MockSet(cls=TransformationInput)
        m2.outputs = MockSet(cls=TransformationOutput)
        for i in range(2):
            inp = m2.inputs.create(dataset_name='b_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m2.outputs.create(dataset_name='b_in_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        self.assertTrue(m1.is_identical(m2))

    def test_identical_when_drivers_unmatched(self):
        driver1 = CodeResourceRevision()
        driver2 = CodeResourceRevision()
        user = User()
        m1 = Method(revision_name='A', driver=driver1, user=user)
        m1.inputs = MockSet(cls=TransformationInput)
        m1.outputs = MockSet(cls=TransformationOutput)
        for i in range(2):
            inp = m1.inputs.create(dataset_name='a_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m1.outputs.create(dataset_name='a_out_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        m2 = Method(revision_name='B', driver=driver2, user=user)
        m2.inputs = MockSet(cls=TransformationInput)
        m2.outputs = MockSet(cls=TransformationOutput)
        for i in range(2):
            inp = m2.inputs.create(dataset_name='b_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m2.outputs.create(dataset_name='b_in_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        self.assertFalse(m1.is_identical(m2))

    def test_identical_when_inputs_unmatched(self):
        driver = CodeResourceRevision()
        user = User()
        m1 = Method(revision_name='A', driver=driver, user=user)
        m1.inputs = MockSet(cls=TransformationInput)
        m1.outputs = MockSet(cls=TransformationOutput)
        for i in range(1):
            inp = m1.inputs.create(dataset_name='a_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m1.outputs.create(dataset_name='a_out_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        m2 = Method(revision_name='B', driver=driver, user=user)
        m2.inputs = MockSet(cls=TransformationInput)
        m2.outputs = MockSet(cls=TransformationOutput)
        for i in range(2):
            inp = m2.inputs.create(dataset_name='b_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m2.outputs.create(dataset_name='b_in_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        self.assertFalse(m1.is_identical(m2))

    def test_identical_when_outputs_unmatched(self):
        driver = CodeResourceRevision()
        user = User()
        m1 = Method(revision_name='A', driver=driver, user=user)
        m1.inputs = MockSet(cls=TransformationInput)
        m1.outputs = MockSet(cls=TransformationOutput)
        for i in range(2):
            inp = m1.inputs.create(dataset_name='a_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(2):
            out = m1.outputs.create(dataset_name='a_out_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        m2 = Method(revision_name='B', driver=driver, user=user)
        m2.inputs = MockSet(cls=TransformationInput)
        m2.outputs = MockSet(cls=TransformationOutput)
        for i in range(2):
            inp = m2.inputs.create(dataset_name='b_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m2.outputs.create(dataset_name='b_in_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        self.assertFalse(m1.is_identical(m2))


@mocked_relations(Method, MethodFamily)
class MethodUpdateMockTests(TestCase):
    def setUp(self):
        self.family = MethodFamily()
        self.old_method = self.family.members.create(family=self.family,
                                                     revision_number=1,
                                                     id=101)
        self.old_method.method = self.old_method

        self.new_method = self.family.members.create(family=self.family,
                                                     revision_number=2,
                                                     id=102)
        self.new_method.method = self.new_method

    def test_find_update_not_found(self):
        update = self.new_method.find_update()

        self.assertEqual(None, update)

    def test_find_update(self):
        update = self.old_method.find_update()

        self.assertEqual(self.new_method, update)

    @mocked_relations(Pipeline, Transformation)
    def test_find_update_not_found_from_transformation(self):
        del Transformation.method
        transformation = Transformation(id=self.new_method.id)
        transformation.method = self.new_method
        update = transformation.find_update()

        self.assertEqual(update, None)
