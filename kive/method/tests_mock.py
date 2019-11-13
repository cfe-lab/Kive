from django.core.files.base import ContentFile
from django.core.files.uploadedfile import SimpleUploadedFile
from mock import patch

from django.core.exceptions import ValidationError
from django.core.urlresolvers import reverse
from django.test import TestCase

from constants import users, groups
from django_mock_queries.query import MockSet
from django_mock_queries.mocks import mocked_relations, PatcherChain

from container.models import Container, ContainerFamily
from kive.tests import ViewMockTestCase
from metadata.models import CompoundDatatype, KiveUser, kive_user, empty_removal_plan
# from method.forms import MethodForm
from method.models import Method, MethodFamily, CodeResourceRevision, \
    CodeResource, MethodDependency, DockerImage
from transformation.models import TransformationInput, TransformationOutput,\
    XputStructure, Transformation, TransformationXput
from django.contrib.auth.models import User, Group
from pipeline.models import Pipeline, PipelineStep, PipelineFamily

# noinspection PyUnresolvedReferences
from kive.mock_setup import convert_to_pks


@mocked_relations(Method, Transformation, TransformationXput, TransformationInput, TransformationOutput)
class MethodMockTests(TestCase):
    def test_with_family_str(self):
        """ expect "Method revision name and family name" """

        family = MethodFamily(name="Example")
        method = Method(revision_name="rounded", revision_number=3, family=family)
        self.assertEqual(str(method),
                         "Example:3 (rounded)")

    def test_without_family_str(self):
        """
        Test unicode representation when family is unset.
        """
        nofamily = Method(revision_name="foo")

        self.assertEqual(str(nofamily),
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
        parent.inputs = MockSet(name='parent.inputs',
                                model=TransformationInput)
        parent.outputs = MockSet(name='parent.outputs',
                                 model=TransformationOutput)
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

        def get_structure(xput_self):
            if xput_self.dataset_idx == 1:
                return structure
            # noinspection PyUnresolvedReferences
            raise XputStructure.DoesNotExist

        TransformationXput.structure = property(get_structure)
        expected_inputs = {inp.dataset_idx for inp in parent.inputs}
        expected_outputs = {out.dataset_idx for out in parent.outputs}

        foo = Method(revision_parent=parent)
        foo.copy_io_from_parent()

        self.assertEqual(expected_inputs,
                         {inp.dataset_idx for inp in foo.inputs})
        self.assertEqual(expected_outputs,
                         {out.dataset_idx for out in foo.outputs})
        # noinspection PyUnresolvedReferences
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
        for i in range(2):
            inp = m1.inputs.create(dataset_name='a_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m1.outputs.create(dataset_name='a_out_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        m2 = Method(revision_name='B', driver=driver, user=user)
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
        for i in range(2):
            inp = m1.inputs.create(dataset_name='a_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m1.outputs.create(dataset_name='a_out_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        m2 = Method(revision_name='B', driver=driver2, user=user)
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
        for i in range(1):
            inp = m1.inputs.create(dataset_name='a_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m1.outputs.create(dataset_name='a_out_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        m2 = Method(revision_name='B', driver=driver, user=user)
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
        for i in range(2):
            inp = m1.inputs.create(dataset_name='a_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(2):
            out = m1.outputs.create(dataset_name='a_out_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        m2 = Method(revision_name='B', driver=driver, user=user)
        for i in range(2):
            inp = m2.inputs.create(dataset_name='b_in_{}'.format(i),
                                   dataset_idx=i + 1)
            inp.transformationinput = inp
        for i in range(3):
            out = m2.outputs.create(dataset_name='b_in_{}'.format(i),
                                    dataset_idx=i + 1)
            out.transformationoutput = out

        self.assertFalse(m1.is_identical(m2))


class MethodDependencyMockTests(TestCase):
    def setUp(self):
        patcher = mocked_relations(Method, MethodDependency, Transformation)
        patcher.start()
        self.addCleanup(patcher.stop)
        driver = CodeResourceRevision(
            coderesource=CodeResource(filename='driver.py'))
        self.method = Method(driver=driver, family=MethodFamily())
        self.dependency = self.add_dependency('helper.py')

    def add_dependency(self, filename):
        helper = CodeResourceRevision(
            coderesource=CodeResource(filename=filename))
        dependency = self.method.dependencies.create(requirement=helper)
        dependency.method = self.method
        return dependency

    def test_dependency_depends_on_nothing_clean_good(self):
        self.method.dependencies.clear()

        self.method.clean()

    def test_dependency_current_folder_same_name_clean_bad(self):
        """
        A depends on B - current folder, same name
        """
        # We're giving the dependency a conflicting filename.
        self.dependency.filename = self.method.driver.coderesource.filename

        self.assertRaisesRegexp(ValidationError,
                                "Conflicting dependencies",
                                self.method.clean)

    def test_dependency_current_folder_different_name_clean_good(self):
        """
        1 depends on 2 - current folder, different name
        """
        self.dependency.filename = 'different_name.py'

        self.method.clean()

    def test_dependency_inner_folder_same_name_clean_good(self):
        """
        1 depends on 2 - different folder, same name
        """
        self.dependency.path = 'subfolder'
        self.dependency.filename = self.method.driver.coderesource.filename

        self.method.clean()

    def test_dependency_inner_folder_different_name_clean_good(self):
        """
        1 depends on 2 - different folder, different name
        """
        self.dependency.path = 'subfolder'
        self.dependency.filename = 'different_name.py'

        self.method.clean()

    def test_dependency_A_depends_BC_same_folder_no_conflicts_clean_good(self):
        """
        A depends on B, A depends on C
        BC in same folder as A
        Nothing conflicts
        """
        self.add_dependency('helper2.py')

        self.method.clean()

    def test_dependency_A_depends_BC_same_folder_B_conflicts_with_C_clean_bad(self):
        """
        A depends on B, A depends on C
        BC in same folder as A, BC conflict
        """
        self.dependency.filename = 'same_name.py'
        self.add_dependency(self.dependency.filename)

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.method.clean)

    def test_list_all_filepaths_unnested_dep_blank_filename(self):
        """
        List all filepaths when dependency has no filename set and is not in a subdirectory.
        """
        expected_filepaths = ['driver.py', 'helper.py']

        filepaths = self.method.list_all_filepaths()

        self.assertEqual(expected_filepaths, filepaths)

    def test_list_all_filepaths_nested_dep_blank_filename(self):
        """
        List all filepaths when dependency has no filename set and is in a subdirectory.
        """
        self.dependency.path = 'nest_folder'
        expected_filepaths = ['driver.py', 'nest_folder/helper.py']

        filepaths = self.method.list_all_filepaths()

        self.assertEqual(expected_filepaths, filepaths)

    def test_list_all_filepaths_unnested_dep_specified_filename(self):
        """List all filepaths when dependency has a custom filename and is not in a subdirectory.
        """
        self.dependency.filename = 'foo.py'
        expected_filepaths = ['driver.py', 'foo.py']

        filepaths = self.method.list_all_filepaths()

        self.assertEqual(expected_filepaths, filepaths)

    def test_list_all_filepaths_nested_dep_specified_filename(self):
        """
        List all filepaths when dependency has a custom filename and is in a subdirectory.
        """
        self.dependency.path = 'nest_folder'
        self.dependency.filename = 'foo.py'
        expected_filepaths = ['driver.py', 'nest_folder/foo.py']

        filepaths = self.method.list_all_filepaths()

        self.assertEqual(expected_filepaths, filepaths)


class MethodUpdateMockTests(TestCase):
    def setUp(self):
        patcher = mocked_relations(Method, MethodFamily, Transformation)
        patcher.start()
        self.addCleanup(patcher.stop)
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

    @mocked_relations(Pipeline)
    def test_find_update_not_found_from_transformation(self):
        transformation = Transformation(id=self.new_method.id)
        transformation.method = self.new_method
        update = transformation.find_update()

        self.assertEqual(update, None)


class CodeResourceViewMockTests(ViewMockTestCase):
    def setUp(self):
        super(CodeResourceViewMockTests, self).setUp()
        patcher = mocked_relations(KiveUser,
                                   CodeResource,
                                   CodeResourceRevision,
                                   User,
                                   Group)
        patcher.start()
        self.addCleanup(patcher.stop)

        # noinspection PyUnresolvedReferences
        patchers = [patch.object(CodeResource._meta,
                                 'default_manager',
                                 CodeResource.objects),
                    patch.object(CodeResourceRevision._meta,
                                 'default_manager',
                                 CodeResource.objects)]

        def dummy_save(r):
            r.id = id(r)

        # noinspection PyUnresolvedReferences
        patchers.append(patch.object(CodeResource, 'save', dummy_save))
        patcher = PatcherChain(patchers)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client = self.create_client()
        self.dev_group = Group(pk=groups.DEVELOPERS_PK)
        self.everyone = Group(pk=groups.EVERYONE_PK)
        Group.objects.add(self.dev_group, self.everyone)
        self.user = kive_user()
        self.user.groups.add(self.dev_group)
        self.content_file = ContentFile('some text', 'existing.txt')
        self.code_resource = CodeResource(pk='99',
                                          user=self.user,
                                          name='existing',
                                          filename='existing.txt')
        self.code_resource._state.adding = False

        self.other_user = User(pk=5)
        self.other_code_resource = CodeResource(pk='150', user=self.other_user)
        CodeResource.objects.add(self.code_resource, self.other_code_resource)

        self.code_resource_revision = CodeResourceRevision(
            pk='199',
            user=self.user,
            content_file=self.content_file)
        self.code_resource_revision.coderesource = self.code_resource
        self.other_code_resource_revision = CodeResourceRevision(
            pk='200',
            user=self.other_user)
        self.other_code_resource_revision.coderesource = self.other_code_resource
        self.other_code_resource.revisions.add(self.other_code_resource_revision)
        CodeResourceRevision.objects.add(self.code_resource_revision,
                                         self.other_code_resource_revision)
        k = KiveUser(pk=users.KIVE_USER_PK)
        k.groups.add(self.dev_group)
        KiveUser.objects.add(k)

    def test_resources(self):
        response = self.client.get(reverse('resources'))

        self.assertEqual(200, response.status_code)
        self.assertFalse(response.context['is_user_admin'])

    def test_resources_admin(self):
        self.user.is_staff = True

        response = self.client.get(reverse('resources'))

        self.assertEqual(200, response.status_code)
        self.assertTrue(response.context['is_user_admin'])

    def test_resource_revisions_404(self):
        response = self.client.get(reverse('resource_revisions',
                                           kwargs=dict(pk='1000')))

        self.assertEqual(404, response.status_code)

    def test_resource_revisions(self):
        response = self.client.get(reverse('resource_revisions',
                                           kwargs=dict(pk='99')))

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.code_resource, response.context['coderesource'])

    def test_resource_revisions_not_accessible(self):
        other_id = self.other_code_resource.pk
        response = self.client.get(reverse('resource_revisions',
                                           kwargs=dict(pk=other_id)))

        self.assertEqual(404, response.status_code)

    def test_resource_revisions_accessible(self):
        self.other_code_resource.groups_allowed.add(self.dev_group)
        other_id = self.other_code_resource.pk
        response = self.client.get(reverse('resource_revisions',
                                           kwargs=dict(pk=other_id)))

        self.assertEqual(200, response.status_code)
        self.assertNotIn('revisions', response.context)

    def test_resource_revisions_accessible_with_child(self):
        self.other_code_resource.groups_allowed.add(self.dev_group)
        self.other_code_resource_revision.groups_allowed.add(self.dev_group)
        other_id = self.other_code_resource.pk
        response = self.client.get(reverse('resource_revisions',
                                           kwargs=dict(pk=other_id)))

        self.assertEqual(200, response.status_code)
        self.assertIn('revisions', response.context)

    def test_resource_add(self):
        response = self.client.get(reverse('resource_add'))

        self.assertEqual(200, response.status_code)
        self.assertIn('resource_form', response.context)

    def test_resource_add_post(self):
        filename = "added.txt"
        upload_file = SimpleUploadedFile(filename, "Hello, World!".encode(encoding="utf-8"))
        response = self.client.post(
            reverse('resource_add'),
            data=dict(resource_name='hello.txt',
                      content_file=upload_file))

        self.assertEqual(302, response.status_code)
        self.assertEqual('/resources', response.url)

    def test_resource_revision_add(self):
        response = self.client.get(reverse('resource_revision_add',
                                           kwargs=dict(pk='199')))

        self.assertEqual(200, response.status_code)
        self.assertIn('revision_form', response.context)

    def test_resource_revision_add_post(self):
        filename = "added1.txt"
        upload_file = SimpleUploadedFile(filename, "Hello, World!".encode(encoding="utf-8"))
        response = self.client.post(
            reverse('resource_revision_add', kwargs=dict(pk='199')),
            data=dict(content_file=upload_file))

        self.assertEqual(302, response.status_code)
        self.assertEqual('/resources', response.url)

    def test_resource_revision_view(self):
        response = self.client.get(reverse('resource_revision_view',
                                           kwargs=dict(pk='199')))

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.code_resource_revision, response.context['revision'])


class MethodViewMockTests(ViewMockTestCase):
    def setUp(self):
        super(MethodViewMockTests, self).setUp()
        patcher = mocked_relations(KiveUser,
                                   MethodFamily,
                                   Method,
                                   CodeResource,
                                   CodeResourceRevision,
                                   CompoundDatatype,
                                   ContainerFamily,
                                   Container,
                                   Transformation,
                                   TransformationInput,
                                   TransformationOutput,
                                   User,
                                   Group)
        patcher.start()
        self.addCleanup(patcher.stop)

        # noinspection PyUnresolvedReferences
        patcher = patch.object(MethodFamily._meta,
                               'default_manager',
                               MethodFamily.objects)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client = self.create_client()
        self.dev_group = Group(pk=groups.DEVELOPERS_PK)
        self.everyone = Group(pk=groups.EVERYONE_PK)
        Group.objects.add(self.dev_group, self.everyone)
        self.user = kive_user()
        self.user.groups.add(self.dev_group)
        self.other_user = User(pk=5)

        self.method_family = MethodFamily(pk='99',
                                          user=self.user)
        MethodFamily.objects.add(self.method_family)

        self.driver = CodeResourceRevision(user=self.user)
        self.driver.pk = 1337  # needed for viewing a method
        self.driver.coderesource = CodeResource()
        self.method = Method(pk='199', user=self.user)
        self.method.driver = self.driver
        self.method.family = self.method_family
        Method.objects.add(self.method)
        KiveUser.objects.add(KiveUser(pk=users.KIVE_USER_PK))

    def test_method_families(self):
        response = self.client.get(reverse('method_families'))

        self.assertEqual(200, response.status_code)
        self.assertFalse(response.context['is_user_admin'])

    def test_method_families_admin(self):
        self.user.is_staff = True

        response = self.client.get(reverse('method_families'))

        self.assertEqual(200, response.status_code)
        self.assertTrue(response.context['is_user_admin'])

    def test_methods_404(self):
        response = self.client.get(reverse('methods',
                                           kwargs=dict(pk='1000')))

        self.assertEqual(404, response.status_code)

    def test_methods(self):
        response = self.client.get(reverse('methods',
                                           kwargs=dict(pk='99')))

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.method_family, response.context['family'])

    def test_method_view(self):
        response = self.client.get(reverse('method_view',
                                           kwargs=dict(pk='199')))

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.method, response.context['method'])

    def test_method_new(self):
        response = self.client.get(reverse('method_new'))

        self.assertEqual(200, response.status_code)
        self.assertEqual(None, response.context['family'])

    # noinspection PyUnresolvedReferences
    @patch.object(TransformationXput, 'clean', lambda xput: None)
    def test_method_new_post(self):
        response = self.client.post(
            reverse('method_new'),
            data=dict(name='foo',
                      memory='100',
                      threads='1',
                      container='20',
                      reusable='1',
                      dataset_name_in_0='in_csv',
                      compounddatatype_in_0='__raw__',
                      min_row_in_0='',
                      max_row_in_0='',
                      dataset_name_out_0='out_csv',
                      compounddatatype_out_0='__raw__',
                      min_row_out_0='',
                      max_row_out_0=''))

        expected_status = 302
        if response.status_code != expected_status:
            for key in response.context.keys():
                if key.endswith('_form'):
                    form = response.context[key]
                    self.assertEqual({}, form.errors)
        self.assertEqual(expected_status, response.status_code)
        self.assertEqual('/methods/None', response.url)

    def test_method_revise(self):
        response = self.client.get(reverse('method_revise',
                                           kwargs=dict(pk='199')))

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.method_family, response.context['family'])

    def test_method_revise_access_denied(self):
        """ Hides ungranted code revisions. """
        revision1 = CodeResourceRevision(pk=101,
                                         revision_name='alpha',
                                         revision_number=1,
                                         user=self.user)
        revision2 = CodeResourceRevision(pk=102,
                                         revision_name='beta',
                                         revision_number=2,
                                         user=self.other_user)
        self.driver.coderesource.revisions.add(revision1, revision2)

        response = self.client.get(reverse('method_revise',
                                           kwargs=dict(pk='199')))

        self.assertEqual(200, response.status_code)
        revisions = response.context['method_form']['driver_revisions']
        self.assertEqual([('101', '1: alpha')],
                         revisions.field.widget.choices)

    def test_method_revise_access_granted(self):
        """ Shows granted code revisions. """
        revision1 = CodeResourceRevision(pk=101,
                                         revision_name='alpha',
                                         revision_number=1,
                                         user=self.user)
        revision2 = CodeResourceRevision(pk=102,
                                         revision_name='beta',
                                         revision_number=2,
                                         user=self.other_user)
        revision2.users_allowed.add(self.user)
        self.driver.coderesource.revisions.add(revision1, revision2)

        response = self.client.get(reverse('method_revise',
                                           kwargs=dict(pk='199')))

        self.assertEqual(200, response.status_code)
        revisions = response.context['method_form']['driver_revisions']
        self.assertEqual([('101', '1: alpha'),
                          ('102', '2: beta')],
                         revisions.field.widget.choices)


class DockerImageViewMockTests(ViewMockTestCase):
    def setUp(self):
        super(DockerImageViewMockTests, self).setUp()
        patcher = mocked_relations(KiveUser,
                                   DockerImage,
                                   User,
                                   Group)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client = self.create_client()
        self.dev_group = Group(pk=groups.DEVELOPERS_PK)
        self.everyone = Group(pk=groups.EVERYONE_PK)
        Group.objects.add(self.dev_group, self.everyone)
        self.user = kive_user()
        self.user.groups.add(self.dev_group)
        self.other_user = User(pk=5)

        self.docker_image = DockerImage(pk='99',
                                        name='git/joe/hello',
                                        tag='v1',
                                        git='http://server1.com/joe/hello.git',
                                        user=self.user)
        DockerImage.objects.add(self.docker_image)

        KiveUser.objects.add(KiveUser(pk=users.KIVE_USER_PK))

    def test_docker_images(self):
        response = self.client.get(reverse('docker_images'))

        self.assertEqual(200, response.status_code)
        self.assertFalse(response.context['is_user_admin'])

    def test_docker_images_admin(self):
        self.user.is_staff = True

        response = self.client.get(reverse('docker_images'))

        self.assertEqual(200, response.status_code)
        self.assertTrue(response.context['is_user_admin'])

    def test_docker_image_view(self):
        response = self.client.get(reverse('docker_image_view',
                                           kwargs=dict(image_id='99')))

        self.assertEqual(200, response.status_code)
        self.assertEqual(self.docker_image, response.context['docker_image'])

    def test_docker_image_update(self):
        new_description = 'new description'
        response = self.client.post(reverse('docker_image_view',
                                            kwargs=dict(image_id='99')),
                                    data=dict(description=new_description))

        self.assertEqual(302, response.status_code)
        self.assertEqual(new_description, self.docker_image.description)

    # noinspection PyUnusedLocal
    @patch('method.models.check_output')
    def test_docker_image_add(self, mock_check_output):
        new_description = 'new description'
        expected_name = 'git/alex/howdy'
        response = self.client.post(
            reverse('docker_image_add'),
            data=dict(name=expected_name,
                      tag='v9',
                      git='http://server1.com/alex/howdy.git',
                      description=new_description))

        self.assertEqual(302, response.status_code)

    def test_docker_image_update_too_long(self):
        new_description = 'X' * 2001
        response = self.client.post(reverse('docker_image_view',
                                            kwargs=dict(image_id='99')),
                                    data=dict(description=new_description))

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {'description': ['Ensure this value has at most 2000 characters'
                             ' (it has 2001).']},
            response.context['docker_image_form'].errors)

    def test_docker_image_new(self):
        response = self.client.get(reverse('docker_image_add'))

        self.assertEqual(200, response.status_code)


@mocked_relations(DockerImage, Method, PipelineStep, Pipeline)
class DockerImageMockTests(TestCase):
    def test_build_removal_plan_for_unused_image(self):
        image = DockerImage.objects.create(id=99, name='doomed')
        DockerImage.objects.create(id=100, name='untouched')
        expected_plan = empty_removal_plan()
        expected_plan['DockerImages'].add(image)

        plan = image.build_removal_plan()

        self.assertEqual(expected_plan, plan)

    def test_build_removal_plan_for_used_image(self):
        image = DockerImage(id=99, name='doomed')
        method = image.methods.create(transformation_ptr_id=100)
        step = method.pipelinesteps.create(id=101)
        step.pipeline = Pipeline(transformation_ptr_id=102)
        step.pipeline.family = PipelineFamily()

        expected_plan = empty_removal_plan()
        expected_plan['DockerImages'].add(image)
        expected_plan['Methods'].add(method)
        expected_plan['Pipelines'].add(step.pipeline)

        plan = image.build_removal_plan()

        self.assertEqual(expected_plan, plan)
