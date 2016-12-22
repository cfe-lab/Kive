from mock import patch, MagicMock, PropertyMock
from django.contrib.auth.models import Group, Permission
from django.db.models.fields.related_descriptors import ManyToManyDescriptor
from django.test import TestCase

from kive.mock_setup import MockOneToOneMap, MockOneToManyMap, PatcherChain, mocked_relations
from django_mock_queries.query import MockSet


class DummyChildModel(object):
    def __init__(self, name):
        self.name = name

    def save(self, force_insert, using):
        pass


class DummyField(object):
    model = DummyChildModel

    def get_accessor_name(self):
        return 'dummy_field'


class DummyRelationDescriptor(object):
    example_attribute = 'Something'

    class RelatedObjectDoesNotExist(AttributeError):
        pass

    def __get__(self, instance, owner):
        if instance is None:
            return self

        raise NotImplementedError()  # database access in a real model

    def __set__(self, instance, value):
        raise NotImplementedError()  # database access in a real model


class DummyModel(object):
    shadow = DummyRelationDescriptor()
    shadow.related = DummyField()
    children = DummyRelationDescriptor()
    children.field = DummyField()


# noinspection PyUnresolvedReferences,PyStatementEffect
class MockOneToOneTests(TestCase):
    def test_not_mocked(self):
        m = DummyModel()

        with self.assertRaises(NotImplementedError):
            m.shadow

    @patch.object(DummyModel, 'shadow', MockOneToOneMap(DummyModel.shadow))
    def test_not_set(self):
        m = DummyModel()

        with self.assertRaises(DummyModel.shadow.RelatedObjectDoesNotExist):
            m.shadow

    @patch.object(DummyModel, 'shadow', MockOneToOneMap(DummyModel.shadow))
    def test_set(self):
        m = DummyModel()
        m2 = DummyModel()
        m.shadow = m2

        self.assertIs(m.shadow, m2)

    @patch.object(DummyModel, 'shadow', MockOneToOneMap(DummyModel.shadow))
    def test_set_on_individual_object(self):
        m = DummyModel()
        m2 = DummyModel()
        m.shadow = m2
        m3 = DummyModel()

        with self.assertRaises(DummyModel.shadow.RelatedObjectDoesNotExist):
            m3.shadow

    @patch.object(DummyModel, 'shadow', MockOneToOneMap(DummyModel.shadow))
    def test_delegation(self):
        self.assertEqual(DummyModel.shadow.example_attribute, 'Something')


# noinspection PyUnresolvedReferences,PyStatementEffect
class MockOneToManyTests(TestCase):
    def test_not_mocked(self):
        m = DummyModel()

        with self.assertRaises(NotImplementedError):
            m.children

    def test_mock_is_removed(self):
        m = DummyModel()

        with patch.object(DummyModel, 'children', MockOneToManyMap(DummyModel.children)):
            m.children = MockSet(DummyChildModel('Bobby'))

        with self.assertRaises(NotImplementedError):
            m.children

    @patch.object(DummyModel, 'children', MockOneToManyMap(DummyModel.children))
    def test_not_set(self):
        m = DummyModel()

        self.assertEqual(0, m.children.count())

    @patch.object(DummyModel, 'children', MockOneToManyMap(DummyModel.children))
    def test_set(self):
        m = DummyModel()
        child = DummyChildModel('Bobby')
        m.children.add(child)

        self.assertIs(m.children.first(), child)

    @patch.object(DummyModel, 'children', MockOneToManyMap(DummyModel.children))
    def test_set_on_individual_object(self):
        m = DummyModel()
        m.children.add(DummyChildModel('Suzy'))
        m3 = DummyModel()

        self.assertEqual(0, m3.children.count())

    @patch.object(DummyModel, 'children', MockOneToManyMap(DummyModel.children))
    def test_set_explicit_collection(self):
        m = DummyModel()
        m.children.add(DummyChildModel('Suzy'))

        child = DummyChildModel('Billy')
        m.children = MockSet(child)

        self.assertIs(m.children.first(), child)

    @patch.object(DummyModel, 'children', MockOneToManyMap(DummyModel.children))
    def test_create(self):
        m = DummyModel()
        child = m.children.create(name='Bobby')

        self.assertIsInstance(child, DummyChildModel)
        self.assertEqual(child.name, 'Bobby')

    @patch.object(DummyModel, 'children', MockOneToManyMap(DummyModel.children))
    def test_delegation(self):
        self.assertEqual(DummyModel.children.example_attribute, 'Something')


# noinspection PyUnusedLocal
def zero_sum(items):
    return 0


class PatcherChainTest(TestCase):
    patch_mock_max = patch('__builtin__.max')
    patch_zero_sum = patch('__builtin__.sum', zero_sum)

    @patch_zero_sum
    def test_patch_dummy(self):
        sum_result = sum([1, 2, 3])

        self.assertEqual(0, sum_result)

    @patch_mock_max
    def test_patch_mock(self, mock_max):
        mock_max.return_value = 42
        max_result = max([1, 2, 3])

        self.assertEqual(42, max_result)

    @PatcherChain([patch_zero_sum, patch_mock_max])
    def test_patch_both(self, mock_max):
        sum_result = sum([1, 2, 3])
        mock_max.return_value = 42
        max_result = max([1, 2, 3])

        self.assertEqual(0, sum_result)
        self.assertEqual(42, max_result)

    @PatcherChain([patch_mock_max, patch_zero_sum])
    def test_patch_both_reversed(self, mock_max):
        sum_result = sum([1, 2, 3])
        mock_max.return_value = 42
        max_result = max([1, 2, 3])

        self.assertEqual(0, sum_result)
        self.assertEqual(42, max_result)

    @PatcherChain([patch_mock_max], pass_mocks=False)
    def test_mocks_not_passed(self):
        """ Create a new mock, but don't pass it to the test method. """

    def test_context_manager(self):
        with PatcherChain([PatcherChainTest.patch_mock_max,
                           PatcherChainTest.patch_zero_sum]) as mocks:
            sum_result = sum([1, 2, 3])
            mocks[0].return_value = 42
            max_result = max([1, 2, 3])

            self.assertEqual(0, sum_result)
            self.assertEqual(42, max_result)
            self.assertEqual(2, len(mocks))
            self.assertIs(zero_sum, mocks[1])

    def test_start(self):
        patcher = PatcherChain([PatcherChainTest.patch_mock_max,
                                PatcherChainTest.patch_zero_sum])
        mocks = patcher.start()
        self.addCleanup(patcher.stop)

        sum_result = sum([1, 2, 3])
        mocks[0].return_value = 42
        max_result = max([1, 2, 3])

        self.assertEqual(0, sum_result)
        self.assertEqual(42, max_result)
        self.assertEqual(2, len(mocks))
        self.assertIs(zero_sum, mocks[1])


@PatcherChain([patch('__builtin__.max'), patch('__builtin__.sum', zero_sum)],
              pass_mocks=False)
class PatcherChainOnClassTest(TestCase):
    def test_patch_dummy(self):
        sum_result = sum([1, 2, 3])

        self.assertEqual(0, sum_result)

    def test_patch_mock(self):
        max_result = max([1, 2, 3])

        self.assertIsInstance(max_result, MagicMock)

    def test_patch_both(self):
        sum_result = sum([1, 2, 3])
        max_result = max([1, 2, 3])

        self.assertEqual(0, sum_result)
        self.assertIsInstance(max_result, MagicMock)


class MockedRelationsTest(TestCase):
    @mocked_relations(Group)
    def test_decorator(self):
        group = Group()

        self.assertEqual(0, group.permissions.count())
        group.permissions.add(Permission())
        self.assertEqual(1, group.permissions.count())

    def test_context_manager(self):
        group = Group()
        with mocked_relations(Group):
            self.assertEqual(0, group.permissions.count())
            group.permissions.add(Permission())
            self.assertEqual(1, group.permissions.count())

    def test_reusing_patcher(self):
        patcher = mocked_relations(Group)
        with patcher:
            self.assertEqual(0, Group.objects.count())
            Group.objects.add(Group())
            self.assertEqual(1, Group.objects.count())

        with patcher:
            self.assertEqual(0, Group.objects.count())
            Group.objects.add(Group())
            self.assertEqual(1, Group.objects.count())

    @mocked_relations(Group)
    def test_relation_with_garbage_collection(self):
        self.longMessage = True
        for group_index in range(10):
            group = Group()
            self.assertEqual(0,
                             group.permissions.count(),
                             'group_index: {}'.format(group_index))
            group.permissions.add(Permission())
            self.assertEqual(1, group.permissions.count())
            del group

    def test_replaces_other_mocks(self):
        self.assertIsInstance(Group.permissions, ManyToManyDescriptor)

        with mocked_relations(Group):
            Group.permissions = PropertyMock('Group.permissions')

        self.assertIsInstance(Group.permissions, ManyToManyDescriptor)
