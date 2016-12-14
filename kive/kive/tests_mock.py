from unittest.case import TestCase

from mock import patch

from kive.mock_setup import MockOneToOneMap, MockOneToManyMap
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
