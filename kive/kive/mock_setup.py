from collections import defaultdict
from functools import partial
from itertools import chain
from mock import Mock, MagicMock, patch

from django_mock_queries.query import MockSet


class MockOneToManyMap(object):
    def __init__(self, original):
        """ Wrap a mock mapping around the original one-to-many relation. """
        self.map = defaultdict(partial(MockSet, cls=original.field.model))
        self.original = original

    def __get__(self, instance, owner):
        """ Look in the map to see if there is a related set.

        If not, create a new set.
        """

        if instance is None:
            # Call was to the class, not an object.
            return self

        return self.map[id(instance)]

    def __set__(self, instance, value):
        """ Set a related object for an instance. """

        self.map[id(instance)] = value

    def __getattr__(self, name):
        """ Delegate all other calls to the original. """

        return getattr(self.original, name)


class MockOneToOneMap(object):
    def __init__(self, original):
        """ Wrap a mock mapping around the original one-to-one relation. """
        self.map = {}
        self.original = original

    def __get__(self, instance, owner):
        """ Look in the map to see if there is a related object.

        If not (the default) raise the expected exception.
        """

        if instance is None:
            # Call was to the class, not an object.
            return self

        rel_obj = self.map.get(id(instance))
        if rel_obj is None:
            raise self.original.RelatedObjectDoesNotExist(
                "Mock %s has no %s." % (
                    owner.__name__,
                    self.original.related.get_accessor_name()
                )
            )
        return rel_obj

    def __set__(self, instance, value):
        """ Set a related object for an instance. """

        self.map[id(instance)] = value

    def __getattr__(self, name):
        """ Delegate all other calls to the original. """

        return getattr(self.original, name)


# noinspection PyUnresolvedReferences
def setup_mock_relations(testcase, *models):
    for model in models:
        if isinstance(model.save, MagicMock):
            # already mocked, so skip it
            continue
        model_name = model._meta.object_name
        patcher = patch.object(model, 'save', Mock(name=model_name + '.save'))
        patcher.start()
        testcase.addCleanup(patcher.stop)
        patcher = patch.object(model, 'objects', MockSet(mock_name=model_name + '.objects', cls=model))
        patcher.start()
        testcase.addCleanup(patcher.stop)
        for related_object in chain(model._meta.related_objects,
                                    model._meta.many_to_many):
            name = related_object.name
            if name in model.__dict__:
                # Only mock direct relations, not inherited ones.
                old_relation = getattr(model, name, None)
                if old_relation is not None:
                    # type_name = type(old_relation).__name__
                    # expected_types = {'ReverseManyToOneDescriptor',
                    #                   'ManyToManyDescriptor',
                    #                   'ReverseOneToOneDescriptor'}
                    # assert type_name in expected_types, model_name + '.' + name + ': ' + type_name
                    if related_object.one_to_one:
                        new_relation = MockOneToOneMap(old_relation)
                    else:
                        new_relation = MockOneToManyMap(old_relation)
                    patcher = patch.object(model, name, new_relation)
                    patcher.start()
                    testcase.addCleanup(patcher.stop)


def mocked_relations(*models):
    """ Mock all related field managers to make pure unit tests possible.

    This can decorate a method or a class. Decorating a class is equivalent to
    decorating all the methods whose names start with "test_".

    @mocked_relations(Dataset):
    def test_dataset(self):
        dataset = Dataset()
        check = dataset.content_checks.create()  # returns a ContentCheck object
    """
    def decorator(target):
        if isinstance(target, type):
            original_setup = target.setUp

            def full_setup(testcase):
                setup_mock_relations(testcase, *models)
                original_setup(testcase)

            target.setUp = full_setup
            return target

        def wrapped(testcase, *args, **kwargs):
            setup_mock_relations(testcase, *models)
            return target(testcase, *args, **kwargs)
        return wrapped
    return decorator
