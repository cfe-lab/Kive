import weakref
from functools import partial
from itertools import chain

from django.db import NotSupportedError

from django_mock_queries.constants import COMPARISON_EXACT, COMPARISON_IEXACT, COMPARISON_CONTAINS, COMPARISON_ICONTAINS, \
    COMPARISON_GT, COMPARISON_GTE, COMPARISON_LT, COMPARISON_LTE, COMPARISON_IN, COMPARISON_ISNULL
from mock import Mock, patch, MagicMock

from django_mock_queries.query import MockSet
import django_mock_queries.query
import django_mock_queries.utils


OriginalMockSet = MockSet


# These are some temporary patches to add model and __len__ attributes
# until we create a pull request for them.
# noinspection PyPep8Naming
def MockSet(*args, **kwargs):
    cls = kwargs.get('cls')
    mock_set = OriginalMockSet(*args, **kwargs)
    if cls is not None:
        mock_set.model = cls

    def values(*fields):
        if not fields:
            raise NotSupportedError('All values not supported.')
        value_list = [{f: getattr(item, f) for f in fields}
                      for item in mock_set]
        return MockSet(*value_list)
    mock_set.values = MagicMock(side_effect=values)

    def mock_length(m):
        i = -1
        for i, _ in enumerate(m):
            pass
        return i + 1
    mock_set.__len__ = mock_length
    return mock_set

django_mock_queries.query.MockSet = MockSet


def is_match(first, second, comparison=None):
    if isinstance(first, django_mock_queries.query.MockBase):
        return any(is_match(item, second, comparison)
                   for item in first)
    if (isinstance(first, (int, str)) and
            isinstance(second, django_mock_queries.query.MockBase)):
        try:
            second = [item.pk for item in second]
        except AttributeError:
            pass  # Didn't have pk's, keep original items
    if not comparison:
        return first == second
    return {
        COMPARISON_EXACT: lambda: first == second,
        COMPARISON_IEXACT: lambda: first.lower() == second.lower(),
        COMPARISON_CONTAINS: lambda: second in first,
        COMPARISON_ICONTAINS: lambda: second.lower() in first.lower(),
        COMPARISON_GT: lambda: first > second,
        COMPARISON_GTE: lambda: first >= second,
        COMPARISON_LT: lambda: first < second,
        COMPARISON_LTE: lambda: first <= second,
        COMPARISON_IN: lambda: first in second,
        COMPARISON_ISNULL: lambda: (first is None) == bool(second),
    }[comparison]()

django_mock_queries.utils.is_match = is_match


# This has all been submitted in a pull request:
# https://github.com/stphivos/django-mock-queries/pull/28
class MockOneToManyMap(object):
    def __init__(self, original):
        """ Wrap a mock mapping around the original one-to-many relation. """
        self.map = {}
        self.original = original

    def __get__(self, instance, owner):
        """ Look in the map to see if there is a related set.

        If not, create a new set.
        """

        if instance is None:
            # Call was to the class, not an object.
            return self

        instance_id = id(instance)
        entry = self.map.get(instance_id)
        old_instance = related_objects = None
        if entry is not None:
            old_instance_weak, related_objects = entry
            old_instance = old_instance_weak()
        if entry is None or old_instance is None:
            related = getattr(self.original, 'related', self.original)
            related_objects = MockSet(cls=related.field.model)
            self.__set__(instance, related_objects)

        return related_objects

    def __set__(self, instance, value):
        """ Set a related object for an instance. """

        self.map[id(instance)] = (weakref.ref(instance), value)

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

        entry = self.map.get(id(instance))
        old_instance = related_object = None
        if entry is not None:
            old_instance_weak, related_object = entry
            old_instance = old_instance_weak()
        if entry is None or old_instance is None:
            raise self.original.RelatedObjectDoesNotExist(
                "Mock %s has no %s." % (
                    owner.__name__,
                    self.original.related.get_accessor_name()
                )
            )
        return related_object

    def __set__(self, instance, value):
        """ Set a related object for an instance. """

        self.map[id(instance)] = (weakref.ref(instance), value)

    def __getattr__(self, name):
        """ Delegate all other calls to the original. """

        return getattr(self.original, name)


def find_all_models(models):
    """ Yield all models and their parents. """
    for model in models:
        yield model
        # noinspection PyProtectedMember
        for parent in model._meta.parents.keys():
            for parent_model in find_all_models((parent,)):
                yield parent_model


# noinspection PyProtectedMember
def mocked_relations(*models):
    """ Mock all related field managers to make pure unit tests possible.

    The resulting patcher can be used just like one from the mock module:
    As a test method decorator, a test class decorator, a context manager,
    or by just calling start() and stop().

    @mocked_relations(Dataset):
    def test_dataset(self):
        dataset = Dataset()
        check = dataset.content_checks.create()  # returns a ContentCheck object
    """
    # noinspection PyUnresolvedReferences
    patch_object = patch.object
    patchers = []
    for model in find_all_models(models):
        if isinstance(model.save, Mock):
            # already mocked, so skip it
            continue
        model_name = model._meta.object_name
        patchers.append(patch_object(model, 'save', new_callable=partial(
            Mock,
            name=model_name + '.save')))
        if hasattr(model, 'objects'):
            patchers.append(patch_object(model, 'objects', new_callable=partial(
                MockSet,
                mock_name=model_name + '.objects',
                cls=model)))
        for related_object in chain(model._meta.related_objects,
                                    model._meta.many_to_many):
            name = related_object.name
            if name not in model.__dict__ and related_object.one_to_many:
                name += '_set'
            if name in model.__dict__:
                # Only mock direct relations, not inherited ones.
                old_relation = getattr(model, name, None)
                if old_relation is not None:
                    if related_object.one_to_one:
                        new_callable = partial(MockOneToOneMap, old_relation)
                    else:
                        new_callable = partial(MockOneToManyMap, old_relation)
                    patchers.append(patch_object(model,
                                                 name,
                                                 new_callable=new_callable))
    return PatcherChain(patchers, pass_mocks=False)


class PatcherChain(object):
    """ Chain a list of mock patchers into one.

    The resulting patcher can be used just like one from the mock module:
    As a test method decorator, a test class decorator, a context manager,
    or by just calling start() and stop().
    """
    def __init__(self, patchers, pass_mocks=True):
        """ Initialize a patcher.

        :param patchers: a list of patchers that should all be applied
        :param pass_mocks: True if any mock objects created by the patchers
        should be passed to any decorated test methods.
        """
        self.patchers = patchers
        self.pass_mocks = pass_mocks

    def __call__(self, func):
        if isinstance(func, type):
            return self.decorate_class(func)
        return self.decorate_callable(func)

    def decorate_class(self, cls):
        for attr in dir(cls):
            # noinspection PyUnresolvedReferences
            if not attr.startswith(patch.TEST_PREFIX):
                continue

            attr_value = getattr(cls, attr)
            if not hasattr(attr_value, "__call__"):
                continue

            setattr(cls, attr, self(attr_value))
        return cls

    def decorate_callable(self, target):
        """ Called as a decorator. """

        # noinspection PyUnusedLocal
        def absorb_mocks(test_case, *args):
            return target(test_case)

        should_absorb = not (self.pass_mocks or isinstance(target, type))
        result = absorb_mocks if should_absorb else target
        for patcher in self.patchers:
            result = patcher(result)
        return result

    def __enter__(self):
        """ Starting a context manager.

        All the patched objects are passed as a list to the with statement.
        """
        return [patcher.__enter__() for patcher in self.patchers]

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Ending a context manager. """
        for patcher in self.patchers:
            patcher.__exit__(exc_type, exc_val, exc_tb)

    def start(self):
        return [patcher.start() for patcher in self.patchers]

    def stop(self):
        for patcher in reversed(self.patchers):
            patcher.stop()
