import weakref
from functools import partial
from itertools import chain
from mock import Mock, MagicMock, patch, ClassTypes

from django_mock_queries.query import MockSet


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
            related_objects = MockSet(cls=self.original.field.model)
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
        if isinstance(model.save, MagicMock):
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
        if isinstance(func, ClassTypes):
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

        should_absorb = not (self.pass_mocks or isinstance(target, ClassTypes))
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
