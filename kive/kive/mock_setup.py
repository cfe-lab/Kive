from contextlib import contextmanager
from functools import partial
from itertools import chain
from mock import Mock
import os
import sys

import django
from django.apps import apps
from django.db import connections
from django.db.utils import ConnectionHandler, NotSupportedError
from django.conf import settings

get_attribute = OriginalMockSet = MockSet = None  # place holder until it can be imported properly

if not apps.ready:
    # Do the Django set up when running as a stand-alone unit test.
    # That's why this module has to be imported before any Django models.
    if 'DJANGO_SETTINGS_MODULE' not in os.environ:
        os.environ['DJANGO_SETTINGS_MODULE'] = 'kive.settings'
    settings.LOGGING['handlers']['console']['level'] = 'CRITICAL'
    django.setup()

    # Disable database access, these are pure unit tests.
    db = connections.databases['default']
    db['PASSWORD'] = '****'
    db['USER'] = '**Database disabled for unit tests**'
    ConnectionHandler.__getitem__ = Mock(name='mock_connection')
    mock_ops = ConnectionHandler.__getitem__.return_value.ops  # @UndefinedVariable
    mock_execute = mock_ops.compiler.return_value.return_value.execute_sql
    mock_execute.side_effect = NotSupportedError("Mock database can't execute sql.")
    mock_ops.integer_field_range.return_value = (-sys.maxint - 1, sys.maxint)


@contextmanager
def mock_relations(*models):
    """ Mock all related field managers to make pure unit tests possible.

    with mock_relations(Dataset):
        dataset = Dataset()
        check = dataset.content_checks.create()  # returns mock object
    """
    try:
        for model in models:
            model_name = model._meta.object_name
            model.old_relations = {}
            model.old_objects = model.objects
            model.old_save = model.save
            model.protected = {}
            for related_object in chain(model._meta.related_objects,
                                        model._meta.many_to_many):
                name = related_object.name
                old_relation = getattr(model, name, None)
                if old_relation is not None:
                    if not related_object.one_to_one:
                        model.old_relations[name] = old_relation
                        new_relation = MockSet(cls=old_relation.field.model)
                        new_relation.order_by = partial(_order_by, new_relation)
                        setattr(model, name, new_relation)
            model.objects = Mock(name=model_name + '.objects')
            model.save = Mock(name=model_name + '.save')

        yield

    finally:
        for model in models:
            old_save = getattr(model, 'old_save', None)
            if old_save is not None:
                model.save = old_save
                del model.old_save
            old_objects = getattr(model, 'old_objects', None)
            if old_objects is not None:
                model.objects = old_objects
                del model.old_objects
            old_relations = getattr(model, 'old_relations', None)
            if old_relations is not None:
                for name, relation in old_relations.iteritems():
                    setattr(model, name, relation)
                del model.old_relations


def mocked_relations(*models):
    """ A decorator version of mock_relations.

    This can decorate a method or a class. Decorating a class is equivalent to
    decorating all the methods whose names start with "test_".
    """
    def decorator(target):
        if isinstance(target, type):
            for attr in dir(target):
                if attr.startswith('test_'):
                    original_method = getattr(target, attr)
                    setattr(target, attr, mocked_relations(*models)(original_method))
            return target

        def wrapped(*args, **kwargs):
            with mock_relations(*models):
                return target(*args, **kwargs)
        return wrapped
    return decorator


def _order_by(mock_set, attr):
    records = mock_set.all()
    ordered = sorted(records, key=lambda r: get_attribute(r, attr))
    return MockSet(*ordered)


def _wrap_mock_set(*args, **kwargs):
    mock_set = OriginalMockSet(*args, **kwargs)
    mock_set.order_by = partial(_order_by, mock_set)
    return mock_set

if MockSet is None:
    # fails if imported before setup
    if 'django_mock_queries.query' in sys.modules:
        raise RuntimeError('django_mock_queries.query imported before mock_setup.')
    from django_mock_queries.query import MockSet as OriginalMockSet
    from django_mock_queries.utils import get_attribute
    MockSet = _wrap_mock_set
    sys.modules['django_mock_queries.query'].MockSet = MockSet
