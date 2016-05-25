from contextlib import contextmanager
from itertools import chain
from mock import Mock
import os

import django
from django.apps import apps
from django.db import connections
from django.db.utils import ConnectionHandler, NotSupportedError
from django.conf import settings

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


@contextmanager
def mock_relations(*models):
    """ Mock all related field managers to make pure unit tests possible.

    with mock_relations(Dataset):
        dataset = Dataset()
        check = dataset.content_checks.create()  # returns mock object
    """
    from django_mock_queries.query import MockSet  # fails if imported before setup
    try:
        for model in models:
            model_name = model._meta.object_name
            model.old_relations = {}
            model.old_objects = model.objects
            model.old_save = model.save
            for related_object in chain(model._meta.related_objects,
                                        model._meta.many_to_many):
                name = related_object.name
                old_relation = getattr(model, name)
                model.old_relations[name] = old_relation
                if related_object.one_to_one:
                    new_relation = Mock(name='{}.{}'.format(model_name, name))
                else:
                    new_relation = MockSet(cls=old_relation.field.model)
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
                    setattr(target, attr, mocked_relations(original_method, *models))
            return target

        def wrapped(*args, **kwargs):
            with mock_relations(*models):
                return target(*args, **kwargs)
        return wrapped
    return decorator
