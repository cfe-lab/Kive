from itertools import chain
from mock import Mock, PropertyMock, MagicMock, patch
import os
import sys

import django
from django.apps import apps
from django.db import connections
from django.db.utils import ConnectionHandler, NotSupportedError
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist


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
    # noinspection PyUnresolvedReferences
    mock_ops = ConnectionHandler.__getitem__.return_value.ops

    def compiler(queryset, connection, using, **kwargs):
        result = Mock(name='mock_connection.ops.compiler()')
        result.execute_sql.side_effect = NotSupportedError(
            "Mock database tried to execute SQL for {} model.".format(
                queryset.model._meta.object_name))
        return result
    mock_execute = mock_ops.compiler.return_value.side_effect = compiler
    mock_ops.integer_field_range.return_value = (-sys.maxint - 1, sys.maxint)

# Import after the Django configuration has been mocked out.
# Can move back to the top if this pull request is accepted:
# https://github.com/stphivos/django-mock-queries/pull/14
from django_mock_queries.query import MockSet  # @IgnorePep8
import django_mock_queries.utils
import django_mock_queries.constants


def patched_get_attribute(obj, attr, default=None):
    result = obj
    comparison = None
    parts = attr.split('__')

    for p in parts:
        if p in django_mock_queries.constants.COMPARISONS:
            comparison = p
        elif result is None:
            break
        else:
            result = getattr(result, p, None)

    value = result if result is not None else default
    return value, comparison

# Remove this monkey patch after pull request is merged and released:
# https://github.com/stphivos/django-mock-queries/pull/16
django_mock_queries.utils.get_attribute = patched_get_attribute


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
                        new_relation = PropertyMock(side_effect=ObjectDoesNotExist)
                    else:
                        new_relation = MockSet(cls=old_relation.field.model)
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
