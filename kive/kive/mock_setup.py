from contextlib import contextmanager

import django
from django.apps import apps
from django.db import connections
from django.conf import settings

from mock import Mock

if not apps.ready:
    # Do the Django set up when running as a stand-alone unit test.
    # That's why this module has to be imported before any Django models.
    settings.LOGGING['handlers']['console']['level'] = 'CRITICAL'
    django.setup()

    # Disable database access, these are pure unit tests.
    db = connections.databases['default']
    db['PASSWORD'] = '****'
    db['USER'] = '**Database disabled for unit tests**'


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
            for related_object in model._meta.related_objects:
                name = related_object.name
                model.old_relations[name] = getattr(model, name)
                setattr(model, name, Mock(name='{}.{}'.format(model_name, name)))
            model.objects = Mock(name=model_name + '.objects')

        yield

    finally:
        for model in models:
            old_objects = getattr(model, 'old_objects', None)
            if old_objects is not None:
                model.objects = old_objects
                del model.old_objects
            old_relations = getattr(model, 'old_relations', None)
            if old_relations is not None:
                for name, relation in old_relations.iteritems():
                    setattr(model, name, relation)
                del model.old_relations
