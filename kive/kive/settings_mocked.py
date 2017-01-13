from django.db.utils import ConnectionHandler
from mock import MagicMock

from django.db.utils import NotSupportedError
from django_mock_queries.mocks import monkey_patch_test_db
import django_mock_queries.mocks

# noinspection PyUnresolvedReferences
from kive.settings import *

monkey_patch_test_db()


original_mock_django_connection = django_mock_queries.mocks.mock_django_connection


# Patch for django_mock_queries until a PR is merged.
def patched_mock_django_connection(disabled_features=None):
    original_mock_django_connection(disabled_features=disabled_features)

    # noinspection PyUnusedLocal
    def patched_compiler(queryset, connection, using, **kwargs):
        result = MagicMock(name='mock_connection.ops.compiler()')
        # noinspection PyProtectedMember
        result.execute_sql.side_effect = NotSupportedError(
            "Mock database tried to execute SQL for {} model.".format(
                queryset.model._meta.object_name))
        result.has_results.side_effect = result.execute_sql.side_effect
        return result

    # noinspection PyUnresolvedReferences
    ConnectionHandler.__getitem__.return_value.ops.compiler.return_value.side_effect = patched_compiler
    # # noinspection PyUnresolvedReferences
    ConnectionHandler.__getitem__.return_value.alias = '**unused**'

django_mock_queries.mocks.mock_django_connection = patched_mock_django_connection

# Disable logging to console so test output isn't polluted.
LOGGING['handlers']['console']['level'] = 'CRITICAL'
