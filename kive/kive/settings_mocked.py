from django_mock_queries.mocks import monkey_patch_test_db

# noinspection PyUnresolvedReferences
from kive.settings import *

monkey_patch_test_db()

# Disable logging to console so test output isn't polluted.
LOGGING['handlers']['console']['level'] = 'CRITICAL'
