import logging

import sys
from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class PortalConfig(AppConfig):
    name = 'portal'

    def ready(self):
        is_manage_py = sys.argv and sys.argv[0].endswith('manage.py')
        if is_manage_py and len(sys.argv) > 1 and sys.argv[1] != 'runserver':
            # Running some other management command, don't check secret key.
            return
        if settings.IS_RANDOM_KEY:
            logger.warning(
                'KIVE_SECRET_KEY environment variable was not set. Sessions '
                'will expire when the server shuts down.')
