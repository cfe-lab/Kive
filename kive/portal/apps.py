import logging

from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class PortalConfig(AppConfig):
    name = 'portal'

    def ready(self):
        if settings.IS_RANDOM_KEY:
            logger.warning(
                'KIVE_SECRET_KEY environment variable was not set. Sessions '
                'will expire when the server shuts down.')
