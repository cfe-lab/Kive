# This file overrides some defaults to enable .

# flake8: noqa

from settings import *  # @UnusedWildImport

INSTALLED_APPS = INSTALLED_APPS + ("django_extensions",)
