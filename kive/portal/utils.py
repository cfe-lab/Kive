from django.apps import apps
from django.contrib.contenttypes.management import update_contenttypes


# This was removed in Django 1.8, according to
# http://stackoverflow.com/questions/29550102/importerror-cannot-import-name-update-all-contenttypes
# As per the solution on that page, we recreate the wheel.
def update_all_contenttypes(**kwargs):
    for app_config in apps.get_app_configs():
        update_contenttypes(app_config, **kwargs)