# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.core.management import call_command
from django.contrib.auth.management import create_permissions
from django.apps import apps as django_apps


def load_initial_groups(apps, schema_editor):
    # update_all_contenttypes(verbosity=0)
    auth_app_config = django_apps.get_app_config("auth")
    create_permissions(auth_app_config, verbosity=0)
    call_command("loaddata", "initial_groups", app_label="metadata")


def load_initial_user(apps, schema_editor):
    call_command("loaddata", "initial_user", app_label="metadata")


def load_initial_data(apps, schema_editor):
    call_command("loaddata", "initial_data", app_label="metadata")


class Migration(migrations.Migration):

    dependencies = [
        ("metadata", "0007_auto_20150218_1045")
    ]

    operations = [
        migrations.RunPython(load_initial_groups),
        migrations.RunPython(load_initial_user),
        migrations.RunPython(load_initial_data)
    ]
