# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion
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
        ('metadata', '0001_squashed_0009_redacted_20150417_1128'),
        ('archive', '0001_squashed_0013_methodoutput_are_checksums_ok')
    ]

    operations = [
        migrations.AddField(
            model_name='datatype',
            name='prototype',
            field=models.OneToOneField(related_name='datatype_modelled', null=True, on_delete=django.db.models.deletion.SET_NULL, blank=True, to='archive.Dataset'),
            preserve_default=True,
        ),
        migrations.RunPython(load_initial_groups),
        migrations.RunPython(load_initial_user),
        migrations.RunPython(load_initial_data),
    ]
