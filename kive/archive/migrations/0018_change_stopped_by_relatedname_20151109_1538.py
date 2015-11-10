# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0017_make_fields_blankable_20151106_1050'),
    ]

    operations = [
        migrations.AlterField(
            model_name='run',
            name='paused_by',
            field=models.ForeignKey(related_name='runs_paused', blank=True, to=settings.AUTH_USER_MODEL, help_text='User that paused this Run', null=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='run',
            name='stopped_by',
            field=models.ForeignKey(related_name='runs_stopped', blank=True, to=settings.AUTH_USER_MODEL, help_text='User that stopped this Run', null=True),
            preserve_default=True,
        ),
    ]
