# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0016_copy_runtoprocess_to_run_20151103_1257'),
    ]

    operations = [
        migrations.AlterField(
            model_name='run',
            name='paused_by',
            field=models.ForeignKey(related_name='pauser', blank=True, to=settings.AUTH_USER_MODEL, help_text='User that paused this Run', null=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='run',
            name='stopped_by',
            field=models.ForeignKey(related_name='stopper', blank=True, to=settings.AUTH_USER_MODEL, help_text='User that stopped this Run', null=True),
            preserve_default=True,
        ),
    ]
