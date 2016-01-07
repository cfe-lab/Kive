# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0023_transition_SD_fks_20151117_1759'),
    ]

    operations = [
        migrations.AddField(
            model_name='run',
            name='_complete',
            field=models.BooleanField(default=False, help_text='Denotes whether this run component has been completed. Private use only'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='run',
            name='_successful',
            field=models.BooleanField(default=True, help_text='Denotes whether this has been successful. Private use only!'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='runcomponent',
            name='_successful',
            field=models.BooleanField(default=True, help_text='Denotes whether this has been successful. Private use only!'),
            preserve_default=True,
        ),
    ]
