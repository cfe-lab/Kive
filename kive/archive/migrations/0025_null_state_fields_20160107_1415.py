# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0024_state_variables_20160107_1159'),
    ]

    operations = [
        migrations.AlterField(
            model_name='run',
            name='_complete',
            field=models.NullBooleanField(help_text='Denotes whether this run component has been completed. Private use only'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='run',
            name='_successful',
            field=models.NullBooleanField(help_text='Denotes whether this has been successful. Private use only!'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='runcomponent',
            name='_complete',
            field=models.NullBooleanField(help_text='Denotes whether this run component has been completed. Private use only'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='runcomponent',
            name='_redacted',
            field=models.NullBooleanField(help_text='Denotes whether this has been redacted. Private use only!'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='runcomponent',
            name='_successful',
            field=models.NullBooleanField(help_text='Denotes whether this has been successful. Private use only!'),
            preserve_default=True,
        ),
    ]
