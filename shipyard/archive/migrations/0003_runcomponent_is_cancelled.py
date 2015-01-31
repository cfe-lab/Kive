# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0002_auto_20150128_0950'),
    ]

    operations = [
        migrations.AddField(
            model_name='runcomponent',
            name='is_cancelled',
            field=models.BooleanField(default=False, help_text='Denotes whether this has been cancelled'),
            preserve_default=True,
        ),
    ]
