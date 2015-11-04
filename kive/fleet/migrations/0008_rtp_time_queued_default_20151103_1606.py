# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('fleet', '0007_runtoprocess_description_20151021_1517'),
    ]

    operations = [
        migrations.AlterField(
            model_name='runtoprocess',
            name='time_queued',
            field=models.DateTimeField(default=django.utils.timezone.now),
            preserve_default=True,
        ),
    ]
