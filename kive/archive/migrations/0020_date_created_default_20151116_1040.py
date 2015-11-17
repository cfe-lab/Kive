# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0019_prepare_merge_dataset_SD_20151116_1012'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='date_created',
            field=models.DateTimeField(default=django.utils.timezone.now, help_text='Date of Dataset creation.'),
            preserve_default=True,
        ),
    ]
