# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0012_dataset_order_by_date'),
    ]

    operations = [
        migrations.AddField(
            model_name='methodoutput',
            name='are_checksums_OK',
            field=models.BooleanField(default=True, help_text='Do code checksums match originals?'),
            preserve_default=True,
        ),
    ]
