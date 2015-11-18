# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0022_transition_SD_to_dataset_20151117_1748'),
    ]

    operations = [
        migrations.RenameField(
            model_name='runinput',
            old_name='symbolicdataset',
            new_name='dataset',
        ),
    ]
