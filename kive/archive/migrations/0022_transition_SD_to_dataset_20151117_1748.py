# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0021_remove_dataset_20151117_1128'),
        ("librarian", "0007_transition_SD_to_dataset_20151117_1748")
    ]

    operations = [
        migrations.AlterField(
            model_name='runinput',
            name='symbolicdataset',
            field=models.ForeignKey(related_name='runinputs', to='librarian.Dataset'),
            preserve_default=True,
        ),
    ]
