# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datachecking', '0006_transition_SD_to_dataset_20151117_1748'),
    ]

    operations = [
        migrations.RenameField(
            model_name='contentchecklog',
            old_name='symbolicdataset',
            new_name='dataset',
        ),
        migrations.RenameField(
            model_name='integritychecklog',
            old_name='symbolicdataset',
            new_name='dataset',
        ),
        migrations.RenameField(
            model_name='md5conflict',
            old_name='conflicting_SD',
            new_name='conflicting_dataset',
        ),
    ]
