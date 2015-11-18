# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('librarian', '0007_transition_SD_to_dataset_20151117_1748'),
        ('metadata', '0011_remove_datatype_prototype'),
    ]

    operations = [
        migrations.RenameField(
            model_name="datatype",
            old_name="proto_SD",
            new_name="prototype"
        )
    ]
