# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('librarian', '0008_transition_SD_fks_20151117_1759'),
    ]

    operations = [
        migrations.RenameField(
            model_name='dataset',
            old_name='created_by',
            new_name='file_source',
        ),
    ]
