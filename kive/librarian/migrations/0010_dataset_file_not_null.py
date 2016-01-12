# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import librarian.models


class Migration(migrations.Migration):

    dependencies = [
        ('librarian', '0009_file_source_20151118_1506'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='dataset_file',
            field=models.FileField(default='',
                                   help_text='Physical path where datasets are stored',
                                   max_length=260,
                                   upload_to=librarian.models.get_upload_path,
                                   blank=True),
            preserve_default=True,
        ),
    ]
