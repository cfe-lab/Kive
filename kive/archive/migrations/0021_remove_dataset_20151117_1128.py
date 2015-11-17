# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0011_remove_datatype_prototype'),
        ('archive', '0020_date_created_default_20151116_1040'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='dataset',
            name='created_by',
        ),
        migrations.RemoveField(
            model_name='dataset',
            name='symbolicdataset',
        ),
        migrations.DeleteModel(
            name='Dataset',
        ),
    ]
