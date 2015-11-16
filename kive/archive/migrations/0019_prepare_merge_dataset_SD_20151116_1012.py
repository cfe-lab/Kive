# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0018_change_stopped_by_relatedname_20151109_1538'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='created_by',
            field=models.ForeignKey(related_name='outputs_OLDFIXME', blank=True, to='archive.RunComponent', null=True),
            preserve_default=True,
        ),
    ]
