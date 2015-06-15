# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0008_pipelinestep_fill_colour'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pipelinestep',
            name='fill_colour',
            field=models.CharField(default='', max_length=100, blank=True),
            preserve_default=True,
        ),
    ]
