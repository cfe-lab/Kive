# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0007_pipeline_ordering_20150601_1348'),
    ]

    operations = [
        migrations.AddField(
            model_name='pipelinestep',
            name='fill_colour',
            field=models.CharField(default='', max_length=7, blank=True),
            preserve_default=True,
        ),
    ]
