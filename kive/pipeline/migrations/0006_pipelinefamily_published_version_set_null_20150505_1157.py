# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0005_pipeline_parent_set_null_20150429_1025'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pipelinefamily',
            name='published_version',
            field=models.ForeignKey(on_delete=django.db.models.deletion.SET_NULL, blank=True, to='pipeline.Pipeline', null=True),
            preserve_default=True,
        ),
    ]
