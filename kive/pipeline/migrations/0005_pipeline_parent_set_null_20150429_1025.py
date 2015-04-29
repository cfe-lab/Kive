# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0004_auto_20150216_1641'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pipeline',
            name='revision_parent',
            field=models.ForeignKey(related_name='descendants', on_delete=django.db.models.deletion.SET_NULL, blank=True, to='pipeline.Pipeline', null=True),
            preserve_default=True,
        ),
    ]
