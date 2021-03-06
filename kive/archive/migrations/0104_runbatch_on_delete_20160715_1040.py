# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-07-15 17:40
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0103_runbatch_20160622_1609'),
    ]

    operations = [
        migrations.AlterField(
            model_name='run',
            name='runbatch',
            field=models.ForeignKey(blank=True, help_text='Run batch that this Run is a part of', null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='runs', to='archive.RunBatch'),
        ),
    ]
