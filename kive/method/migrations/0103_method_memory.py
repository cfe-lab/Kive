# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2017-07-21 20:03
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('method', '0102_cr_filename_validator_message_20160624_1552'),
    ]

    operations = [
        migrations.AddField(
            model_name='method',
            name='memory',
            field=models.PositiveIntegerField(default=6000, help_text='Megabytes of memory Slurm will allocate for this Method (0 allocates all memory)', verbose_name='Memory required (MB)'),
        ),
    ]
