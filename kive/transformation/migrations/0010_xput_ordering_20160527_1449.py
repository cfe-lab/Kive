# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-05-27 21:49
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '0009_permissions_remove_null_20160203_1033'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='transformationinput',
            options={'ordering': ('dataset_idx',)},
        ),
        migrations.AlterModelOptions(
            name='transformationoutput',
            options={'ordering': ('dataset_idx',)},
        ),
    ]
