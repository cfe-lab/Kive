# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-11-30 23:33
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0104_runbatch_on_delete_20160715_1040'),
    ]

    operations = [
        migrations.AddField(
            model_name='run',
            name='priority',
            field=models.IntegerField(default=0, help_text='Priority of this Run'),
        ),
    ]
