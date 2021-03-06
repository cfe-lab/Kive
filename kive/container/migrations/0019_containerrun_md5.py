# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-03-01 18:58
from __future__ import unicode_literals

import django.core.validators
from django.db import migrations, models
import re


class Migration(migrations.Migration):

    dependencies = [
        ('container', '0018_containerrun_original_run'),
    ]

    operations = [
        migrations.AddField(
            model_name='containerrun',
            name='md5',
            field=models.CharField(blank=True, help_text="Summary of MD5's for inputs, outputs, and containers.", max_length=64, validators=[django.core.validators.RegexValidator(message='MD5 checksum is not either 32 hex characters or blank', regex=re.compile('(^[0-9A-Fa-f]{32}$)|(^$)'))]),
        ),
    ]
