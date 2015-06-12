# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import re
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '0006_redacted_20150417_1128'),
    ]

    operations = [
        migrations.AlterField(
            model_name='transformationinput',
            name='dataset_name',
            field=models.CharField(help_text='Name for input as an alternative to index', max_length=60, verbose_name='input name', validators=[django.core.validators.RegexValidator(re.compile('^[-a-zA-Z0-9_]+$'), "Enter a valid 'slug' consisting of letters, numbers, underscores or hyphens.", 'invalid')]),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='transformationoutput',
            name='dataset_name',
            field=models.CharField(help_text='Name for output as an alternative to index', max_length=60, verbose_name='output name', validators=[django.core.validators.RegexValidator(re.compile('^[-a-zA-Z0-9_]+$'), "Enter a valid 'slug' consisting of letters, numbers, underscores or hyphens.", 'invalid')]),
            preserve_default=True,
        ),
    ]
