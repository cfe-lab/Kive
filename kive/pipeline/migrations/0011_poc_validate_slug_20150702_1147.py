# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import re
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0010_pipelinestep_order_by_stepnum'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pipelineoutputcable',
            name='output_name',
            field=models.CharField(help_text='Pipeline output hole name', max_length=60, verbose_name='Output hole name', validators=[django.core.validators.RegexValidator(re.compile('^[-a-zA-Z0-9_]+$'), "Enter a valid 'slug' consisting of letters, numbers, underscores or hyphens.", 'invalid')]),
            preserve_default=True,
        ),
    ]
