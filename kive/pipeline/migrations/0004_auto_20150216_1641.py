# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0003_auto_20150213_1703'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pipelinefamily',
            name='name',
            field=models.CharField(help_text='The name given to a group of methods/pipelines', max_length=60, verbose_name='Transformation family name'),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='pipelinefamily',
            unique_together=set([('name', 'user')]),
        ),
    ]
