# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '0003_auto_20150204_1703'),
    ]

    operations = [
        migrations.AlterField(
            model_name='xputstructure',
            name='max_row',
            field=models.PositiveIntegerField(help_text='Maximum number of rows this input/output returns', null=True, verbose_name='Maximum rows', blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='xputstructure',
            name='min_row',
            field=models.PositiveIntegerField(help_text='Minimum number of rows this input/output returns', null=True, verbose_name='Minimum rows', blank=True),
            preserve_default=True,
        ),
    ]
