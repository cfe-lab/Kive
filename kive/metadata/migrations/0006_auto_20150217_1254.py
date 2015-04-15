# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0005_kiveuser'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datatype',
            name='name',
            field=models.CharField(help_text='The name for this Datatype', max_length=60, verbose_name='Datatype name'),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='datatype',
            unique_together=set([('user', 'name')]),
        ),
    ]
