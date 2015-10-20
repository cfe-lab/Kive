# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0013_methodoutput_are_checksums_ok'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='name',
            field=models.CharField(help_text='Name of this Dataset.', max_length=260),
            preserve_default=True,
        ),
    ]
