# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0009_auto_20150615_1403'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='pipelinestep',
            options={'ordering': ['step_num']},
        ),
    ]
