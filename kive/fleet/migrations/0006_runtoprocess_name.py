# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('fleet', '0005_runtoprocessinput_related_name_20150713_1434'),
    ]

    operations = [
        migrations.AddField(
            model_name='runtoprocess',
            name='name',
            field=models.CharField(default=b'', max_length=60, blank=True),
            preserve_default=True,
        ),
    ]
