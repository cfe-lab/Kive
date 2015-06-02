# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('fleet', '0002_auto_20150220_1339'),
    ]

    operations = [
        migrations.AddField(
            model_name='runtoprocess',
            name='purged',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
    ]
