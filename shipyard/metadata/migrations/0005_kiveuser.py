# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('auth', '0001_initial'),
        ('metadata', '0004_auto_20150213_1703'),
    ]

    operations = [
        migrations.CreateModel(
            name='KiveUser',
            fields=[
            ],
            options={
                'proxy': True,
            },
            bases=('auth.user',),
        ),
    ]
