# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0011_poc_validate_slug_20150702_1147'),
    ]

    operations = [
        migrations.AddField(
            model_name='pipeline',
            name='published',
            field=models.BooleanField(default=False, verbose_name='Is this Pipeline public?'),
            preserve_default=True,
        ),
    ]
