# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('method', '0005_redacted_20150417_1128'),
    ]

    operations = [
        migrations.AlterField(
            model_name='method',
            name='driver',
            field=models.ForeignKey(related_name='methods', to='method.CodeResourceRevision'),
            preserve_default=True,
        ),
    ]
