# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('datachecking', '0004_redacted_20150417_1128'),
    ]

    operations = [
        migrations.AlterField(
            model_name='md5conflict',
            name='conflicting_SD',
            field=models.ForeignKey(related_name='usurps', null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    to='librarian.SymbolicDataset'),
            preserve_default=True,
        ),
    ]
