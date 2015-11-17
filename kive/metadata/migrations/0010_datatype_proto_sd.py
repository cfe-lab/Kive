# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('librarian', '0005_merge_dataset_SD_20151116_1012'),
        ('metadata', '0009_redacted_20150417_1128'),
    ]

    operations = [
        migrations.AddField(
            model_name='datatype',
            name='proto_SD',
            field=models.OneToOneField(related_name='datatype_modelled', null=True, on_delete=django.db.models.deletion.SET_NULL, blank=True, to='librarian.SymbolicDataset'),
            preserve_default=True,
        ),
    ]
