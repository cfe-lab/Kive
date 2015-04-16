# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0009_redacted_20150414_1635'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datatype',
            name='prototype',
            field=models.OneToOneField(related_name='datatype_modelled', null=True, on_delete=django.db.models.deletion.SET_NULL, blank=True, to='archive.Dataset'),
            preserve_default=True,
        ),
    ]
