# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0006_auto_20150217_1254'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='datatype',
            name='custom_constraint',
        ),
        migrations.AddField(
            model_name='customconstraint',
            name='datatype',
            field=models.OneToOneField(related_name='custom_constraint', default=1, to='metadata.Datatype'),
            preserve_default=False,
        ),
    ]
