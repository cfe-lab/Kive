# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0008_load_initial_data_users_groups_20150303_1209'),
    ]

    operations = [
        migrations.AlterField(
            model_name='compounddatatypemember',
            name='datatype',
            field=models.ForeignKey(related_name='CDTMs', to='metadata.Datatype', help_text='Specifies which DataType this member is'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='datatype',
            name='prototype',
            field=models.OneToOneField(related_name='datatype_modelled', null=True, on_delete=django.db.models.deletion.SET_NULL, blank=True, to='archive.Dataset'),
            preserve_default=True,
        ),
    ]
