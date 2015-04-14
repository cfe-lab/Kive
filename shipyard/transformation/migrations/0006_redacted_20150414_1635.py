# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '0005_auto_20150213_1703'),
    ]

    operations = [
        migrations.AlterField(
            model_name='xputstructure',
            name='compounddatatype',
            field=models.ForeignKey(related_name='xput_structures', to='metadata.CompoundDatatype'),
            preserve_default=True,
        ),
    ]
