# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('fleet', '0004_run_to_process_run_one_to_one_20150713_1111'),
    ]

    operations = [
        migrations.AlterField(
            model_name='runtoprocessinput',
            name='symbolicdataset',
            field=models.ForeignKey(related_name='runtoprocessinputs', to='librarian.SymbolicDataset'),
            preserve_default=True,
        ),
    ]
