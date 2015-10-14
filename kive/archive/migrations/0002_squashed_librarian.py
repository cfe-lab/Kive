# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0001_squashed_0013_methodoutput_are_checksums_ok'),
        ('librarian', '__first__'),
    ]

    operations = [
        migrations.AddField(
            model_name='runcomponent',
            name='execrecord',
            field=models.ForeignKey(related_name='used_by_components', blank=True, to='librarian.ExecRecord', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='dataset',
            name='symbolicdataset',
            field=models.OneToOneField(related_name='dataset', to='librarian.SymbolicDataset'),
            preserve_default=True,
        ),
    ]
