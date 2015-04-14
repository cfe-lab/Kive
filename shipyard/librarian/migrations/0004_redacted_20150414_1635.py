# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0009_redacted_20150414_1635'),
        ('librarian', '0003_auto_20150213_1703'),
    ]

    operations = [
        migrations.AddField(
            model_name='execrecord',
            name='adopter',
            field=models.OneToOneField(related_name='adopted_execrecord', null=True, blank=True, to='archive.RunComponent'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='symbolicdataset',
            name='_redacted',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='execrecord',
            name='generator',
            field=models.OneToOneField(related_name='execrecord', null=True, blank=True, to='archive.ExecLog'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='execrecordin',
            name='symbolicdataset',
            field=models.ForeignKey(related_name='execrecordins', to='librarian.SymbolicDataset', help_text='Symbol for the dataset fed to this input'),
            preserve_default=True,
        ),
    ]
