# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import re
import django.utils.timezone
from django.conf import settings
import librarian.models
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('auth', '0001_initial'),
        # ('archive', '0022_transition_SD_to_dataset_20151117_1748'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('librarian', '0006_transfer_datasets_to_SDs_20151116_1022'),
        ('datachecking', '0005_conflicting_SD_workaround_20151118_1025'),
    ]

    operations = [
        migrations.RenameModel(
            old_name="SymbolicDataset",
            new_name="Dataset"
        ),
        migrations.AlterField(
            model_name='dataset',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='librarian_dataset_has_access_to', null=True, to='auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='dataset',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='librarian_dataset_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='datasetstructure',
            name='symbolicdataset',
            field=models.OneToOneField(related_name='structure', to='librarian.Dataset'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='execrecordin',
            name='symbolicdataset',
            field=models.ForeignKey(related_name='execrecordins', to='librarian.Dataset', help_text='Dataset fed to this input'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='execrecordout',
            name='symbolicdataset',
            field=models.ForeignKey(related_name='execrecordouts', to='librarian.Dataset', help_text='Dataset coming from this output'),
            preserve_default=True,
        ),
    ]
