# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('datachecking', '0005_conflicting_SD_workaround_20151118_1025'),
        ("librarian", "0007_transition_SD_to_dataset_20151117_1748")
    ]

    operations = [
        migrations.AlterField(
            model_name='contentchecklog',
            name='symbolicdataset',
            field=models.ForeignKey(related_name='content_checks', to='librarian.Dataset'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='integritychecklog',
            name='symbolicdataset',
            field=models.ForeignKey(related_name='integrity_checks', to='librarian.Dataset'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='md5conflict',
            name='conflicting_SD',
            field=models.ForeignKey(related_name='usurps', null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    to='librarian.Dataset'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='md5conflict',
            name='conflicting_SD',
            field=models.OneToOneField(related_name='usurps', null=True, on_delete=django.db.models.deletion.SET_NULL, to='librarian.Dataset'),
            preserve_default=True,
        ),
    ]
