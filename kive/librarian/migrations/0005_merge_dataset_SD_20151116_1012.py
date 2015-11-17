# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone
import librarian.models


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0019_prepare_merge_dataset_SD_20151116_1012'),
        ('librarian', '0004_redacted_20150417_1128'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='symbolicdataset',
            options={'ordering': ['-date_created', 'name']},
        ),
        migrations.AddField(
            model_name='symbolicdataset',
            name='created_by',
            field=models.ForeignKey(related_name='outputs', blank=True, to='archive.RunComponent', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='symbolicdataset',
            name='dataset_file',
            field=models.FileField(help_text='Physical path where datasets are stored', max_length=260, null=True, upload_to=librarian.models.get_upload_path),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='symbolicdataset',
            name='date_created',
            field=models.DateTimeField(default=django.utils.timezone.now, help_text='Date of Dataset creation.'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='symbolicdataset',
            name='description',
            field=models.TextField(help_text='Description of this Dataset.', max_length=1000, blank=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='symbolicdataset',
            name='name',
            field=models.CharField(help_text='Name of this Dataset.', max_length=260, blank=True),
            preserve_default=True,
        ),
    ]
