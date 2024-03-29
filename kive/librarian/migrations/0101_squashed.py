# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-06-03 00:07
from __future__ import unicode_literals

from django.conf import settings
import django.core.validators
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
import librarian.models
import re


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('transformation', '__first__'),
        # ('metadata', '__first__'),
        # ('archive', '0001_initial'),
        # ('archive', '0019_prepare_merge_dataset_SD_20151116_1012'),
        # ('datachecking', '0101_squashed'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('auth', '0001_initial'),
        # ('metadata', '0101_squashed'),
        # ('archive', '0020_date_created_default_20151116_1040'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasetStructure',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('num_rows', models.IntegerField(default=-1, validators=[django.core.validators.MinValueValidator(-1)], verbose_name='number of rows')),
                ('compounddatatype', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='conforming_datasets', to='metadata.CompoundDatatype')),
            ],
        ),
        migrations.CreateModel(
            name='ExecRecord',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('generator', models.IntegerField(db_column='generator_id')),
            ],
        ),
        migrations.CreateModel(
            name='ExecRecordIn',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('execrecord', models.ForeignKey(help_text='Parent ExecRecord', on_delete=django.db.models.deletion.CASCADE, related_name='execrecordins', to='librarian.ExecRecord')),
                ('generic_input', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='transformation.TransformationXput')),
            ],
        ),
        migrations.CreateModel(
            name='ExecRecordOut',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('execrecord', models.ForeignKey(help_text='Parent ExecRecord', on_delete=django.db.models.deletion.CASCADE, related_name='execrecordouts', to='librarian.ExecRecord')),
                ('generic_output', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='execrecordouts_referencing', to='transformation.TransformationXput')),
            ],
        ),
        migrations.CreateModel(
            name='Dataset',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('MD5_checksum', models.CharField(blank=True, default='', help_text='Validates file integrity', max_length=64, validators=[django.core.validators.RegexValidator(message='MD5 checksum is not either 32 hex characters or blank', regex=re.compile('(^[0-9A-Fa-f]{32}$)|(^$)'))])),
                ('groups_allowed', models.ManyToManyField(blank=True, help_text='What groups have access?', null=True, related_name='librarian_symbolicdataset_has_access_to', to='auth.Group')),
                ('user', models.ForeignKey(default=1, on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
                ('users_allowed', models.ManyToManyField(blank=True, help_text='Which users have access?', null=True, related_name='librarian_symbolicdataset_has_access_to', to=settings.AUTH_USER_MODEL)),
                ('_redacted', models.BooleanField(default=False)),
            ],
        ),
        migrations.AddField(
            model_name='execrecordout',
            name='dataset',
            field=models.ForeignKey(help_text='Symbol for the dataset coming from this output', on_delete=django.db.models.deletion.CASCADE, related_name='execrecordouts', to='librarian.Dataset'),
        ),
        migrations.AlterUniqueTogether(
            name='execrecordout',
            unique_together=set([('execrecord', 'generic_output')]),
        ),
        migrations.AddField(
            model_name='execrecordin',
            name='dataset',
            field=models.ForeignKey(help_text='Symbol for the dataset fed to this input', on_delete=django.db.models.deletion.CASCADE, related_name='execrecordins', to='librarian.Dataset'),
        ),
        migrations.AlterUniqueTogether(
            name='execrecordin',
            unique_together=set([('execrecord', 'generic_input')]),
        ),
        migrations.AddField(
            model_name='datasetstructure',
            name='dataset',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='structure', to='librarian.Dataset'),
        ),
        migrations.AlterModelOptions(
            name='dataset',
            options={'ordering': ['-date_created', 'name']},
        ),
        migrations.AddField(
            model_name='dataset',
            name='file_source',
            field=models.IntegerField(db_column='file_source_id', null=True),
        ),
        migrations.AddField(
            model_name='dataset',
            name='dataset_file',
            field=models.FileField(help_text='Physical path where datasets are stored', max_length=260, null=True, upload_to=librarian.models.get_upload_path),
        ),
        migrations.AddField(
            model_name='dataset',
            name='date_created',
            field=models.DateTimeField(default=django.utils.timezone.now, help_text='Date of Dataset creation.'),
        ),
        migrations.AddField(
            model_name='dataset',
            name='description',
            field=models.TextField(blank=True, help_text='Description of this Dataset.', max_length=1000),
        ),
        migrations.AddField(
            model_name='dataset',
            name='name',
            field=models.CharField(blank=True, help_text='Name of this Dataset.', max_length=260),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='groups_allowed',
            field=models.ManyToManyField(blank=True, help_text='What groups have access?', null=True, related_name='librarian_dataset_has_access_to', to='auth.Group'),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='users_allowed',
            field=models.ManyToManyField(blank=True, help_text='Which users have access?', null=True, related_name='librarian_dataset_has_access_to', to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterField(
            model_name='datasetstructure',
            name='dataset',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='structure', to='librarian.Dataset'),
        ),
        migrations.AlterField(
            model_name='execrecordin',
            name='dataset',
            field=models.ForeignKey(help_text='Dataset fed to this input', on_delete=django.db.models.deletion.CASCADE, related_name='execrecordins', to='librarian.Dataset'),
        ),
        migrations.AlterField(
            model_name='execrecordout',
            name='dataset',
            field=models.ForeignKey(help_text='Dataset coming from this output', on_delete=django.db.models.deletion.CASCADE, related_name='execrecordouts', to='librarian.Dataset'),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='dataset_file',
            field=models.FileField(blank=True, default='', help_text='Physical path where datasets are stored', max_length=260, upload_to=librarian.models.get_upload_path),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='groups_allowed',
            field=models.ManyToManyField(blank=True, help_text='What groups have access?', related_name='librarian_dataset_has_access_to', to='auth.Group'),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='users_allowed',
            field=models.ManyToManyField(blank=True, help_text='Which users have access?', related_name='librarian_dataset_has_access_to', to=settings.AUTH_USER_MODEL),
        ),
        migrations.CreateModel(
            name='ExternalFileDirectory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(help_text='Human-readable name for this external file directory', max_length=4096, unique=True)),
                ('path', models.CharField(help_text='Absolute path', max_length=4096)),
            ],
        ),
        migrations.AddField(
            model_name='dataset',
            name='external_path',
            field=models.CharField(blank=True, help_text='Relative path of the file within the specified external file directory', max_length=4096),
        ),
        migrations.AddField(
            model_name='dataset',
            name='externalfiledirectory',
            field=models.ForeignKey(blank=True, help_text='External file directory containing the data file', null=True, on_delete=django.db.models.deletion.CASCADE, to='librarian.ExternalFileDirectory', verbose_name='External file directory'),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='dataset_file',
            field=models.FileField(blank=True, db_index=True, default='', help_text='Physical path where datasets are stored', max_length=260, upload_to=librarian.models.get_upload_path),
        ),
    ]
