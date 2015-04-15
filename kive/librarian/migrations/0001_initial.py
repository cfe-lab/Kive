# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import re
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '__first__'),
        ('archive', '0001_initial'),
        ('metadata', '__first__'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasetStructure',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('num_rows', models.IntegerField(default=-1, verbose_name='number of rows', validators=[django.core.validators.MinValueValidator(-1)])),
                ('compounddatatype', models.ForeignKey(related_name='conforming_datasets', to='metadata.CompoundDatatype')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ExecRecord',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('generator', models.ForeignKey(related_name='execrecords', to='archive.ExecLog')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ExecRecordIn',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('execrecord', models.ForeignKey(related_name='execrecordins', to='librarian.ExecRecord', help_text='Parent ExecRecord')),
                ('generic_input', models.ForeignKey(to='transformation.TransformationXput')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ExecRecordOut',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('execrecord', models.ForeignKey(related_name='execrecordouts', to='librarian.ExecRecord', help_text='Parent ExecRecord')),
                ('generic_output', models.ForeignKey(related_name='execrecordouts_referencing', to='transformation.TransformationXput')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='SymbolicDataset',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('MD5_checksum', models.CharField(default='', help_text='Validates file integrity', max_length=64, blank=True, validators=[django.core.validators.RegexValidator(regex=re.compile('(^[0-9A-Fa-f]{32}$)|(^$)'), message='MD5 checksum is not either 32 hex characters or blank')])),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='execrecordout',
            name='symbolicdataset',
            field=models.ForeignKey(related_name='execrecordouts', to='librarian.SymbolicDataset', help_text='Symbol for the dataset coming from this output'),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='execrecordout',
            unique_together=set([('execrecord', 'generic_output')]),
        ),
        migrations.AddField(
            model_name='execrecordin',
            name='symbolicdataset',
            field=models.ForeignKey(help_text='Symbol for the dataset fed to this input', to='librarian.SymbolicDataset'),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='execrecordin',
            unique_together=set([('execrecord', 'generic_input')]),
        ),
        migrations.AddField(
            model_name='datasetstructure',
            name='symbolicdataset',
            field=models.OneToOneField(related_name='structure', to='librarian.SymbolicDataset'),
            preserve_default=True,
        ),
    ]
