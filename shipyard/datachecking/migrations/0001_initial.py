# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0001_initial'),
        ('archive', '0002_auto_20150128_0950'),
        ('contenttypes', '0001_initial'),
        ('librarian', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='BadData',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('missing_output', models.BooleanField(default=False)),
                ('bad_header', models.NullBooleanField()),
                ('bad_num_rows', models.NullBooleanField()),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='CellError',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('row_num', models.PositiveIntegerField()),
                ('object_id', models.PositiveIntegerField(null=True)),
                ('baddata', models.ForeignKey(related_name='cell_errors', to='datachecking.BadData')),
                ('column', models.ForeignKey(to='metadata.CompoundDatatypeMember')),
                ('content_type', models.ForeignKey(blank=True, to='contenttypes.ContentType', null=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ContentCheckLog',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('start_time', models.DateTimeField(help_text='Starting time', null=True, verbose_name='start time', blank=True)),
                ('end_time', models.DateTimeField(help_text='Ending time', null=True, verbose_name='end time', blank=True)),
                ('execlog', models.ForeignKey(related_name='content_checks', to='archive.ExecLog', null=True)),
                ('symbolicdataset', models.ForeignKey(related_name='content_checks', to='librarian.SymbolicDataset')),
            ],
            options={
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='IntegrityCheckLog',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('start_time', models.DateTimeField(help_text='Starting time', null=True, verbose_name='start time', blank=True)),
                ('end_time', models.DateTimeField(help_text='Ending time', null=True, verbose_name='end time', blank=True)),
                ('execlog', models.ForeignKey(related_name='integrity_checks', to='archive.ExecLog', null=True)),
                ('symbolicdataset', models.ForeignKey(related_name='integrity_checks', to='librarian.SymbolicDataset')),
            ],
            options={
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='MD5Conflict',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('conflicting_SD', models.OneToOneField(related_name='usurps', to='librarian.SymbolicDataset')),
                ('integritychecklog', models.OneToOneField(related_name='usurper', to='datachecking.IntegrityCheckLog')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='VerificationLog',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('start_time', models.DateTimeField(help_text='Starting time', null=True, verbose_name='start time', blank=True)),
                ('end_time', models.DateTimeField(help_text='Ending time', null=True, verbose_name='end time', blank=True)),
                ('return_code', models.IntegerField(null=True)),
                ('output_log', models.FileField(upload_to='VerificationLogs')),
                ('error_log', models.FileField(upload_to='VerificationLogs')),
                ('CDTM', models.ForeignKey(to='metadata.CompoundDatatypeMember')),
                ('contentchecklog', models.ForeignKey(related_name='verification_logs', to='datachecking.ContentCheckLog')),
            ],
            options={
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='baddata',
            name='contentchecklog',
            field=models.OneToOneField(related_name='baddata', to='datachecking.ContentCheckLog'),
            preserve_default=True,
        ),
    ]
