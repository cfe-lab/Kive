# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings
import archive.models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Dataset',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(help_text='Name of this Dataset.', max_length=60)),
                ('description', models.TextField(help_text='Description of this Dataset.', max_length=1000, blank=True)),
                ('date_created', models.DateTimeField(help_text='Date of Dataset creation.', auto_now_add=True)),
                ('date_modified', models.DateTimeField(help_text='Date of Dataset modification.', auto_now_add=True)),
                ('dataset_file', models.FileField(help_text='Physical path where datasets are stored', max_length=260, upload_to=archive.models.get_upload_path)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ExecLog',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('start_time', models.DateTimeField(help_text='Starting time', null=True, verbose_name='start time', blank=True)),
                ('end_time', models.DateTimeField(help_text='Ending time', null=True, verbose_name='end time', blank=True)),
            ],
            options={
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='MethodOutput',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('return_code', models.IntegerField(null=True, verbose_name='return code')),
                ('output_log', models.FileField(help_text='Terminal output of the RunStep Method, i.e. stdout.', upload_to='Logs', verbose_name='output log')),
                ('error_log', models.FileField(help_text='Terminal error output of the RunStep Method, i.e. stderr.', upload_to='Logs', verbose_name='error log')),
                ('execlog', models.OneToOneField(related_name='methodoutput', to='archive.ExecLog')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Run',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('start_time', models.DateTimeField(help_text='Starting time', null=True, verbose_name='start time', blank=True)),
                ('end_time', models.DateTimeField(help_text='Ending time', null=True, verbose_name='end time', blank=True)),
                ('name', models.CharField(max_length=60, verbose_name='Run name')),
                ('description', models.TextField(max_length=1000, verbose_name='Run description', blank=True)),
            ],
            options={
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='RunComponent',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('start_time', models.DateTimeField(help_text='Starting time', null=True, verbose_name='start time', blank=True)),
                ('end_time', models.DateTimeField(help_text='Ending time', null=True, verbose_name='end time', blank=True)),
                ('reused', models.NullBooleanField(default=None, help_text='Denotes whether this reuses an ExecRecord')),
            ],
            options={
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='run',
            name='user',
            field=models.ForeignKey(help_text='User who performed this run', to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='execlog',
            name='invoking_record',
            field=models.ForeignKey(related_name='invoked_logs', to='archive.RunComponent'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='execlog',
            name='record',
            field=models.OneToOneField(related_name='log', to='archive.RunComponent'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='dataset',
            name='created_by',
            field=models.ForeignKey(related_name='outputs', blank=True, to='archive.RunComponent', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='dataset',
            name='user',
            field=models.ForeignKey(help_text='User that uploaded this Dataset.', to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
    ]
