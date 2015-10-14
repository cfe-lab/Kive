# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings
import archive.models


class Migration(migrations.Migration):

    replaces = [(b'archive', '0001_initial'), (b'archive', '0002_auto_20150128_0950'), (b'archive', '0003_runcomponent_is_cancelled'), (b'archive', '0004_auto_20150204_1703'), (b'archive', '0005_auto_20150213_1703'), (b'archive', '0006_runstep_permissions'), (b'archive', '0007_runstep_permissions_undo'), (b'archive', '0008_runcomponent_complete_n_success'), (b'archive', '0009_redacted_20150417_1128'), (b'archive', '0010_rename_redacted'), (b'archive', '0011_dataset_order_by_name'), (b'archive', '0012_dataset_order_by_date'), (b'archive', '0013_methodoutput_are_checksums_ok')]

    dependencies = [
        ('pipeline', '__first__'),
        ('auth', '0001_initial'),
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
                ('output_log', models.FileField(help_text='Terminal output of the RunStep Method, i.e. stdout.', upload_to='Logs', null=True, verbose_name='output log', blank=True)),
                ('error_log', models.FileField(help_text='Terminal error output of the RunStep Method, i.e. stderr.', upload_to='Logs', null=True, verbose_name='error log', blank=True)),
                ('execlog', models.OneToOneField(related_name='methodoutput', to='archive.ExecLog')),
                ('code_redacted', models.BooleanField(default=False)),
                ('error_redacted', models.BooleanField(default=False)),
                ('output_redacted', models.BooleanField(default=False)),
                ('are_checksums_OK', models.BooleanField(default=True, help_text='Do code checksums match originals?')),
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
                ('user', models.ForeignKey(help_text='User who performed this run', to=settings.AUTH_USER_MODEL)),
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
        migrations.CreateModel(
            name='RunOutputCable',
            fields=[
                ('runcomponent_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='archive.RunComponent')),
                ('pipelineoutputcable', models.ForeignKey(related_name='poc_instances', to='pipeline.PipelineOutputCable')),
                ('run', models.ForeignKey(related_name='runoutputcables', to='archive.Run')),
            ],
            options={
            },
            bases=('archive.runcomponent',),
        ),
        migrations.CreateModel(
            name='RunSIC',
            fields=[
                ('runcomponent_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='archive.RunComponent')),
                ('PSIC', models.ForeignKey(related_name='psic_instances', to='pipeline.PipelineStepInputCable')),
            ],
            options={
            },
            bases=('archive.runcomponent',),
        ),
        migrations.CreateModel(
            name='RunStep',
            fields=[
                ('runcomponent_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='archive.RunComponent')),
                ('pipelinestep', models.ForeignKey(related_name='pipelinestep_instances', to='pipeline.PipelineStep')),
                ('run', models.ForeignKey(related_name='runsteps', to='archive.Run')),
            ],
            options={
            },
            bases=('archive.runcomponent',),
        ),
        migrations.AlterUniqueTogether(
            name='runstep',
            unique_together=set([('run', 'pipelinestep')]),
        ),
        migrations.AlterUniqueTogether(
            name='runoutputcable',
            unique_together=set([('run', 'pipelineoutputcable')]),
        ),
        migrations.AddField(
            model_name='run',
            name='pipeline',
            field=models.ForeignKey(related_name='pipeline_instances', to='pipeline.Pipeline', help_text='Pipeline used in this run'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='runsic',
            name='runstep',
            field=models.ForeignKey(related_name='RSICs', to='archive.RunStep'),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='runsic',
            unique_together=set([('runstep', 'PSIC')]),
        ),
        migrations.AddField(
            model_name='run',
            name='parent_runstep',
            field=models.OneToOneField(related_name='child_run', null=True, blank=True, to='archive.RunStep', help_text='Step of parent run initiating this one as a sub-run'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='runcomponent',
            name='is_cancelled',
            field=models.BooleanField(default=False, help_text='Denotes whether this has been cancelled'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='run',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='archive_run_has_access_to', null=True, to=b'auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='run',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='archive_run_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='run',
            name='user',
            field=models.ForeignKey(to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='runcomponent',
            name='_complete',
            field=models.BooleanField(default=False, help_text='Denotes whether this run component has been completed. Private use only'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='runcomponent',
            name='_successful',
            field=models.BooleanField(default=False, help_text='Denotes whether this has been successful. Private use only!'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='runcomponent',
            name='_redacted',
            field=models.BooleanField(default=False, help_text='Denotes whether this has been redacted. Private use only!'),
            preserve_default=True,
        ),
        migrations.AlterModelOptions(
            name='dataset',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='dataset',
            options={'ordering': ['-date_created', 'name']},
        ),
    ]
