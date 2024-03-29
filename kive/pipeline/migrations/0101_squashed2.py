# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-06-24 23:47
from __future__ import unicode_literals

from django.conf import settings
import django.core.validators
from django.db import migrations, models
import django.db.models.deletion
import re


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('auth', '0001_initial'),
        # ('transformation', '__first__'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        # ('metadata', '0101_squashed'),
    ]

    operations = [
        migrations.CreateModel(
            name='CustomCableWire',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
            ],
        ),
        migrations.CreateModel(
            name='Pipeline',
            fields=[
                ('transformation_ptr', models.IntegerField(db_column='transformation_ptr_id', primary_key=True, serialize=False)),
                ('revision_number', models.PositiveIntegerField(blank=True, help_text='Revision number of this Pipeline in its family', verbose_name='Pipeline revision number')),
                ('published', models.BooleanField(default=False, verbose_name='Is this Pipeline public?')),
            ],
        ),
        migrations.CreateModel(
            name='PipelineCable',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
            ],
        ),
        migrations.CreateModel(
            name='PipelineFamily',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(help_text='The name given to a group of methods/pipelines', max_length=60, verbose_name='Transformation family name')),
                ('description', models.TextField(blank=True, help_text='A description for this collection of methods/pipelines', max_length=1000, verbose_name='Transformation family description')),
                ('groups_allowed', models.ManyToManyField(blank=True, help_text='What groups have access?', related_name='pipeline_pipelinefamily_has_access_to', to='auth.Group')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
                ('users_allowed', models.ManyToManyField(blank=True, help_text='Which users have access?', related_name='pipeline_pipelinefamily_has_access_to', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'ordering': ('name',),
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='PipelineOutputCable',
            fields=[
                ('pipelinecable_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='pipeline.PipelineCable')),
                ('output_name', models.CharField(help_text='Pipeline output hole name', max_length=60, validators=[django.core.validators.RegexValidator(re.compile('^[-a-zA-Z0-9_]+\\Z'), "Enter a valid 'slug' consisting of letters, numbers, underscores or hyphens.", 'invalid')], verbose_name='Output hole name')),
                ('output_idx', models.PositiveIntegerField(help_text='Pipeline output hole index', validators=[django.core.validators.MinValueValidator(1)], verbose_name='Output hole index')),
                ('source_step', models.PositiveIntegerField(help_text='Source step at which output comes from', validators=[django.core.validators.MinValueValidator(1)], verbose_name='Source pipeline step number')),
                ('output_cdt', models.IntegerField(db_column='output_cdt_id', null=True)),
                ('pipeline', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='outcables', to='pipeline.Pipeline')),
                ('source', models.IntegerField(db_column='source_id')),
            ],
            bases=('pipeline.pipelinecable',),
        ),
        migrations.CreateModel(
            name='PipelineStep',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('step_num', models.PositiveIntegerField(validators=[django.core.validators.MinValueValidator(1)])),
                ('x', models.FloatField(default=0, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(1)])),
                ('y', models.FloatField(default=0, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(1)])),
                ('name', models.CharField(blank=True, default='', max_length=60)),
                ('pipeline', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='steps', to='pipeline.Pipeline')),
                ('transformation', models.IntegerField(db_column='transformation_id')),
            ],
        ),
        migrations.CreateModel(
            name='PipelineStepInputCable',
            fields=[
                ('pipelinecable_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='pipeline.PipelineCable')),
                ('source_step', models.PositiveIntegerField(help_text='Cabling source step', verbose_name='Step providing the input source')),
                ('keep_output', models.BooleanField(default=False, help_text='Keep or delete output', verbose_name='Whether or not to retain the output of this PSIC')),
                ('dest', models.IntegerField(db_column='dest_id')),
                ('pipelinestep', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='cables_in', to='pipeline.PipelineStep')),
                ('source', models.IntegerField(db_column='source_id')),
            ],
            bases=('pipeline.pipelinecable',),
        ),
        migrations.AlterUniqueTogether(
            name='pipelineoutputcable',
            unique_together=set([('pipeline', 'output_name'), ('pipeline', 'output_idx')]),
        ),
        migrations.AddField(
            model_name='pipeline',
            name='family',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='members', to='pipeline.PipelineFamily'),
        ),
        migrations.AddField(
            model_name='pipeline',
            name='revision_parent',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='descendants', to='pipeline.Pipeline'),
        ),
        migrations.AlterUniqueTogether(
            name='pipeline',
            unique_together=set([('family', 'revision_number')]),
        ),
        migrations.AddField(
            model_name='customcablewire',
            name='cable',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='custom_wires', to='pipeline.PipelineCable'),
        ),
        migrations.AddField(
            model_name='customcablewire',
            name='dest_pin',
            field=models.IntegerField(db_column='dest_pin_id'),
        ),
        migrations.AddField(
            model_name='customcablewire',
            name='source_pin',
            field=models.IntegerField(db_column='source_pin_id'),
        ),
        migrations.AlterUniqueTogether(
            name='customcablewire',
            unique_together=set([('cable', 'dest_pin')]),
        ),
        migrations.AlterUniqueTogether(
            name='pipelinefamily',
            unique_together=set([('name', 'user')]),
        ),
        migrations.AlterModelOptions(
            name='pipeline',
            options={'ordering': ['family__name', '-revision_number']},
        ),
        migrations.AddField(
            model_name='pipelinestep',
            name='fill_colour',
            field=models.CharField(blank=True, default='', max_length=100),
        ),
        migrations.AlterModelOptions(
            name='pipelinestep',
            options={'ordering': ['step_num']},
        ),
        migrations.AlterField(
            model_name='pipelinefamily',
            name='groups_allowed',
            field=models.ManyToManyField(blank=True, help_text='What groups have access?', related_name='pipeline_pipelinefamily_has_access_to', to='auth.Group'),
        ),
        migrations.AlterField(
            model_name='pipelinefamily',
            name='users_allowed',
            field=models.ManyToManyField(blank=True, help_text='Which users have access?', related_name='pipeline_pipelinefamily_has_access_to', to=settings.AUTH_USER_MODEL),
        ),
        migrations.CreateModel(
            name='PipelineStepOutputsToDeleteTemp',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('transformationoutput_id', models.IntegerField()),
                ('pipelinestep', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='pipeline.PipelineStep')),
            ],
            options={
                'db_table': 'pipeline_pipelinestep_outputs_to_delete',
            },
        ),
    ]
