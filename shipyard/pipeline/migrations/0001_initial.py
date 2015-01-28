# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0001_initial'),
        ('transformation', '__first__'),
    ]

    operations = [
        migrations.CreateModel(
            name='CustomCableWire',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Pipeline',
            fields=[
                ('transformation_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='transformation.Transformation')),
                ('revision_number', models.PositiveIntegerField(help_text='Revision number of this Pipeline in its family', verbose_name='Pipeline revision number', blank=True)),
            ],
            options={
            },
            bases=('transformation.transformation',),
        ),
        migrations.CreateModel(
            name='PipelineCable',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='PipelineFamily',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(help_text='The name given to a group of methods/pipelines', unique=True, max_length=60, verbose_name='Transformation family name')),
                ('description', models.TextField(help_text='A description for this collection of methods/pipelines', max_length=1000, verbose_name='Transformation family description', blank=True)),
                ('published_version', models.ForeignKey(blank=True, to='pipeline.Pipeline', null=True)),
            ],
            options={
                'ordering': ('name',),
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='PipelineOutputCable',
            fields=[
                ('pipelinecable_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='pipeline.PipelineCable')),
                ('output_name', models.CharField(help_text='Pipeline output hole name', max_length=60, verbose_name='Output hole name')),
                ('output_idx', models.PositiveIntegerField(help_text='Pipeline output hole index', verbose_name='Output hole index', validators=[django.core.validators.MinValueValidator(1)])),
                ('source_step', models.PositiveIntegerField(help_text='Source step at which output comes from', verbose_name='Source pipeline step number', validators=[django.core.validators.MinValueValidator(1)])),
                ('output_cdt', models.ForeignKey(related_name='cables_leading_to', blank=True, to='metadata.CompoundDatatype', null=True)),
                ('pipeline', models.ForeignKey(related_name='outcables', to='pipeline.Pipeline')),
                ('source', models.ForeignKey(help_text='Source output hole', to='transformation.TransformationOutput')),
            ],
            options={
            },
            bases=('pipeline.pipelinecable',),
        ),
        migrations.CreateModel(
            name='PipelineStep',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('step_num', models.PositiveIntegerField(validators=[django.core.validators.MinValueValidator(1)])),
                ('x', models.FloatField(default=0, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(1)])),
                ('y', models.FloatField(default=0, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(1)])),
                ('name', models.CharField(default='', max_length=60, blank=True)),
                ('outputs_to_delete', models.ManyToManyField(help_text='TransformationOutputs whose data should not be retained', related_name='pipeline_steps_deleting', to='transformation.TransformationOutput')),
                ('pipeline', models.ForeignKey(related_name='steps', to='pipeline.Pipeline')),
                ('transformation', models.ForeignKey(related_name='pipelinesteps', to='transformation.Transformation')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='PipelineStepInputCable',
            fields=[
                ('pipelinecable_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='pipeline.PipelineCable')),
                ('source_step', models.PositiveIntegerField(help_text='Cabling source step', verbose_name='Step providing the input source')),
                ('keep_output', models.BooleanField(default=False, help_text='Keep or delete output', verbose_name='Whether or not to retain the output of this PSIC')),
                ('dest', models.ForeignKey(related_name='cables_leading_in', to='transformation.TransformationInput', help_text='Wiring destination input hole')),
                ('pipelinestep', models.ForeignKey(related_name='cables_in', to='pipeline.PipelineStep')),
                ('source', models.ForeignKey(to='transformation.TransformationXput')),
            ],
            options={
            },
            bases=('pipeline.pipelinecable',),
        ),
        migrations.AlterUniqueTogether(
            name='pipelineoutputcable',
            unique_together=set([('pipeline', 'output_name'), ('pipeline', 'output_idx')]),
        ),
        migrations.AddField(
            model_name='pipeline',
            name='family',
            field=models.ForeignKey(related_name='members', to='pipeline.PipelineFamily'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='pipeline',
            name='revision_parent',
            field=models.ForeignKey(related_name='descendants', blank=True, to='pipeline.Pipeline', null=True),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='pipeline',
            unique_together=set([('family', 'revision_number')]),
        ),
        migrations.AddField(
            model_name='customcablewire',
            name='cable',
            field=models.ForeignKey(related_name='custom_wires', to='pipeline.PipelineCable'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='customcablewire',
            name='dest_pin',
            field=models.ForeignKey(related_name='dest_pins', to='metadata.CompoundDatatypeMember'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='customcablewire',
            name='source_pin',
            field=models.ForeignKey(related_name='source_pins', to='metadata.CompoundDatatypeMember'),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='customcablewire',
            unique_together=set([('cable', 'dest_pin')]),
        ),
    ]
