# -*- coding: utf-8 -*-
# Generated by Django 1.11.21 on 2019-06-19 17:47
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0102_relink_apps'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='customcablewire',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='customcablewire',
            name='cable',
        ),
        migrations.RemoveField(
            model_name='customcablewire',
            name='dest_pin',
        ),
        migrations.RemoveField(
            model_name='customcablewire',
            name='source_pin',
        ),
        migrations.AlterUniqueTogether(
            name='pipeline',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='pipeline',
            name='family',
        ),
        migrations.RemoveField(
            model_name='pipeline',
            name='revision_parent',
        ),
        migrations.RemoveField(
            model_name='pipeline',
            name='transformation_ptr',
        ),
        migrations.AlterUniqueTogether(
            name='pipelinefamily',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='pipelinefamily',
            name='groups_allowed',
        ),
        migrations.RemoveField(
            model_name='pipelinefamily',
            name='user',
        ),
        migrations.RemoveField(
            model_name='pipelinefamily',
            name='users_allowed',
        ),
        migrations.AlterUniqueTogether(
            name='pipelineoutputcable',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='pipelineoutputcable',
            name='output_cdt',
        ),
        migrations.RemoveField(
            model_name='pipelineoutputcable',
            name='pipeline',
        ),
        migrations.RemoveField(
            model_name='pipelineoutputcable',
            name='pipelinecable_ptr',
        ),
        migrations.RemoveField(
            model_name='pipelineoutputcable',
            name='source',
        ),
        migrations.RemoveField(
            model_name='pipelinestep',
            name='outputs_to_delete',
        ),
        migrations.RemoveField(
            model_name='pipelinestep',
            name='pipeline',
        ),
        migrations.RemoveField(
            model_name='pipelinestep',
            name='transformation',
        ),
        migrations.RemoveField(
            model_name='pipelinestepinputcable',
            name='dest',
        ),
        migrations.RemoveField(
            model_name='pipelinestepinputcable',
            name='pipelinecable_ptr',
        ),
        migrations.RemoveField(
            model_name='pipelinestepinputcable',
            name='pipelinestep',
        ),
        migrations.RemoveField(
            model_name='pipelinestepinputcable',
            name='source',
        ),
        migrations.DeleteModel(
            name='CustomCableWire',
        ),
        migrations.DeleteModel(
            name='Pipeline',
        ),
        migrations.DeleteModel(
            name='PipelineCable',
        ),
        migrations.DeleteModel(
            name='PipelineFamily',
        ),
        migrations.DeleteModel(
            name='PipelineOutputCable',
        ),
        migrations.DeleteModel(
            name='PipelineStep',
        ),
        migrations.DeleteModel(
            name='PipelineStepInputCable',
        ),
    ]
