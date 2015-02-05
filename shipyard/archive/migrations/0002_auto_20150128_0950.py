# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0001_initial'),
        ('librarian', '__first__'),
        ('pipeline', '__first__'),
    ]

    operations = [
        migrations.AddField(
            model_name='runcomponent',
            name='execrecord',
            field=models.ForeignKey(related_name='used_by_components', blank=True, to='librarian.ExecRecord', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='dataset',
            name='symbolicdataset',
            field=models.OneToOneField(related_name='dataset', to='librarian.SymbolicDataset'),
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
    ]
