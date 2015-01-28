# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Transformation',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('revision_name', models.CharField(help_text='The name of this transformation revision', max_length=60, verbose_name='Transformation revision name', blank=True)),
                ('revision_DateTime', models.DateTimeField(auto_now_add=True, verbose_name='Revision creation date')),
                ('revision_desc', models.TextField(help_text='Description of this transformation revision', max_length=1000, verbose_name='Transformation revision description', blank=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='TransformationXput',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('x', models.FloatField(default=0, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(1)])),
                ('y', models.FloatField(default=0, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(1)])),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='TransformationOutput',
            fields=[
                ('transformationxput_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='transformation.TransformationXput')),
                ('dataset_name', models.CharField(help_text='Name for output as an alternative to index', max_length=60, verbose_name='output name')),
                ('dataset_idx', models.PositiveIntegerField(help_text='Index defining the relative order of this output', verbose_name='output index', validators=[django.core.validators.MinValueValidator(1)])),
                ('transformation', models.ForeignKey(related_name='outputs', to='transformation.Transformation')),
            ],
            options={
            },
            bases=('transformation.transformationxput',),
        ),
        migrations.CreateModel(
            name='TransformationInput',
            fields=[
                ('transformationxput_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='transformation.TransformationXput')),
                ('dataset_name', models.CharField(help_text='Name for input as an alternative to index', max_length=60, verbose_name='input name')),
                ('dataset_idx', models.PositiveIntegerField(help_text='Index defining the relative order of this input', verbose_name='input index', validators=[django.core.validators.MinValueValidator(1)])),
                ('transformation', models.ForeignKey(related_name='inputs', to='transformation.Transformation')),
            ],
            options={
            },
            bases=('transformation.transformationxput',),
        ),
        migrations.AlterUniqueTogether(
            name='transformationoutput',
            unique_together=set([('transformation', 'dataset_name'), ('transformation', 'dataset_idx')]),
        ),
        migrations.AlterUniqueTogether(
            name='transformationinput',
            unique_together=set([('transformation', 'dataset_name'), ('transformation', 'dataset_idx')]),
        ),
    ]
