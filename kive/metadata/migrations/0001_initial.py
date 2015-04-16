# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import re
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('method', '__first__'),
        ('archive', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='BasicConstraint',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('ruletype', models.CharField(max_length=32, choices=[('minlen', 'minimum string length'), ('maxlen', 'maximum string length'), ('minval', 'minimum numeric value'), ('maxval', 'maximum numeric value'), ('regexp', 'Perl regular expression'), ('datetimeformat', 'date format string (1989 C standard)')], verbose_name='Type of rule', validators=[django.core.validators.RegexValidator(re.compile('minlen|maxlen|minval|maxval|regexp|datetimeformat'))])),
                ('rule', models.CharField(max_length=100, verbose_name='Rule specification')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='CompoundDatatype',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='CompoundDatatypeMember',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('column_name', models.CharField(help_text="Gives datatype a 'column name' as an alternative to column index", max_length=60, verbose_name='Column name')),
                ('column_idx', models.PositiveIntegerField(help_text='The column number of this DataType', validators=[django.core.validators.MinValueValidator(1)])),
                ('compounddatatype', models.ForeignKey(related_name='members', to='metadata.CompoundDatatype', help_text='Links this DataType member to a particular CompoundDataType')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='CustomConstraint',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('verification_method', models.ForeignKey(related_name='custom_constraints', to='method.Method')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Datatype',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(help_text='The name for this Datatype', unique=True, max_length=60, verbose_name='Datatype name')),
                ('description', models.TextField(help_text='A description for this Datatype', max_length=1000, verbose_name='Datatype description')),
                ('date_created', models.DateTimeField(help_text='Date Datatype was defined', verbose_name='Date created', auto_now_add=True)),
                ('custom_constraint', models.OneToOneField(null=True, blank=True, to='metadata.CustomConstraint')),
                ('prototype', models.OneToOneField(related_name='datatype_modelled', null=True, blank=True, to='archive.Dataset')),
                ('restricts', models.ManyToManyField(help_text='Captures hierarchical is-a classifications among Datatypes', related_name='restricted_by', null=True, to='metadata.Datatype', blank=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='compounddatatypemember',
            name='datatype',
            field=models.ForeignKey(help_text='Specifies which DataType this member is', to='metadata.Datatype'),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='compounddatatypemember',
            unique_together=set([('compounddatatype', 'column_idx'), ('compounddatatype', 'column_name')]),
        ),
        migrations.AddField(
            model_name='basicconstraint',
            name='datatype',
            field=models.ForeignKey(related_name='basic_constraints', to='metadata.Datatype'),
            preserve_default=True,
        ),
    ]
