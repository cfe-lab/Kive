# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '__first__'),
    ]

    operations = [
        migrations.CreateModel(
            name='CodeResource',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(help_text='The name for this resource and all subsequent revisions.', unique=True, max_length=60, verbose_name='Resource name')),
                ('filename', models.CharField(blank=True, help_text='The filename for this resource', max_length=260, verbose_name='Resource file name', validators=[django.core.validators.RegexValidator(regex='^(\x08|([-_.()\\w]+ *)*[-_.()\\w]+)$', message='Invalid code resource filename')])),
                ('description', models.TextField(max_length=1000, verbose_name='Resource description', blank=True)),
            ],
            options={
                'ordering': ('name',),
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='CodeResourceDependency',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('depPath', models.CharField(help_text="Where a code resource dependency must exist in the sandbox relative to it's parent", max_length=255, verbose_name='Dependency path', blank=True)),
                ('depFileName', models.CharField(help_text='The file name the dependency is given on the sandbox at execution', max_length=255, verbose_name='Dependency file name', blank=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='CodeResourceRevision',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('revision_number', models.IntegerField(help_text='Revision number of code resource', verbose_name='Revision number', blank=True)),
                ('revision_name', models.CharField(help_text='A name to differentiate revisions of a CodeResource', max_length=60, blank=True)),
                ('revision_DateTime', models.DateTimeField(help_text='Date this resource revision was uploaded', auto_now_add=True)),
                ('revision_desc', models.TextField(help_text='A description for this particular resource revision', max_length=1000, verbose_name='Revision description', blank=True)),
                ('content_file', models.FileField(help_text='File contents of this code resource revision', upload_to='CodeResources', null=True, verbose_name='File contents', blank=True)),
                ('MD5_checksum', models.CharField(help_text='Used to validate file contents of this resource revision', max_length=64, blank=True)),
                ('coderesource', models.ForeignKey(related_name='revisions', to='method.CodeResource')),
                ('revision_parent', models.ForeignKey(related_name='descendants', blank=True, to='method.CodeResourceRevision', null=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Method',
            fields=[
                ('transformation_ptr', models.OneToOneField(parent_link=True, auto_created=True, primary_key=True, serialize=False, to='transformation.Transformation')),
                ('revision_number', models.PositiveIntegerField(help_text='Revision number of this Method in its family', verbose_name='Method revision number', blank=True)),
                ('reusable', models.PositiveSmallIntegerField(default=1, help_text='Is the output of this method the same if you run it again with the same inputs?\n\ndeterministic: always exactly the same\n\nreusable: the same but with some insignificant differences (e.g., rows are shuffled)\n\nnon-reusable: no -- there may be meaningful differences each time (e.g., timestamp)\n', choices=[(1, 'deterministic'), (2, 'reusable'), (3, 'non-reusable')])),
                ('tainted', models.BooleanField(default=False, help_text='Is this Method broken?')),
                ('threads', models.PositiveIntegerField(default=1, help_text='How many threads does this Method use during execution?', verbose_name='Number of threads', validators=[django.core.validators.MinValueValidator(1)])),
                ('driver', models.ForeignKey(to='method.CodeResourceRevision')),
            ],
            options={
            },
            bases=('transformation.transformation',),
        ),
        migrations.CreateModel(
            name='MethodFamily',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(help_text='The name given to a group of methods/pipelines', unique=True, max_length=60, verbose_name='Transformation family name')),
                ('description', models.TextField(help_text='A description for this collection of methods/pipelines', max_length=1000, verbose_name='Transformation family description', blank=True)),
            ],
            options={
                'ordering': ('name',),
                'abstract': False,
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='method',
            name='family',
            field=models.ForeignKey(related_name='members', to='method.MethodFamily'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='method',
            name='revision_parent',
            field=models.ForeignKey(related_name='descendants', blank=True, to='method.Method', null=True),
            preserve_default=True,
        ),
        migrations.AlterUniqueTogether(
            name='method',
            unique_together=set([('family', 'revision_number')]),
        ),
        migrations.AddField(
            model_name='coderesourcedependency',
            name='coderesourcerevision',
            field=models.ForeignKey(related_name='dependencies', to='method.CodeResourceRevision'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='coderesourcedependency',
            name='requirement',
            field=models.ForeignKey(related_name='needed_by', to='method.CodeResourceRevision'),
            preserve_default=True,
        ),
    ]
