# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import re
import django.db.models.deletion
from django.conf import settings
import django.core.validators


class Migration(migrations.Migration):

    replaces = [(b'metadata', '0001_initial'),
                (b'metadata', '0002_compounddatatypemember_blankable'),
                (b'metadata', '0003_auto_20150212_1013'),
                (b'metadata', '0004_auto_20150213_1703'),
                (b'metadata', '0005_kiveuser'),
                (b'metadata', '0006_auto_20150217_1254'),
                (b'metadata', '0007_auto_20150218_1045'),
                (b'metadata', '0008_load_initial_data_users_groups_20150303_1209'),
                (b'metadata', '0009_redacted_20150417_1128')]

    dependencies = [
        ('auth', '0001_initial'),
        ('method', '__first__'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='BasicConstraint',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('ruletype', models.CharField(max_length=32,
                                              choices=[('minlen', 'minimum string length'),
                                                       ('maxlen', 'maximum string length'),
                                                       ('minval', 'minimum numeric value'),
                                                       ('maxval', 'maximum numeric value'),
                                                       ('regexp', 'Perl regular expression'),
                                                       ('datetimeformat', 'date format string (1989 C standard)')],
                                              verbose_name='Type of rule',
                                              validators=[django.core.validators.RegexValidator(
                                                re.compile('minlen|maxlen|minval|maxval|regexp|datetimeformat'))])),
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
                ('name', models.CharField(help_text='The name for this Datatype', max_length=60, verbose_name='Datatype name')),
                ('description', models.TextField(help_text='A description for this Datatype', max_length=1000, verbose_name='Datatype description')),
                ('date_created', models.DateTimeField(help_text='Date Datatype was defined', verbose_name='Date created', auto_now_add=True)),
                ('custom_constraint', models.OneToOneField(null=True, blank=True, to='metadata.CustomConstraint')),
                ('restricts', models.ManyToManyField(help_text='Captures hierarchical is-a classifications among Datatypes', related_name='restricted_by', null=True, to=b'metadata.Datatype', blank=True)),
                ('groups_allowed', models.ManyToManyField(help_text='What groups have access?', related_name='metadata_datatype_has_access_to', null=True, to=b'auth.Group', blank=True)),
                ('user', models.ForeignKey(default=1, to=settings.AUTH_USER_MODEL)),
                ('users_allowed', models.ManyToManyField(help_text='Which users have access?', related_name='metadata_datatype_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True)),
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
        migrations.AddField(
            model_name='compounddatatypemember',
            name='blankable',
            field=models.BooleanField(default=False, help_text='Can this entry be left blank?'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='metadata_compounddatatype_has_access_to', null=True, to=b'auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='user',
            field=models.ForeignKey(default=1, to=settings.AUTH_USER_MODEL),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='metadata_compounddatatype_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
        migrations.CreateModel(
            name='KiveUser',
            fields=[
            ],
            options={
                'proxy': True,
            },
            bases=('auth.user',),
        ),
        migrations.AlterUniqueTogether(
            name='datatype',
            unique_together=set([('user', 'name')]),
        ),
        migrations.RemoveField(
            model_name='datatype',
            name='custom_constraint',
        ),
        migrations.AddField(
            model_name='customconstraint',
            name='datatype',
            field=models.OneToOneField(related_name='custom_constraint', default=1, to='metadata.Datatype'),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='compounddatatypemember',
            name='datatype',
            field=models.ForeignKey(related_name='CDTMs', to='metadata.Datatype', help_text='Specifies which DataType this member is'),
            preserve_default=True,
        ),
    ]
