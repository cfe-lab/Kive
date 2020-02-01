# -*- coding: utf-8 -*-
# Generated by Django 1.11.21 on 2019-06-19 18:40
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '0103_no_default_user'),
        ('method', '0110_drop_dockerimage'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='coderesource',
            name='groups_allowed',
        ),
        migrations.RemoveField(
            model_name='coderesource',
            name='user',
        ),
        migrations.RemoveField(
            model_name='coderesource',
            name='users_allowed',
        ),
        migrations.AlterUniqueTogether(
            name='coderesourcerevision',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='coderesourcerevision',
            name='coderesource',
        ),
        migrations.RemoveField(
            model_name='coderesourcerevision',
            name='groups_allowed',
        ),
        migrations.RemoveField(
            model_name='coderesourcerevision',
            name='revision_parent',
        ),
        migrations.RemoveField(
            model_name='coderesourcerevision',
            name='user',
        ),
        migrations.RemoveField(
            model_name='coderesourcerevision',
            name='users_allowed',
        ),
        migrations.AlterUniqueTogether(
            name='method',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='method',
            name='container',
        ),
        migrations.RemoveField(
            model_name='method',
            name='driver',
        ),
        migrations.RemoveField(
            model_name='method',
            name='family',
        ),
        migrations.RemoveField(
            model_name='method',
            name='revision_parent',
        ),
        migrations.RemoveField(
            model_name='method',
            name='transformation_ptr',
        ),
        migrations.RemoveField(
            model_name='methoddependency',
            name='method',
        ),
        migrations.RemoveField(
            model_name='methoddependency',
            name='requirement',
        ),
        migrations.AlterUniqueTogether(
            name='methodfamily',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='methodfamily',
            name='groups_allowed',
        ),
        migrations.RemoveField(
            model_name='methodfamily',
            name='user',
        ),
        migrations.RemoveField(
            model_name='methodfamily',
            name='users_allowed',
        ),
        migrations.DeleteModel(
            name='CodeResource',
        ),
        migrations.DeleteModel(
            name='CodeResourceRevision',
        ),
        migrations.DeleteModel(
            name='Method',
        ),
        migrations.DeleteModel(
            name='MethodDependency',
        ),
        migrations.DeleteModel(
            name='MethodFamily',
        ),
    ]