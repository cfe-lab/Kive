# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-06-02 21:31
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('datachecking', '0101_squashed'),
        ('archive', '0101_squashed'),
        # ('librarian', '0101_squashed'),
        ('metadata', '0101_squashed'),
    ]

    operations = [
        migrations.AlterField(
            model_name='cellerror',
            name='column',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='metadata.CompoundDatatypeMember'),
        ),
        migrations.AlterField(
            model_name='contentchecklog',
            name='dataset',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='content_checks', to='librarian.Dataset'),
        ),
        migrations.AlterField(
            model_name='contentchecklog',
            name='execlog',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='content_checks', to='archive.ExecLog'),
        ),
        migrations.AlterField(
            model_name='integritychecklog',
            name='dataset',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='integrity_checks', to='librarian.Dataset'),
        ),
        migrations.AlterField(
            model_name='integritychecklog',
            name='execlog',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='integrity_checks', to='archive.ExecLog'),
        ),
        migrations.AlterField(
            model_name='integritychecklog',
            name='runsic',
            field=models.OneToOneField(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='input_integrity_check', to='archive.RunSIC'),
        ),
        migrations.AlterField(
            model_name='md5conflict',
            name='conflicting_dataset',
            field=models.OneToOneField(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='usurps', to='librarian.Dataset'),
        ),
        migrations.AlterField(
            model_name='verificationlog',
            name='CDTM',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='metadata.CompoundDatatypeMember'),
        ),
    ]
