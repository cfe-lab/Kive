# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-06-02 00:10
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0101_squashed'),
        ('librarian', '0101_squashed'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datatype',
            name='prototype',
            field=models.OneToOneField(blank=True,
                                       null=True,
                                       on_delete=django.db.models.deletion.SET_NULL,
                                       related_name='datatype_modelled',
                                       to='librarian.Dataset'),
        ),
    ]
