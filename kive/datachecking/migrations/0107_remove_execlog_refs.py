# -*- coding: utf-8 -*-
# Generated by Django 1.11.21 on 2019-06-14 23:45
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datachecking', '0106_baddata_file_not_stable'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='contentchecklog',
            name='execlog',
        ),
        migrations.RemoveField(
            model_name='integritychecklog',
            name='execlog',
        ),
        migrations.RemoveField(
            model_name='integritychecklog',
            name='runcomponent',
        ),
    ]
