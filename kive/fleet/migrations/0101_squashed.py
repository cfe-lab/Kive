# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-06-03 00:17
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('auth', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        # ('archive', '0002_auto_20150128_0950'),
        # ('pipeline', '0001_initial'),
        # ('librarian', '0001_initial'),
    ]

    operations = [
    ]
