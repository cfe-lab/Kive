# -*- coding: utf-8 -*-
# Generated by Django 1.11.21 on 2019-06-14 22:37
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('librarian', '0111_is_uploaded_false'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='dataset',
            name='file_source',
        ),
    ]
