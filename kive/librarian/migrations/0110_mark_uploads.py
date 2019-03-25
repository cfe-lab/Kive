# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-03-20 21:27
from __future__ import unicode_literals

from django.db import migrations


# noinspection PyUnusedLocal
def mark_uploads(apps, schema_editor):
    # noinspection PyPep8Naming
    Dataset = apps.get_model('librarian', 'Dataset')
    # noinspection PyPep8Naming
    ContainerDataset = apps.get_model('container', 'ContainerDataset')
    # noinspection PyPep8Naming
    MD5Conflict = apps.get_model('datachecking', 'MD5Conflict')

    Dataset.objects.exclude(file_source=None).update(is_uploaded=False)
    container_output_ids = ContainerDataset.objects.filter(
        argument__type='O').values_list('dataset_id').order_by()
    Dataset.objects.filter(
        pk__in=container_output_ids).update(is_uploaded=False)
    md5_conflict_ids = MD5Conflict.objects.values_list(
        'conflicting_dataset_id').order_by()
    Dataset.objects.filter(pk__in=md5_conflict_ids).update(is_uploaded=False)


# noinspection PyUnusedLocal
def unmark_uploads(apps, schema_editor):
    # noinspection PyPep8Naming
    Dataset = apps.get_model('librarian', 'Dataset')
    Dataset.objects.update(is_uploaded=True)


class Migration(migrations.Migration):
    dependencies = [('librarian', '0109_dataset_is_uploaded')]

    operations = [migrations.RunPython(mark_uploads, unmark_uploads)]