# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations, transaction
from django.core.files.base import ContentFile

import tempfile
import os

# We hard-code the upload directory for SymbolicDatasets.
UPLOAD_DIR = "Datasets"


@transaction.atomic
def dataset_to_symbolicdataset(apps, schema_editor):
    """
    Merge Datasets into their SymbolicDatasets.

    Lots of hacks in here:
     - replacing the SymbolicDataset's dataset_file with the Dataset's
     - manually setting UPLOAD_DIR for a Dataset (the callable in Dataset's
       upload_to field requires this)
    """
    Dataset = apps.get_model("archive", "Dataset")
    Datatype = apps.get_model("metadata", "Datatype")

    for ds in Dataset.objects.all():
        sd = ds.symbolicdataset
        sd.name = ds.name
        sd.description = ds.description
        sd.date_created = ds.date_created
        sd.created_by = ds.created_by
        # This procedure isn't documented....
        sd.dataset_file = ds.dataset_file
        sd.save()

        dt_prototyped_by = Datatype.objects.filter(prototype=ds)
        if dt_prototyped_by.exists():
            dt = dt_prototyped_by.first()
            dt.proto_SD = sd
            dt.prototype = None
            dt.save()

        # Since the SymbolicDataset is using the same filename as the Dataset,
        # we replace the Dataset's file with a dummy for now before we delete it.
        try:
            dummy_fd, dummy_filename = tempfile.mkstemp()
        finally:
            os.close(dummy_fd)

        ds.UPLOAD_DIR = UPLOAD_DIR
        ds.dataset_file.save(os.path.basename(dummy_filename), ContentFile(""), save=True)
        ds.dataset_file.delete()
        ds.delete()
        os.remove(dummy_filename)


@transaction.atomic
def symbolicdataset_to_dataset(apps, schema_editor):
    """
    Reverse the operation of dataset_to_symbolicdataset.

    This has the same hacks as dataset_to_symbolicdataset.
    """
    SymbolicDataset = apps.get_model("librarian", "SymbolicDataset")
    Dataset = apps.get_model("archive", "Dataset")
    Datatype = apps.get_model("metadata", "Datatype")

    # Create Datasets for any SymbolicDataset that doesn't already have an
    # associated Dataset.
    already_have_datasets = Dataset.objects.values_list("symbolicdataset__pk", flat=True)
    for sd in SymbolicDataset.objects.filter(dataset_file__isnull=False).exclude(pk__in=already_have_datasets):
        ds = Dataset(
            symbolicdataset=sd,
            name=sd.name,
            description=sd.description,
            date_created=sd.date_created,
            created_by=sd.created_by,
            dataset_file=sd.dataset_file
        )
        ds.save()

        dt_prototyped_by = Datatype.objects.filter(proto_SD=sd)
        if dt_prototyped_by.exists():
            dt = dt_prototyped_by.first()
            dt.prototype = ds
            dt.proto_SD = None
            dt.save()

        sd.UPLOAD_DIR = UPLOAD_DIR
        sd.dataset_file = None
        sd.save()


class Migration(migrations.Migration):

    dependencies = [
        ('librarian', '0005_merge_dataset_SD_20151116_1012'),
        ('archive', '0020_date_created_default_20151116_1040'),
        ('metadata', '0010_datatype_proto_sd')
    ]

    operations = [
        migrations.RunPython(dataset_to_symbolicdataset, symbolicdataset_to_dataset)
    ]
