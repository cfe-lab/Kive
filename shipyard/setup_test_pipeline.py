#! /usr/bin/env python

# Add a FASTA file to the database for testing purposes.

from librarian.models import SymbolicDataset
from fleet.models import RunToProcess
from django.contrib.auth.models import User
from django.db import transaction
from pipeline.models import Pipeline

shipyard_user = User.objects.get(pk=1)
test_pipeline = Pipeline.objects.get(pk=2)

test_fasta = SymbolicDataset.create_SD(
    "../samplecode/step_0_raw.fasta",
    cdt=None,
    make_dataset=True,
    user=shipyard_user,
    name="TestFASTA",
    description="Toy FASTA file for testing pipelines"
    )

with transaction.atomic():
    new_job = RunToProcess(user=shipyard_user, pipeline=test_pipeline)
    new_job.save()

    new_job.inputs.create(symbolicdataset=test_fasta, index=1)
