#! /usr/bin/env python

from librarian.models import SymbolicDataset
from fleet.models import RunToProcess
from django.contrib.auth.models import User
from django.db import transaction
from pipeline.models import Pipeline

shipyard_user = User.objects.get(pk=1)
test_pipeline = Pipeline.objects.get(pk=2)
test_fasta = SymbolicDataset.objects.get(pk=1)

with transaction.atomic():
    new_job = RunToProcess(user=shipyard_user, pipeline=test_pipeline)
    new_job.save()
    new_job.inputs.create(symbolicdataset=test_fasta, index=1)
