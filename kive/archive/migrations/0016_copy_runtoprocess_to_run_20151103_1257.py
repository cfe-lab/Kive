# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations, transaction
from django.utils import timezone


@transaction.atomic
def rtp_to_run(apps, schema_editor):
    """
    Copy the contents of RunToProcess over to Run.

    Create new Runs for those RunToProcesses that don't have Runs yet.
    """
    Run = apps.get_model("archive", "Run")
    RunExceedsSystemCapabilities = apps.get_model("archive", "ExceedsSystemCapabilities")
    RunToProcess = apps.get_model("fleet", "RunToProcess")

    for rtp in RunToProcess.objects.all():
        run_to_combine = rtp.run
        if run_to_combine is None:
            # Create a new dummy Run for this RunToProcess.
            run_to_combine = Run(user=rtp.user)
        run_to_combine.pipeline = rtp.pipeline
        run_to_combine.sandbox_path = rtp.sandbox_path
        run_to_combine.time_queued = rtp.time_queued
        run_to_combine.purged = rtp.purged
        run_to_combine.name = rtp.name
        run_to_combine.description = rtp.description

        run_to_combine.save()
        run_to_combine.users_allowed.add(*rtp.users_allowed.all())
        run_to_combine.groups_allowed.add(*rtp.groups_allowed.all())

        for rtp_in in rtp.inputs.all():
            run_to_combine.inputs.get_or_create(
                symbolicdataset=rtp_in.symbolicdataset,
                index=rtp_in.index
            )

        if hasattr(rtp, "not_enough_CPUs"):
            if not hasattr(run_to_combine, "not_enough_CPUs"):
                esc = RunExceedsSystemCapabilities(run=run_to_combine)
            else:
                esc = run_to_combine.not_enough_CPUs

            esc.threads_requested = rtp.not_enough_CPUs.threads_requested
            esc.max_available = rtp.not_enough_CPUs.max_available
            esc.save()

        rtp.run = run_to_combine
        rtp.save()


@transaction.atomic
def run_to_rtp(apps, schema_editor):
    """
    Reverse the operation of rtp_to_run.
    """
    Run = apps.get_model("archive", "Run")
    RunToProcess = apps.get_model("fleet", "RunToProcess")
    ExceedsSystemCapabilities = apps.get_model("fleet", "ExceedsSystemCapabilities")

    # For every top-level Run that has a time_queued (i.e. was run by the fleet) and
    # does not already have a matching RunToProcess, we create one.
    runs_with_rtp = RunToProcess.objects.values_list("run__pk", flat=True)
    for top_level_run in Run.objects.exclude(pk__in=runs_with_rtp).filter(time_queued__isnull=False,
                                                                          parent_runstep__isnull=True):
        if hasattr(top_level_run, "runtoprocess"):
            rtp_to_recreate = top_level_run.runtoprocess
        else:
            rtp_to_recreate = RunToProcess(run=top_level_run)

        rtp_to_recreate.user = top_level_run.user
        rtp_to_recreate.pipeline = top_level_run.pipeline
        rtp_to_recreate.sandbox_path = top_level_run.sandbox_path

        time_queued = top_level_run.time_queued
        if time_queued is None:
            if top_level_run.start_time is not None:
                time_queued = top_level_run.start_time
            else:
                time_queued = timezone.now()
        rtp_to_recreate.time_queued = time_queued

        rtp_to_recreate.purged = top_level_run.purged
        rtp_to_recreate.name = top_level_run.name
        rtp_to_recreate.description = top_level_run.description

        rtp_to_recreate.save()
        rtp_to_recreate.users_allowed.add(*top_level_run.users_allowed.all())
        rtp_to_recreate.groups_allowed.add(*top_level_run.groups_allowed.all())

        for run_in in top_level_run.inputs.all():
            rtp_to_recreate.inputs.get_or_create(
                symbolicdataset=run_in.symbolicdataset,
                index=run_in.index
            )

        if hasattr(top_level_run, "not_enough_CPUs"):
            if not hasattr(rtp_to_recreate, "not_enough_CPUs"):
                esc = ExceedsSystemCapabilities(runtoprocess=rtp_to_recreate)
            else:
                esc = rtp_to_recreate.not_enough_CPUs

            esc.threads_requested = top_level_run.not_enough_CPUs.threads_requested
            esc.max_available = top_level_run.not_enough_CPUs.max_available
            esc.save()


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0015_combine_run_runtoprocess_20151103_1606'),
        ('fleet', '0008_rtp_time_queued_default_20151103_1606'),
    ]

    operations = [
        migrations.RunPython(rtp_to_run, run_to_rtp)
    ]
