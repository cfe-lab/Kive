# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-05-04 20:06
from __future__ import unicode_literals

from django.db import migrations, transaction


@transaction.atomic
def create_states(apps, schema_editor):
    """
    Create the built-in RunStates and RunComponentStates.
    """
    RunState = apps.get_model("archive", "RunState")

    pending, _ = RunState.objects.get_or_create(
        name="Pending",
        description="Has not started execution yet"
    )
    assert pending.pk == 1

    running, _ = RunState.objects.get_or_create(
        name="Running",
        description="Executing; no issues reported"
    )
    assert running.pk == 2

    successful, _ = RunState.objects.get_or_create(
        name="Successful",
        description="Execution complete and successful"
    )
    assert successful.pk == 3

    cancelling, _ = RunState.objects.get_or_create(
        name="Cancelling",
        description="Execution cancelling; will finalize when all components are stopped"
    )
    assert cancelling.pk == 4

    cancelled, _ = RunState.objects.get_or_create(
        name="Cancelled",
        description="Execution cancelled and finalized"
    )
    assert cancelled.pk == 5

    failing, _ = RunState.objects.get_or_create(
        name="Failing",
        description="Execution failed; some components running"
    )
    assert failing.pk == 6

    failed, _ = RunState.objects.get_or_create(
        name="Failed",
        description="Execution failed; no components running"
    )
    assert failed.pk == 7

    quarantined, _ = RunState.objects.get_or_create(
        name="Quarantined",
        description="Execution finished successfully but a component is quarantined"
    )
    assert quarantined.pk == 8

    # Create RunComponentStates.
    RunComponentState = apps.get_model("archive", "RunComponentState")

    pending, _ = RunComponentState.objects.get_or_create(
        name="Pending",
        description="Has not started execution yet"
    )
    assert pending.pk == 1

    running, _ = RunComponentState.objects.get_or_create(
        name="Running",
        description="Executing"
    )
    assert running.pk == 2

    successful, _ = RunComponentState.objects.get_or_create(
        name="Successful",
        description="Execution complete and successful"
    )
    assert successful.pk == 3

    cancelled, _ = RunComponentState.objects.get_or_create(
        name="Cancelled",
        description="Execution cancelled"
    )
    assert cancelled.pk == 4

    failed, _ = RunComponentState.objects.get_or_create(
        name="Failed",
        description="Execution finished unsuccessfully"
    )
    assert failed.pk == 5

    quarantined, _ = RunComponentState.objects.get_or_create(
        name="Quarantined",
        description="Execution finished successfully but results have been invalidated"
    )
    assert quarantined.pk == 6


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0028_add_state_classes'),
    ]

    operations = [
        migrations.RunPython(create_states, migrations.RunPython.noop)
    ]
