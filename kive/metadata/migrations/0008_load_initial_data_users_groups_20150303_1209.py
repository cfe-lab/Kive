# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations, transaction
from django.core.management import call_command
from django.contrib.auth.management import create_permissions
from django.apps import apps as django_apps

from datetime import datetime
from pytz import timezone

import portal.utils


@transaction.atomic
def load_initial_groups(apps, schema_editor):
    # update_all_contenttypes(verbosity=0)
    portal.utils.update_all_contenttypes(verbosity=0)
    auth_app_config = django_apps.get_app_config("auth")
    create_permissions(auth_app_config, verbosity=0)
    call_command("loaddata", "initial_groups", app_label="metadata")


@transaction.atomic
def load_initial_user(apps, schema_editor):
    call_command("loaddata", "initial_user", app_label="metadata")


@transaction.atomic
def load_initial_data(apps, schema_editor):
    """
    Defines some system built-in Datatypes and CompoundDatatypes.

    The DTs we create are:
     - string
     - boolean
     - integer
     - float
     - natural number
     - molecular sequence
     - nucleotide sequence

    The CDTs we create are
     - verif_in: (string to_test?)
     - verif_out: (NaturalNumber failed_row)
     - prototype_cdt: (string example?, bool valid)
     - fasta_cdt: (string FASTA header?, molecular sequence FASTA sequence?)
    """
    User = apps.get_model("auth", "User")
    Group = apps.get_model("auth", "Group")
    kive_user = User.objects.get(pk=1)
    everyone_group = Group.objects.get(pk=1)

    local_tz = timezone("America/Vancouver")

    Datatype = apps.get_model("metadata", "Datatype")
    CompoundDatatype = apps.get_model("metadata", "CompoundDatatype")

    # Note: even though Datatype typically has auto_now_add=True set, here we
    # need to specify it.  This is good, as we would rather not have the date
    # change every time you do this migration.
    STR_DT = Datatype(
        pk=1,
        name="string",
        description="basic string type",
        user=kive_user,
        date_created=datetime(2013, 11, 13, 19, 47, 19, 853000, local_tz)  # "2013-11-13T19:47:19.853Z"
    )
    STR_DT.save()
    STR_DT.groups_allowed.add(everyone_group)

    BOOL_DT = Datatype(
        pk=2,
        name="boolean",
        description="basic boolean type",
        user=kive_user,
        date_created=datetime(2013, 11, 13, 19, 47, 19, 856000, local_tz)  # "2013-11-13T19:47:19.856Z"
    )
    BOOL_DT.save()
    BOOL_DT.restricts.add(STR_DT)
    BOOL_DT.groups_allowed.add(everyone_group)

    FLOAT_DT = Datatype(
        pk=3,
        name="float",
        description="basic float type",
        user=kive_user,
        date_created=datetime(2013, 11, 13, 19, 47, 19, 874000, local_tz)  # "2013-11-13T19:47:19.874Z"
    )
    FLOAT_DT.save()
    FLOAT_DT.restricts.add(STR_DT)
    FLOAT_DT.groups_allowed.add(everyone_group)

    INT_DT = Datatype(
        pk=4,
        name="integer",
        description="basic integer type",
        user=kive_user,
        date_created=datetime(2013, 11, 13, 19, 47, 19, 882000, local_tz)  # "2013-11-13T19:47:19.882Z"
    )
    INT_DT.save()
    # We don't need to add STR_DT as a restricted type because FLOAT_DT
    # already restricts it.
    INT_DT.restricts.add(FLOAT_DT)
    INT_DT.groups_allowed.add(everyone_group)

    NaturalNumber_DT = Datatype(
        pk=5,
        name="natural number",
        description="positive integer",
        user=kive_user,
        date_created=datetime(2013, 11, 13, 19, 47, 19, 890000, local_tz)  # "2013-11-13T19:47:19.890Z"
    )
    NaturalNumber_DT.save()
    NaturalNumber_DT.restricts.add(INT_DT)
    NaturalNumber_DT.basic_constraints.create(
        pk=1,
        ruletype="minval",
        rule="1"
    )
    NaturalNumber_DT.groups_allowed.add(everyone_group)

    MolSeq_DT = Datatype(
        pk=6,
        name="molecular sequence",
        description="String of IUPAC symbols representing either the primary protein sequence "
                    "(amino acids) or a nucleotide sequence (RNA or DNA).  Only standard "
                    "placeholders permitted (gap character '-', ambiguous amino acid '?', "
                    "stop codon '*').",
        user=kive_user,
        date_created=datetime(2014, 6, 10, 16, 36, 44, 948000, local_tz)  # "2014-06-10T16:36:44.948Z"
    )
    MolSeq_DT.save()
    MolSeq_DT.restricts.add(STR_DT)
    MolSeq_DT.basic_constraints.create(
        pk=3,
        ruletype="regexp",
        rule="[A-Za-z*?-]*"
    )
    MolSeq_DT.groups_allowed.add(everyone_group)

    NucSeq_DT = Datatype(
        pk=7,
        name="nucleotide sequence",
        description="A string of IUPAC symbols representing DNA or RNA, including ambiguous bases.",
        user=kive_user,
        date_created=datetime(2014, 6, 10, 18, 32, 15, 268000, local_tz)  # "2014-06-10T18:32:15.268Z"
    )
    NucSeq_DT.save()
    NucSeq_DT.restricts.add(STR_DT)
    NucSeq_DT.basic_constraints.create(
        pk=5,
        ruletype="regexp",
        rule="[ACGTUNacgtuWRKYSMBDHVNwrkysmbdhvn-]*"
    )
    NucSeq_DT.groups_allowed.add(everyone_group)

    verif_in = CompoundDatatype(pk=1, user=kive_user)
    verif_in.save()
    verif_in.members.create(pk=1, datatype=STR_DT, column_name="to_test", blankable=True, column_idx=1)
    verif_in.groups_allowed.add(everyone_group)

    verif_out = CompoundDatatype(pk=2, user=kive_user)
    verif_out.save()
    verif_out.members.create(pk=2, datatype=NaturalNumber_DT, column_name="failed_row", blankable=False, column_idx=1)
    verif_out.groups_allowed.add(everyone_group)

    prototype_cdt = CompoundDatatype(pk=3, user=kive_user)
    prototype_cdt.save()
    prototype_cdt.members.create(pk=3, datatype=STR_DT, column_name="example", blankable=True, column_idx=1)
    prototype_cdt.members.create(pk=4, datatype=BOOL_DT, column_name="valid", blankable=False, column_idx=2)
    prototype_cdt.groups_allowed.add(everyone_group)

    fasta_cdt = CompoundDatatype(pk=4, user=kive_user)
    fasta_cdt.save()
    fasta_cdt.members.create(pk=5, datatype=STR_DT, column_name="FASTA header", blankable=True, column_idx=1)
    fasta_cdt.members.create(pk=6, datatype=MolSeq_DT, column_name="FASTA sequence", blankable=True, column_idx=2)
    fasta_cdt.groups_allowed.add(everyone_group)


@transaction.atomic
def purge_initial_data(apps, schema_editor):
    """
    Reverse operation of load_initial_data.
    """
    Datatype = apps.get_model("metadata", "Datatype")
    CompoundDatatype = apps.get_model("metadata", "CompoundDatatype")

    for cdt_pk in [1,2,3,4]:
        cdt = CompoundDatatype.objects.get(pk=cdt_pk)
        for member in cdt.members.all():
            member.delete()
        cdt.delete()

    for dt_pk in range(1, 8):
        dt = Datatype.objects.get(pk=dt_pk)
        for bc in dt.basic_constraints.all():
            bc.delete()
        dt.delete()


class Migration(migrations.Migration):

    dependencies = [
        ("metadata", "0007_auto_20150218_1045"),
        ("contenttypes", "0002_remove_content_type_name")
    ]

    operations = [
        migrations.RunPython(load_initial_groups, reverse_code=migrations.RunPython.noop),
        migrations.RunPython(load_initial_user, reverse_code=migrations.RunPython.noop),
        migrations.RunPython(load_initial_data, reverse_code=purge_initial_data)
    ]
