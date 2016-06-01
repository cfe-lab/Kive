# -*- coding: utf-8 -*-
# Generated by Django 1.9.2 on 2016-06-01 23:53
from __future__ import unicode_literals

import re
from datetime import datetime
from pytz import timezone

from django.apps import apps as django_apps
from django.conf import settings
from django.contrib.auth.management import create_permissions
from django.core.management.color import no_style
from django.core.management import call_command
import django.core.validators
from django.db import models, migrations, transaction, DEFAULT_DB_ALIAS, connections
import django.db.models.deletion

import portal.utils


@transaction.atomic
def load_initial_groups(apps, schema_editor):
    # update_all_contenttypes(verbosity=0)
    portal.utils.update_all_contenttypes(verbosity=0)
    auth_app_config = django_apps.get_app_config("auth")
    create_permissions(auth_app_config, verbosity=0)
    call_command("loaddata", "initial_groups", app_label="metadata")


def load_initial_user(apps, schema_editor):
    call_command("loaddata", "initial_user", app_label="metadata")


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
    BasicConstraint = apps.get_model("metadata", "BasicConstraint")
    CompoundDatatypeMember = apps.get_model("metadata", "CompoundDatatypeMember")

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

    conn = connections[DEFAULT_DB_ALIAS]
    if conn.features.supports_sequence_reset:
        sql_list = conn.ops.sequence_reset_sql(
            no_style(), [Datatype, BasicConstraint, CompoundDatatype, CompoundDatatypeMember])
        if sql_list:
            with transaction.atomic(using=DEFAULT_DB_ALIAS):
                cursor = conn.cursor()
                for sql in sql_list:
                    cursor.execute(sql)


@transaction.atomic
def purge_initial_data(apps, schema_editor):
    """
    Reverse operation of load_initial_data.
    """
    Datatype = apps.get_model("metadata", "Datatype")
    CompoundDatatype = apps.get_model("metadata", "CompoundDatatype")

    for cdt_pk in [1, 2, 3, 4]:
        cdt = CompoundDatatype.objects.get(pk=cdt_pk)
        for member in cdt.members.all():
            member.delete()
        cdt.delete()

    for dt_pk in range(1, 8):
        dt = Datatype.objects.get(pk=dt_pk)
        for bc in dt.basic_constraints.all():
            bc.delete()
        dt.delete()


@transaction.atomic
def set_cdt_names(apps, schema_editor):
    """
    Set the names of all CompoundDatatypes.
    """
    CompoundDatatype = apps.get_model("metadata", "CompoundDatatype")
    CompoundDatatypeMember = apps.get_model("metadata", "CompoundDatatypeMember")
    Datatype = apps.get_model("metadata", "Datatype")
    for cdt in CompoundDatatype.objects.all():
        # Since methods of the CDT are not available here (and we wouldn't want
        # this procedure to change later), we recreate the _format() method.
        members = CompoundDatatypeMember.objects.filter(compounddatatype=cdt).order_by("column_idx")

        string_rep = "("

        member_reps = []
        for member in members:
            # This is a copy of the _str_ method of CompoundDatatypeMember.
            dt = Datatype.objects.get(pk=member.datatype.pk)
            blankable_marker = "?" if member.blankable else ""
            member_rep = '{}: {}{}'.format(member.column_name,
                                           dt.name,
                                           blankable_marker)
            member_reps.append(member_rep)

        string_rep += ", ".join(member_reps)

        string_rep += ")"
        if string_rep == "()":
            string_rep = "[empty CompoundDatatype]"

        cdt.name = string_rep
        cdt.save()


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    replaces = [(b'metadata', '0001_initial'),
                (b'metadata', '0002_compounddatatypemember_blankable'),
                (b'metadata', '0003_auto_20150212_1013'),
                (b'metadata', '0004_auto_20150213_1703'),
                (b'metadata', '0005_kiveuser'),
                (b'metadata', '0006_auto_20150217_1254'),
                (b'metadata', '0007_auto_20150218_1045'),
                (b'metadata', '0008_load_initial_data_users_groups_20150303_1209'),
                (b'metadata', '0009_redacted_20150417_1128'),
                (b'metadata', '0010_datatype_proto_sd'),
                (b'metadata', '0011_remove_datatype_prototype'),
                (b'metadata', '0012_transition_SD_to_dataset_20151117_1748'),
                (b'metadata', '0013_permissions_remove_null_20160203_1033'),
                (b'metadata', '0014_restricts_remove_null_20160203_1038'),
                (b'metadata', '0015_compounddatatype_name'),
                (b'metadata', '0016_set_cdt_names_20160215_1525'),
                (b'metadata', '0017_order_cdt_by_name_20160215_1637'),
                (b'metadata', '0100_unlink_apps')]
    initial = True

    dependencies = [
        ('contenttypes', '0002_remove_content_type_name'),
        ('method', '__first__'),
        ('archive', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('auth', '0001_initial'),
        # ('librarian', '0005_merge_dataset_SD_20151116_1012'),
        # ('librarian', '0007_transition_SD_to_dataset_20151117_1748'),
    ]

    operations = [
        migrations.CreateModel(
            name='BasicConstraint',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ruletype',
                 models.CharField(choices=[('minlen', 'minimum string length'),
                                           ('maxlen', 'maximum string length'),
                                           ('minval', 'minimum numeric value'),
                                           ('maxval', 'maximum numeric value'),
                                           ('regexp', 'Perl regular expression'),
                                           ('datetimeformat', 'date format string (1989 C standard)')],
                                  max_length=32,
                                  validators=[django.core.validators.RegexValidator(
                                    re.compile('minlen|maxlen|minval|maxval|regexp|datetimeformat'))],
                                  verbose_name='Type of rule')),
                ('rule', models.CharField(max_length=100, verbose_name='Rule specification')),
            ],
        ),
        migrations.CreateModel(
            name='CompoundDatatype',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
            ],
        ),
        migrations.CreateModel(
            name='CompoundDatatypeMember',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('column_name',
                 models.CharField(help_text="Gives datatype a 'column name' as an alternative to column index",
                                  max_length=60,
                                  verbose_name='Column name')),
                ('column_idx',
                 models.PositiveIntegerField(help_text='The column number of this DataType',
                                             validators=[django.core.validators.MinValueValidator(1)])),
                ('compounddatatype',
                 models.ForeignKey(help_text='Links this DataType member to a particular CompoundDataType',
                                   on_delete=django.db.models.deletion.CASCADE,
                                   related_name='members',
                                   to='metadata.CompoundDatatype')),
            ],
        ),
        migrations.CreateModel(
            name='CustomConstraint',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('verification_method', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE,
                                                          related_name='custom_constraints',
                                                          to='method.Method')),
            ],
        ),
        migrations.CreateModel(
            name='Datatype',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(help_text='The name for this Datatype',
                                          max_length=60,
                                          verbose_name='Datatype name')),
                ('description',
                 models.TextField(help_text='A description for this Datatype',
                                  max_length=1000,
                                  verbose_name='Datatype description')),
                ('date_created', models.DateTimeField(auto_now_add=True,
                                                      help_text='Date Datatype was defined',
                                                      verbose_name='Date created')),
                ('prototype', models.IntegerField(db_column='prototype_id', null=True)),
                ('restricts',
                 models.ManyToManyField(blank=True,
                                        help_text='Captures hierarchical is-a classifications among Datatypes',
                                        null=True,
                                        related_name='restricted_by',
                                        to=b'metadata.Datatype')),
                ('groups_allowed', models.ManyToManyField(blank=True,
                                                          help_text='What groups have access?',
                                                          null=True,
                                                          related_name='metadata_datatype_has_access_to',
                                                          to=b'auth.Group')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE,
                                           to=settings.AUTH_USER_MODEL)),
                ('users_allowed', models.ManyToManyField(blank=True,
                                                         help_text='Which users have access?',
                                                         null=True,
                                                         related_name='metadata_datatype_has_access_to',
                                                         to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.AddField(
            model_name='compounddatatypemember',
            name='datatype',
            field=models.ForeignKey(help_text='Specifies which DataType this member is',
                                    on_delete=django.db.models.deletion.CASCADE,
                                    to='metadata.Datatype'),
        ),
        migrations.AddField(
            model_name='compounddatatypemember',
            name='blankable',
            field=models.BooleanField(default=False, help_text='Can this entry be left blank?'),
        ),
        migrations.AlterUniqueTogether(
            name='compounddatatypemember',
            unique_together=set([('compounddatatype', 'column_idx'), ('compounddatatype', 'column_name')]),
        ),
        migrations.AddField(
            model_name='basicconstraint',
            name='datatype',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE,
                                    related_name='basic_constraints',
                                    to='metadata.Datatype'),
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='groups_allowed',
            field=models.ManyToManyField(blank=True,
                                         help_text='What groups have access?',
                                         null=True,
                                         related_name='metadata_compounddatatype_has_access_to',
                                         to=b'auth.Group'),
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='user',
            field=models.ForeignKey(default=1,
                                    on_delete=django.db.models.deletion.CASCADE,
                                    to=settings.AUTH_USER_MODEL),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='users_allowed',
            field=models.ManyToManyField(blank=True,
                                         help_text='Which users have access?',
                                         null=True,
                                         related_name='metadata_compounddatatype_has_access_to',
                                         to=settings.AUTH_USER_MODEL),
        ),
        migrations.CreateModel(
            name='KiveUser',
            fields=[
            ],
            options={
                'proxy': True,
            },
            bases=('auth.user',),
        ),
        migrations.AlterUniqueTogether(
            name='datatype',
            unique_together=set([('user', 'name')]),
        ),
        migrations.AddField(
            model_name='customconstraint',
            name='datatype',
            field=models.OneToOneField(default=1,
                                       on_delete=django.db.models.deletion.CASCADE,
                                       related_name='custom_constraint',
                                       to='metadata.Datatype'),
            preserve_default=False,
        ),
        migrations.RunPython(code=load_initial_groups, reverse_code=noop),
        migrations.RunPython(code=load_initial_user, reverse_code=noop),
        migrations.RunPython(code=load_initial_data, reverse_code=purge_initial_data),
        migrations.AlterField(
            model_name='compounddatatypemember',
            name='datatype',
            field=models.ForeignKey(help_text='Specifies which DataType this member is',
                                    on_delete=django.db.models.deletion.CASCADE,
                                    related_name='CDTMs',
                                    to='metadata.Datatype'),
        ),
        migrations.AlterField(
            model_name='compounddatatype',
            name='groups_allowed',
            field=models.ManyToManyField(blank=True,
                                         help_text='What groups have access?',
                                         related_name='metadata_compounddatatype_has_access_to',
                                         to=b'auth.Group'),
        ),
        migrations.AlterField(
            model_name='compounddatatype',
            name='users_allowed',
            field=models.ManyToManyField(blank=True,
                                         help_text='Which users have access?',
                                         related_name='metadata_compounddatatype_has_access_to',
                                         to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterField(
            model_name='datatype',
            name='groups_allowed',
            field=models.ManyToManyField(blank=True,
                                         help_text='What groups have access?',
                                         related_name='metadata_datatype_has_access_to',
                                         to=b'auth.Group'),
        ),
        migrations.AlterField(
            model_name='datatype',
            name='users_allowed',
            field=models.ManyToManyField(blank=True,
                                         help_text='Which users have access?',
                                         related_name='metadata_datatype_has_access_to',
                                         to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterField(
            model_name='datatype',
            name='restricts',
            field=models.ManyToManyField(blank=True,
                                         help_text='Captures hierarchical is-a classifications among Datatypes',
                                         related_name='restricted_by',
                                         to=b'metadata.Datatype'),
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='name',
            field=models.TextField(blank=True, help_text='The name of this CompoundDatatype', verbose_name='Name'),
        ),
        migrations.RunPython(code=set_cdt_names, reverse_code=noop),
        migrations.AlterModelOptions(
            name='compounddatatype',
            options={'ordering': ['name']},
        ),
    ]
