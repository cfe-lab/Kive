# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '0001_initial'),
        ('metadata', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='XputStructure',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('min_row', models.PositiveIntegerField(help_text='Minimum number of rows this input/output returns', null=True, verbose_name='Minimum row', blank=True)),
                ('max_row', models.PositiveIntegerField(help_text='Maximum number of rows this input/output returns', null=True, verbose_name='Maximum row', blank=True)),
                ('compounddatatype', models.ForeignKey(to='metadata.CompoundDatatype')),
                ('transf_xput', models.OneToOneField(related_name='structure', to='transformation.TransformationXput')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
    ]
