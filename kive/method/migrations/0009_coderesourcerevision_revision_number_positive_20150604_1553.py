# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('method', '0008_coderesourcerevision_ordering_20150604_1501'),
    ]

    operations = [
        migrations.AlterField(
            model_name='coderesourcerevision',
            name='revision_number',
            field=models.PositiveIntegerField(help_text='Revision number of code resource', verbose_name='Revision number', blank=True),
            preserve_default=True,
        ),
    ]
