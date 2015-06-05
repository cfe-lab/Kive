# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('method', '0009_coderesourcerevision_revision_number_positive_20150604_1553'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='coderesourcerevision',
            unique_together=set([('coderesource', 'revision_number')]),
        ),
    ]
