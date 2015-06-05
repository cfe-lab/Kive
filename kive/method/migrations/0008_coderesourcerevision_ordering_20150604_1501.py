# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('method', '0007_method_ordering_20150601_1348'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='coderesourcerevision',
            options={'ordering': ['coderesource__name', '-revision_number']},
        ),
    ]
