# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('method', '0006_add_method_crr_related_name_20150417_1656'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='method',
            options={'ordering': ['family__name', '-revision_number']},
        ),
    ]
