# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0006_pipelinefamily_published_version_set_null_20150505_1157'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='pipeline',
            options={'ordering': ['family__name', '-revision_number']},
        ),
    ]
