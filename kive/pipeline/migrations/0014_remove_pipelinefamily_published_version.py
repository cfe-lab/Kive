# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('pipeline', '0013_pipeline_convert_published_version_20150917_1511'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='pipelinefamily',
            name='published_version',
        ),
    ]
