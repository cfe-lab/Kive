# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('metadata', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='compounddatatypemember',
            name='blankable',
            field=models.BooleanField(default=False, help_text='Can this entry be left blank?'),
            preserve_default=True,
        ),
    ]
