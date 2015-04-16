# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('librarian', '0004_redacted_20150414_1635'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='execrecord',
            name='adopter',
        ),
        migrations.AlterField(
            model_name='execrecord',
            name='generator',
            field=models.OneToOneField(related_name='execrecord', to='archive.ExecLog'),
            preserve_default=True,
        ),
    ]
