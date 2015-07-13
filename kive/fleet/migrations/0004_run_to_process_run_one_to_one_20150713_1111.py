# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('fleet', '0003_runtoprocess_purged'),
    ]

    operations = [
        migrations.AlterField(
            model_name='runtoprocess',
            name='run',
            field=models.OneToOneField(related_name='runtoprocess', null=True, to='archive.Run'),
            preserve_default=True,
        ),
    ]
