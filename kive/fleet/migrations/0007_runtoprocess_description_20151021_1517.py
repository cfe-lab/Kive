# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('fleet', '0006_runtoprocess_name'),
    ]

    operations = [
        migrations.AddField(
            model_name='runtoprocess',
            name='description',
            field=models.CharField(default=b'', max_length=1000, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='runtoprocess',
            name='run',
            field=models.OneToOneField(related_name='runtoprocess', null=True, blank=True, to='archive.Run'),
            preserve_default=True,
        ),
    ]
