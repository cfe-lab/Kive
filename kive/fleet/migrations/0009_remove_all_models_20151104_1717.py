# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('fleet', '0008_rtp_time_queued_default_20151103_1606'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='exceedssystemcapabilities',
            name='runtoprocess',
        ),
        migrations.DeleteModel(
            name='ExceedsSystemCapabilities',
        ),
        migrations.RemoveField(
            model_name='runtoprocess',
            name='groups_allowed',
        ),
        migrations.RemoveField(
            model_name='runtoprocess',
            name='pipeline',
        ),
        migrations.RemoveField(
            model_name='runtoprocess',
            name='run',
        ),
        migrations.RemoveField(
            model_name='runtoprocess',
            name='user',
        ),
        migrations.RemoveField(
            model_name='runtoprocess',
            name='users_allowed',
        ),
        migrations.RemoveField(
            model_name='runtoprocessinput',
            name='runtoprocess',
        ),
        migrations.DeleteModel(
            name='RunToProcess',
        ),
        migrations.RemoveField(
            model_name='runtoprocessinput',
            name='symbolicdataset',
        ),
        migrations.DeleteModel(
            name='RunToProcessInput',
        ),
    ]
