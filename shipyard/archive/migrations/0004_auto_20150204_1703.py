# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('auth', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('archive', '0003_runcomponent_is_cancelled'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='dataset',
            name='user',
        ),
        migrations.AddField(
            model_name='run',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='archive_run_has_access_to', to='auth.Group'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='run',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='archive_run_has_access_to', to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='run',
            name='user',
            field=models.ForeignKey(to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
    ]
