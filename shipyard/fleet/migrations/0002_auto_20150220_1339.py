# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('auth', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('fleet', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='runtoprocess',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='fleet_runtoprocess_has_access_to', null=True, to='auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='runtoprocess',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='fleet_runtoprocess_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
    ]
