# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('transformation', '0004_auto_20150210_0947'),
    ]

    operations = [
        migrations.AlterField(
            model_name='transformation',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='transformation_transformation_has_access_to', null=True, to='auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='transformation',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='transformation_transformation_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
    ]
