# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('method', '0002_auto_20150204_1703'),
    ]

    operations = [
        migrations.AlterField(
            model_name='coderesource',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='method_coderesource_has_access_to', null=True, to='auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='coderesource',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='method_coderesource_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='coderesourcerevision',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='method_coderesourcerevision_has_access_to', null=True, to='auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='coderesourcerevision',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='method_coderesourcerevision_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='methodfamily',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='method_methodfamily_has_access_to', null=True, to='auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='methodfamily',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='method_methodfamily_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
    ]
