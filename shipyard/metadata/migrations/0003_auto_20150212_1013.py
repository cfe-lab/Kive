# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('auth', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('metadata', '0002_compounddatatypemember_blankable'),
    ]

    operations = [
        migrations.AddField(
            model_name='compounddatatype',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='metadata_compounddatatype_has_access_to', to='auth.Group'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='user',
            field=models.ForeignKey(default=1, to=settings.AUTH_USER_MODEL),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='compounddatatype',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='metadata_compounddatatype_has_access_to', to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='datatype',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='metadata_datatype_has_access_to', to='auth.Group'),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='datatype',
            name='user',
            field=models.ForeignKey(default=1, to=settings.AUTH_USER_MODEL),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='datatype',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='metadata_datatype_has_access_to', to=settings.AUTH_USER_MODEL),
            preserve_default=True,
        ),
    ]
