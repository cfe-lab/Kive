# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='StagedFile',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('uploaded_file', models.FileField(help_text='Uploaded file held for further server-side processing', upload_to=b'StagedFiles', verbose_name=b'Uploaded file')),
                ('date_uploaded', models.DateTimeField(help_text='Date and time of upload', verbose_name=b'Upload date', auto_now_add=True)),
                ('user', models.ForeignKey(help_text='User that uploaded this file', to=settings.AUTH_USER_MODEL)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
    ]
